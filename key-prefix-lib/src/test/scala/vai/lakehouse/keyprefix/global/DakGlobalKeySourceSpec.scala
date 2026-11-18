package vai.lakehouse.keyprefix

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.StubHttpServer.{Response, tokenResponse}

/** Hình dạng response theo docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md mục 2. */
class DakGlobalKeySourceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val TokenPath = "/realms/tenant-a/protocol/openid-connect/token"
  private val ApiPath   = "/api/v1/keys/global"
  private val Zone      = "khach_hang"

  private var server: StubHttpServer = _

  override def beforeEach(): Unit = server = new StubHttpServer

  override def afterEach(): Unit = server.close()

  // Client nội bộ, nhưng endpoint global không có tham số tag: dù cấu hình có tag cũng không được gửi.
  private def cfg = DakConfig(addr = server.url, tokenUrl = server.url + TokenPath, clientId = "dak.internal", clientSecret = "sec",
    tag = Some("internal"), caller = Some("silver-vtnet"))

  private def source =
    new DakGlobalKeySource(cfg, new KeycloakTokenProvider(cfg.tokenUrl, cfg.clientId, cfg.clientSecret, "TEST_hint"))

  private def stub(responses: Response*): Unit = {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", ApiPath, responses: _*)
  }

  private def found(zone: String = Zone, key: String = "k-global-secret", version: String = "1") =
    Response(200, s"""{"zone":"$zone","keyPrefix":"$key","keyVersion":$version}""", Map("X-Request-Id" -> "req-global"))

  "read" should "gọi key global với zone, không gửi tag, trả key khi DAK trả 200" in {
    stub(found())

    source.read(Zone) shouldBe GlobalKey(Zone, "k-global-secret", Some(1L))

    val req = server.requestsTo(ApiPath).head
    req.params shouldBe Map("zone" -> Zone)
    req.headers("authorization") shouldBe "Bearer tok-1"
    req.headers("x-dak-caller") shouldBe "silver-vtnet"
  }

  it should "chấp nhận keyVersion là chuỗi số hoặc null (chỉ để ghi log)" in {
    Seq("\"3\"" -> Some(3L), "null" -> None).foreach { case (raw, expected) =>
      server.close()
      server = new StubHttpServer
      stub(found(version = raw))

      source.read(Zone).keyVersion shouldBe expected
    }
  }

  it should "từ chối zone sai định dạng trước khi gọi DAK" in {
    Seq("", "Khach-Hang", "a" * 65, "kh ach", "a/b").foreach { zone =>
      intercept[IllegalArgumentException](source.read(zone)).getMessage should include("zone")
    }
    server.requestsTo(ApiPath) shouldBe empty
  }

  it should "xin token mới và thử lại một lần khi DAK trả 401" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"), tokenResponse("tok-2"))
    server.stub("GET", ApiPath, Response(401, """{"error":"invalid_token"}"""), found())

    source.read(Zone).keyPrefix shouldBe "k-global-secret"
    server.requestsTo(ApiPath).map(_.headers("authorization")) shouldBe List("Bearer tok-1", "Bearer tok-2")
  }

  it should "báo lỗi có requestId, không lộ body, với 400/403/429" in {
    Seq(400 -> "invalid_zone", 403 -> "access_denied", 429 -> "rate_limited").foreach { case (status, code) =>
      server.close()
      server = new StubHttpServer
      stub(Response(status, s"""{"error":"$code","message":"secret detail","requestId":"req-$status"}"""))

      val ex = intercept[IllegalStateException](source.read(Zone))

      ex.getMessage should include(s"HTTP $status $code")
      ex.getMessage should include(s"requestId=req-$status")
      ex.getMessage should not include "secret detail"
    }
  }

  it should "phân biệt 403 key_disabled (key bị khoá) với 403 access_denied (chưa có phiếu hiệu lực hoặc vùng không tồn tại)" in {
    stub(Response(403, """{"error":"key_disabled","requestId":"req-dis"}"""))
    intercept[IllegalStateException](source.read(Zone)).getMessage should include("disabled")

    server.close()
    server = new StubHttpServer
    stub(Response(403, """{"error":"access_denied","requestId":"req-den"}"""))
    val ex = intercept[IllegalStateException](source.read(Zone))
    ex.getMessage should include("no effective silver request")
    ex.getMessage should not include "disabled"
  }

  it should "coi response 200 là không hợp lệ khi keyPrefix thiếu/rỗng hoặc zone khác vùng đã hỏi" in {
    Seq(
      Response(200, s"""{"zone":"$Zone","keyVersion":1}"""),
      Response(200, s"""{"zone":"$Zone","keyPrefix":"","keyVersion":1}"""),
      Response(200, s"""{"zone":"$Zone","keyPrefix":7}"""),
      found(zone = "vung_khac")).foreach { response =>
      server.close()
      server = new StubHttpServer
      stub(response)

      val ex = intercept[IllegalStateException](source.read(Zone))

      ex.getMessage should include("global")
      ex.getMessage should not include "k-global-secret"
    }
  }

  "GlobalColumnKeySource" should "trả cùng một key global cho mọi cột (tên cột chữ thường), chỉ gọi DAK một lần" in {
    stub(found())
    val columns = new GlobalColumnKeySource(source, Zone)

    columns.readColumns("demo_db.subscriber", Seq("MSISDN", "imei"), KeyIntent.Encrypt) shouldBe
      Map("msisdn" -> "k-global-secret", "imei" -> "k-global-secret")
    columns.readColumns("demo_db.subscriber", Seq("msisdn"), KeyIntent.Encrypt) shouldBe Map("msisdn" -> "k-global-secret")

    server.requestsTo(ApiPath) should have size 1
  }

  "GlobalKey.toString" should "che keyPrefix" in {
    GlobalKey(Zone, "k-global-secret", Some(1L)).toString should not include "k-global-secret"
  }
}
