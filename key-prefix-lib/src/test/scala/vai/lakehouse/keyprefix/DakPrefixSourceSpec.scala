package vai.lakehouse.keyprefix

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.StubHttpServer.{Response, tokenResponse}

class DakPrefixSourceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val TokenPath = "/realms/tenant-a/protocol/openid-connect/token"
  private val KeyPath   = "/api/v1/keys/demo_db/users_cdr"
  private val Secret    = "client-secret-xyz"
  private val Prefix    = "9f2c4b-prefix-value"

  private var server: StubHttpServer = _

  override def beforeEach(): Unit = server = new StubHttpServer

  override def afterEach(): Unit = server.close()

  private def cfg(addr: String = server.url) =
    DakConfig(addr = addr, tokenUrl = server.url + TokenPath, clientId = "dak.w1.team-a", clientSecret = Secret)

  private def source(c: DakConfig = cfg()) =
    new DakPrefixSource(c, new KeycloakTokenProvider(c.tokenUrl, c.clientId, c.clientSecret, "TEST_hint"))

  private def keyOk(prefix: String = Prefix) =
    Response(200, s"""{"database":"demo_db","table":"users_cdr","keyPrefix":"$prefix","keyVersion":1,"newField":"x"}""",
      Map("X-Request-Id" -> "req-200"))

  private def denied(status: Int, error: String) =
    Response(status, s"""{"error":"$error","message":"denied","requestId":"req-$status"}""")

  "read" should "gọi GET /api/v1/keys/{database}/{table} kèm Bearer token và trả keyPrefix, bỏ qua field lạ" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, keyOk())

    source().read("demo_db.users_cdr") shouldBe Prefix

    val req = server.requestsTo(KeyPath).head
    req.headers("authorization") shouldBe "Bearer tok-1"
    req.headers("accept") shouldBe "application/json"
    req.headers.get("x-request-id").exists(_.nonEmpty) shouldBe true
  }

  it should "chuẩn hoá tên bảng về chữ thường trước khi gọi DAK" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, keyOk())

    source().read("Demo_DB.Users_CDR") shouldBe Prefix
  }

  it should "dùng lại access token cho các bảng khác nhau" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, keyOk())
    server.stub("GET", "/api/v1/keys/demo_db/orders", keyOk("other"))
    val s = source()

    s.read("demo_db.users_cdr") shouldBe Prefix
    s.read("demo_db.orders") shouldBe "other"
    server.requestsTo(TokenPath) should have size 1
  }

  it should "từ chối tên không đúng dạng database.table trước khi gọi Keycloak/DAK" in {
    Seq("users_cdr", "a.b.c", "../x", "").foreach { bad =>
      intercept[IllegalArgumentException](source().read(bad))
    }
    server.requests shouldBe empty
  }

  it should "xin token mới và thử lại đúng 1 lần khi DAK trả 401" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"), tokenResponse("tok-2"))
    server.stub("GET", KeyPath, denied(401, "invalid_token"), keyOk())

    source().read("demo_db.users_cdr") shouldBe Prefix

    server.requestsTo(KeyPath).map(_.headers("authorization")) shouldBe List("Bearer tok-1", "Bearer tok-2")
    server.requestsTo(TokenPath) should have size 2
  }

  it should "báo lỗi sau lần 401 thứ hai, tổng cộng 2 request tới DAK" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"), tokenResponse("tok-2"))
    server.stub("GET", KeyPath, denied(401, "invalid_token"))

    val ex = intercept[IllegalStateException](source().read("demo_db.users_cdr"))

    ex.getMessage should include("401")
    ex.getMessage should include("invalid_token")
    server.requestsTo(KeyPath) should have size 2
  }

  it should "báo lỗi 403 access_denied ngay, kèm requestId và gợi ý nguyên nhân, không retry" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, denied(403, "access_denied"))

    val ex = intercept[IllegalStateException](source().read("demo_db.users_cdr"))

    ex.getMessage should include("demo_db.users_cdr")
    ex.getMessage should include("403")
    ex.getMessage should include("access_denied")
    ex.getMessage should include("requestId=req-403")
    ex.getMessage should include("grant expired")
    server.requestsTo(KeyPath) should have size 1
  }

  it should "không retry với 403 key_disabled, 400, 429, 500, 503" in {
    Seq(403 -> "key_disabled", 400 -> "invalid_table", 429 -> "rate_limited", 500 -> "internal_error",
      503 -> "upstream_unavailable").foreach { case (status, error) =>
      server.close()
      server = new StubHttpServer
      server.stub("POST", TokenPath, tokenResponse("tok-1"))
      server.stub("GET", KeyPath, denied(status, error))

      val ex = intercept[IllegalStateException](source().read("demo_db.users_cdr"))

      ex.getMessage should include(status.toString)
      ex.getMessage should include(error)
      ex.getMessage should include("demo_db.users_cdr")
      server.requestsTo(KeyPath) should have size 1
    }
  }

  it should "lấy requestId từ header khi body lỗi không phải JSON" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, Response(502, "<html>Bad Gateway</html>", Map("X-Request-Id" -> "req-hdr")))

    val ex = intercept[IllegalStateException](source().read("demo_db.users_cdr"))

    ex.getMessage should include("502")
    ex.getMessage should include("requestId=req-hdr")
    ex.getMessage should not include "<html>"
  }

  it should "không đưa token, secret hay nội dung body vào thông báo lỗi" in {
    server.stub("POST", TokenPath, tokenResponse("tok-secret-123"))
    server.stub("GET", KeyPath,
      Response(403, s"""{"error":"access_denied","message":"leak $Secret","requestId":"r1"}"""))

    val ex = intercept[IllegalStateException](source().read("demo_db.users_cdr"))

    ex.getMessage should not include "tok-secret-123"
    ex.getMessage should not include Secret
    ex.getMessage should not include "leak"
  }

  it should "báo lỗi khi keyPrefix thiếu, rỗng hoặc không phải chuỗi, không lộ giá trị field khác" in {
    Seq("""{"other":"leaky-value"}""", """{"keyPrefix":""}""", """{"keyPrefix":123,"other":"leaky-value"}""")
      .foreach { body =>
        server.stub("POST", TokenPath, tokenResponse("tok-1"))
        server.stub("GET", KeyPath, Response(200, body))

        val ex = intercept[IllegalStateException](source().read("demo_db.users_cdr"))

        ex.getMessage should include("keyPrefix")
        ex.getMessage should not include "leaky-value"
      }
  }

  it should "báo lỗi kèm địa chỉ DAK khi không kết nối được, không lộ secret" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    val dead = new StubHttpServer
    val deadAddr = dead.url
    dead.close()

    val ex = intercept[IllegalStateException](source(cfg(addr = deadAddr)).read("demo_db.users_cdr"))

    ex.getMessage should include(deadAddr)
    ex.getMessage should not include Secret
  }

  it should "chấp nhận dak.addr có dấu '/' ở cuối" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, keyOk())

    source(cfg(addr = server.url + "/")).read("demo_db.users_cdr") shouldBe Prefix
  }

  "DakConfig.toString" should "che client secret" in {
    val text = cfg().toString

    text should not include Secret
    text should include("clientSecret=***")
  }
}
