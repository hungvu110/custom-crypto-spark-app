package vai.lakehouse.keyprefix

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.StubHttpServer.{Response, tokenResponse}

/**
 * Body trả về giống hệt dak-mock (`dak-mock/src/test/java/vai/lakehouse/dakmock/ColumnKeyApiTest.java`): field không áp dụng
 * cho trạng thái của cột thì vắng mặt, không phải `null`.
 */
class DakColumnKeySourceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val TokenPath  = "/realms/tenant-a/protocol/openid-connect/token"
  private val ColumnPath = "/api/v1/keys/by-column"
  private val Secret     = "client-secret-xyz"

  private var server: StubHttpServer = _

  override def beforeEach(): Unit = server = new StubHttpServer

  override def afterEach(): Unit = server.close()

  private def cfg(tag: Option[String] = Some("internal"), caller: Option[String] = None) =
    DakConfig(addr = server.url, tokenUrl = server.url + TokenPath, clientId = "dak.internal", clientSecret = Secret,
      tag = tag, caller = caller)

  private def source(c: DakConfig = cfg()) =
    new DakColumnKeySource(c, new KeycloakTokenProvider(c.tokenUrl, c.clientId, c.clientSecret, "TEST_hint"))

  private def ready(column: String, key: String) =
    s"""{"column":"$column","state":"READY","keyPrefix":"$key","keyVersion":1}"""

  private def body(columns: String*)(complete: Boolean = false, table: String = "users_cdr") =
    Response(200, s"""{"database":"demo_db","table":"$table","complete":$complete,"columns":[${columns.mkString(",")}]}""",
      Map("X-Request-Id" -> "req-col"))

  private def error(status: Int, code: String) =
    Response(status, s"""{"error":"$code","message":"denied","requestId":"req-$status"}""")

  private def stubOk(response: Response*): Unit = {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", ColumnPath, response: _*)
  }

  "readColumns" should "gọi by-column đúng tham số và trả key của mọi cột khi tất cả READY" in {
    stubOk(body(ready("msisdn", "k-msisdn"), ready("imei", "k-imei"))(complete = true))

    val keys = source().readColumns("demo_db.users_cdr", Seq("msisdn", "imei"), KeyIntent.Encrypt)

    keys shouldBe Map("msisdn" -> "k-msisdn", "imei" -> "k-imei")
    val req = server.requestsTo(ColumnPath).head
    req.params shouldBe Map("database" -> "demo_db", "table" -> "users_cdr", "columns" -> "msisdn,imei",
      "onMissing" -> "request", "tag" -> "internal")
    req.headers("authorization") shouldBe "Bearer tok-1"
    req.headers.get("x-request-id").exists(_.nonEmpty) shouldBe true
  }

  it should "gửi onMissing=none khi giải mã" in {
    stubOk(body(ready("msisdn", "k"))(complete = true))

    source().readColumns("demo_db.users_cdr", Seq("msisdn"), KeyIntent.Decrypt)

    server.requestsTo(ColumnPath).head.params("onMissing") shouldBe "none"
  }

  it should "không gửi tag khi không cấu hình (luồng client team)" in {
    stubOk(body(ready("msisdn", "k"))(complete = true))

    source(cfg(tag = None)).readColumns("demo_db.users_cdr", Seq("msisdn"), KeyIntent.Decrypt)

    server.requestsTo(ColumnPath).head.params.keySet should not contain "tag"
  }

  it should "gửi X-Dak-Caller khi cấu hình" in {
    stubOk(body(ready("msisdn", "k"))(complete = true))

    source(cfg(caller = Some("job-cdr.daily"))).readColumns("demo_db.users_cdr", Seq("msisdn"), KeyIntent.Decrypt)

    server.requestsTo(ColumnPath).head.headers("x-dak-caller") shouldBe "job-cdr.daily"
  }

  it should "chuẩn hoá chữ thường và gộp tên cột trùng trước khi gửi" in {
    stubOk(body(ready("msisdn", "k-msisdn"), ready("imei", "k-imei"))(complete = true))

    val keys = source().readColumns("Demo_DB.Users_CDR", Seq("MSISDN", "imei", "msisdn"), KeyIntent.Decrypt)

    keys shouldBe Map("msisdn" -> "k-msisdn", "imei" -> "k-imei")
    server.requestsTo(ColumnPath).head.params("columns") shouldBe "msisdn,imei"
  }

  it should "chia lô khi vượt 100 cột và gộp kết quả" in {
    val first = (0 until 100).map(i => s"c$i")
    val second = (100 until 120).map(i => s"c$i")
    stubOk(body(first.map(c => ready(c, s"k$c")): _*)(complete = true),
      body(second.map(c => ready(c, s"k$c")): _*)(complete = true))

    val keys = source().readColumns("demo_db.users_cdr", first ++ second, KeyIntent.Decrypt)

    keys should have size 120
    keys("c119") shouldBe "kc119"
    server.requestsTo(ColumnPath).map(_.params("columns").split(",").length) shouldBe List(100, 20)
  }

  it should "báo lỗi cho cả lời gọi khi có cột không READY, liệt kê từng cột, không lộ key của cột READY" in {
    stubOk(body(
      ready("msisdn", "k-msisdn-secret"),
      """{"column":"imei","state":"PENDING","requestCode":"YC-2026-000002"}""",
      """{"column":"cell_id","state":"MISSING","reason":"not_requested"}""",
      """{"column":"lac","state":"MISSING","reason":"yc_limit_reached"}""",
      """{"column":"ci","state":"REJECTED","requestCode":"YC-2026-000005","reasonText":"not sensitive"}""",
      """{"column":"tac","state":"DISABLED"}""")())

    val ex = intercept[ColumnKeysUnavailableException](
      source().readColumns("demo_db.users_cdr", Seq("msisdn", "imei", "cell_id", "lac", "ci", "tac"), KeyIntent.Encrypt))

    ex.dataset shouldBe "demo_db.users_cdr"
    ex.issues.map(i => i.column -> i.state) shouldBe Seq("imei" -> "PENDING", "cell_id" -> "MISSING",
      "lac" -> "MISSING", "ci" -> "REJECTED", "tac" -> "DISABLED")
    ex.issues.find(_.column == "imei").flatMap(_.requestCode) shouldBe Some("YC-2026-000002")
    ex.issues.find(_.column == "lac").flatMap(_.reason) shouldBe Some("yc_limit_reached")
    ex.getMessage should include("YC-2026-000002")
    ex.getMessage should include("yc_limit_reached")
    ex.getMessage should include("requestId=req-col")
    ex.getMessage should not include "k-msisdn-secret"
    ex.getMessage should not include "not sensitive"
  }

  it should "coi response là không hợp lệ khi READY mà keyPrefix thiếu hoặc rỗng" in {
    Seq("""{"column":"msisdn","state":"READY"}""", """{"column":"msisdn","state":"READY","keyPrefix":""}""",
      """{"column":"msisdn","state":"READY","keyPrefix":123}""").foreach { col =>
      server.close()
      server = new StubHttpServer
      stubOk(body(col)(complete = true))

      val ex = intercept[IllegalStateException](source().readColumns("demo_db.users_cdr", Seq("msisdn"), KeyIntent.Decrypt))

      ex should not be a[ColumnKeysUnavailableException]
      ex.getMessage should include("keyPrefix")
    }
  }

  it should "coi response là không hợp lệ khi thiếu cột, lặp cột, thừa cột, lệch bảng, trạng thái lạ hoặc complete mâu thuẫn" in {
    val cases = Seq(
      body(ready("msisdn", "k"))(complete = true) -> Seq("msisdn", "imei"),
      body(ready("msisdn", "k"), ready("msisdn", "k"))(complete = true) -> Seq("msisdn"),
      body(ready("msisdn", "k"), ready("other", "k"))(complete = true) -> Seq("msisdn"),
      body(ready("msisdn", "k"))(complete = true, table = "orders") -> Seq("msisdn"),
      body("""{"column":"msisdn","state":"ARCHIVED"}""")() -> Seq("msisdn"),
      body("""{"column":"msisdn","state":"PENDING","requestCode":"YC-1"}""")(complete = true) -> Seq("msisdn"),
      Response(200, """{"database":"demo_db","table":"users_cdr"}""") -> Seq("msisdn"))
    cases.foreach { case (response, columns) =>
      server.close()
      server = new StubHttpServer
      stubOk(response)

      val ex = intercept[IllegalStateException](source().readColumns("demo_db.users_cdr", columns, KeyIntent.Decrypt))

      ex should not be a[ColumnKeysUnavailableException]
      ex.getMessage should include("invalid")
    }
  }

  it should "xin token mới và thử lại đúng 1 lần khi DAK trả 401" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"), tokenResponse("tok-2"))
    server.stub("GET", ColumnPath, error(401, "invalid_token"), body(ready("msisdn", "k"))(complete = true))

    source().readColumns("demo_db.users_cdr", Seq("msisdn"), KeyIntent.Decrypt) shouldBe Map("msisdn" -> "k")

    server.requestsTo(ColumnPath).map(_.headers("authorization")) shouldBe List("Bearer tok-1", "Bearer tok-2")
  }

  it should "báo lỗi kèm gợi ý, không retry, với 400, 403, 429, 500, 503" in {
    Seq(400 -> "invalid_tag" -> "dak.tag", 400 -> "invalid_column" -> "invalid_column",
      403 -> "access_denied" -> "grant", 429 -> "rate_limited" -> "rate_limited",
      500 -> "internal_error" -> "internal_error", 503 -> "upstream_unavailable" -> "upstream_unavailable")
      .foreach { case ((status, code), hint) =>
        server.close()
        server = new StubHttpServer
        stubOk(error(status, code))

        val ex = intercept[IllegalStateException](source().readColumns("demo_db.users_cdr", Seq("msisdn"), KeyIntent.Decrypt))

        ex.getMessage should include(status.toString)
        ex.getMessage should include(code)
        ex.getMessage should include(hint)
        ex.getMessage should include("demo_db.users_cdr")
        ex.getMessage should include(s"requestId=req-$status")
        server.requestsTo(ColumnPath) should have size 1
      }
  }

  it should "không đưa token, secret hay body thô vào thông báo lỗi" in {
    server.stub("POST", TokenPath, tokenResponse("tok-secret-123"))
    server.stub("GET", ColumnPath,
      Response(403, s"""{"error":"access_denied","message":"leak $Secret","requestId":"r1"}"""))

    val ex = intercept[IllegalStateException](source().readColumns("demo_db.users_cdr", Seq("msisdn"), KeyIntent.Decrypt))

    ex.getMessage should not include "tok-secret-123"
    ex.getMessage should not include Secret
    ex.getMessage should not include "leak"
  }

  it should "từ chối tên bảng, tên cột sai hoặc danh sách cột rỗng trước khi gọi Keycloak/DAK" in {
    intercept[IllegalArgumentException](source().readColumns("users_cdr", Seq("msisdn"), KeyIntent.Decrypt))
    intercept[IllegalArgumentException](source().readColumns("demo_db.users_cdr", Seq("start-time"), KeyIntent.Decrypt))
    intercept[IllegalArgumentException](source().readColumns("demo_db.users_cdr", Nil, KeyIntent.Decrypt))
    server.requests shouldBe empty
  }
}
