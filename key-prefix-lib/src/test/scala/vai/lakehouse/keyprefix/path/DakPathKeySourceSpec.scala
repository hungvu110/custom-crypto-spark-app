package vai.lakehouse.keyprefix

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.StubHttpServer.{Response, tokenResponse}

/**
 * Hình dạng response theo docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md mục 2, giống dak-mock
 * (`dak-mock/src/test/java/vai/lakehouse/dakmock/ColumnKeyApiTest.java`, phần by-path).
 */
class DakPathKeySourceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val TokenPath = "/realms/tenant-a/protocol/openid-connect/token"
  private val ApiPath   = "/api/v1/keys/by-path"
  private val Path      = "vtnet_cdr/20261027/cdr.enc"

  private var server: StubHttpServer = _

  override def beforeEach(): Unit = server = new StubHttpServer

  override def afterEach(): Unit = server.close()

  private def cfg(tag: Option[String] = Some("internal")) =
    DakConfig(addr = server.url, tokenUrl = server.url + TokenPath, clientId = "dak.internal", clientSecret = "sec",
      tag = tag, caller = Some("silver-vtnet"))

  private def source(c: DakConfig = cfg()) =
    new DakPathKeySource(c, new KeycloakTokenProvider(c.tokenUrl, c.clientId, c.clientSecret, "TEST_hint"))

  private def stub(responses: Response*): Unit = {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", ApiPath, responses: _*)
  }

  private def found(path: String = Path, key: String = "k-path-secret") =
    Response(200, s"""{"path":"$path","keyPrefix":"$key","keyVersion":1}""", Map("X-Request-Id" -> "req-path"))

  "readPath" should "gọi by-path với path và tag, trả key khi DAK trả 200" in {
    stub(found())

    val result = source().readPath(Path)

    result shouldBe PathKeyResult.Available(PathKey(Path, "k-path-secret", Some(1L)))
    val req = server.requestsTo(ApiPath).head
    req.params shouldBe Map("path" -> Path, "tag" -> "internal")
    req.headers("authorization") shouldBe "Bearer tok-1"
    req.headers("x-dak-caller") shouldBe "silver-vtnet"
  }

  it should "gửi đường dẫn ở dạng chuẩn: gộp //, bỏ / đầu và cuối" in {
    stub(found(path = "vtnet_cdr/20261027"))

    source().readPath("/vtnet_cdr//20261027/") shouldBe a[PathKeyResult.Available]

    server.requestsTo(ApiPath).head.params("path") shouldBe "vtnet_cdr/20261027"
  }

  it should "trả NotAvailable khi DAK trả 204 rỗng (không có key: file không mã hoá)" in {
    stub(Response(204, ""))

    source().readPath(Path) shouldBe PathKeyResult.NotAvailable
  }

  it should "báo lỗi khi DAK trả 202 (mở yêu cầu key — cách cũ đã bỏ): không đoán là file không mã hoá" in {
    stub(Response(202, """{"requestCode":"YC-2026-000123","state":"PENDING","message":"Chưa có key"}"""))

    val ex = intercept[IllegalStateException](source().readPath(Path))

    ex.getMessage should include("202")
    ex.getMessage should include("YC-2026-000123")
  }

  it should "không đưa giá trị lạ của mã YC vào thông báo lỗi" in {
    stub(Response(202, """{"requestCode":"YC\n2026 bad","state":"OPEN<script>"}"""))

    intercept[IllegalStateException](source().readPath(Path)).getMessage should not include "<script>"
  }

  it should "xin token mới và thử lại một lần khi DAK trả 401" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"), tokenResponse("tok-2"))
    server.stub("GET", ApiPath, Response(401, """{"error":"invalid_token"}"""), found())

    source().readPath(Path) shouldBe a[PathKeyResult.Available]
    server.requestsTo(ApiPath).map(_.headers("authorization")) shouldBe List("Bearer tok-1", "Bearer tok-2")
  }

  it should "báo lỗi có requestId, không lộ body, với 400/403/429" in {
    Seq(400 -> "invalid_path", 403 -> "access_denied", 429 -> "rate_limited").foreach { case (status, code) =>
      server.close()
      server = new StubHttpServer
      stub(Response(status, s"""{"error":"$code","message":"secret detail","requestId":"req-$status"}"""))

      val ex = intercept[IllegalStateException](source().readPath(Path))

      ex.getMessage should include(s"HTTP $status $code")
      ex.getMessage should include(s"requestId=req-$status")
      ex.getMessage should not include "secret detail"
    }
  }

  it should "báo key bị khoá khi DAK trả 403 key_disabled, khác với 403 sai loại client, và không bao giờ coi là không có key" in {
    stub(Response(403, """{"error":"key_disabled","message":"secret detail","requestId":"req-dis"}"""))

    val ex = intercept[IllegalStateException](source().readPath(Path))

    ex.getMessage should include("disabled")
    ex.getMessage should include("requestId=req-dis")
    ex.getMessage should not include "internal client only"
    ex.getMessage should not include "secret detail"

    server.close()
    server = new StubHttpServer
    stub(Response(403, """{"error":"access_denied","requestId":"req-403"}"""))
    intercept[IllegalStateException](source().readPath(Path)).getMessage should include("internal client only")
  }

  it should "gợi ý sửa dak.tag khi DAK trả 400 invalid_tag" in {
    stub(Response(400, """{"error":"invalid_tag","requestId":"req-tag"}"""))

    intercept[IllegalStateException](source(cfg(tag = None)).readPath(Path)).getMessage should include("tag=internal")
  }

  it should "coi response 200 là không hợp lệ khi keyPrefix thiếu/rỗng hoặc path khác đường dẫn đã hỏi" in {
    Seq(
      Response(200, s"""{"path":"$Path","keyVersion":1}"""),
      Response(200, s"""{"path":"$Path","keyPrefix":""}"""),
      Response(200, s"""{"path":"$Path","keyPrefix":7}"""),
      Response(200, s"""{"filePath":"$Path","keyPrefix":"k-path-secret"}"""),
      found(path = "/other/path")).foreach { response =>
      server.close()
      server = new StubHttpServer
      stub(response)

      val ex = intercept[IllegalStateException](source().readPath(Path))

      ex.getMessage should include("by-path")
      ex.getMessage should not include "k-path-secret"
    }
  }

  it should "từ chối đường dẫn sai trước khi gọi DAK" in {
    stub(found())
    Seq("", "  ", "/", "//", "a/../b", "a/b\nc", "x" * 1025, "hdfs://nn:8020/landing/x").foreach { bad =>
      intercept[IllegalArgumentException](source().readPath(bad))
    }
    server.requestsTo(ApiPath) shouldBe empty
  }

  it should "giữ nguyên hoa thường của đường dẫn" in {
    stub(found(path = "VTNET_cdr/File.ENC"))

    source().readPath("VTNET_cdr/File.ENC") shouldBe a[PathKeyResult.Available]
    server.requestsTo(ApiPath).head.params("path") shouldBe "VTNET_cdr/File.ENC"
  }

  "PathKey" should "che keyPrefix khi in" in {
    PathKey(Path, "k-path-secret", Some(1L)).toString should not include "k-path-secret"
  }

  "PathKeySource.canonical" should "ra cùng một chuỗi tương đối cho mọi cách viết của cùng đường dẫn, giữ hoa thường" in {
    Seq("vtnet_cdr/20261027", "vtnet_cdr/20261027/", "/vtnet_cdr/20261027", "//vtnet_cdr//20261027//",
      " vtnet_cdr/20261027 ").map(PathKeySource.canonical).distinct shouldBe Seq("vtnet_cdr/20261027")
    PathKeySource.canonical("VTNET/File.ENC") shouldBe "VTNET/File.ENC"
  }

  "PathKeySource.relativeTo" should "lấy phần đường dẫn sau gốc, bỏ qua scheme/authority và cách viết của gốc" in {
    PathKeySource.relativeTo("hdfs://nn:8020/landing/vtnet/vtnet_cdr/20261027", "hdfs://nn:8020/landing/vtnet/") shouldBe
      Some("vtnet_cdr/20261027")
    PathKeySource.relativeTo("hdfs://nn:8020/landing//vtnet/vtnet_cdr/20261027/", "/landing/vtnet") shouldBe
      Some("vtnet_cdr/20261027")
    PathKeySource.relativeTo("/landing/vtnet/vtnet_cdr", "/landing/vtnet") shouldBe Some("vtnet_cdr")
    PathKeySource.relativeTo("/landing/vtnet", "/landing/vtnet") shouldBe None // chính gốc: không có phần tương đối
    PathKeySource.relativeTo("/landing/vtnetx/a", "/landing/vtnet") shouldBe None
    PathKeySource.relativeTo("/landing/vtt/a", "/landing/vtnet") shouldBe None
  }
}
