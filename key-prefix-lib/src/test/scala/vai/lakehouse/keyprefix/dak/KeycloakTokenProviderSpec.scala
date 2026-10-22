package vai.lakehouse.keyprefix

import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.StubHttpServer.{Response, tokenResponse}

import scala.collection.JavaConverters._

class KeycloakTokenProviderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val TokenPath = "/realms/tenant-a/protocol/openid-connect/token"
  private val Secret    = "s3cr&t=va+lue x"
  private val Hint      = "TEST_dak.clientId / TEST_dak.clientSecret"

  private var server: StubHttpServer = _
  private var nowNanos: Long = _

  override def beforeEach(): Unit = {
    server = new StubHttpServer
    nowNanos = 0L
  }

  override def afterEach(): Unit = server.close()

  private def tokenUrl = server.url + TokenPath

  private def provider(url: String = tokenUrl) =
    new KeycloakTokenProvider(url, "dak.w1.team-a", Secret, Hint, () => nowNanos)

  private def advanceSeconds(s: Long): Unit = nowNanos += TimeUnit.SECONDS.toNanos(s)

  "token" should "gửi grant client_credentials dạng form-urlencoded, secret có ký tự đặc biệt vẫn đúng" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))

    provider().token() shouldBe "tok-1"

    val req = server.requestsTo(TokenPath).head
    req.headers("content-type") should startWith("application/x-www-form-urlencoded")
    req.form shouldBe Map(
      "grant_type" -> "client_credentials", "client_id" -> "dak.w1.team-a", "client_secret" -> Secret)
  }

  it should "dùng lại token khi còn hạn và xin mới khi đã qua expires_in - skew (skew = 30 giây)" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1", 300), tokenResponse("tok-2", 300))
    val p = provider()

    p.token() shouldBe "tok-1"
    advanceSeconds(269)
    p.token() shouldBe "tok-1"
    server.requestsTo(TokenPath) should have size 1

    advanceSeconds(2) // 271 giây > 300 - 30
    p.token() shouldBe "tok-2"
    server.requestsTo(TokenPath) should have size 2
  }

  it should "dùng skew = expires_in / 2 khi token sống ngắn" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1", 40), tokenResponse("tok-2", 40))
    val p = provider()

    p.token() shouldBe "tok-1"
    advanceSeconds(19)
    p.token() shouldBe "tok-1"
    advanceSeconds(2) // 21 giây > 40 - 20
    p.token() shouldBe "tok-2"
  }

  "invalidate" should "chỉ xoá khi token trong cache đúng là token bị từ chối" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"), tokenResponse("tok-2"))
    val p = provider()
    p.token() shouldBe "tok-1"

    p.invalidate("tok-khac")
    p.token() shouldBe "tok-1"

    p.invalidate("tok-1")
    p.token() shouldBe "tok-2"
    server.requestsTo(TokenPath) should have size 2
  }

  "token" should "chỉ tạo 1 request khi nhiều thread cùng cần token lúc cache trống" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1").copy(delayMillis = 300))
    val p = provider()
    val pool = Executors.newFixedThreadPool(20)
    try {
      val tasks = (1 to 20).map(_ => new Callable[String] { override def call(): String = p.token() })
      pool.invokeAll(tasks.asJava).asScala.map(_.get()).toSet shouldBe Set("tok-1")
    } finally pool.shutdownNow()

    server.requestsTo(TokenPath) should have size 1
  }

  it should "báo lỗi kèm mã lỗi OAuth và tên cấu hình cần kiểm tra, không lộ secret" in {
    server.stub("POST", TokenPath,
      Response(401, s"""{"error":"invalid_client","error_description":"bad secret $Secret"}"""))

    val ex = intercept[IllegalStateException](provider().token())

    ex.getMessage should include("401")
    ex.getMessage should include("invalid_client")
    ex.getMessage should include(Hint)
    ex.getMessage should not include Secret
  }

  it should "không đưa mã lỗi lạ (không phải định danh OAuth) vào thông báo" in {
    server.stub("POST", TokenPath, Response(400, """{"error":"<script>x</script>"}"""))

    val ex = intercept[IllegalStateException](provider().token())

    ex.getMessage should include("400")
    ex.getMessage should not include "<script>"
  }

  it should "báo lỗi khi response thiếu access_token hoặc expires_in" in {
    Seq("""{"expires_in":300}""", """{"access_token":"tok-1"}""", """{"access_token":"","expires_in":300}""",
      """{"access_token":"tok-1","expires_in":0}""").foreach { body =>
      server.stub("POST", TokenPath, Response(200, body))
      val ex = intercept[IllegalStateException](provider().token())
      ex.getMessage should (include("access_token") or include("expires_in"))
    }
  }

  it should "báo lỗi kèm địa chỉ khi không kết nối được, không lộ secret" in {
    val deadUrl = tokenUrl
    server.close()

    val ex = intercept[IllegalStateException](provider(deadUrl).token())

    ex.getMessage should include(deadUrl)
    ex.getMessage should not include Secret
  }

  "shared" should "trả cùng instance cho cùng (tokenUrl, clientId, secret), khác instance khi khác realm" in {
    val a1 = KeycloakTokenProvider.shared(tokenUrl, "c1", "s", Hint)
    val a2 = KeycloakTokenProvider.shared(tokenUrl, "c1", "s", Hint)
    val otherRealm = KeycloakTokenProvider.shared(server.url + "/realms/tenant-b/protocol/openid-connect/token",
      "c1", "s", Hint)

    a1 should be theSameInstanceAs a2
    otherRealm should not be theSameInstanceAs(a1)
  }

  "toString" should "không chứa secret" in {
    provider().toString should not include Secret
  }
}
