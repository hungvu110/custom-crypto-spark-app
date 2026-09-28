package vai.lakehouse.columncrypto.prefix

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.CopyOnWriteArrayList

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class VaultPrefixSourceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private case class Recorded(method: String, path: String, token: Option[String], body: String)

  private val Jwt        = "sa-jwt-value"
  private val Token      = "client-token-123"
  private val LoginPath  = "/v1/auth/kubernetes/login"
  private val KvPath     = "/v1/kv/data/hla/key-prefix/sample_table"
  private val RevokePath = "/v1/auth/token/revoke-self"

  private var server: HttpServer = _
  private var stubs: Map[(String, String), (Int, String)] = _
  private val recorded = new CopyOnWriteArrayList[Recorded]()

  override def beforeEach(): Unit = {
    stubs = Map.empty
    recorded.clear()
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", (ex: HttpExchange) => {
      val in = ex.getRequestBody
      val buf = new ByteArrayOutputStream()
      val chunk = new Array[Byte](1024)
      Iterator.continually(in.read(chunk)).takeWhile(_ != -1).foreach(n => buf.write(chunk, 0, n))
      val body = new String(buf.toByteArray, StandardCharsets.UTF_8)
      recorded.add(Recorded(ex.getRequestMethod, ex.getRequestURI.getPath,
        Option(ex.getRequestHeaders.getFirst("X-Vault-Token")), body))
      val (status, resp) = stubs.getOrElse((ex.getRequestMethod, ex.getRequestURI.getPath), (404, """{"errors":[]}"""))
      val bytes = resp.getBytes(StandardCharsets.UTF_8)
      ex.sendResponseHeaders(status, if (bytes.isEmpty) -1L else bytes.length.toLong)
      if (bytes.nonEmpty) ex.getResponseBody.write(bytes)
      ex.close()
    })
    server.start()
  }

  override def afterEach(): Unit = server.stop(0)

  private def stub(method: String, path: String, status: Int, body: String): Unit =
    stubs = stubs + ((method, path) -> ((status, body)))

  private def stubHappyPath(field: String = "keyPrefix", value: String = "vault_prefix"): Unit = {
    stub("POST", LoginPath, 200, s"""{"auth":{"client_token":"$Token"}}""")
    stub("GET", KvPath, 200, s"""{"data":{"data":{"$field":"$value"},"metadata":{"version":3}}}""")
    stub("POST", RevokePath, 204, "")
  }

  private def jwtFile(content: String = Jwt + "\n"): String = {
    val f = Files.createTempFile("sa-token", "")
    Files.write(f, content.getBytes(StandardCharsets.UTF_8))
    f.toFile.deleteOnExit()
    f.toString
  }

  private def cfg(basePath: String = "hla/key-prefix", jwt: String = jwtFile(), addr: String = null) =
    VaultConfig(
      addr = Option(addr).getOrElse(s"http://127.0.0.1:${server.getAddress.getPort}"),
      role = "spark-role",
      authMount = "kubernetes",
      kvMount = "kv",
      kvBasePath = basePath,
      keyField = "keyPrefix",
      jwtPath = jwt)

  private def requests = recorded.asScala.toList

  "VaultPrefixSource.read" should "đăng nhập bằng service account JWT rồi đọc keyPrefix từ KV v2" in {
    stubHappyPath()

    new VaultPrefixSource(cfg()).read("sample_table") shouldBe "vault_prefix"

    val login = requests.find(_.path == LoginPath).get
    login.method shouldBe "POST"
    login.body should include(""""role":"spark-role"""")
    login.body should include(s""""jwt":"$Jwt"""")
    val kv = requests.find(_.path == KvPath).get
    kv.method shouldBe "GET"
    kv.token shouldBe Some(Token)
  }

  it should "thu hồi token (revoke-self) sau khi đọc xong" in {
    stubHappyPath()

    new VaultPrefixSource(cfg()).read("sample_table")

    val revoke = requests.find(_.path == RevokePath).get
    revoke.method shouldBe "POST"
    revoke.token shouldBe Some(Token)
    requests.map(_.path) shouldBe List(LoginPath, KvPath, RevokePath)
  }

  it should "vẫn thu hồi token khi đọc secret thất bại, lỗi không lộ token hay JWT" in {
    stub("POST", LoginPath, 200, s"""{"auth":{"client_token":"$Token"}}""")
    stub("GET", KvPath, 404, """{"errors":[]}""")
    stub("POST", RevokePath, 204, "")

    val ex = intercept[IllegalStateException](new VaultPrefixSource(cfg()).read("sample_table"))

    ex.getMessage should include("404")
    ex.getMessage should include("sample_table")
    ex.getMessage should not include Token
    ex.getMessage should not include Jwt
    requests.map(_.path) should contain(RevokePath)
  }

  it should "ném lỗi rõ ràng nếu đăng nhập thất bại và không đọc secret" in {
    stub("POST", LoginPath, 403, """{"errors":["permission denied"]}""")

    val ex = intercept[IllegalStateException](new VaultPrefixSource(cfg()).read("sample_table"))

    ex.getMessage should include("login")
    ex.getMessage should include("403")
    ex.getMessage should not include Jwt
    requests.map(_.path) shouldBe List(LoginPath)
  }

  it should "ném lỗi nếu response đăng nhập không có client_token" in {
    stub("POST", LoginPath, 200, """{"auth":{}}""")

    val ex = intercept[IllegalStateException](new VaultPrefixSource(cfg()).read("sample_table"))
    ex.getMessage should include("client_token")
  }

  it should "ném lỗi liệt kê tên field có sẵn (không lộ giá trị) nếu thiếu field keyPrefix" in {
    stubHappyPath(field = "other", value = "leaky-value")

    val ex = intercept[IllegalStateException](new VaultPrefixSource(cfg()).read("sample_table"))

    ex.getMessage should include("keyPrefix")
    ex.getMessage should include("other")
    ex.getMessage should not include "leaky-value"
  }

  it should "ném lỗi nếu giá trị keyPrefix rỗng" in {
    stubHappyPath(value = "")

    val ex = intercept[IllegalStateException](new VaultPrefixSource(cfg()).read("sample_table"))
    ex.getMessage should include("empty")
  }

  it should "chuẩn hoá dấu '/' thừa ở đầu/cuối base path" in {
    stubHappyPath()

    new VaultPrefixSource(cfg(basePath = "/hla/key-prefix/")).read("sample_table") shouldBe "vault_prefix"
  }

  it should "từ chối tên bảng không hợp lệ trước khi gọi Vault" in {
    Seq("../x", "a/b", "..", "", "a b").foreach { bad =>
      intercept[IllegalArgumentException](new VaultPrefixSource(cfg()).read(bad))
    }
    requests shouldBe empty
  }

  it should "ném lỗi rõ ràng nếu không đọc được file JWT, không gọi Vault" in {
    val ex = intercept[IllegalStateException](
      new VaultPrefixSource(cfg(jwt = "/khong/ton/tai/token")).read("sample_table"))

    ex.getMessage should include("/khong/ton/tai/token")
    requests shouldBe empty
  }

  it should "ném lỗi rõ ràng nếu không kết nối được Vault" in {
    val deadAddr = s"http://127.0.0.1:${server.getAddress.getPort}"
    server.stop(0)

    val ex = intercept[IllegalStateException](new VaultPrefixSource(cfg(addr = deadAddr)).read("sample_table"))

    ex.getMessage should include(deadAddr)
    ex.getMessage should not include Jwt
  }

  private val StaticToken = "static-token-abc"

  "VaultPrefixSource với token tĩnh" should "đọc secret bằng token đó, KHÔNG login và KHÔNG revoke" in {
    stub("GET", KvPath, 200, """{"data":{"data":{"keyPrefix":"vault_prefix"}}}""")

    new VaultPrefixSource(cfg().copy(token = Some(StaticToken))).read("sample_table") shouldBe "vault_prefix"

    requests.map(_.path) shouldBe List(KvPath)
    requests.head.token shouldBe Some(StaticToken)
  }

  it should "không cần đọc file JWT" in {
    stub("GET", KvPath, 200, """{"data":{"data":{"keyPrefix":"vault_prefix"}}}""")

    new VaultPrefixSource(cfg(jwt = "/khong/ton/tai/token").copy(token = Some(StaticToken)))
      .read("sample_table") shouldBe "vault_prefix"
  }

  it should "báo lỗi không lộ token khi Vault từ chối" in {
    stub("GET", KvPath, 403, """{"errors":["permission denied"]}""")

    val ex = intercept[IllegalStateException](
      new VaultPrefixSource(cfg().copy(token = Some(StaticToken))).read("sample_table"))

    ex.getMessage should include("403")
    ex.getMessage should not include StaticToken
    requests.map(_.path) shouldBe List(KvPath) // vẫn không revoke khi lỗi
  }

  "VaultConfig.toString" should "che token" in {
    val text = cfg().copy(token = Some(StaticToken)).toString

    text should not include StaticToken
    text should include("token=***")
  }

  "FilePrefixSource.read" should "đọc keyPrefix từ file tên bảng trong thư mục" in {
    val dir = Files.createTempDirectory("fps")
    val f = Files.write(dir.resolve("sample_table"), "file_prefix\n".getBytes(StandardCharsets.UTF_8))
    f.toFile.deleteOnExit()
    dir.toFile.deleteOnExit()

    new FilePrefixSource(dir.toString).read("sample_table") shouldBe "file_prefix"
  }
}
