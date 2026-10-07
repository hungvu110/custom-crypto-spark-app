package vai.lakehouse.columncrypto.sql

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.ExtendedMode
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.{PrefixSourceFactory, SparkConfSource}

/**
 * Tích hợp `column_encrypt`/`column_decrypt` với nguồn `source=dak` qua đúng đường cấu hình của sql-engine:
 * Spark conf `spark.columncrypto.dak.*` -> `PrefixSourceFactory` -> Keycloak + DAK (server HTTP giả).
 */
class ColumnCryptoExtensionDakSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val Prefix = "DAK_PREFIX_7f3e"

  private val tokenRequests = new AtomicInteger()
  private val keyRequests = new AtomicInteger()
  private var server: HttpServer = _
  private var spark: SparkSession = _

  private def respond(ex: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    ex.getResponseHeaders.add("Content-Type", "application/json")
    ex.sendResponseHeaders(status, bytes.length.toLong)
    ex.getResponseBody.write(bytes)
    ex.close()
  }

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/token", (ex: HttpExchange) => {
      tokenRequests.incrementAndGet()
      respond(ex, 200, """{"access_token":"tok-1","expires_in":300}""")
    })
    server.createContext("/api/v1/keys/", (ex: HttpExchange) => {
      keyRequests.incrementAndGet()
      ex.getRequestURI.getPath match {
        case "/api/v1/keys/demo_db/customers" =>
          respond(ex, 200, s"""{"database":"demo_db","table":"customers","keyPrefix":"$Prefix","keyVersion":1}""")
        case _ =>
          respond(ex, 403, """{"error":"access_denied","message":"denied","requestId":"req-denied"}""")
      }
    })
    server.start()
    val url = s"http://127.0.0.1:${server.getAddress.getPort}"

    val ns = ColumnCryptoExtension.ConfPrefix
    val conf = new SparkConf(false)
      .set(s"${ns}source", "dak")
      .set(s"${ns}dak.addr", url)
      .set(s"${ns}dak.tokenUrl", s"$url/token")
      .set(s"${ns}dak.clientId", "dak.w1.team-a")
      .set(s"${ns}dak.clientSecret", "client-secret-xyz")
      .set(s"${ns}dak.allowInsecureHttp", "true")
      // Cấu hình Vault giữ sẵn cho fallback thủ công: có mặt nhưng không được dùng khi source=dak.
      .set(s"${ns}vault.addr", "http://127.0.0.1:1")
      .set(s"${ns}vault.authMethod", "token")
      .set(s"${ns}vault.token", "vault-token")
      .set(s"${ns}vault.kvBasePath", "base")
    val source = PrefixSourceFactory.create(new SparkConfSource(conf, ns))

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder()
      .appName("ColumnCryptoExtensionDakSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .withExtensions(new ColumnCryptoExtension(() => source))
      .getOrCreate()
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true)))
    val rows = Seq(Row(1L, "Alice Nguyen"), Row(2L, "Bob Tran"), Row(3L, null))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema).createOrReplaceTempView("people")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    if (server != null) server.stop(0)
  }

  private def sql(q: String) = spark.sql(q)

  "column_encrypt / column_decrypt với source=dak" should "roundtrip với tham số 'database.table'" in {
    val names = sql("SELECT column_decrypt('demo_db.customers', column_encrypt('demo_db.customers', name, id), id) " +
      "AS name FROM people ORDER BY id").collect().map(_.getAs[String]("name")).toSeq

    names shouldBe Seq("Alice Nguyen", "Bob Tran", null)
  }

  it should "giải mã được dữ liệu khi viết tên bảng khác hoa thường (cùng một key)" in {
    sql("SELECT id, column_encrypt('demo_db.customers', name, id) AS name FROM people")
      .createOrReplaceTempView("people_enc")

    sql("SELECT column_decrypt('DEMO_DB.Customers', name, id) AS name FROM people_enc WHERE id = 1")
      .collect().head.getString(0) shouldBe "Alice Nguyen"
  }

  it should "từ chối tên một phần ngay ở bước analyze, không gọi Keycloak/DAK" in {
    val tokensBefore = tokenRequests.get()
    val keysBefore = keyRequests.get()

    val ex = intercept[IllegalArgumentException](sql("SELECT column_encrypt('customers', name, id) FROM people"))

    ex.getMessage should include("'<database>.<table>'")
    tokenRequests.get() shouldBe tokensBefore
    keyRequests.get() shouldBe keysBefore
  }

  it should "báo 403 của DAK ngay ở bước analyze, kèm requestId, không lộ prefix" in {
    val ex = intercept[IllegalStateException](sql("SELECT column_encrypt('demo_db.secret_tbl', name, id) FROM people"))

    ex.getMessage should include("demo_db.secret_tbl")
    ex.getMessage should include("access_denied")
    ex.getMessage should include("requestId=req-denied")
    ex.getMessage should not include Prefix
  }

  it should "chỉ xin token một lần và cache prefix theo bảng" in {
    sql("SELECT column_encrypt('demo_db.customers', name, id) FROM people").collect() // chắc chắn đã có trong cache
    val keysBefore = keyRequests.get()

    (1 to 3).foreach(_ => sql("SELECT column_encrypt('demo_db.customers', name, id) FROM people").collect())

    keyRequests.get() shouldBe keysBefore
    tokenRequests.get() shouldBe 1
  }

  it should "che keyPrefix lấy từ DAK khỏi EXPLAIN" in {
    val query = "SELECT column_encrypt('demo_db.customers', name, id) FROM people"
    sql(query).collect()

    sql(query).queryExecution.explainString(ExtendedMode) should not include Prefix
  }
}
