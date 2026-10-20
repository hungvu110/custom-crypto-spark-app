package vai.lakehouse.columncrypto.cdr

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.{PrefixSourceFactory, SparkConfSource}

/**
 * Tích hợp `cdr_encrypt`/`cdr_decrypt` với nguồn `source=dak` qua đúng đường cấu hình của sql-engine:
 * Spark conf `spark.cdrcrypto.dak.*` -> `PrefixSourceFactory` -> Keycloak + DAK (server HTTP giả).
 */
class CdrCryptoExtensionDakSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val Prefix = "231x#@Vie3"

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
    server.createContext("/token", (ex: HttpExchange) =>
      respond(ex, 200, """{"access_token":"tok-1","expires_in":300}"""))
    server.createContext("/api/v1/keys/", (ex: HttpExchange) => {
      keyRequests.incrementAndGet()
      ex.getRequestURI.getPath match {
        case "/api/v1/keys/demo_db/users_cdr" =>
          respond(ex, 200, s"""{"database":"demo_db","table":"users_cdr","keyPrefix":"$Prefix","keyVersion":1}""")
        case _ =>
          respond(ex, 403, """{"error":"access_denied","message":"denied","requestId":"req-denied"}""")
      }
    })
    server.start()
    val url = s"http://127.0.0.1:${server.getAddress.getPort}"

    val ns = CdrCrypto.ConfPrefix
    val conf = new SparkConf(false)
      .set(s"${ns}source", "dak")
      .set(s"${ns}dak.addr", url)
      .set(s"${ns}dak.tokenUrl", s"$url/token")
      .set(s"${ns}dak.clientId", "dak.w1.team-a")
      .set(s"${ns}dak.clientSecret", "client-secret-xyz")
      .set(s"${ns}dak.allowInsecureHttp", "true")
    val source = PrefixSourceFactory.create(new SparkConfSource(conf, ns))

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder()
      .appName("CdrCryptoExtensionDakSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .withExtensions(new CdrCryptoExtension(() => source))
      .getOrCreate()
    val schema = StructType(Seq(
      StructField("isdn", StringType, nullable = true),
      StructField("start_datetime", StringType, nullable = false)))
    val rows = Seq(Row("978356099", "20260928123456"), Row(null, "20260101000001"))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema).createOrReplaceTempView("cdr_sample")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    if (server != null) server.stop(0)
  }

  private def sql(q: String) = spark.sql(q)

  "cdr_encrypt / cdr_decrypt với source=dak" should "roundtrip với tham số 'database.table'" in {
    val out = sql("SELECT cdr_decrypt('demo_db.users_cdr', cdr_encrypt('demo_db.users_cdr', isdn, start_datetime), " +
      "start_datetime) AS isdn FROM cdr_sample ORDER BY start_datetime DESC").collect().map(_.getAs[String]("isdn")).toSeq

    out shouldBe Seq("978356099", null)
  }

  it should "cho cùng ciphertext với cdr_encrypt gọi thẳng lõi bằng prefix DAK trả về" in {
    val viaSql = sql("SELECT cdr_encrypt('demo_db.users_cdr', isdn, start_datetime) FROM cdr_sample " +
      "WHERE isdn IS NOT NULL").collect().head.getString(0)

    viaSql shouldBe CdrCipherCore.encrypt("978356099", Prefix + "20260928123456")
  }

  it should "từ chối tên một phần ngay ở bước analyze, không gọi DAK" in {
    val before = keyRequests.get()

    val ex = intercept[IllegalArgumentException](
      sql("SELECT cdr_decrypt('users_cdr', isdn, start_datetime) FROM cdr_sample"))

    ex.getMessage should include("'<database>.<table>'")
    keyRequests.get() shouldBe before
  }

  it should "báo 403 của DAK ngay ở bước analyze, kèm requestId, không lộ prefix" in {
    val ex = intercept[IllegalStateException](
      sql("SELECT cdr_decrypt('demo_db.other_tbl', isdn, start_datetime) FROM cdr_sample"))

    ex.getMessage should include("access_denied")
    ex.getMessage should include("requestId=req-denied")
    ex.getMessage should not include Prefix
  }
}
