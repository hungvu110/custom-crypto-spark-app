package vai.lakehouse.columncrypto.cdr

import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.{ColumnKeySourceFactory, ColumnKeysUnavailableException, SparkConfSource}

/**
 * Tích hợp `cdr_encrypt`/`cdr_decrypt` với key theo cột qua đúng đường cấu hình của sql-engine:
 * Spark conf `spark.cdrcrypto.dak.*` -> `ColumnKeySourceFactory` -> Keycloak + `GET /api/v1/keys/by-column` (server giả, body
 * giống dak-mock). Cột `isdn` có key; cột `imei` đang chờ DAK tạo key (`PENDING`).
 */
class CdrCryptoExtensionDakSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val IsdnKey = "231x#@Vie3"

  private val queries = new ConcurrentLinkedQueue[Map[String, String]]()
  private var server: HttpServer = _
  private var spark: SparkSession = _

  private def respond(ex: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    ex.getResponseHeaders.add("Content-Type", "application/json")
    ex.sendResponseHeaders(status, bytes.length.toLong)
    ex.getResponseBody.write(bytes)
    ex.close()
  }

  private def params(ex: HttpExchange): Map[String, String] =
    Option(ex.getRequestURI.getRawQuery).getOrElse("").split("&").filter(_.nonEmpty).map { pair =>
      val kv = pair.split("=", 2)
      URLDecoder.decode(kv(0), "UTF-8") -> URLDecoder.decode(if (kv.length > 1) kv(1) else "", "UTF-8")
    }.toMap

  private def column(name: String): String = name match {
    case "isdn" => s"""{"column":"isdn","state":"READY","keyPrefix":"$IsdnKey","keyVersion":1}"""
    case "imei" => """{"column":"imei","state":"PENDING","requestCode":"YC-2026-000002"}"""
    case other  => s"""{"column":"$other","state":"MISSING","reason":"not_requested"}"""
  }

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/token", (ex: HttpExchange) =>
      respond(ex, 200, """{"access_token":"tok-1","expires_in":300}"""))
    server.createContext("/api/v1/keys/by-column", (ex: HttpExchange) => {
      val p = params(ex)
      queries.add(p)
      if (p.get("table").contains("users_cdr")) {
        val cols = p("columns").split(",").toSeq
        respond(ex, 200, s"""{"database":"${p("database")}","table":"users_cdr",""" +
          s""""complete":${cols.forall(_ == "isdn")},"columns":[${cols.map(column).mkString(",")}]}""")
      } else {
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
      .set(s"${ns}dak.clientId", "dak.internal")
      .set(s"${ns}dak.clientSecret", "client-secret-xyz")
      .set(s"${ns}dak.allowInsecureHttp", "true")
      .set(s"${ns}dak.tag", "internal")
      .set(s"${ns}cacheTtlSeconds", "0")
    val source = ColumnKeySourceFactory.create(new SparkConfSource(conf, ns))

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

  "cdr_encrypt / cdr_decrypt với key theo cột qua DAK" should "roundtrip với tham số 'database.table' và tên cột" in {
    val out = sql("SELECT cdr_decrypt('demo_db.users_cdr', 'isdn', " +
      "cdr_encrypt('demo_db.users_cdr', 'isdn', isdn, start_datetime), start_datetime) AS isdn " +
      "FROM cdr_sample ORDER BY start_datetime DESC").collect().map(_.getAs[String]("isdn")).toSeq

    out shouldBe Seq("978356099", null)
  }

  it should "cho cùng ciphertext với lõi gọi thẳng bằng key cột DAK trả về" in {
    val viaSql = sql("SELECT cdr_encrypt('demo_db.users_cdr', 'isdn', isdn, start_datetime) FROM cdr_sample " +
      "WHERE isdn IS NOT NULL").collect().head.getString(0)

    viaSql shouldBe CdrCipherCore.encrypt("978356099", IsdnKey + "20260928123456")
  }

  it should "gửi by-column đúng tham số: onMissing theo hàm, tag=internal, tên chữ thường" in {
    // Key được xin ở bước analyze (lúc gọi sql()), nên không cần chạy câu lệnh.
    queries.clear()
    sql("SELECT cdr_encrypt('Demo_DB.Users_CDR', 'ISDN', isdn, start_datetime) FROM cdr_sample")
    val encrypt = queries.asScala.toList
    queries.clear()
    sql("SELECT cdr_decrypt('demo_db.users_cdr', 'isdn', isdn, start_datetime) FROM cdr_sample")
    val decrypt = queries.asScala.toList

    encrypt should not be empty
    all(encrypt) shouldBe Map("database" -> "demo_db", "table" -> "users_cdr", "columns" -> "isdn",
      "onMissing" -> "request", "tag" -> "internal")
    decrypt should not be empty
    all(decrypt.map(_("onMissing"))) shouldBe "none"
  }

  it should "báo cột đang chờ DAK tạo key ngay ở bước analyze, kèm mã YC" in {
    val ex = intercept[ColumnKeysUnavailableException](
      sql("SELECT cdr_encrypt('demo_db.users_cdr', 'imei', isdn, start_datetime) FROM cdr_sample"))

    ex.getMessage should include("PENDING")
    ex.getMessage should include("YC-2026-000002")
  }

  it should "từ chối tên một phần ngay ở bước analyze, không gọi DAK" in {
    queries.clear()

    val ex = intercept[IllegalArgumentException](
      sql("SELECT cdr_decrypt('users_cdr', 'isdn', isdn, start_datetime) FROM cdr_sample"))

    ex.getMessage should include("'<database>.<table>'")
    queries shouldBe empty
  }

  it should "báo 403 của DAK ngay ở bước analyze, kèm requestId, không lộ key" in {
    val ex = intercept[IllegalStateException](
      sql("SELECT cdr_decrypt('demo_db.other_tbl', 'isdn', isdn, start_datetime) FROM cdr_sample"))

    ex.getMessage should include("access_denied")
    ex.getMessage should include("requestId=req-denied")
    ex.getMessage should not include IsdnKey
  }
}
