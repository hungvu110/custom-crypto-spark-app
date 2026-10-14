package vai.lakehouse.pseudonymizetable.app

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.pseudonym.core.{AesSivPseudonymizer, HmacPseudonymizer, PrefixSourceKeyProvider, PseudonymKey}
import vai.lakehouse.pseudonym.spark.PseudonymConf

/**
 * Chạy app thật (`execute`) trên bảng thật trong catalog của 1 SparkSession local, khoá lấy qua `spark.pseudonym.*`
 * (nguồn `file`) — đúng đường cấu hình của production.
 */
class PseudonymizeTableAppSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val workDir = Files.createTempDirectory("pseudonymize-table")
  private val keyDir = Files.createDirectory(workDir.resolve("keys"))
  private var spark: SparkSession = _

  private def writeKey(name: String, size: Int): Unit =
    Files.write(keyDir.resolve(name), PseudonymKey.randomEncoded(size).getBytes(UTF_8))

  override def beforeAll(): Unit = {
    writeKey(PrefixSourceKeyProvider.hmacName(1), HmacPseudonymizer.KeyBytes)
    writeKey(PrefixSourceKeyProvider.aesSivName(1), AesSivPseudonymizer.KeyBytes)
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().appName("PseudonymizeTableAppSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", workDir.resolve("warehouse").toString)
      .config("spark.sql.extensions", SparkSessions.DeltaExtension)
      .config("spark.sql.catalog.spark_catalog", SparkSessions.DeltaCatalog)
      .config("spark.pseudonym.source", "file")
      .config("spark.pseudonym.file.dir", keyDir.toString)
      .getOrCreate()

    spark.sql("CREATE DATABASE crm")
    spark.sql("CREATE TABLE crm.subscriber (id BIGINT, isdn STRING, imei STRING, province STRING) USING parquet")
    spark.sql(
      """INSERT INTO crm.subscriber VALUES
        |  (1, '84903217465', '352099001761481', 'Ha Noi'),
        |  (2, '84903217466', '352099001761499', 'Da Nang'),
        |  (3, '84903217465', NULL,              'Ha Noi'),
        |  (4, NULL,          '352099001761481', 'Can Tho')""".stripMargin)
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def run(extra: (String, String)*): Int =
    PseudonymizeTableApp.execute(Map("DB_NAME" -> "crm", "TABLE_NAME" -> "subscriber", "PSEUDO_COLUMNS" -> "isdn,imei",
      "ENABLE_HIVE_SUPPORT" -> "false") ++ extra, Some(spark))

  private def rows(table: String) = spark.table(table).orderBy("id").collect()

  "PseudonymizeTableApp" should "HASH: ghi bảng đích đã giả danh, giữ nguyên null và cột khác, KHÔNG sửa bảng nguồn" in {
    val sourceBefore = rows("crm.subscriber").map(_.toString).toSeq

    run("OUTPUT_TABLE" -> "subscriber_hash") shouldBe PseudonymizeTableApp.ExitOk

    val out = rows("crm.subscriber_hash")
    out.map(_.getAs[String]("isdn")).filter(_ != null).foreach(_ should fullyMatch regex "k1_[0-9a-f]{32}")
    out(0).getAs[String]("isdn") shouldBe out(2).getAs[String]("isdn") // cùng số -> cùng mã (liên kết được)
    out(0).getAs[String]("isdn") should not be out(1).getAs[String]("isdn")
    out(3).isNullAt(out(3).fieldIndex("isdn")) shouldBe true
    out(2).isNullAt(out(2).fieldIndex("imei")) shouldBe true
    out.map(_.getAs[String]("province")).toSeq shouldBe Seq("Ha Noi", "Da Nang", "Ha Noi", "Can Tho")
    out.map(_.getAs[Long]("id")).toSeq shouldBe Seq(1L, 2L, 3L, 4L)
    rows("crm.subscriber").map(_.toString).toSeq shouldBe sourceBefore
  }

  it should "chỉ giả danh các cột được yêu cầu" in {
    run("PSEUDO_COLUMNS" -> "isdn", "OUTPUT_TABLE" -> "subscriber_one") shouldBe PseudonymizeTableApp.ExitOk

    val first = rows("crm.subscriber_one")(0)
    first.getAs[String]("isdn") should startWith("k1_")
    first.getAs[String]("imei") shouldBe "352099001761481"
  }

  it should "ENCRYPT: giải mã lại được đúng giá trị gốc" in {
    run("PSEUDO_MODE" -> "ENCRYPT", "OUTPUT_TABLE" -> "subscriber_enc") shouldBe PseudonymizeTableApp.ExitOk

    val enc = rows("crm.subscriber_enc")(0).getAs[String]("isdn")
    enc should startWith("e1_")
    val key = PseudonymConf.keyProvider(spark.sparkContext.getConf).aesSivKey
    new AesSivPseudonymizer(key).decrypt(enc) shouldBe "84903217465"
  }

  it should "MAPPING: sinh token t_ và lưu bảng ánh xạ ở đường dẫn riêng" in {
    val mapPath = workDir.resolve("pseudonym_map").toString

    run("PSEUDO_MODE" -> "MAPPING", "MAPPING_TABLE_PATH" -> mapPath, "OUTPUT_TABLE" -> "subscriber_map") shouldBe
      PseudonymizeTableApp.ExitOk

    rows("crm.subscriber_map")(0).getAs[String]("isdn") should fullyMatch regex "t_[0-9a-f]{32}"
    spark.read.format("delta").load(mapPath).count() shouldBe 4 // 2 isdn + 2 imei khác nhau
  }

  it should "OUTPUT_MODE=append ghi nối vào bảng đích, overwrite thay thế" in {
    run("OUTPUT_TABLE" -> "subscriber_modes") shouldBe PseudonymizeTableApp.ExitOk
    run("OUTPUT_TABLE" -> "subscriber_modes", "OUTPUT_MODE" -> "append") shouldBe PseudonymizeTableApp.ExitOk
    spark.table("crm.subscriber_modes").count() shouldBe 8

    run("OUTPUT_TABLE" -> "subscriber_modes") shouldBe PseudonymizeTableApp.ExitOk
    spark.table("crm.subscriber_modes").count() shouldBe 4
  }

  it should "ghi sang DB đích khác và định dạng hive (parquet)" in {
    run("OUTPUT_DB" -> "analytics", "OUTPUT_TABLE" -> "subscriber_safe", "OUTPUT_FORMAT" -> "hive") shouldBe
      PseudonymizeTableApp.ExitOk

    spark.table("analytics.subscriber_safe").count() shouldBe 4
  }

  it should "exit 2 khi sai cấu hình (không dựng Spark)" in {
    PseudonymizeTableApp.execute(Map("DB_NAME" -> "crm"), Some(spark)) shouldBe PseudonymizeTableApp.ExitConfigError
    run("OUTPUT_TABLE" -> "subscriber") shouldBe PseudonymizeTableApp.ExitConfigError
  }

  it should "exit 1 khi bảng nguồn không tồn tại, cột không có trong bảng, hoặc MAPPING thiếu đường dẫn" in {
    run("TABLE_NAME" -> "khong_ton_tai") shouldBe PseudonymizeTableApp.ExitRuntimeError
    run("PSEUDO_COLUMNS" -> "cot_la", "OUTPUT_TABLE" -> "x1") shouldBe PseudonymizeTableApp.ExitRuntimeError
    run("PSEUDO_MODE" -> "MAPPING", "OUTPUT_TABLE" -> "x2") shouldBe PseudonymizeTableApp.ExitRuntimeError
    spark.catalog.tableExists("crm", "x1") shouldBe false // fail sớm: chưa tạo bảng đích
  }
}
