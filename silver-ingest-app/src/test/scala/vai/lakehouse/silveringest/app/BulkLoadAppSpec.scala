package vai.lakehouse.silveringest.app

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.columncrypto.cdr.CdrCrypto
import vai.lakehouse.keyprefix.{ColumnKeySource, KeyIntent, PathKeyResult, PathKeySource}
import vai.lakehouse.silveringest.config.Mode

/**
 * Bulk load: một lần chạy nạp NHIỀU ngày (`BULK_FROM`..`BULK_TO`), lần lượt từng ngày trong cùng một Spark session, mỗi ngày một partition `dt`
 * (docs/silver-ingest/BACKFILL.md). Ngày đã nạp trước đó (kể cả chạy thử) chỉ bị thay đúng partition của ngày đó.
 */
class BulkLoadAppSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val workDir = Files.createTempDirectory("bulk-load")
  private var spark: SparkSession = _
  private val globalPrefix = "TEST-GLOBAL-PREFIX-"

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().appName("BulkLoadAppSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", workDir.resolve("warehouse").toString)
      .config("spark.sql.extensions", SparkSessions.DeltaExtension)
      .config("spark.sql.catalog.spark_catalog", SparkSessions.DeltaCatalog)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  // ---------------------------------------------------------------- helpers

  /** `<root>/date=<day>/part-1.csv`; `rows` are `isdn,amount` lines. Returns the root. */
  private def writeDay(root: String, day: String, rows: Seq[String]): Path = {
    val dir = Files.createDirectories(workDir.resolve(root).resolve(s"date=$day"))
    Files.write(dir.resolve("part-1.csv"), ("isdn,amount" +: rows).mkString("", "\n", "\n").getBytes(UTF_8))
    workDir.resolve(root)
  }

  private def bulkEnv(root: String, table: String, from: String, to: String, extra: (String, String)*): Map[String, String] =
    Map("INPUT_PATH" -> s"${workDir.resolve(root)}/date=%DAY%", "INPUT_FORMAT" -> "csv", "INPUT_OPTION_header" -> "true",
      "OUTPUT_DB" -> "silver", "OUTPUT_TABLE" -> table, "ENABLE_HIVE_SUPPORT" -> "false", "BATCH_COLUMN" -> "dt",
      "BULK_FROM" -> from, "BULK_TO" -> to) ++ extra

  /** One normal run of ONE day (what a test run of the DAG does today). */
  private def oneDay(root: String, table: String, day: String): Int =
    SilverApp.execute(Mode.Passthrough, Map("INPUT_PATH" -> s"${workDir.resolve(root)}/date=$day", "INPUT_FORMAT" -> "csv",
      "INPUT_OPTION_header" -> "true", "OUTPUT_DB" -> "silver", "OUTPUT_TABLE" -> table, "ENABLE_HIVE_SUPPORT" -> "false",
      "BATCH_COLUMN" -> "dt", "BATCH_VALUE" -> day), Some(spark))

  private def bulk(env: Map[String, String]): Int = SilverApp.execute(Mode.Passthrough, env, Some(spark))

  private def counts(table: String): Map[String, Long] =
    spark.table(s"silver.$table").groupBy("dt").count().collect().map(r => r.getString(0) -> r.getLong(1)).toMap

  private def partitionColumns(table: String): Seq[String] =
    spark.sql(s"DESCRIBE DETAIL silver.$table").select("partitionColumns").head().getSeq[String](0)

  // ---------------------------------------------------------------- nạp nhiều ngày

  "Bulk load" should "nạp từng ngày của khoảng vào partition dt của ngày đó, trong một lần chạy" in {
    writeDay("a", "20261104", Seq("1,10", "2,20"))
    writeDay("a", "20261105", Seq("3,30"))
    writeDay("a", "20261106", Seq("4,40", "5,50", "6,60"))

    bulk(bulkEnv("a", "bulk_a", "20261104", "20261106")) shouldBe SilverApp.ExitOk

    counts("bulk_a") shouldBe Map("20261104" -> 2L, "20261105" -> 1L, "20261106" -> 3L)
    partitionColumns("bulk_a") shouldBe Seq("dt")
    spark.table("silver.bulk_a").where("dt = '20261105'").select("amount").head().getString(0) shouldBe "30" // no INPUT_SCHEMA: read as text
  }

  it should "chạy lại cùng khoảng không ghi trùng" in {
    writeDay("b", "20261104", Seq("1,10"))
    writeDay("b", "20261105", Seq("2,20"))

    bulk(bulkEnv("b", "bulk_b", "20261104", "20261105")) shouldBe SilverApp.ExitOk
    bulk(bulkEnv("b", "bulk_b", "20261104", "20261105")) shouldBe SilverApp.ExitOk

    counts("bulk_b") shouldBe Map("20261104" -> 1L, "20261105" -> 1L)
  }

  it should "thay các ngày đã chạy thử trước đó và không đụng ngày ngoài khoảng" in {
    // Test runs of the DAG on some days: day 03 (outside the range to load) and day 05 with OLD content.
    writeDay("c-old", "20261103", Seq("9,90"))
    writeDay("c-old", "20261105", Seq("7,70", "8,80", "9,90"))
    oneDay("c-old", "bulk_c", "20261103") shouldBe SilverApp.ExitOk
    oneDay("c-old", "bulk_c", "20261105") shouldBe SilverApp.ExitOk
    counts("bulk_c") shouldBe Map("20261103" -> 1L, "20261105" -> 3L)

    // The first load of 04..06 with the real (new) content.
    writeDay("c", "20261104", Seq("1,10"))
    writeDay("c", "20261105", Seq("2,20"))
    writeDay("c", "20261106", Seq("3,30"))
    bulk(bulkEnv("c", "bulk_c", "20261104", "20261106")) shouldBe SilverApp.ExitOk

    counts("bulk_c") shouldBe Map("20261103" -> 1L, "20261104" -> 1L, "20261105" -> 1L, "20261106" -> 1L)
    spark.table("silver.bulk_c").where("dt = '20261105'").select("isdn").head().getString(0) shouldBe "2" // the old rows of the test run are gone
    spark.table("silver.bulk_c").where("dt = '20261103'").select("isdn").head().getString(0) shouldBe "9" // outside the range: untouched
  }

  it should "dừng ở ngày thiếu (mặc định), giữ các ngày đã nạp trước đó và không nạp các ngày sau" in {
    writeDay("d", "20261104", Seq("1,10"))
    writeDay("d", "20261106", Seq("3,30")) // 05 is missing

    bulk(bulkEnv("d", "bulk_d", "20261104", "20261106")) shouldBe SilverApp.ExitRuntimeError

    counts("bulk_d") shouldBe Map("20261104" -> 1L)
  }

  it should "BULK_MISSING_DAY=skip: bỏ ngày thiếu, nạp các ngày còn lại, vẫn thành công" in {
    writeDay("e", "20261104", Seq("1,10"))
    writeDay("e", "20261106", Seq("3,30"))

    bulk(bulkEnv("e", "bulk_e", "20261104", "20261106", "BULK_MISSING_DAY" -> "skip")) shouldBe SilverApp.ExitOk

    counts("bulk_e") shouldBe Map("20261104" -> 1L, "20261106" -> 1L)
  }

  it should "BULK_MISSING_DAY=skip nhưng không có ngày nào để nạp là lỗi, không tạo bảng" in {
    Files.createDirectories(workDir.resolve("f"))

    bulk(bulkEnv("f", "bulk_f", "20261104", "20261106", "BULK_MISSING_DAY" -> "skip")) shouldBe SilverApp.ExitRuntimeError

    spark.catalog.tableExists("silver", "bulk_f") shouldBe false
  }

  it should "trả exit 2 và không ghi gì khi cấu hình bulk sai (không dựng Spark, không nạp ngày nào)" in {
    writeDay("g", "20261104", Seq("1,10"))

    bulk(bulkEnv("g", "bulk_g1", "20261106", "20261104")) shouldBe SilverApp.ExitConfigError
    bulk(bulkEnv("g", "bulk_g2", "20261104", "20261105", "OUTPUT_MODE" -> "append")) shouldBe SilverApp.ExitConfigError
    bulk(bulkEnv("g", "bulk_g3", "20261104", "20261105", "BATCH_VALUE" -> "20261104")) shouldBe SilverApp.ExitConfigError
    bulk(bulkEnv("g", "bulk_g4", "20261104", "20261105") + ("INPUT_PATH" -> workDir.resolve("g").toString)) shouldBe SilverApp.ExitConfigError

    Seq("bulk_g1", "bulk_g2", "bulk_g3", "bulk_g4").foreach(t => spark.catalog.tableExists("silver", t) shouldBe false)
  }

  it should "trả exit 2 khi cấu hình của ngày (không phải của bulk) sai, trước khi nạp ngày nào" in {
    writeDay("h", "20261104", Seq("1,10"))

    bulk(bulkEnv("h", "bulk_h", "20261104", "20261105", "OUTPUT_FORMAT" -> "bad-format")) shouldBe SilverApp.ExitConfigError

    spark.catalog.tableExists("silver", "bulk_h") shouldBe false
  }

  it should "nạp đúng cột giờ partition (dt, hour) cho nhiều ngày" in {
    val root = "i"
    Seq("20261104" -> Seq("1,5", "2,17"), "20261105" -> Seq("3,5")).foreach { case (day, rows) =>
      val dir = Files.createDirectories(workDir.resolve(root).resolve(s"date=$day"))
      Files.write(dir.resolve("part-1.csv"), ("isdn,hour_id" +: rows).mkString("", "\n", "\n").getBytes(UTF_8))
    }

    bulk(bulkEnv(root, "bulk_i", "20261104", "20261105", "HOUR_FROM_COLUMN" -> "hour_id", "OUTPUT_PARTITION_BY" -> "hour")) shouldBe
      SilverApp.ExitOk

    partitionColumns("bulk_i") shouldBe Seq("dt", "hour")
    spark.table("silver.bulk_i").groupBy("dt", "hour").count().collect().map(r => (r.getString(0), r.getString(1)) -> r.getLong(2)).toMap shouldBe
      Map(("20261104", "05") -> 1L, ("20261104", "17") -> 1L, ("20261105", "05") -> 1L)
  }

  // ---------------------------------------------------------------- cùng pipeline mã hoá cho từng ngày

  private class GlobalKeys extends ColumnKeySource {
    val calls = new AtomicInteger()
    override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
      calls.incrementAndGet()
      columns.map(_.toLowerCase -> globalPrefix).toMap
    }
  }

  it should "mã hoá từng ngày bằng cùng key global và cột hằng số: cùng một giá trị ra cùng một bản mã ở mọi ngày" in {
    writeDay("j", "20261104", Seq("84900000001,10"))
    writeDay("j", "20261105", Seq("84900000001,20", "84900000002,30"))
    val keys = new GlobalKeys
    val env = bulkEnv("j", "bulk_j", "20261104", "20261105", "DECRYPT_MODE" -> "none", "CDR_ENCRYPT_COLUMNS" -> "isdn",
      "CDR_KEY_FIELD" -> "key_scope", "CDR_KEY_FIELD_VALUE" -> "vtt", "GLOBAL_KEY_ZONE" -> "vtt")
    val pathKeys = new PathKeySource { override def readPath(path: String): PathKeyResult = PathKeyResult.NotAvailable }

    SilverApp.execute(Mode.DecryptEncrypt, env, Some(spark), Some(KeySources(pathKeys, keys))) shouldBe SilverApp.ExitOk

    val expected = CdrCrypto.transform(globalPrefix, encryptMode = true)("84900000001", "vtt")
    spark.table("silver.bulk_j").where("isdn = '" + expected + "'").groupBy("dt").count().collect()
      .map(r => r.getString(0) -> r.getLong(1)).toMap shouldBe Map("20261104" -> 1L, "20261105" -> 1L)
    counts("bulk_j") shouldBe Map("20261104" -> 1L, "20261105" -> 2L)
    keys.calls.get() shouldBe 2 // the keys are asked once per day
  }
}
