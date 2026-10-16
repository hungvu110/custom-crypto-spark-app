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

  it should "mỗi cột một chế độ: isdn HASH, imei ENCRYPT" in {
    run("PSEUDO_COLUMNS" -> "isdn:HASH,imei:ENCRYPT", "OUTPUT_TABLE" -> "subscriber_mixed") shouldBe PseudonymizeTableApp.ExitOk

    val first = rows("crm.subscriber_mixed")(0)
    first.getAs[String]("isdn") should fullyMatch regex "k1_[0-9a-f]{32}"
    val enc = first.getAs[String]("imei")
    enc should startWith("e1_")
    val key = PseudonymConf.keyProvider(spark.sparkContext.getConf).aesSivKey
    new AesSivPseudonymizer(key).decrypt(enc) shouldBe "352099001761481"
  }

  private def runFile(extra: (String, String)*): Int =
    PseudonymizeTableApp.execute(Map("OUTPUT_DB" -> "landing", "ENABLE_HIVE_SUPPORT" -> "false") ++ extra, Some(spark))

  it should "đọc thư mục file txt (csv không header, cột _c0.._cN) và ghi bảng đích" in {
    val dir = Files.createDirectory(workDir.resolve("landing_txt"))
    Files.write(dir.resolve("part-0.txt"), "84903217465|352099001761481|10\n84903217466|352099001761499|20\n".getBytes(UTF_8))
    Files.write(dir.resolve("part-1.txt"), "84903217465||30\n".getBytes(UTF_8))

    runFile("INPUT_PATH" -> dir.toString, "INPUT_FORMAT" -> "txt", "INPUT_OPTION_delimiter" -> "|",
      "PSEUDO_COLUMNS" -> "_c0:HASH,_c1:ENCRYPT", "OUTPUT_TABLE" -> "charge_txt") shouldBe PseudonymizeTableApp.ExitOk

    val out = spark.table("landing.charge_txt").orderBy("_c2").collect()
    out.length shouldBe 3
    out.map(_.getString(0)).foreach(_ should fullyMatch regex "k1_[0-9a-f]{32}")
    out(0).getString(0) shouldBe out(2).getString(0) // cùng số -> cùng mã
    out(2).isNullAt(1) shouldBe true                  // chuỗi rỗng trong csv là NULL
    out.map(_.getString(2)).toSeq shouldBe Seq("10", "20", "30")
  }

  it should "đọc parquet, json và csv có header + INPUT_SCHEMA" in {
    val session = spark
    import session.implicits._
    val df = Seq((1L, "84903217465", "Ha Noi"), (2L, "84903217466", "Da Nang")).toDF("id", "isdn", "province")
    val parquet = workDir.resolve("in_parquet").toString
    val json = workDir.resolve("in_json").toString
    df.write.parquet(parquet)
    df.write.json(json)
    val csv = Files.createDirectory(workDir.resolve("in_csv"))
    Files.write(csv.resolve("a.csv"), "id,isdn,province\n1,84903217465,Ha Noi\n".getBytes(UTF_8))

    runFile("INPUT_PATH" -> parquet, "INPUT_FORMAT" -> "parquet", "PSEUDO_COLUMNS" -> "isdn", "OUTPUT_TABLE" -> "in_p") shouldBe 0
    runFile("INPUT_PATH" -> json, "INPUT_FORMAT" -> "json", "PSEUDO_COLUMNS" -> "isdn", "OUTPUT_TABLE" -> "in_j") shouldBe 0
    runFile("INPUT_PATH" -> csv.toString, "INPUT_FORMAT" -> "csv", "INPUT_OPTION_header" -> "true",
      "INPUT_SCHEMA" -> "id BIGINT, isdn STRING, province STRING", "PSEUDO_COLUMNS" -> "isdn", "OUTPUT_TABLE" -> "in_c") shouldBe 0

    Seq("in_p", "in_j", "in_c").foreach { t =>
      spark.table(s"landing.$t").select("isdn").collect().map(_.getString(0)).foreach(_ should startWith("k1_"))
    }
  }

  it should "exit 1 khi INPUT_PATH không tồn tại hoặc cột không có trong file" in {
    runFile("INPUT_PATH" -> workDir.resolve("khong_co").toString, "INPUT_FORMAT" -> "parquet", "PSEUDO_COLUMNS" -> "isdn",
      "OUTPUT_TABLE" -> "f1") shouldBe PseudonymizeTableApp.ExitRuntimeError
    runFile("INPUT_PATH" -> workDir.resolve("in_parquet").toString, "INPUT_FORMAT" -> "parquet", "PSEUDO_COLUMNS" -> "cot_la",
      "OUTPUT_TABLE" -> "f2") shouldBe PseudonymizeTableApp.ExitRuntimeError
  }

  /** Thư mục có 2 file tốt và 1 file có dòng 25 000 trường (vượt maxColumns=20480 của parser CSV). */
  private def folderWithBadFile(name: String) = {
    val dir = Files.createDirectory(workDir.resolve(name))
    Files.write(dir.resolve("good-1.txt"), "84903217465|a|10\n84903217466|b|20\n".getBytes(UTF_8))
    Files.write(dir.resolve("good-2.txt"), "84903217467|c|30\n".getBytes(UTF_8))
    Files.write(dir.resolve("bad.txt"), (Seq.fill(25000)("x").mkString("|") + "\n").getBytes(UTF_8))
    dir
  }

  private def badFileEnv(dir: java.nio.file.Path, table: String, policy: String) = Seq("INPUT_PATH" -> dir.toString,
    "INPUT_FORMAT" -> "txt", "INPUT_OPTION_delimiter" -> "|", "PSEUDO_COLUMNS" -> "_c0", "OUTPUT_TABLE" -> table,
    "INPUT_BAD_FILES" -> policy)

  it should "INPUT_BAD_FILES=fail (mặc định): job exit 1 và không tạo bảng đích" in {
    val dir = folderWithBadFile("bad_fail")

    runFile(badFileEnv(dir, "bad_fail", "fail"): _*) shouldBe PseudonymizeTableApp.ExitRuntimeError
    spark.catalog.tableExists("landing", "bad_fail") shouldBe false
  }

  it should "INPUT_BAD_FILES=skip: xử lý file tốt, bỏ file lỗi nhưng job vẫn THÀNH CÔNG (exit 0)" in {
    val dir = folderWithBadFile("bad_skip")

    runFile(badFileEnv(dir, "bad_skip", "skip"): _*) shouldBe PseudonymizeTableApp.ExitOk

    spark.table("landing.bad_skip").count() shouldBe 3L
  }

  it should "INPUT_BAD_FILES=report: chỉ quét (exit 4 nếu có file lỗi, exit 0 nếu sạch), không ghi bảng" in {
    runFile(badFileEnv(folderWithBadFile("bad_report"), "bad_report", "report"): _*) shouldBe PseudonymizeTableApp.ExitBadFiles
    spark.catalog.tableExists("landing", "bad_report") shouldBe false

    val clean = Files.createDirectory(workDir.resolve("clean_report"))
    Files.write(clean.resolve("a.txt"), "84903217465|a|10\n".getBytes(UTF_8))
    runFile(badFileEnv(clean, "clean_report", "report"): _*) shouldBe PseudonymizeTableApp.ExitOk
  }

  it should "INPUT_BAD_FILES=skip không quét trước: vẫn bỏ file lỗi, ghi file tốt và liệt kê đúng file bị bỏ" in {
    val dir = folderWithBadFile("bad_skip_list")
    val env = badFileEnv(dir, "bad_skip_list", "skip").toMap ++ Map("OUTPUT_DB" -> "landing", "ENABLE_HIVE_SUPPORT" -> "false")
    val cfg = vai.lakehouse.pseudonymizetable.config.JobConfig.fromEnv(env).toOption.get

    val result = vai.lakehouse.pseudonymizetable.job.PseudonymizeTableJob.run(spark, cfg)

    result.badFiles.map(_.path.split("/").last) shouldBe Seq("bad.txt")
    result.badFiles.head.reason should include("maxColumns")
    spark.table("landing.bad_skip_list").count() shouldBe 3L
  }

  it should "INPUT_CHECK_FULL chỉ đổi mức quét của chế độ report" in {
    val dir = folderWithBadFile("bad_report_full")

    runFile(badFileEnv(dir, "bad_report_full", "report") :+ ("INPUT_CHECK_FULL" -> "true"): _*) shouldBe
      PseudonymizeTableApp.ExitBadFiles
    spark.catalog.tableExists("landing", "bad_report_full") shouldBe false
  }

  // ---- khoảng ngày/giờ + partition đầu ra ----

  /** base/date=D/hour=H/part.txt, mỗi file 2 dòng "isdn|<D><H>|<n>" (isdn cố định theo ngày/giờ, 2 dòng giống nhau). */
  private def partitionedTree(name: String, cells: Seq[(String, String)], payload: String = "v1"): java.nio.file.Path = {
    val base = Files.createDirectory(workDir.resolve(name))
    cells.foreach { case (d, h) =>
      val dir = Files.createDirectories(base.resolve(s"date=$d").resolve(s"hour=$h"))
      Files.write(dir.resolve("part-0.txt"), s"9$d$h|$payload|1\n9$d$h|$payload|2\n".getBytes(UTF_8))
    }
    base
  }

  private def rangeEnv(base: java.nio.file.Path, table: String, extra: (String, String)*) =
    Seq("INPUT_PATH" -> base.toString, "INPUT_FORMAT" -> "txt", "INPUT_OPTION_delimiter" -> "|",
      "PSEUDO_COLUMNS" -> "_c0", "OUTPUT_TABLE" -> table) ++ extra

  private def partitionsOf(table: String): Set[String] =
    spark.table(s"landing.$table").select("date", "hour").distinct().collect().map(r => s"${r.getString(0)}/${r.getString(1)}").toSet

  private val cells = Seq("20261012" -> "05", "20261012" -> "18", "20261013" -> "05", "20261013" -> "18", "20261014" -> "18")

  it should "INPUT_FROM/INPUT_TO (yyyyMMdd): xử lý nguyên các ngày trong khoảng, ghi bảng partition date/hour" in {
    val base = partitionedTree("tree_days", cells)

    runFile(rangeEnv(base, "range_days", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261013"): _*) shouldBe 0

    partitionsOf("range_days") shouldBe Set("20261012/05", "20261012/18", "20261013/05", "20261013/18")
    spark.table("landing.range_days").count() shouldBe 8L
    // partition vật lý trên đĩa: date=…/hour=… (giữ nguyên "05")
    val files = spark.table("landing.range_days").inputFiles.mkString("\n")
    files should include("date=20261012/hour=05")
    files should include("date=20261013/hour=18")
  }

  it should "INPUT_FROM/INPUT_TO có giờ: khoảng liên tục từ ngày-giờ đến ngày-giờ" in {
    val base = partitionedTree("tree_cont", cells)

    runFile(rangeEnv(base, "range_cont", "INPUT_FROM" -> "2026101218", "INPUT_TO" -> "2026101305"): _*) shouldBe 0

    partitionsOf("range_cont") shouldBe Set("20261012/18", "20261013/05")
  }

  it should "INPUT_HOURS: chỉ lấy các giờ trong ngày" in {
    val base = partitionedTree("tree_hours", cells)

    runFile(rangeEnv(base, "range_hours", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261014", "INPUT_HOURS" -> "0-6"): _*) shouldBe 0

    partitionsOf("range_hours") shouldBe Set("20261012/05", "20261013/05")
  }

  it should "overwrite-partitions (mặc định khi có partition): chạy lại / chạy ngày khác không xoá partition còn lại" in {
    val base = partitionedTree("tree_op", cells)
    val env = (to: String) => rangeEnv(base, "range_op", "INPUT_FROM" -> "20261012", "INPUT_TO" -> to)

    runFile(env("20261012"): _*) shouldBe 0
    partitionsOf("range_op") shouldBe Set("20261012/05", "20261012/18")

    runFile(rangeEnv(base, "range_op", "INPUT_FROM" -> "20261013", "INPUT_TO" -> "20261013"): _*) shouldBe 0
    partitionsOf("range_op") shouldBe Set("20261012/05", "20261012/18", "20261013/05", "20261013/18")
    spark.table("landing.range_op").count() shouldBe 8L

    // dữ liệu ngày 12 đổi -> chạy lại ngày 12 thay đúng partition của ngày 12, ngày 13 giữ nguyên, không nhân đôi dòng
    Files.write(base.resolve("date=20261012/hour=05/part-0.txt"), "9new|v2|1\n".getBytes(UTF_8))
    runFile(rangeEnv(base, "range_op", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012"): _*) shouldBe 0

    spark.table("landing.range_op").count() shouldBe 7L // 8 - 2 (hour=05 cũ của ngày 12) + 1 (dòng mới)
    partitionsOf("range_op") should contain allOf ("20261012/05", "20261013/05", "20261013/18")
  }

  it should "OUTPUT_MODE=overwrite vẫn thay toàn bộ bảng; OUTPUT_PARTITION_BY=none tắt partition" in {
    val base = partitionedTree("tree_full", cells)
    runFile(rangeEnv(base, "full_tbl", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012"): _*) shouldBe 0
    runFile(rangeEnv(base, "full_tbl", "INPUT_FROM" -> "20261013", "INPUT_TO" -> "20261013", "OUTPUT_MODE" -> "overwrite"): _*) shouldBe 0
    partitionsOf("full_tbl") shouldBe Set("20261013/05", "20261013/18")

    runFile(rangeEnv(base, "flat_tbl", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012", "OUTPUT_PARTITION_BY" -> "none"): _*) shouldBe 0
    spark.table("landing.flat_tbl").inputFiles.mkString should not include "date=2026"
  }

  it should "báo lỗi (exit 1) khi không có thư mục nào trong khoảng, hoặc partition đã có khác cấu hình" in {
    val base = partitionedTree("tree_none", cells)

    runFile(rangeEnv(base, "none_tbl", "INPUT_FROM" -> "20270101", "INPUT_TO" -> "20270102"): _*) shouldBe
      PseudonymizeTableApp.ExitRuntimeError

    runFile(rangeEnv(base, "mismatch_tbl", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012", "OUTPUT_PARTITION_BY" -> "date"): _*) shouldBe 0
    runFile(rangeEnv(base, "mismatch_tbl", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012"): _*) shouldBe
      PseudonymizeTableApp.ExitRuntimeError
  }

  // ---- 20-minute batch folders: <base>/dt=<yyyyMMdd>/batch=<HHMM>/ ----

  private def batchTree(name: String, cells: Seq[(String, String)]): java.nio.file.Path = {
    val base = Files.createDirectory(workDir.resolve(name))
    cells.foreach { case (d, b) =>
      val dir = Files.createDirectories(base.resolve(s"dt=$d").resolve(s"batch=$b"))
      Files.write(dir.resolve("part-0.csv"), s"imsi$d$b,1\nimsi$d$b,2\n".getBytes(UTF_8))
    }
    base
  }

  private def batchEnv(base: java.nio.file.Path, table: String, extra: (String, String)*) =
    Seq("INPUT_PATH" -> base.toString, "INPUT_FORMAT" -> "csv", "INPUT_DATE_KEY" -> "dt", "INPUT_HOUR_KEY" -> "batch",
      "PSEUDO_COLUMNS" -> "_c0", "OUTPUT_TABLE" -> table) ++ extra

  private def batchesOf(table: String): Set[String] =
    spark.table(s"landing.$table").select("dt", "batch").distinct().collect().map(r => s"${r.getString(0)}/${r.getString(1)}").toSet

  private val batchCells = Seq("20261014" -> "0000", "20261014" -> "0020", "20261014" -> "0100", "20261014" -> "0120",
    "20261014" -> "1740", "20261015" -> "0000")

  it should "read 20-minute batch folders by minute-resolution range" in {
    val base = batchTree("batch_minutes", batchCells)

    runFile(batchEnv(base, "batch_range", "INPUT_FROM" -> "202610140020", "INPUT_TO" -> "202610140100"): _*) shouldBe 0

    batchesOf("batch_range") shouldBe Set("20261014/0020", "20261014/0100")
    spark.table("landing.batch_range").inputFiles.mkString should include("dt=20261014/batch=0020")
  }

  it should "treat an hour-only FROM/TO as the whole hour, and INPUT_HOURS as a daily hour window" in {
    val base = batchTree("batch_hours", batchCells)

    runFile(batchEnv(base, "batch_hour", "INPUT_FROM" -> "2026101401", "INPUT_TO" -> "2026101401"): _*) shouldBe 0
    batchesOf("batch_hour") shouldBe Set("20261014/0100", "20261014/0120")

    runFile(batchEnv(base, "batch_window", "INPUT_FROM" -> "20261014", "INPUT_TO" -> "20261015", "INPUT_HOURS" -> "0-0"): _*) shouldBe 0
    batchesOf("batch_window") shouldBe Set("20261014/0000", "20261014/0020", "20261015/0000")
  }

  it should "read a whole day of batches and write one partition per batch with per-partition progress" in {
    val base = batchTree("batch_day", batchCells)

    runFile(batchEnv(base, "batch_day_tbl", "INPUT_FROM" -> "20261014", "INPUT_TO" -> "20261014"): _*) shouldBe 0

    batchesOf("batch_day_tbl") shouldBe Set("20261014/0000", "20261014/0020", "20261014/0100", "20261014/0120", "20261014/1740")
  }

  // ---- resume (OUTPUT_SKIP_EXISTING) ----

  it should "OUTPUT_SKIP_EXISTING: a rerun leaves already-written partitions untouched and writes only the missing ones" in {
    val base = partitionedTree("tree_resume", Seq("20261012" -> "05", "20261012" -> "18"))
    val env = rangeEnv(base, "resume_tbl", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012", "OUTPUT_SKIP_EXISTING" -> "true")
    def payloads(hour: String) = spark.table("landing.resume_tbl").where(s"hour = '$hour'").select("_c1").distinct()
      .collect().map(_.getString(0)).toSet

    runFile(env: _*) shouldBe 0
    partitionsOf("resume_tbl") shouldBe Set("20261012/05", "20261012/18")

    // simulate a restart after a partial run: the source changed for hour 05, and hour 19 is new
    Files.write(base.resolve("date=20261012/hour=05/part-0.txt"), "9new|CHANGED|1\n".getBytes(UTF_8))
    val newDir = Files.createDirectories(base.resolve("date=20261012").resolve("hour=19"))
    Files.write(newDir.resolve("part-0.txt"), "919|v1|1\n".getBytes(UTF_8))

    runFile(env: _*) shouldBe 0

    partitionsOf("resume_tbl") shouldBe Set("20261012/05", "20261012/18", "20261012/19")
    payloads("05") shouldBe Set("v1")        // skipped: the earlier data is kept, not replaced by CHANGED
    payloads("19") shouldBe Set("v1")        // written by the rerun
    // without the flag the changed partition is rewritten
    runFile(rangeEnv(base, "resume_tbl", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012"): _*) shouldBe 0
    payloads("05") shouldBe Set("CHANGED")
  }
}
