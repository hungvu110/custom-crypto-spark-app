package vai.lakehouse.pseudonymizetable.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.pseudonym.PseudonymMode

class JobConfigSpec extends AnyFlatSpec with Matchers {

  private val base = Map("DB_NAME" -> "crm", "TABLE_NAME" -> "subscriber", "PSEUDO_COLUMNS" -> "isdn, imei")

  "JobConfig.fromEnv" should "dùng mặc định: HASH, bảng đích <table>_pseudonymized cùng DB, delta, overwrite, verify" in {
    val cfg = JobConfig.fromEnv(base).toOption.get

    cfg.columns shouldBe Seq("isdn", "imei")
    cfg.mode shouldBe PseudonymMode.Hash
    cfg.source shouldBe "crm.subscriber"
    cfg.target shouldBe "crm.subscriber_pseudonymized"
    cfg.outputFormat shouldBe OutputFormat.Delta
    cfg.outputMode shouldBe "overwrite"
    cfg.enableHiveSupport shouldBe true
    cfg.verify shouldBe true
  }

  it should "nhận cấu hình tuỳ chỉnh" in {
    val cfg = JobConfig.fromEnv(base ++ Map("PSEUDO_MODE" -> "encrypt", "OUTPUT_DB" -> "analytics",
      "OUTPUT_TABLE" -> "subscriber_safe", "OUTPUT_FORMAT" -> "hive", "OUTPUT_MODE" -> "append",
      "ENABLE_HIVE_SUPPORT" -> "false", "VERIFY" -> "false", "MAPPING_TABLE_PATH" -> "hdfs://x/map")).toOption.get

    cfg.mode shouldBe PseudonymMode.Encrypt
    cfg.target shouldBe "analytics.subscriber_safe"
    cfg.outputFormat.sparkFormat shouldBe "parquet"
    cfg.outputMode shouldBe "append"
    cfg.enableHiveSupport shouldBe false
    cfg.verify shouldBe false
    cfg.mappingTablePath shouldBe Some("hdfs://x/map")
  }

  it should "bắt buộc DB_NAME, TABLE_NAME, PSEUDO_COLUMNS và gom mọi lỗi 1 lượt" in {
    val errors = JobConfig.fromEnv(Map("PSEUDO_MODE" -> "SHA2", "OUTPUT_MODE" -> "merge")).left.toOption.get

    errors.exists(_.startsWith("DB_NAME")) shouldBe true
    errors.exists(_.startsWith("TABLE_NAME")) shouldBe true
    errors.exists(_.startsWith("PSEUDO_COLUMNS")) shouldBe true
    errors.exists(_.startsWith("PSEUDO_MODE")) shouldBe true
    errors.exists(_.startsWith("OUTPUT_MODE")) shouldBe true
  }

  it should "nhận WAREHOUSE_PATH là URI đầy đủ và từ chối đường dẫn tương đối" in {
    val ok = JobConfig.fromEnv(base + ("WAREHOUSE_PATH" -> "hdfs://nn:8020/prod8/warehouse")).toOption.get
    ok.warehousePath shouldBe Some("hdfs://nn:8020/prod8/warehouse")
    JobConfig.fromEnv(base).toOption.get.warehousePath shouldBe None

    JobConfig.fromEnv(base + ("WAREHOUSE_PATH" -> "prod8/warehouse")).left.toOption.get.head should
      include("absolute URI")
  }

  it should "từ chối ghi đè bảng nguồn (bảng đích trùng nguồn)" in {
    val errors = JobConfig.fromEnv(base ++ Map("OUTPUT_TABLE" -> "subscriber")).left.toOption.get

    errors.head should include("must not equal the source table")
  }

  it should "từ chối tên không hợp lệ (chống chèn SQL qua tên DB/bảng/cột) và cột lặp" in {
    val errors = JobConfig.fromEnv(Map("DB_NAME" -> "crm; DROP DATABASE x", "TABLE_NAME" -> "t",
      "PSEUDO_COLUMNS" -> "isdn, isdn, a b")).left.toOption.get

    errors.exists(_.startsWith("DB_NAME")) shouldBe true
    errors.exists(_.contains("invalid column name 'a b'")) shouldBe true
    errors.exists(_.contains("duplicated column(s) isdn")) shouldBe true
  }

  private val file = Map("INPUT_PATH" -> "/landing/vtt/charge/date=20261012/hour=18/", "INPUT_FORMAT" -> "csv",
    "PSEUDO_COLUMNS" -> "_c0", "OUTPUT_DB" -> "landing", "OUTPUT_TABLE" -> "charge_safe")

  it should "nhận nguồn file với định dạng, schema và option" in {
    val cfg = JobConfig.fromEnv(file ++ Map("INPUT_FORMAT" -> "txt", "INPUT_SCHEMA" -> "a STRING, b STRING",
      "INPUT_OPTION_delimiter" -> "|", "INPUT_OPTION_header" -> "true")).toOption.get

    cfg.input shouldBe FileInput("/landing/vtt/charge/date=20261012/hour=18/", InputFormat.Csv,
      Some("a STRING, b STRING"), Map("delimiter" -> "|", "header" -> "true"))
    cfg.target shouldBe "landing.charge_safe"
  }

  it should "bắt buộc OUTPUT_DB/OUTPUT_TABLE và INPUT_FORMAT với nguồn file, không trộn với nguồn bảng" in {
    val errors = JobConfig.fromEnv(Map("INPUT_PATH" -> "/x", "PSEUDO_COLUMNS" -> "a")).left.toOption.get
    errors.exists(_.startsWith("INPUT_FORMAT")) shouldBe true
    errors.exists(_.startsWith("OUTPUT_DB")) shouldBe true
    errors.exists(_.startsWith("OUTPUT_TABLE")) shouldBe true

    JobConfig.fromEnv(file + ("DB_NAME" -> "crm")).left.toOption.get.head should include("choose exactly one source")
    JobConfig.fromEnv(base + ("INPUT_FORMAT" -> "csv")).left.toOption.get.head should include("only valid together with INPUT_PATH")
    JobConfig.fromEnv(file + ("INPUT_FORMAT" -> "xls")).left.toOption.get.head should include("INPUT_FORMAT='xls'")
    JobConfig.fromEnv(file + ("INPUT_PATH" -> "landing/x")).left.toOption.get.head should include("absolute path")
  }

  it should "cho mỗi cột một chế độ riêng, cột không ghi mode dùng PSEUDO_MODE" in {
    val cfg = JobConfig.fromEnv(base ++ Map("PSEUDO_COLUMNS" -> "isdn:hash, imei:ENCRYPT, cust",
      "PSEUDO_MODE" -> "MAPPING")).toOption.get

    cfg.pseudoColumns shouldBe Seq(PseudoColumn("isdn", PseudonymMode.Hash), PseudoColumn("imei", PseudonymMode.Encrypt),
      PseudoColumn("cust", PseudonymMode.Mapping))
    cfg.columns shouldBe Seq("isdn", "imei", "cust")
    cfg.modes shouldBe Set(PseudonymMode.Hash, PseudonymMode.Encrypt, PseudonymMode.Mapping)
  }

  it should "từ chối mode sai hoặc cú pháp cột sai" in {
    val errors = JobConfig.fromEnv(base + ("PSEUDO_COLUMNS" -> "isdn:ROT13, imei:HASH:x")).left.toOption.get

    errors.exists(_.contains("invalid mode")) shouldBe true
    errors.exists(_.contains("column:MODE")) shouldBe true
  }

  it should "nhận INPUT_BAD_FILES chỉ với nguồn file" in {
    val cfg = JobConfig.fromEnv(file ++ Map("INPUT_BAD_FILES" -> "skip", "INPUT_CHECK_PARALLELISM" -> "8")).toOption.get
    cfg.badFiles shouldBe BadFilePolicy.Skip
    cfg.checkParallelism shouldBe 8
    JobConfig.fromEnv(file).toOption.get.badFiles shouldBe BadFilePolicy.Fail

    JobConfig.fromEnv(base + ("INPUT_BAD_FILES" -> "skip")).left.toOption.get.head should include("only valid together with INPUT_PATH")
    JobConfig.fromEnv(file + ("INPUT_BAD_FILES" -> "ignore")).left.toOption.get.head should include("INPUT_BAD_FILES='ignore'")
  }

  it should "INPUT_CHECK_FULL mặc định false và chỉ dùng với nguồn file" in {
    JobConfig.fromEnv(file).toOption.get.checkFull shouldBe false
    JobConfig.fromEnv(file + ("INPUT_CHECK_FULL" -> "true")).toOption.get.checkFull shouldBe true
    JobConfig.fromEnv(base + ("INPUT_CHECK_FULL" -> "true")).left.toOption.get.head should include("only valid together with INPUT_PATH")
  }

  it should "nhận khoảng ngày/giờ và mặc định partition (date, hour) + overwrite-partitions" in {
    val cfg = JobConfig.fromEnv(file ++ Map("INPUT_FROM" -> "20261012", "INPUT_TO" -> "2026101405", "INPUT_HOURS" -> "6-18")).toOption.get
    val range = cfg.input.asInstanceOf[FileInput].range.get

    range.from shouldBe 202610120000L
    range.to shouldBe 202610140559L
    range.hours shouldBe Some((6, 18))
    cfg.outputPartitionBy shouldBe Seq("date", "hour")
    cfg.outputMode shouldBe "overwrite-partitions"
    JobConfig.fromEnv(file ++ Map("INPUT_FROM" -> "20261012", "OUTPUT_PARTITION_BY" -> "none")).toOption.get.outputMode shouldBe "overwrite"
    JobConfig.fromEnv(file).toOption.get.outputPartitionBy shouldBe Nil
  }

  it should "từ chối khoảng ngày/giờ sai" in {
    def errors(extra: (String, String)*) = JobConfig.fromEnv(file ++ extra).left.toOption.get.mkString(" | ")

    errors("INPUT_FROM" -> "20261340") should include("invalid date")
    errors("INPUT_FROM" -> "2026101225") should include("hour must be 00..23")
    errors("INPUT_FROM" -> "20261014", "INPUT_TO" -> "20261012") should include("must be <=")
    errors("INPUT_HOURS" -> "18-6") should include("INPUT_HOURS")
    errors("INPUT_FROM" -> "20261012", "OUTPUT_PARTITION_BY" -> "none", "OUTPUT_MODE" -> "overwrite-partitions") should
      include("requires OUTPUT_PARTITION_BY")
    JobConfig.fromEnv(base + ("INPUT_DATE_KEY" -> "dt")).left.toOption.get.mkString should include("only valid together with INPUT_PATH")
  }

  it should "accept a date/hour range on a table source and default to partition (date, hour) + overwrite-partitions" in {
    val cfg = JobConfig.fromEnv(base ++ Map("INPUT_FROM" -> "20261012", "INPUT_TO" -> "2026101405", "INPUT_HOURS" -> "6-18")).toOption.get
    val range = cfg.input.asInstanceOf[TableInput].range.get

    cfg.source shouldBe "crm.subscriber"
    range.from shouldBe 202610120000L
    range.to shouldBe 202610140559L
    range.hours shouldBe Some((6, 18))
    range.dateKey shouldBe "date"
    range.hourKey shouldBe "hour"
    cfg.outputPartitionBy shouldBe Seq("date", "hour")
    cfg.outputMode shouldBe "overwrite-partitions"
  }

  it should "keep overwrite as the default for a table without a range, and honour an explicit OUTPUT_MODE with a range" in {
    val plain = JobConfig.fromEnv(base).toOption.get
    plain.input.asInstanceOf[TableInput].range shouldBe None
    plain.outputPartitionBy shouldBe Nil
    plain.outputMode shouldBe "overwrite"
    JobConfig.fromEnv(base ++ Map("INPUT_HOURS" -> "18", "OUTPUT_MODE" -> "overwrite")).toOption.get.outputMode shouldBe "overwrite"
    JobConfig.fromEnv(base ++ Map("INPUT_HOURS" -> "18", "OUTPUT_MODE" -> "append")).toOption.get.outputMode shouldBe "append"
  }

  it should "reject an invalid range, minute resolution and a custom partition key on a table source" in {
    def errors(extra: (String, String)*) = JobConfig.fromEnv(base ++ extra).left.toOption.get.mkString(" | ")

    errors("INPUT_FROM" -> "20261340") should include("invalid date")
    errors("INPUT_FROM" -> "20261014", "INPUT_TO" -> "20261012") should include("must be <=")
    errors("INPUT_HOURS" -> "18-6") should include("INPUT_HOURS")
    errors("INPUT_FROM" -> "202610140020") should include("INPUT_FROM: minutes (yyyyMMddHHmm) are only valid together with INPUT_PATH")
    errors("INPUT_TO" -> "202610140020") should include("INPUT_TO: minutes (yyyyMMddHHmm) are only valid together with INPUT_PATH")
    errors("INPUT_FROM" -> "20261012", "INPUT_HOUR_KEY" -> "h") should include("only valid together with INPUT_PATH")
    errors("OUTPUT_MODE" -> "overwrite-partitions") should include("requires OUTPUT_PARTITION_BY")
    errors("INPUT_FROM" -> "20261012", "PSEUDO_COLUMNS" -> "date") should include("must not be listed in PSEUDO_COLUMNS")
  }

  it should "accept minute-resolution FROM/TO (yyyyMMddHHmm) and reject a bad minute" in {
    val range = JobConfig.fromEnv(file ++ Map("INPUT_FROM" -> "202610140020", "INPUT_TO" -> "202610140100",
      "INPUT_DATE_KEY" -> "dt", "INPUT_HOUR_KEY" -> "batch")).toOption.get.input.asInstanceOf[FileInput].range.get

    range.from shouldBe 202610140020L
    range.to shouldBe 202610140100L
    range.dateKey shouldBe "dt"
    range.hourKey shouldBe "batch"
    range.accepts("20261014", Some(20)) shouldBe true
    range.accepts("20261014", Some(120)) shouldBe false
    JobConfig.fromEnv(file + ("INPUT_FROM" -> "202610140075")).left.toOption.get.mkString should include("minute must be 00..59")
  }

  it should "default to 2 write retries for overwrite-partitions and none otherwise" in {
    val ranged = file ++ Map("INPUT_FROM" -> "20261012")

    JobConfig.fromEnv(ranged).toOption.get.writeRetries shouldBe 2
    JobConfig.fromEnv(ranged).toOption.get.retryWaitSeconds shouldBe 30
    JobConfig.fromEnv(file).toOption.get.writeRetries shouldBe 0
    val tuned = JobConfig.fromEnv(ranged ++ Map("OUTPUT_WRITE_RETRIES" -> "4", "OUTPUT_RETRY_WAIT_SECONDS" -> "5",
      "OUTPUT_SKIP_EXISTING" -> "true")).toOption.get
    tuned.writeRetries shouldBe 4
    tuned.retryWaitSeconds shouldBe 5
    tuned.skipExisting shouldBe true
  }

  it should "reject unsafe or invalid resume/retry settings" in {
    val ranged = file ++ Map("INPUT_FROM" -> "20261012")
    def errors(extra: (String, String)*) = JobConfig.fromEnv(ranged ++ extra).left.toOption.get.mkString(" | ")

    errors("OUTPUT_MODE" -> "append", "OUTPUT_WRITE_RETRIES" -> "2") should include("only safe with OUTPUT_MODE=overwrite-partitions")
    errors("OUTPUT_WRITE_RETRIES" -> "9") should include("integer in 0..5")
    errors("OUTPUT_RETRY_WAIT_SECONDS" -> "-1") should include("integer in 0..600")
    JobConfig.fromEnv(file + ("OUTPUT_SKIP_EXISTING" -> "true")).left.toOption.get.mkString should
      include("OUTPUT_SKIP_EXISTING: requires")
    JobConfig.fromEnv(ranged ++ Map("OUTPUT_MODE" -> "append", "OUTPUT_SKIP_EXISTING" -> "true")).isRight shouldBe true
  }
}
