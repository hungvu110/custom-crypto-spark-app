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
      include("URI tuyệt đối")
  }

  it should "từ chối ghi đè bảng nguồn (bảng đích trùng nguồn)" in {
    val errors = JobConfig.fromEnv(base ++ Map("OUTPUT_TABLE" -> "subscriber")).left.toOption.get

    errors.head should include("không được trùng bảng nguồn")
  }

  it should "từ chối tên không hợp lệ (chống chèn SQL qua tên DB/bảng/cột) và cột lặp" in {
    val errors = JobConfig.fromEnv(Map("DB_NAME" -> "crm; DROP DATABASE x", "TABLE_NAME" -> "t",
      "PSEUDO_COLUMNS" -> "isdn, isdn, a b")).left.toOption.get

    errors.exists(_.startsWith("DB_NAME")) shouldBe true
    errors.exists(_.contains("tên cột không hợp lệ 'a b'")) shouldBe true
    errors.exists(_.contains("cột bị lặp isdn")) shouldBe true
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

    JobConfig.fromEnv(file + ("DB_NAME" -> "crm")).left.toOption.get.head should include("chỉ chọn một nguồn")
    JobConfig.fromEnv(base + ("INPUT_FORMAT" -> "csv")).left.toOption.get.head should include("chỉ dùng cùng INPUT_PATH")
    JobConfig.fromEnv(file + ("INPUT_FORMAT" -> "xls")).left.toOption.get.head should include("INPUT_FORMAT='xls'")
    JobConfig.fromEnv(file + ("INPUT_PATH" -> "landing/x")).left.toOption.get.head should include("tuyệt đối")
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

    errors.exists(_.contains("chế độ không hợp lệ")) shouldBe true
    errors.exists(_.contains("cột:MODE")) shouldBe true
  }
}
