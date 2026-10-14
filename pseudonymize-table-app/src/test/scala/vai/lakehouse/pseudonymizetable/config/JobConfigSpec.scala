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
}
