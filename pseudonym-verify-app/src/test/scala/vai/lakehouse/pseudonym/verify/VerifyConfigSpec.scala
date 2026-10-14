package vai.lakehouse.pseudonym.verify

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.pseudonym.PseudonymMode

class VerifyConfigSpec extends AnyFlatSpec with Matchers {

  private val base = Map("RUN_ID" -> "pv-1", "OUTPUT_PATH" -> "hdfs://nn:8020/tmp/pv")

  "VerifyConfig.fromEnv" should "dùng mặc định của plan: full, HASH+ENCRYPT (MAPPING tắt), khoá ephemeral" in {
    val cfg = VerifyConfig.fromEnv(base).toOption.get

    cfg.runMode shouldBe RunMode.Full
    cfg.pseudoModes shouldBe Seq(PseudonymMode.Hash, PseudonymMode.Encrypt)
    cfg.keySource shouldBe "ephemeral"
    cfg.nSubjects shouldBe 1000000L
    cfg.runPath shouldBe "hdfs://nn:8020/tmp/pv/pv-1"
    cfg.mappingTablePath shouldBe "hdfs://nn:8020/tmp/pv/pv-1/_restricted/pseudonym_map"
    cfg.writeOutput shouldBe true
  }

  it should "bật MAPPING qua PSEUDO_MODES" in {
    VerifyConfig.fromEnv(base + ("PSEUDO_MODES" -> "hash, encrypt,MAPPING")).toOption.get.pseudoModes shouldBe PseudonymMode.All
  }

  it should "gom MỌI lỗi cấu hình trong 1 lượt" in {
    val errors = VerifyConfig.fromEnv(Map("RUN_MODE" -> "fast", "N_SUBJECTS" -> "0", "NULL_RATE" -> "2",
      "PSEUDO_MODES" -> "SHA2", "KEY_SOURCE" -> "aws")).left.toOption.get

    errors.exists(_.startsWith("RUN_MODE")) shouldBe true
    errors.exists(_.startsWith("RUN_ID")) shouldBe true
    errors.exists(_.startsWith("OUTPUT_PATH")) shouldBe true
    errors.exists(_.startsWith("N_SUBJECTS")) shouldBe true
    errors.exists(_.startsWith("NULL_RATE")) shouldBe true
    errors.exists(_.startsWith("PSEUDO_MODES")) shouldBe true
    errors.exists(_.startsWith("KEY_SOURCE")) shouldBe true
  }

  it should "từ chối bảng ánh xạ nằm dưới thư mục output (phải lưu riêng — mục c)" in {
    val errors = VerifyConfig.fromEnv(base + ("MAPPING_TABLE_PATH" -> "hdfs://nn:8020/tmp/pv/pv-1/output/map")).left.toOption.get
    errors.head should include("lưu RIÊNG")
  }

  it should "bắt buộc INPUT_TABLE, GUARD_TABLE và khoá thật cho RUN_MODE=guard" in {
    val errors = VerifyConfig.fromEnv(base + ("RUN_MODE" -> "guard")).left.toOption.get
    errors.exists(_.startsWith("INPUT_TABLE")) shouldBe true
    errors.exists(_.startsWith("GUARD_TABLE")) shouldBe true
    errors.exists(_.startsWith("KEY_SOURCE")) shouldBe true
  }

  it should "từ chối OUTPUT_PATH không phải URI tuyệt đối và RUN_ID có ký tự lạ" in {
    val errors = VerifyConfig.fromEnv(Map("RUN_ID" -> "a/b", "OUTPUT_PATH" -> "relative/path")).left.toOption.get
    errors.exists(_.startsWith("RUN_ID")) shouldBe true
    errors.exists(_.startsWith("OUTPUT_PATH")) shouldBe true
  }
}
