package vai.lakehouse.silveringest.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BulkLoadSpec extends AnyFlatSpec with Matchers {

  private val base = Map("INPUT_PATH" -> "hdfs://nn:8020/landing/vtt/f001-mpre-call/date=%DAY%", "OUTPUT_DB" -> "silver",
    "OUTPUT_TABLE" -> "vtt_f001")

  private val range = base ++ Map("BULK_FROM" -> "20261104", "BULK_TO" -> "20261106")

  private def errors(env: Map[String, String]): String = BulkLoad.fromEnv(env).left.toOption.getOrElse(fail("expected errors")).mkString(" | ")

  "BulkLoad.fromEnv" should "không bật bulk khi không có biến BULK_*: một ngày như cũ" in {
    BulkLoad.fromEnv(base) shouldBe Right(None)
    BulkLoad.fromEnv(Map.empty) shouldBe Right(None)
    BulkLoad.fromEnv(base + ("BULK_FROM" -> "  ")) shouldBe Right(None) // an empty value is "not set", like every other env of the job
  }

  it should "liệt kê các ngày từ BULK_FROM đến BULK_TO, gồm cả hai đầu, theo thứ tự" in {
    val bulk = BulkLoad.fromEnv(range).toOption.get.get

    bulk.days shouldBe Seq("20261104", "20261105", "20261106")
    bulk.onMissingDay shouldBe MissingDay.Fail
  }

  it should "BULK_TO bỏ trống thì bằng BULK_FROM (một ngày), và đi qua cuối tháng, cuối năm đúng lịch" in {
    BulkLoad.fromEnv(base + ("BULK_FROM" -> "20261104")).toOption.get.get.days shouldBe Seq("20261104")
    BulkLoad.fromEnv(base ++ Map("BULK_FROM" -> "20261130", "BULK_TO" -> "20261202")).toOption.get.get.days shouldBe
      Seq("20261130", "20261201", "20261202")
    BulkLoad.fromEnv(base ++ Map("BULK_FROM" -> "20271231", "BULK_TO" -> "20280101")).toOption.get.get.days shouldBe Seq("20271231", "20280101")
    BulkLoad.fromEnv(base ++ Map("BULK_FROM" -> "20280228", "BULK_TO" -> "20280301")).toOption.get.get.days.size shouldBe 3 // 2028 is a leap year
  }

  it should "nhận BULK_MISSING_DAY=skip (không phân biệt hoa thường), mặc định fail" in {
    BulkLoad.fromEnv(range + ("BULK_MISSING_DAY" -> "SKIP")).toOption.get.get.onMissingDay shouldBe MissingDay.Skip
    BulkLoad.fromEnv(range + ("BULK_MISSING_DAY" -> "fail")).toOption.get.get.onMissingDay shouldBe MissingDay.Fail
    errors(range + ("BULK_MISSING_DAY" -> "ignore")) should include("BULK_MISSING_DAY='ignore'")
  }

  it should "nhận đúng 62 ngày và từ chối 63 ngày" in {
    BulkLoad.fromEnv(range ++ Map("BULK_FROM" -> "20261001", "BULK_TO" -> "20261201")).toOption.get.get.days.size shouldBe 62
    errors(range ++ Map("BULK_FROM" -> "20261001", "BULK_TO" -> "20261202")) should include("at most 62 days")
  }

  it should "từ chối ngày không thật, sai định dạng, BULK_FROM sau BULK_TO, BULK_TO thiếu BULK_FROM" in {
    for (bad <- Seq("2026-11-04", "20261304", "20260230", "2026110", "abcdefgh")) {
      errors(range + ("BULK_FROM" -> bad)) should include("BULK_FROM")
    }
    errors(range + ("BULK_TO" -> "20261231x")) should include("BULK_TO")
    errors(range ++ Map("BULK_FROM" -> "20261106", "BULK_TO" -> "20261104")) should include("BULK_FROM must not be after BULK_TO")
    errors(base + ("BULK_TO" -> "20261106")) should include("BULK_FROM: is required")
    errors(base + ("BULK_MISSING_DAY" -> "skip")) should include("BULK_FROM: is required")
  }

  it should "bắt buộc INPUT_PATH có %DAY% (chỗ của ngày), và không nhận BATCH_VALUE, BATCH_HOURS" in {
    errors(range + ("INPUT_PATH" -> "hdfs://nn:8020/landing/vtt/f001-mpre-call/date=20261104")) should include("INPUT_PATH: must contain %DAY%")
    errors(range - "INPUT_PATH") should include("INPUT_PATH: must contain %DAY%")
    errors(range + ("BATCH_VALUE" -> "20261104")) should include("BATCH_VALUE: not used with a bulk load")
    errors(range + ("BATCH_HOURS" -> "10")) should include("BATCH_HOURS: not used with a bulk load")
  }

  it should "chỉ ghi overwrite-partitions: ngày đã nạp (kể cả chạy thử) được thay, không bao giờ ghi trùng" in {
    BulkLoad.fromEnv(range + ("OUTPUT_MODE" -> "overwrite-partitions")).isRight shouldBe true
    BulkLoad.fromEnv(range + ("OUTPUT_MODE" -> "OVERWRITE-PARTITIONS")).isRight shouldBe true
    errors(range + ("OUTPUT_MODE" -> "append")) should include("OUTPUT_MODE: a bulk load needs overwrite-partitions")
    errors(range + ("OUTPUT_MODE" -> "overwrite")) should include("OUTPUT_MODE: a bulk load needs overwrite-partitions")
  }

  it should "gom mọi lỗi một lượt" in {
    val all = errors(Map("BULK_FROM" -> "bad", "BULK_TO" -> "worse", "INPUT_PATH" -> "/x", "OUTPUT_MODE" -> "append", "BATCH_VALUE" -> "1"))

    all should include("BULK_FROM")
    all should include("BULK_TO")
    all should include("INPUT_PATH")
    all should include("OUTPUT_MODE")
    all should include("BATCH_VALUE")
  }

  "BulkLoad.dayEnv" should "dựng env của MỘT ngày: INPUT_PATH của ngày đó, BATCH_VALUE là ngày, không còn biến BULK_*" in {
    val env = BulkLoad.dayEnv(range ++ Map("BULK_MISSING_DAY" -> "skip", "BATCH_COLUMN" -> "dt"), "20261105")

    env("INPUT_PATH") shouldBe "hdfs://nn:8020/landing/vtt/f001-mpre-call/date=20261105"
    env("BATCH_VALUE") shouldBe "20261105"
    env("BATCH_COLUMN") shouldBe "dt"
    env("OUTPUT_TABLE") shouldBe "vtt_f001"
    env.keys.filter(_.startsWith("BULK_")) shouldBe empty
  }

  it should "đặt OUTPUT_MODE=overwrite-partitions cho từng ngày (mặc định của job khi có BATCH_VALUE)" in {
    BulkLoad.dayEnv(range, "20261104").get("OUTPUT_MODE") shouldBe Some("overwrite-partitions")
  }
}
