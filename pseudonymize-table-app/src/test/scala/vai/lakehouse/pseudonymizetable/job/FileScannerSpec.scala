package vai.lakehouse.pseudonymizetable.job

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Giả lập lớp MetaException của Hive (chỉ cần tên lớp, không kéo thêm dependency vào test). */
class MetaException(message: String) extends RuntimeException(message)

class FileScannerSpec extends AnyFlatSpec with Matchers {

  "countOccurrences" should "đếm ký tự phân tách và phân tách nhiều ký tự không chồng lấn" in {
    FileScanner.countOccurrences("a|b||c", "|") shouldBe 3
    FileScanner.countOccurrences("", "|") shouldBe 0
    FileScanner.countOccurrences("a||b||c", "||") shouldBe 2
    FileScanner.countOccurrences("|||", "||") shouldBe 1
  }

  "isReadFailure" should "chỉ true cho lỗi đọc file, không cho lỗi metastore/ghi" in {
    val parse = new RuntimeException("Encountered error while reading file x.txt", new RuntimeException("boom"))
    val metastore = new RuntimeException("MetaException(message:Got exception: java.io.IOException ... Read timed out)",
      new MetaException("timeout"))
    val other = new IllegalStateException("Schema khác")

    PseudonymizeTableJob.isReadFailure(parse) shouldBe true
    PseudonymizeTableJob.isReadFailure(metastore) shouldBe false
    PseudonymizeTableJob.isReadFailure(other) shouldBe false
  }
}
