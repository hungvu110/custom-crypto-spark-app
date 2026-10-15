package vai.lakehouse.pseudonymizetable.job

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileScannerSpec extends AnyFlatSpec with Matchers {

  "countOccurrences" should "đếm ký tự phân tách và phân tách nhiều ký tự không chồng lấn" in {
    FileScanner.countOccurrences("a|b||c", "|") shouldBe 3
    FileScanner.countOccurrences("", "|") shouldBe 0
    FileScanner.countOccurrences("a||b||c", "||") shouldBe 2
    FileScanner.countOccurrences("|||", "||") shouldBe 1
  }
}
