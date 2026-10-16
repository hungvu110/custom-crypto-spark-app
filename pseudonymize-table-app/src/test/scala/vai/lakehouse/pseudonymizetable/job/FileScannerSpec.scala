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

  "isTransient" should "retry flaky infrastructure errors but never configuration or read errors" in {
    val timeout = new RuntimeException("write failed", new java.net.SocketTimeoutException("60000 millis timeout"))
    val metastore = new RuntimeException("x", new MetaException("Got exception: java.io.IOException"))
    val config = new IllegalStateException("Target table already exists with partitioning [a]")
    val readError = new RuntimeException("Encountered error while reading file x.csv", new java.io.IOException("boom"))

    PseudonymizeTableJob.isTransient(timeout) shouldBe true
    PseudonymizeTableJob.isTransient(metastore) shouldBe true
    PseudonymizeTableJob.isTransient(config) shouldBe false
    PseudonymizeTableJob.isTransient(readError) shouldBe false
  }

  "retrying" should "retry transient failures up to the attempt limit and then give up" in {
    val waits = scala.collection.mutable.ArrayBuffer.empty[Long]
    var calls = 0
    val ok = PseudonymizeTableJob.retrying[String](3, 7000L, _ => true, (_, _) => (), waits += _) { attempt =>
      calls += 1
      if (attempt < 3) throw new java.io.IOException("flaky") else "done"
    }
    ok shouldBe "done"
    calls shouldBe 3
    waits.toList shouldBe List(7000L, 7000L)

    var tries = 0
    an[java.io.IOException] should be thrownBy
      PseudonymizeTableJob.retrying[Unit](2, 0L, _ => true, (_, _) => (), _ => ()) { _ => tries += 1; throw new java.io.IOException("down") }
    tries shouldBe 2
  }

  it should "not retry permanent failures" in {
    var tries = 0
    an[IllegalStateException] should be thrownBy
      PseudonymizeTableJob.retrying[Unit](3, 0L, PseudonymizeTableJob.isTransient, (_, _) => (), _ => ()) { _ =>
        tries += 1; throw new IllegalStateException("bad schema")
      }
    tries shouldBe 1
  }
}
