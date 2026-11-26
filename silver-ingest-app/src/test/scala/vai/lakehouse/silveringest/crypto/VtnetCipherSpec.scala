package vai.lakehouse.silveringest.crypto

import java.util.concurrent.{Callable, Executors, TimeUnit}

import scala.collection.JavaConverters._

import com.viettel.security.PassTranformer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Lớp bọc an toàn của `PassTranformer` (VTNET): cùng kết quả với thư viện, an toàn khi gọi song song, không bao giờ dùng key mặc định. */
class VtnetCipherSpec extends AnyFlatSpec with Matchers {

  private val inputKey = "test-input-key-not-real"
  private val imsis = (0 until 200).map(i => f"45204${i * 7919L}%010d") // synthetic 15-digit values

  /** Kết quả của chính thư viện, gọi tuần tự từ một luồng (cách duy nhất thư viện cho kết quả đúng). */
  private def libraryEncrypt(value: String): String = classOf[PassTranformer].synchronized {
    PassTranformer.setInputKey(inputKey)
    PassTranformer.encrypt(value)
  }

  "VtnetCipher" should "cho đúng bản mã và bản rõ của PassTranformer (golden, từng giá trị)" in {
    imsis.foreach { v =>
      val expected = libraryEncrypt(v)
      VtnetCipher.encrypt(inputKey, v) shouldBe expected
      expected should fullyMatch regex "[0-9a-f]{32}" // 15 chữ số = một khối AES
      VtnetCipher.decrypt(inputKey, expected) shouldBe v
    }
  }

  it should "giải mã đúng khi gọi song song từ nhiều luồng (thư viện dùng Cipher tĩnh)" in {
    val pairs = imsis.map(v => v -> libraryEncrypt(v))
    val pool = Executors.newFixedThreadPool(8)
    try {
      val tasks = (0 until 8).map { t =>
        new Callable[Int] {
          override def call(): Int = (0 until 5000).count { i =>
            val (plain, cipher) = pairs((i + t * 31) % pairs.size)
            VtnetCipher.decrypt(inputKey, cipher) != plain
          }
        }
      }
      val wrong = pool.invokeAll(tasks.asJava).asScala.map(_.get(2, TimeUnit.MINUTES)).sum
      wrong shouldBe 0
    } finally pool.shutdownNow()
  }

  it should "để NULL là NULL và chuỗi rỗng là chuỗi rỗng" in {
    VtnetCipher.decrypt(inputKey, null) shouldBe null
    VtnetCipher.decrypt(inputKey, "") shouldBe ""
    VtnetCipher.encrypt(inputKey, null) shouldBe null
  }

  it should "từ chối key rỗng (thư viện sẽ dùng key mặc định của nó)" in {
    val cipher = libraryEncrypt("452041234567890")
    an[IllegalArgumentException] should be thrownBy VtnetCipher.decrypt("", cipher)
    an[IllegalArgumentException] should be thrownBy VtnetCipher.decrypt(null, cipher)
    an[IllegalArgumentException] should be thrownBy VtnetCipher.encrypt("", "452041234567890")
  }

  it should "từ chối giá trị không phải bản mã VTNET, không lộ giá trị trong thông báo" in {
    Seq("452041234567890", "6313CDFE2B612B82A333535B6EF74DE1", "6313cdfe2b612b82a333535b6ef74de", "6313cdfe2b612b82a333535b6ef74dzz").foreach { bad =>
      val e = intercept[IllegalArgumentException](VtnetCipher.decrypt(inputKey, bad, "dataset x, column imsi"))
      e.getMessage should include("not a VTNET ciphertext")
      e.getMessage should include("dataset x, column imsi")
      e.getMessage should not include bad
    }
  }

  it should "báo key sai khi bản mã được mã hoá bằng key khác" in {
    // Với key sai, khoảng 1/256 giá trị vẫn qua được kiểm padding (ra rác): trong 50 giá trị chắc chắn có lỗi.
    val ciphers = imsis.take(50).map(libraryEncrypt)
    val errors = ciphers.flatMap { c =>
      try { VtnetCipher.decrypt("another-key", c); None }
      catch { case e: IllegalStateException => Some(e) }
    }
    errors should not be empty
    errors.head.getMessage should include("wrong key")
    PassTranformer.getInputKey should not be null // the library never falls back to its properties file
  }

  "shapeOf" should "mô tả độ dài và loại ký tự" in {
    VtnetCipher.shapeOf("ab12") shouldBe "4 chars (digits, a-f)"
    VtnetCipher.shapeOf("AZ-") shouldBe "3 chars (A-F, other letters, other)"
  }
}
