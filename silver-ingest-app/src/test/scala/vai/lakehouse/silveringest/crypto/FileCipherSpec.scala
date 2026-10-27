package vai.lakehouse.silveringest.crypto

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileCipherSpec extends AnyFlatSpec with Matchers {

  private val keyPrefix = Base64.getEncoder.encodeToString(Array.tabulate[Byte](32)(i => i.toByte))
  private val cipher = AesGcmFileCipher

  "AesGcmFileCipher" should "giải mã đúng file đã mã hoá theo định dạng IV ‖ ciphertext ‖ tag" in {
    val key = cipher.key(keyPrefix)
    val plain = "a,b,c\n1,2,3\n".getBytes(UTF_8)

    val encrypted = cipher.encrypt(plain, key)

    encrypted.length shouldBe 12 + plain.length + 16
    cipher.decrypt(encrypted, key) shouldBe plain
  }

  it should "dùng IV ngẫu nhiên: hai lần mã hoá cùng nội dung ra hai kết quả khác nhau" in {
    val key = cipher.key(keyPrefix)

    cipher.encrypt(Array[Byte](1, 2, 3), key) should not equal cipher.encrypt(Array[Byte](1, 2, 3), key)
  }

  it should "báo lỗi rõ khi sai key hoặc file bị sửa, không lộ key" in {
    val key = cipher.key(keyPrefix)
    val encrypted = cipher.encrypt("secret".getBytes(UTF_8), key)
    val otherKey = cipher.key(Base64.getEncoder.encodeToString(Array.fill[Byte](32)(7)))
    val tampered = encrypted.clone()
    tampered(20) = (tampered(20) ^ 1).toByte

    Seq(() => cipher.decrypt(encrypted, otherKey), () => cipher.decrypt(tampered, key)).foreach { attempt =>
      val ex = intercept[FileDecryptionException](attempt())
      ex.getMessage should include("wrong key or corrupted file")
      ex.getMessage should not include keyPrefix
    }
  }

  it should "báo lỗi khi file ngắn hơn IV + tag" in {
    intercept[FileDecryptionException](cipher.decrypt(new Array[Byte](10), cipher.key(keyPrefix))).getMessage should
      include("too short")
  }

  it should "chỉ nhận keyPrefix là base64 chuẩn của đúng 32 byte" in {
    Seq("not base64!!", Base64.getEncoder.encodeToString(new Array[Byte](16)), "").foreach { bad =>
      val ex = intercept[IllegalArgumentException](cipher.key(bad))
      ex.getMessage should include("32 bytes")
      if (bad.nonEmpty) ex.getMessage should not include bad
    }
  }

  "FileCipher.forName" should "trả cipher theo tên và từ chối tên lạ" in {
    FileCipher.forName("aes-256-gcm") shouldBe AesGcmFileCipher
    FileCipher.forName("AES-256-GCM") shouldBe AesGcmFileCipher
    intercept[IllegalArgumentException](FileCipher.forName("rot13"))
  }
}
