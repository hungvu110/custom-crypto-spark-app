package vai.lakehouse.pseudonym.core

import vai.lakehouse.pseudonym.{InvalidKeyException, InvalidPseudonymException}
import vai.lakehouse.pseudonym.TestKeys
import java.nio.charset.StandardCharsets.UTF_8

import com.google.crypto.tink.subtle.AesSiv
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AesSivPseudonymizerSpec extends AnyFlatSpec with Matchers {

  private val pseudonymizer = new AesSivPseudonymizer(TestKeys.aesSiv)

  "AesSivPseudonymizer" should "giải mã lại đúng giá trị gốc (kể cả chuỗi dài, unicode, rỗng)" in {
    Seq("84903217465", "KH0000123456", "Nguyễn Văn An - một chuỗi dài hơn 16 byte", "").foreach { v =>
      pseudonymizer.decrypt(pseudonymizer.encrypt(v)) shouldBe v
    }
  }

  it should "tất định (cùng input -> cùng output, nên vẫn liên kết được bản ghi) và mang tiền tố e1_" in {
    val a = pseudonymizer.encrypt("84903217465")
    a shouldBe pseudonymizer.encrypt("84903217465")
    a should fullyMatch regex "e1_[A-Za-z0-9_-]+"
    a should not be pseudonymizer.encrypt("84903217466")
  }

  it should "đúng là AES-SIV của Tink với khoá này (không có associated data)" in {
    val direct = new AesSiv(TestKeys.aesSiv.exportBytes()).encryptDeterministically("84903217465".getBytes(UTF_8), Array.emptyByteArray)
    pseudonymizer.encrypt("84903217465") shouldBe "e1_" + Encoding.base64Url(direct)
  }

  it should "null vào -> null ra" in {
    pseudonymizer.encrypt(null) shouldBe null
    pseudonymizer.decrypt(null) shouldBe null
  }

  it should "báo lỗi xác thực khi sai khoá — không trả rác" in {
    val wrong = new AesSivPseudonymizer(TestKeys.randomProvider().aesSivKey)
    val ex = intercept[InvalidPseudonymException](wrong.decrypt(pseudonymizer.encrypt("84903217465")))
    ex.getMessage should include("wrong key or tampered")
  }

  it should "báo lỗi khi bản mã bị sửa 1 ký tự" in {
    val token = pseudonymizer.encrypt("84903217465")
    val last = token.last
    val tampered = token.dropRight(1) + (if (last == 'A') 'B' else 'A')
    intercept[InvalidPseudonymException](pseudonymizer.decrypt(tampered))
  }

  it should "từ chối tiền tố sai version hoặc không phải base64url" in {
    intercept[InvalidPseudonymException](pseudonymizer.decrypt("e2_abc")).getMessage should include("expected prefix 'e1_'")
    intercept[InvalidPseudonymException](pseudonymizer.decrypt("e1_@@@")).getMessage should include("base64url")
  }

  it should "dùng được sau khi serialize (closure UDF gửi xuống executor)" in {
    val bytes = new java.io.ByteArrayOutputStream()
    val out = new java.io.ObjectOutputStream(bytes)
    out.writeObject(pseudonymizer)
    out.close()
    val copy = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
      .readObject().asInstanceOf[AesSivPseudonymizer]

    copy.decrypt(pseudonymizer.encrypt("84903217465")) shouldBe "84903217465"
  }

  it should "từ chối khoá không phải 64 byte" in {
    intercept[InvalidKeyException](new AesSivPseudonymizer(TestKeys.hmac))
  }
}
