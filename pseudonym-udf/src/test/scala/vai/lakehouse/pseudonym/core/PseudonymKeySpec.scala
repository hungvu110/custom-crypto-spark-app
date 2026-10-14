package vai.lakehouse.pseudonym.core

import vai.lakehouse.pseudonym.InvalidKeyException
import vai.lakehouse.pseudonym.TestKeys
import java.util.Base64

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PseudonymKeySpec extends AnyFlatSpec with Matchers {

  private val secretHex = TestKeys.hexOfRange(32)

  "PseudonymKey.parse" should "đọc được cả dạng hex: và base64:" in {
    val fromHex = PseudonymKey.parse("k", 1, "hex:" + secretHex, 32)
    val fromB64 = PseudonymKey.parse("k", 1, "base64:" + Base64.getEncoder.encodeToString(fromHex.exportBytes()), 32)

    fromB64.exportBytes() shouldBe fromHex.exportBytes()
    fromHex.version shouldBe 1
  }

  it should "bỏ khoảng trắng/xuống dòng ở cuối (file Secret thường có newline)" in {
    PseudonymKey.parse("k", 2, s"  hex:$secretHex\n", 32).length shouldBe 32
  }

  it should "từ chối sai độ dài" in {
    val ex = intercept[InvalidKeyException](PseudonymKey.parse("k", 1, "hex:" + TestKeys.hexOfRange(16), 32))
    ex.getMessage should include("must be 32 bytes but is 16 bytes")
  }

  it should "từ chối khoá toàn byte 0" in {
    val ex = intercept[InvalidKeyException](PseudonymKey.parse("k", 1, "hex:" + "00" * 32, 32))
    ex.getMessage should include("all zero")
  }

  it should "từ chối thiếu tiền tố, base64/hex hỏng, version < 1" in {
    intercept[InvalidKeyException](PseudonymKey.parse("k", 1, secretHex, 32)).getMessage should include("must start with")
    intercept[InvalidKeyException](PseudonymKey.parse("k", 1, "base64:@@@", 32)).getMessage should include("not valid base64")
    intercept[InvalidKeyException](PseudonymKey.parse("k", 1, "hex:zz", 32)).getMessage should include("not valid hex")
    intercept[InvalidKeyException](PseudonymKey.parse("k", 0, "hex:" + secretHex, 32)).getMessage should include("version")
  }

  it should "KHÔNG đưa giá trị khoá vào toString hay message lỗi" in {
    val key = PseudonymKey.parse("pseudonym.hmac.v1", 1, "hex:" + secretHex, 32)
    key.toString should not include secretHex
    key.toString should include("***")

    val wrongLength = intercept[InvalidKeyException](PseudonymKey.parse("k", 1, "hex:" + secretHex, 64))
    wrongLength.getMessage should not include secretHex
    val broken = intercept[InvalidKeyException](PseudonymKey.parse("k", 1, s"base64:$secretHex!!", 32))
    broken.getMessage should not include secretHex
  }

  "PseudonymKey.randomEncoded" should "sinh khoá parse được, đúng độ dài, mỗi lần khác nhau" in {
    val a = PseudonymKey.randomEncoded(64)
    val b = PseudonymKey.randomEncoded(64)

    a should startWith("base64:")
    a should not be b
    PseudonymKey.parse("k", 1, a, 64).length shouldBe 64
  }
}
