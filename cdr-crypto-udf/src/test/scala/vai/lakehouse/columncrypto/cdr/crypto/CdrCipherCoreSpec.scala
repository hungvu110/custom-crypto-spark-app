package vai.lakehouse.columncrypto.cdr

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test đối chiếu với test vector đối tác gửi qua email (đã xác nhận đúng bằng
 * `tools/datalake-security-test/EncryptDecryptTest.java` chạy thật với jar đối tác).
 */
class CdrCipherCoreSpec extends AnyFlatSpec with Matchers {

  private val Ciphertext = "MmQtOaOWlG1rRkt49Gn6Bw=="
  private val Prefix     = "231x#@Vie3"
  private val Field      = "20260928123456"
  private val KeyInput   = Prefix + Field
  private val Plaintext  = "978356099"

  "decrypt" should "đọc đúng ciphertext mẫu của đối tác" in {
    CdrCipherCore.decrypt(Ciphertext, KeyInput) shouldBe Plaintext
  }

  "encrypt" should "ra đúng byte-for-byte ciphertext mẫu (AES/ECB tất định, không IV)" in {
    CdrCipherCore.encrypt(Plaintext, KeyInput) shouldBe Ciphertext
  }

  it should "roundtrip đúng với dữ liệu bất kỳ (encrypt rồi decrypt lại)" in {
    val plaintext = "0987654321"
    val keyInput = "some-prefix" + "20240101000000"
    CdrCipherCore.decrypt(CdrCipherCore.encrypt(plaintext, keyInput), keyInput) shouldBe plaintext
  }

  it should "roundtrip đúng khi field rỗng (nvl(field,'') trong SQL đối tác gửi)" in {
    val plaintext = "123456"
    val keyInputPrefixOnly = "only-prefix"
    CdrCipherCore.decrypt(CdrCipherCore.encrypt(plaintext, keyInputPrefixOnly), keyInputPrefixOnly) shouldBe plaintext
  }

  it should "roundtrip với key cột dạng DAK sinh: Base64 của 32 byte (44 ký tự, có '+', '/', '=')" in {
    val dakKey = "q+3Z/0aB1cD2eF3gH4iJ5kL6mN7oP8qR9sT0uV1wX2Y="
    dakKey should have length 44
    val keyInput = dakKey + Field
    val encrypted = CdrCipherCore.encrypt(Plaintext, keyInput)

    CdrCipherCore.decrypt(encrypted, keyInput) shouldBe Plaintext
    encrypted should not be CdrCipherCore.encrypt(Plaintext, Prefix + Field)
  }

  it should "ném lỗi rõ ràng thay vì lộ nguyên exception gốc khi decrypt sai key" in {
    val ex = intercept[IllegalStateException](CdrCipherCore.decrypt(Ciphertext, "sai-key-hoan-toan"))
    ex.getMessage should include("CDR partner decrypt failed")
  }

  "verifyCompatibility" should "pass với jar đối tác hiện tại" in {
    noException should be thrownBy CdrCipherCore.verifyCompatibility()
  }
}
