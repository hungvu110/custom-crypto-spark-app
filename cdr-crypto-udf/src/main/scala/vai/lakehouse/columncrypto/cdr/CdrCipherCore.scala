package vai.lakehouse.columncrypto.cdr

import com.viettel.datalake.security.TransformDL

import java.lang.reflect.Method
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

/**
 * Bọc `TransformDL` (jar đối tác `DataLakeSecurity_jv8.jar`, KHÔNG commit vào repo — xem
 * `.gitignore`) thành 2 chiều:
 *   - `decrypt`: gọi THẲNG method public thật của đối tác, không viết lại logic.
 *   - `encrypt`: đối tác không cung cấp — tự viết, TÁI DÙNG chính xác phần sinh key của họ qua
 *     reflection (method private `a`, field `key`/`algorithm`) thay vì gõ lại thuật toán cộng
 *     dồn bằng tay, để loại trừ rủi ro chép sai. Chỉ tự viết phần đảo chiều `Cipher`
 *     (`ENCRYPT_MODE` thay `DECRYPT_MODE`). Đã xác nhận đúng byte-for-byte với test vector đối
 *     tác — xem `tools/datalake-security-test/EncryptDecryptTest.java` và
 *     `docs/PARTNER_CDR_CRYPTO_UDF_PLAN.md` mục 0.
 *
 * `keyInput = prefix ‖ fieldValue`, ghép thẳng không dấu phân cách, không băm — đúng công thức
 * trong `docs/VDL - Mã hóa CDR.docx`. Không tự thêm bất kỳ dấu phân cách nào ở đây; caller
 * (`CdrCryptoExtension`) chịu trách nhiệm ghép đúng.
 */
object CdrCipherCore {

  def decrypt(ciphertextB64: String, keyInput: String): String =
    try new TransformDL().decrypt(ciphertextB64, keyInput)
    catch { case e: Exception => throw new IllegalStateException("CDR partner decrypt failed", e) }

  def encrypt(plaintext: String, keyInput: String): String = {
    val lib = new TransformDL()
    lib.setInputKey(keyInput)

    val keyField = classOf[TransformDL].getDeclaredField("key")
    keyField.setAccessible(true)
    val defaultKey = keyField.get(lib).asInstanceOf[Array[Byte]]

    val algorithmField = classOf[TransformDL].getDeclaredField("algorithm")
    algorithmField.setAccessible(true)
    val algorithm = algorithmField.get(lib).asInstanceOf[String]

    val deriveKey: Method = classOf[TransformDL].getDeclaredMethod("a", classOf[Array[Byte]], classOf[String])
    deriveKey.setAccessible(true)
    val secretKey = deriveKey.invoke(lib, defaultKey, algorithm).asInstanceOf[SecretKeySpec]

    val cipher = Cipher.getInstance(algorithm) // "AES" -> mặc định JDK: AES/ECB/PKCS5Padding
    cipher.init(Cipher.ENCRYPT_MODE, secretKey)
    Base64.getEncoder.encodeToString(cipher.doFinal(plaintext.getBytes))
  }

  // Test vector đã xác nhận với đối tác qua email, đối chiếu docs/VDL - Mã hóa CDR.docx.
  private val SelfCheckCiphertext = "MmQtOaOWlG1rRkt49Gn6Bw=="
  private val SelfCheckKeyInput   = "231x#@Vie3" + "20260928123456"
  private val SelfCheckPlaintext  = "978356099"

  /**
   * Tự-kiểm tra bằng test vector đã xác nhận với đối tác, chạy 1 lần lúc `CdrCryptoExtension`
   * khởi tạo. Nếu đối tác đổi version jar và hành vi lệch đi (vd đổi tên method private `a`, đổi
   * thuật toán), lỗi hiện NGAY lúc engine khởi động thay vì âm thầm mã hoá/giải mã sai trong
   * production — xem docs/PARTNER_CDR_CRYPTO_UDF_PLAN.md mục 2.
   */
  def verifyCompatibility(): Unit = {
    val actualDecrypt = decrypt(SelfCheckCiphertext, SelfCheckKeyInput)
    require(actualDecrypt == SelfCheckPlaintext,
      s"CdrCipherCore self-check FAILED (decrypt): expected '$SelfCheckPlaintext', got '$actualDecrypt'. " +
        "Jar đối tác có thể đã đổi version/hành vi — KHÔNG dùng cdr_encrypt/cdr_decrypt cho tới khi rà lại.")
    val actualEncrypt = encrypt(SelfCheckPlaintext, SelfCheckKeyInput)
    require(actualEncrypt == SelfCheckCiphertext,
      s"CdrCipherCore self-check FAILED (encrypt): expected '$SelfCheckCiphertext', got '$actualEncrypt'. " +
        "Logic sinh key qua reflection có thể không còn khớp jar đối tác.")
  }
}
