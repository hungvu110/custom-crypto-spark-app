package vai.lakehouse.pseudonym.core

import vai.lakehouse.pseudonym.{InvalidKeyException, InvalidPseudonymException}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.GeneralSecurityException

import com.google.crypto.tink.subtle.AesSiv

import scala.util.Try

/**
 * Chế độ ENCRYPT — "mã hoá" + AES-256 của tài liệu: AES-256-SIV (RFC 5297, qua Tink có sẵn trong Spark 3.5.1).
 * Output `e<version>_` + base64url(bản mã). Tất định (cùng input -> cùng output, nên vẫn liên kết được bản ghi),
 * giải mã lại được bằng khoá, có kiểm tra toàn vẹn: sai khoá hoặc bản mã bị sửa -> lỗi, không trả rác.
 *
 * Không dùng GCM (IV ngẫu nhiên -> mất tính liên kết) hay ECB (giá trị dài > 16 byte lộ khối trùng nhau).
 */
final class AesSivPseudonymizer(key: PseudonymKey) extends Serializable {
  import AesSivPseudonymizer._

  if (key.length != KeyBytes)
    throw new InvalidKeyException(s"AES-SIV key '${key.name}' must be $KeyBytes bytes (2 AES-256 keys)")

  private val prefix = s"e${key.version}_"

  // Primitive của Tink thread-safe; @transient vì không serialize được, dựng lại trên executor.
  @transient private lazy val siv: AesSiv = new AesSiv(key.exportBytes())

  def encrypt(value: String): String =
    if (value == null) null
    else prefix + Encoding.base64Url(siv.encryptDeterministically(value.getBytes(UTF_8), NoAssociatedData))

  /** @throws InvalidPseudonymException nếu sai tiền tố/version, sai khoá hoặc bản mã bị sửa. */
  def decrypt(pseudonym: String): String =
    if (pseudonym == null) null
    else {
      if (!pseudonym.startsWith(prefix))
        throw new InvalidPseudonymException(s"Not an ENCRYPT pseudonym of key version ${key.version}: expected prefix '$prefix'")
      val cipherText = Try(Encoding.unbase64Url(pseudonym.substring(prefix.length))).getOrElse(
        throw new InvalidPseudonymException("Pseudonym is not valid base64url"))
      try new String(siv.decryptDeterministically(cipherText, NoAssociatedData), UTF_8)
      catch {
        case _: GeneralSecurityException =>
          throw new InvalidPseudonymException("Pseudonym decryption failed: wrong key or tampered value")
      }
    }
}

object AesSivPseudonymizer {
  /** AES-256-SIV = 2 khoá AES-256: 1 cho S2V/CMAC, 1 cho CTR. */
  val KeyBytes = 64

  private val NoAssociatedData = Array.emptyByteArray
}
