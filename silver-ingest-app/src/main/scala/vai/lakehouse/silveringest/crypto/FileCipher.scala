package vai.lakehouse.silveringest.crypto

import java.security.{GeneralSecurityException, SecureRandom}
import java.util.{Base64, Locale}
import javax.crypto.Cipher
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}

import scala.util.Try

/** Giải mã cả file thất bại (sai key, file hỏng). Thông báo nêu file, không bao giờ chứa key. */
final class FileDecryptionException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

/**
 * Thuật toán giải mã CẢ FILE của nguồn VTNET. Pluggable: thuật toán thật của VTNET chưa được xác nhận (docs/silver-ingest/PLAN.md
 * mục 2), nên thêm thuật toán mới chỉ là thêm một object và một tên trong [[FileCipher.forName]] cùng `IngestConfig.FileCiphers`.
 *
 * Chạy trên executor nên phải `Serializable`; key đã giải mã ([[key]]) được dựng ở driver và gửi kèm closure.
 */
trait FileCipher extends Serializable {

  def name: String

  /** Dựng key từ `keyPrefix` của DAK `by-path`; sai định dạng thì `IllegalArgumentException` không in lại giá trị. */
  def key(keyPrefix: String): Array[Byte]

  /** Giải mã nguyên nội dung một file; lỗi là [[FileDecryptionException]]. */
  def decrypt(content: Array[Byte], key: Array[Byte]): Array[Byte]
}

object FileCipher {

  def forName(name: String): FileCipher = name.trim.toLowerCase(Locale.ROOT) match {
    case AesGcmFileCipher.name => AesGcmFileCipher
    case other => throw new IllegalArgumentException(s"Unknown file cipher '$other'")
  }
}

/**
 * AES-256-GCM, file = IV 12 byte ‖ ciphertext ‖ tag 16 byte (định dạng mặc định, GIẢ ĐỊNH cho tới khi VTNET xác nhận). Key =
 * `keyPrefix` của DAK `by-path`, base64 chuẩn của đúng 32 byte. GCM kiểm tag trên cả file nên file sai key hoặc bị sửa đều bị từ chối, không
 * bao giờ ra dữ liệu rác.
 */
object AesGcmFileCipher extends FileCipher {

  val name = "aes-256-gcm"

  val IvBytes = 12
  val TagBits = 128
  val KeyBytes = 32

  private val Transformation = "AES/GCM/NoPadding"
  private lazy val random = new SecureRandom()

  override def key(keyPrefix: String): Array[Byte] = {
    val decoded = Try(Base64.getDecoder.decode(Option(keyPrefix).getOrElse("").trim)).toOption
    decoded.filter(_.length == KeyBytes).getOrElse(throw new IllegalArgumentException(
      s"The file key from DAK has an unsupported format: $name needs a standard base64 value of exactly $KeyBytes bytes"))
  }

  override def decrypt(content: Array[Byte], key: Array[Byte]): Array[Byte] = {
    if (content.length < IvBytes + TagBits / 8) {
      throw new FileDecryptionException(s"$name: file is too short (${content.length} bytes) to hold the IV and the tag")
    }
    try {
      val cipher = Cipher.getInstance(Transformation)
      cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(TagBits, content, 0, IvBytes))
      cipher.doFinal(content, IvBytes, content.length - IvBytes)
    } catch {
      case e: GeneralSecurityException =>
        throw new FileDecryptionException(s"$name: cannot decrypt (wrong key or corrupted file: ${e.getClass.getSimpleName})", e)
    }
  }

  /** Đảo chiều của [[decrypt]] với IV ngẫu nhiên: dùng để tạo file thử và trong test, job không gọi. */
  def encrypt(plain: Array[Byte], key: Array[Byte]): Array[Byte] = {
    val iv = new Array[Byte](IvBytes)
    random.nextBytes(iv)
    val cipher = Cipher.getInstance(Transformation)
    cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(TagBits, iv))
    iv ++ cipher.doFinal(plain)
  }
}
