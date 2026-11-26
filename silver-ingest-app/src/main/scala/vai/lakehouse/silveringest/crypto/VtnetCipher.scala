package vai.lakehouse.silveringest.crypto

import java.util.regex.Pattern
import javax.crypto.{BadPaddingException, IllegalBlockSizeException}

import com.viettel.security.PassTranformer

/**
 * Giải mã (và mã hoá, để kiểm thử) một giá trị bằng thư viện của VTNET: `com.viettel.security.PassTranformer` (viettelsecurity-2.1.jar, jar của
 * đối tác, KHÔNG shade: image chép nguyên jar vào /opt/app và manifest nạp qua `spark.jars`).
 *
 * Những điều thư viện làm mà job phải chặn:
 *  - API tĩnh, dùng chung hai `Cipher` tĩnh và một `inputKey` tĩnh: gọi song song từ nhiều task của một executor là sai kết quả (đo được
 *    khoảng 2,4% lỗi với 8 luồng). Mọi lời gọi ở đây giữ khoá trên lớp `PassTranformer` và đặt lại `inputKey` ngay trước khi gọi.
 *  - `inputKey` rỗng là dùng key mặc định trong jar; `inputKey` null là đọc `../conf/viettelsecurity.properties`. Cả hai đều bị từ chối: key
 *    luôn là key `by-path` của DAK.
 *  - Ký tự ngoài bảng hex của thư viện được giải mã thành rác mà không báo lỗi: giá trị phải là bản mã đúng dạng (chữ số và a-f thường, độ dài
 *    là bội của 32 ký tự = một khối AES), không thì báo lỗi.
 *
 * Giá trị NULL ra NULL, chuỗi rỗng ra chuỗi rỗng (như cdr-crypto). Thông báo lỗi không chứa giá trị hay key, chỉ dạng của giá trị.
 */
object VtnetCipher {

  /** Một khối AES (16 byte) viết thành 32 ký tự. */
  val BlockChars = 32

  private val Ciphertext = Pattern.compile("^[0-9a-f]+$")

  def decrypt(inputKey: String, value: String, context: String = ""): String =
    if (value == null) null
    else if (value.isEmpty) value
    else {
      requireKey(inputKey)
      if (value.length % BlockChars != 0 || !Ciphertext.matcher(value).matches()) {
        throw new IllegalArgumentException(failure("decrypt", context, "not a VTNET ciphertext (expected lowercase 0-9a-f, a multiple of " +
          s"$BlockChars characters: a plain value, or a cut or changed one)", value))
      }
      try locked(inputKey)(PassTranformer.decrypt(value))
      catch {
        case e @ (_: BadPaddingException | _: IllegalBlockSizeException) =>
          throw new IllegalStateException(failure("decrypt", context, "wrong key: the by-path key of DAK is not the inputKey that VTNET encrypted with",
            value), e)
      }
    }

  def encrypt(inputKey: String, value: String): String =
    if (value == null) null
    else if (value.isEmpty) value
    else {
      requireKey(inputKey)
      locked(inputKey)(PassTranformer.encrypt(value))
    }

  /** Một lời gọi tại một thời điểm trong JVM, với đúng key của lời gọi đó (trạng thái của thư viện là tĩnh). */
  private def locked[T](inputKey: String)(body: => T): T = classOf[PassTranformer].synchronized {
    PassTranformer.setInputKey(inputKey)
    body
  }

  private def requireKey(inputKey: String): Unit =
    if (inputKey == null || inputKey.isEmpty) {
      throw new IllegalArgumentException("VTNET inputKey is empty: the library would fall back to its built-in default key; the key must be the " +
        "by-path key of DAK")
    }

  private def failure(action: String, context: String, reason: String, value: String): String =
    s"VTNET $action failed${if (context.isEmpty) "" else s" ($context)"}: $reason; value: ${shapeOf(value)}"

  /** Dạng của giá trị (độ dài và loại ký tự), không lộ giá trị. */
  private[crypto] def shapeOf(value: String): String = {
    val kinds = Seq(
      "digits" -> value.exists(_.isDigit),
      "a-f" -> value.exists(c => c >= 'a' && c <= 'f'),
      "A-F" -> value.exists(c => c >= 'A' && c <= 'F'),
      "other letters" -> value.exists(c => c.isLetter && !(c >= 'a' && c <= 'f') && !(c >= 'A' && c <= 'F')),
      "other" -> value.exists(c => !c.isLetterOrDigit)
    ).collect { case (name, true) => name }
    s"${value.length} chars (${kinds.mkString(", ")})"
  }
}
