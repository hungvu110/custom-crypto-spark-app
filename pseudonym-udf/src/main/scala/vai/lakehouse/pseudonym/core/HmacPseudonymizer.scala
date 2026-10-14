package vai.lakehouse.pseudonym.core

import vai.lakehouse.pseudonym.InvalidKeyException
import java.nio.charset.StandardCharsets.UTF_8

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * Chế độ HASH — "băm có salt" của tài liệu, làm đúng chuẩn bằng HMAC-SHA256 với khoá bí mật 256-bit:
 * `k<version>_` + hex(16 byte đầu của HMAC-SHA256(key, value)). Một chiều, tất định. Thuần JVM, không Spark.
 *
 * Không đổi input/format sau khi phát hành: đổi là đổi toàn bộ định danh giả (golden vector trong test chặn việc này).
 */
final class HmacPseudonymizer(key: PseudonymKey) extends Serializable {
  import HmacPseudonymizer._

  if (key.length != KeyBytes)
    throw new InvalidKeyException(s"HMAC key '${key.name}' must be $KeyBytes bytes")

  private val prefix = s"k${key.version}_"

  // Mac không thread-safe -> 1 instance/thread; @transient vì Mac không serialize được sang executor.
  @transient private lazy val mac: ThreadLocal[Mac] = {
    val keyBytes = key.exportBytes()
    ThreadLocal.withInitial(() => newMac(keyBytes))
  }

  /** `null` vào -> `null` ra; mọi giá trị khác được băm nguyên trạng. */
  def pseudonymize(value: String): String =
    if (value == null) null
    else prefix + Encoding.hex(mac.get().doFinal(value.getBytes(UTF_8)), OutputBytes)
}

object HmacPseudonymizer {
  val Algorithm   = "HmacSHA256"
  val KeyBytes    = 32
  /** 128-bit: xác suất trùng với 10^9 giá trị ≈ 1.5e-21. */
  val OutputBytes = 16

  private def newMac(key: Array[Byte]): Mac = {
    val m = Mac.getInstance(Algorithm)
    m.init(new SecretKeySpec(key, Algorithm))
    m
  }

  /** HMAC-SHA256 đầy đủ 32 byte — tách ra để kiểm bằng test vector RFC 4231 (khoá độ dài bất kỳ). */
  private[pseudonym] def hmacSha256(key: Array[Byte], data: Array[Byte]): Array[Byte] = newMac(key).doFinal(data)
}
