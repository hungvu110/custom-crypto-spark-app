package vai.lakehouse.pseudonym

import java.security.SecureRandom
import java.util.Base64

import scala.util.Try

/**
 * Khoá bí mật của 1 chế độ giả danh (HMAC hoặc AES-256-SIV), kèm version để biết định danh giả thuộc thế hệ
 * khoá nào. `toString` và mọi thông báo lỗi KHÔNG chứa giá trị khoá (cùng quy ước với `PrefixSource`).
 *
 * Serializable vì nằm trong closure của UDF gửi xuống executor.
 */
final class PseudonymKey private (val name: String, val version: Int, keyBytes: Array[Byte]) extends Serializable {

  /** Bản sao — không để bên ngoài sửa khoá đang dùng. */
  private[pseudonym] def bytes: Array[Byte] = keyBytes.clone()

  def length: Int = keyBytes.length

  override def toString: String = s"PseudonymKey($name, v$version, ***)"
}

object PseudonymKey {

  val Base64Prefix = "base64:"
  val HexPrefix    = "hex:"

  /**
   * `encoded` là đúng giá trị lưu trong Vault/Secret: `base64:<...>` hoặc `hex:<...>`.
   * Từ chối sai độ dài, khoá toàn byte 0, thiếu tiền tố.
   */
  def parse(name: String, version: Int, encoded: String, expectedBytes: Int): PseudonymKey = {
    require(version >= 1, s"Key '$name': version must be >= 1 (got $version)")
    val bytes = decode(name, Option(encoded).map(_.trim).getOrElse(""))
    require(bytes.length == expectedBytes,
      s"Key '$name' must be $expectedBytes bytes but is ${bytes.length} bytes")
    require(bytes.exists(_ != 0), s"Key '$name' must not be all zero bytes")
    new PseudonymKey(name, version, bytes)
  }

  /** Sinh khoá ngẫu nhiên ở dạng lưu được vào Vault/Secret (`base64:<...>`) — tương đương `openssl rand -base64 N`. */
  def randomEncoded(sizeBytes: Int, random: SecureRandom = new SecureRandom()): String = {
    val bytes = new Array[Byte](sizeBytes)
    random.nextBytes(bytes)
    Base64Prefix + Base64.getEncoder.encodeToString(bytes)
  }

  private def decode(name: String, value: String): Array[Byte] =
    if (value.startsWith(Base64Prefix))
      Try(Base64.getDecoder.decode(value.substring(Base64Prefix.length))).getOrElse(
        throw new IllegalArgumentException(s"Key '$name' is not valid base64"))
    else if (value.startsWith(HexPrefix))
      Try(Encoding.unhex(value.substring(HexPrefix.length))).getOrElse(
        throw new IllegalArgumentException(s"Key '$name' is not valid hex"))
    else
      throw new IllegalArgumentException(s"Key '$name' must start with '$Base64Prefix' or '$HexPrefix'")
}
