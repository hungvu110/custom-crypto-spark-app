package vai.lakehouse.pseudonym.core

import java.security.SecureRandom
import java.util.Base64

import vai.lakehouse.pseudonym.InvalidKeyException

import scala.util.Try

/**
 * Khoá bí mật của 1 chế độ giả danh (HMAC hoặc AES-256-SIV), kèm version để biết định danh giả thuộc thế hệ
 * khoá nào. `toString` và mọi thông báo lỗi KHÔNG chứa giá trị khoá (cùng quy ước với `PrefixSource`).
 *
 * Serializable vì nằm trong closure của UDF gửi xuống executor.
 */
final class PseudonymKey private (val name: String, val version: Int, keyBytes: Array[Byte]) extends Serializable {

  /**
   * Bản sao byte của khoá. Public vì app kiểm chứng cần quét xem khoá có rò rỉ vào log/plan/conf không (check P7);
   * mọi code khác KHÔNG nên gọi. Không bao giờ ghi giá trị này ra log.
   */
  def exportBytes(): Array[Byte] = keyBytes.clone()

  def length: Int = keyBytes.length

  override def toString: String = s"PseudonymKey($name, v$version, ***)"
}

object PseudonymKey {

  val Base64Prefix = "base64:"
  val HexPrefix    = "hex:"

  /**
   * `encoded` là đúng giá trị lưu trong Vault/Secret: `base64:<...>` hoặc `hex:<...>`.
   * Từ chối sai độ dài, khoá toàn byte 0, thiếu tiền tố.
   *
   * @throws InvalidKeyException khi khoá không hợp lệ
   */
  def parse(name: String, version: Int, encoded: String, expectedBytes: Int): PseudonymKey = {
    if (version < 1) throw new InvalidKeyException(s"Key '$name': version must be >= 1 (got $version)")
    val bytes = decode(name, Option(encoded).map(_.trim).getOrElse(""))
    if (bytes.length != expectedBytes)
      throw new InvalidKeyException(s"Key '$name' must be $expectedBytes bytes but is ${bytes.length} bytes")
    if (bytes.forall(_ == 0)) throw new InvalidKeyException(s"Key '$name' must not be all zero bytes")
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
        throw new InvalidKeyException(s"Key '$name' is not valid base64"))
    else if (value.startsWith(HexPrefix))
      Try(Encoding.unhex(value.substring(HexPrefix.length))).getOrElse(
        throw new InvalidKeyException(s"Key '$name' is not valid hex"))
    else
      throw new InvalidKeyException(s"Key '$name' must start with '$Base64Prefix' or '$HexPrefix'")
}
