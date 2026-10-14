package vai.lakehouse.pseudonym

import vai.lakehouse.keyprefix.PrefixSource

/** Cung cấp khoá cho chế độ HASH / ENCRYPT. Chỉ driver gọi; executor nhận khoá trong closure của UDF. */
trait KeyProvider {
  def hmacKey: PseudonymKey
  def aesSivKey: PseudonymKey
}

/**
 * Đọc khoá qua `key-prefix-lib` (K8s Secret / Vault / DAK) — khoá "lưu trữ riêng biệt" theo mục c của tài liệu.
 * Tên khoá trong nguồn: `pseudonym.hmac.v<N>` và `pseudonym.aes-siv.v<N>`, giá trị `base64:<...>`/`hex:<...>`.
 * Khoá chỉ được đọc khi cần lần đầu (chế độ không dùng thì không đụng tới khoá của nó).
 */
final class PrefixSourceKeyProvider(
  source: PrefixSource,
  hmacVersion: Int = 1,
  aesSivVersion: Int = 1
) extends KeyProvider {
  import PrefixSourceKeyProvider._

  override lazy val hmacKey: PseudonymKey = load(hmacName(hmacVersion), hmacVersion, HmacPseudonymizer.KeyBytes)

  override lazy val aesSivKey: PseudonymKey = load(aesSivName(aesSivVersion), aesSivVersion, AesSivPseudonymizer.KeyBytes)

  private def load(name: String, version: Int, size: Int): PseudonymKey =
    PseudonymKey.parse(name, version, source.read(name), size)
}

object PrefixSourceKeyProvider {
  def hmacName(version: Int): String   = s"pseudonym.hmac.v$version"
  def aesSivName(version: Int): String = s"pseudonym.aes-siv.v$version"
}
