package vai.lakehouse.pseudonym.core

import vai.lakehouse.keyprefix.{DeidKey, PrefixSource}
import vai.lakehouse.pseudonym.ConfigurationException

/** Cung cấp khoá cho chế độ HASH / ENCRYPT. Chỉ driver gọi; executor nhận khoá trong closure của UDF. */
trait KeyProvider {
  def hmacKey: PseudonymKey
  def aesSivKey: PseudonymKey
}

/**
 * Đọc khoá qua `key-prefix-lib` (K8s Secret / Vault / DAK) — khoá "lưu trữ riêng biệt" theo mục c của tài liệu.
 * Tên khoá trong nguồn: `pseudonym.hmac_v<N>` và `pseudonym.aes_siv_v<N>`, giá trị `base64:<...>`/`hex:<...>`.
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
  def hmacName(version: Int): String   = s"pseudonym.hmac_v$version"
  def aesSivName(version: Int): String = s"pseudonym.aes_siv_v$version"
}

/**
 * Khoá HASH lấy từ DAK qua `GET /api/v1/keys/deid` (docs/key-access/YEU-CAU-TU-DAK-BO-K8S-SECRET.md), thay cho K8s Secret
 * `pseudonym.hmac_v1` mount vào `/etc/pseudonym-keys`. `keyPrefix` của DAK là base64 chuẩn của 32 byte, tức phần sau `base64:` của
 * giá trị trong Secret cũ. Version của khoá là `keyVersion` ở Vault của DAK.
 *
 * Chỉ HASH: DAK chỉ giao một khoá cho phiếu khử định danh, nên `aesSivKey` báo lỗi cấu hình. Khoá được lấy khi cần lần đầu và giữ
 * trong đối tượng này (một lượt chạy); không cache giữa các lượt chạy (rào hạn phiếu của DAK kiểm lúc GET).
 *
 * @param fetch một lần GET tới DAK; lỗi (401/403/409/400, `requestCode` lệch...) ném ra và dừng job
 */
final class DakDeidKeyProvider(fetch: () => DeidKey) extends KeyProvider {

  /** Lần GET đầu tiên; `requestCode` và `keyVersion` dùng để ghi log/metadata của job (không chứa giá trị khoá). */
  lazy val deidKey: DeidKey = fetch()

  override lazy val hmacKey: PseudonymKey =
    PseudonymKey.parse(DakDeidKeyProvider.HmacName, deidKey.keyVersion, PseudonymKey.Base64Prefix + deidKey.keyPrefix,
      HmacPseudonymizer.KeyBytes)

  override def aesSivKey: PseudonymKey =
    throw new ConfigurationException("The key from DAK (keys/deid) is for HASH only: AES-SIV (ENCRYPT) is not available with source=dak")
}

object DakDeidKeyProvider {
  val HmacName = "pseudonym.hmac"
}
