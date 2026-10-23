package vai.lakehouse.columncrypto.cdr

import java.util.Locale

/**
 * Cấu hình mã hoá CDR cho 1 dataset khi dùng DataFrame API: key của TỪNG cột (bí mật, đã tra từ nguồn key ở driver), cột
 * `keyField` (giữ nguyên plaintext, giá trị của từng dòng ghép sau key thành `keyInput`) và các cột cần mã hoá. `keyField` không
 * được nằm trong `encryptedColumns`. `datasetName` dùng để ghi audit (`CdrCrypto.loadConfig` tự điền).
 *
 * @param keyPrefixes tên cột (chữ thường) -> key của cột đó. Tên cột của DataFrame chính là tên dùng để xin key.
 */
case class CdrCryptoConfig(
  keyPrefixes: Map[String, String],
  keyField: String,
  encryptedColumns: Seq[String],
  datasetName: String = ""
) {

  /** Key của cột `column` (không phân biệt hoa thường), nếu có. */
  def keyFor(column: String): Option[String] = keyPrefixes.get(column.toLowerCase(Locale.ROOT))

  // Che mọi key để không lộ ra log khi in nguyên case class; chỉ in tên cột có key.
  override def toString: String =
    s"CdrCryptoConfig(keyPrefixes=${keyPrefixes.keys.toSeq.sorted.mkString("[", ", ", "]")}->***, keyField=$keyField, " +
      s"encryptedColumns=${encryptedColumns.mkString("[", ", ", "]")}, datasetName=$datasetName)"
}
