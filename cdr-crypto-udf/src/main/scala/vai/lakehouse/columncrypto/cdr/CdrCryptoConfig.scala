package vai.lakehouse.columncrypto.cdr

/**
 * Cấu hình mã hoá CDR cho 1 dataset khi dùng DataFrame API: `keyPrefix` (bí mật, đã tra từ nguồn prefix
 * ở driver), cột `keyField` (giữ nguyên plaintext, giá trị của từng dòng ghép sau prefix thành `keyInput`) và
 * các cột cần mã hoá. `keyField` không được nằm trong `encryptedColumns`.
 */
case class CdrCryptoConfig(
  keyPrefix: String,
  keyField: String,
  encryptedColumns: Seq[String]
) {
  // Che keyPrefix để không lộ ra log khi in nguyên case class.
  override def toString: String =
    s"CdrCryptoConfig(keyPrefix=***, keyField=$keyField, encryptedColumns=${encryptedColumns.mkString("[", ", ", "]")})"
}
