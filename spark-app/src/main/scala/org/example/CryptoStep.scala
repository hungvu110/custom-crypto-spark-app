package org.example

import java.util.Locale

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{call_function, col, lit}

/**
 * Cấu hình bước mã hoá cột. Tên 2 hàm SQL do lúc deploy quyết định (vd `column_encrypt` /
 * `cdr_encrypt`): app KHÔNG biết và KHÔNG nhúng lib crypto nào, chỉ gọi hàm theo tên — hàm có mặt
 * hay không phụ thuộc extension nào được nạp qua `spark.jars` + `spark.sql.extensions`.
 */
case class CryptoSettings(
  encryptFunction: String,
  decryptFunction: String,
  encryptedColumns: Seq[String],
  keyField: String,
  keyScope: KeyScope = KeyScope.Table
) {
  /** Mô tả ngắn để log. */
  def description: String = s"SQL functions $encryptFunction/$decryptFunction (key per ${keyScope.name})"
}

/**
 * Hàm mã hoá dùng key theo bảng hay theo cột (`CRYPTO_KEY_SCOPE`), quyết định số tham số app truyền vào hàm:
 *  - `table` (mặc định): `fn('database.table', value, keyValue)` — vd `column_encrypt`.
 *  - `column`: `fn('database.table', 'column', value, keyValue)` — vd `cdr_encrypt` key theo cột; tên cột chữ thường là tên
 *    dùng để xin key.
 * App không đoán theo tên hàm vì nó không biết extension nào đang được nạp.
 */
sealed abstract class KeyScope(val name: String)

object KeyScope {
  case object Table extends KeyScope("table")
  case object Column extends KeyScope("column")

  val All: Seq[KeyScope] = Seq(Table, Column)
}

/**
 * Hợp đồng với mọi hàm mã hoá được hỗ trợ: tham số đầu là hằng chuỗi tên key `'database.table'` (nơi extension tra key, xem
 * [[keyName]]), rồi (chỉ khi [[KeyScope.Column]]) hằng chuỗi tên cột, rồi cột cần xử lý, cuối cùng là giá trị cột `keyField`
 * (giữ nguyên plaintext) của chính dòng đó. `column_encrypt` theo dạng 3 tham số, `cdr_encrypt` theo dạng 4 tham số.
 */
object CryptoStep {

  private val FunctionName = "^[A-Za-z_][A-Za-z0-9_]*$".r

  /**
   * Tên key tra keyPrefix: `DB_NAME.TABLE_NAME` chữ thường. Nguồn `dak` bắt buộc dạng hai phần; chữ thường để
   * fallback thủ công về Vault (vốn không chuẩn hoá hoa thường) vẫn tìm đúng secret `.../<database>.<table>`.
   */
  def keyName(dbName: String, tableName: String): String =
    s"${dbName.trim}.${tableName.trim}".toLowerCase(Locale.ROOT)

  /**
   * `None` nếu cả 2 hàm đều rỗng (không mã hoá). Ném `IllegalArgumentException` nếu cấu hình
   * dở dang hoặc sai (chỉ 1 hàm, tên hàm không phải identifier, thiếu cột/keyField, keyField nằm trong cột mã hoá).
   */
  def parse(
    encryptFunction: String,
    decryptFunction: String,
    encryptedColumns: String,
    keyField: String,
    keyScope: String = KeyScope.Table.name
  ): Option[CryptoSettings] = {
    val enc = encryptFunction.trim
    val dec = decryptFunction.trim
    if (enc.isEmpty && dec.isEmpty) None
    else {
      require(enc.nonEmpty && dec.nonEmpty,
        "CRYPTO_ENCRYPT_FUNCTION and CRYPTO_DECRYPT_FUNCTION must be set together (or both left empty to disable crypto)")
      Seq("CRYPTO_ENCRYPT_FUNCTION" -> enc, "CRYPTO_DECRYPT_FUNCTION" -> dec).foreach { case (name, fn) =>
        require(FunctionName.pattern.matcher(fn).matches(), s"$name must be a plain SQL function name, got: '$fn'")
      }
      val columns = encryptedColumns.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      require(columns.nonEmpty, "CRYPTO_ENCRYPTED_COLUMNS must list at least one column when crypto is enabled")
      val key = keyField.trim
      require(key.nonEmpty, "CRYPTO_KEY_FIELD must be set when crypto is enabled")
      require(!columns.contains(key),
        s"CRYPTO_KEY_FIELD '$key' must not be in CRYPTO_ENCRYPTED_COLUMNS — it must stay plaintext to be used as key material")
      val scope = KeyScope.All.find(_.name == keyScope.trim.toLowerCase(Locale.ROOT)).getOrElse(throw new IllegalArgumentException(
        s"CRYPTO_KEY_SCOPE must be one of ${KeyScope.All.map(_.name).mkString(", ")}, got: '${keyScope.trim}'"))
      Some(CryptoSettings(enc, dec, columns, key, scope))
    }
  }

  private def validate(df: DataFrame, settings: CryptoSettings): Unit = {
    val fields = df.schema.fieldNames.toSet
    require(fields.contains(settings.keyField),
      s"keyField '${settings.keyField}' does not exist in schema: ${fields.mkString(", ")}")
    val missing = settings.encryptedColumns.filterNot(fields.contains)
    require(missing.isEmpty, s"encryptedColumns not found in schema: ${missing.mkString(", ")}")
  }

  private def applyFunction(df: DataFrame, fn: String, keyName: String, settings: CryptoSettings): DataFrame = {
    validate(df, settings)
    val keyCol = col(BusinessLogic.quoteIdent(settings.keyField))
    settings.encryptedColumns.foldLeft(df) { (acc, c) =>
      val value = col(BusinessLogic.quoteIdent(c))
      val result = settings.keyScope match {
        case KeyScope.Table  => call_function(fn, lit(keyName), value, keyCol)
        case KeyScope.Column => call_function(fn, lit(keyName), lit(c.toLowerCase(Locale.ROOT)), value, keyCol)
      }
      acc.withColumn(c, result)
    }
  }

  /**
   * Mã hoá các cột đã cấu hình bằng key `keyName` (dùng [[keyName]] để dựng). Ném `AnalysisException` nếu hàm
   * chưa được extension nào đăng ký.
   */
  def encrypt(df: DataFrame, keyName: String, settings: CryptoSettings): DataFrame =
    applyFunction(df, settings.encryptFunction, keyName, settings)

  def decrypt(df: DataFrame, keyName: String, settings: CryptoSettings): DataFrame =
    applyFunction(df, settings.decryptFunction, keyName, settings)
}
