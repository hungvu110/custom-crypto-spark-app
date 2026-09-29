package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{call_function, col, lit}
import vai.lakehouse.columncrypto.cdr.CdrCrypto

/**
 * Cấu hình bước mã hoá cột — 2 cách gọi lib crypto, chọn bằng `CRYPTO_PROVIDER`:
 *  - `sql` (mặc định): gọi hàm SQL theo TÊN (vd `column_encrypt`, `cdr_encrypt`); app không biết lib nào, hàm có
 *    mặt hay không phụ thuộc extension nào được nạp qua `spark.jars` + `spark.sql.extensions`.
 *  - `cdr`: gọi thẳng DataFrame API của `cdr-crypto-udf` (`CdrCrypto.encryptColumns/decryptColumns`). Jar lib
 *    vẫn KHÔNG nằm trong fat jar (scope provided): phải nạp qua `spark.jars` (image target `cdr-crypto`).
 */
sealed trait CryptoSettings {
  def encryptedColumns: Seq[String]
  def keyField: String
  /** Mô tả ngắn để log. */
  def description: String
}

case class SqlFunctionCrypto(
  encryptFunction: String,
  decryptFunction: String,
  encryptedColumns: Seq[String],
  keyField: String
) extends CryptoSettings {
  override def description: String = s"SQL functions $encryptFunction/$decryptFunction"
}

case class CdrDataFrameCrypto(encryptedColumns: Seq[String], keyField: String) extends CryptoSettings {
  override def description: String = "DataFrame API (cdr-crypto-udf CdrCrypto)"
}

/**
 * Hợp đồng chung: mã hoá/giải mã các `encryptedColumns` của bảng `tableName`, dùng giá trị cột `keyField`
 * (giữ nguyên plaintext) của chính dòng đó làm nguyên liệu sinh key, kèm keyPrefix tra theo tên bảng.
 */
object CryptoStep {

  val ProviderSql = "sql"
  val ProviderCdr = "cdr"

  private val FunctionName = "^[A-Za-z_][A-Za-z0-9_]*$".r

  /**
   * `None` nếu không bật mã hoá (provider `sql` mà cả 2 tên hàm đều rỗng). Ném `IllegalArgumentException`
   * nếu cấu hình dở dang hoặc sai.
   */
  def parse(
    provider: String,
    encryptFunction: String,
    decryptFunction: String,
    encryptedColumns: String,
    keyField: String
  ): Option[CryptoSettings] = {
    val prov = provider.trim.toLowerCase match {
      case "" => ProviderSql
      case p  => p
    }
    val enc = encryptFunction.trim
    val dec = decryptFunction.trim
    require(prov == ProviderSql || prov == ProviderCdr,
      s"CRYPTO_PROVIDER must be '$ProviderSql' or '$ProviderCdr', got: '$provider'")

    if (prov == ProviderSql && enc.isEmpty && dec.isEmpty) None
    else {
      val columns = parseColumns(encryptedColumns)
      val key = keyField.trim
      require(key.nonEmpty, "CRYPTO_KEY_FIELD must be set when crypto is enabled")
      require(!columns.contains(key),
        s"CRYPTO_KEY_FIELD '$key' must not be in CRYPTO_ENCRYPTED_COLUMNS — it must stay plaintext to be used as key material")

      if (prov == ProviderCdr) {
        require(enc.isEmpty && dec.isEmpty,
          s"CRYPTO_ENCRYPT_FUNCTION/CRYPTO_DECRYPT_FUNCTION must be empty when CRYPTO_PROVIDER=$ProviderCdr " +
            "(the DataFrame API is called directly, not through SQL function names)")
        Some(CdrDataFrameCrypto(columns, key))
      } else {
        require(enc.nonEmpty && dec.nonEmpty,
          "CRYPTO_ENCRYPT_FUNCTION and CRYPTO_DECRYPT_FUNCTION must be set together (or both left empty to disable crypto)")
        Seq("CRYPTO_ENCRYPT_FUNCTION" -> enc, "CRYPTO_DECRYPT_FUNCTION" -> dec).foreach { case (name, fn) =>
          require(FunctionName.pattern.matcher(fn).matches(), s"$name must be a plain SQL function name, got: '$fn'")
        }
        Some(SqlFunctionCrypto(enc, dec, columns, key))
      }
    }
  }

  private def parseColumns(raw: String): Seq[String] = {
    val columns = raw.split(",").map(_.trim).filter(_.nonEmpty).toSeq
    require(columns.nonEmpty, "CRYPTO_ENCRYPTED_COLUMNS must list at least one column when crypto is enabled")
    columns
  }

  private def validate(df: DataFrame, settings: CryptoSettings): Unit = {
    val fields = df.schema.fieldNames.toSet
    require(fields.contains(settings.keyField),
      s"keyField '${settings.keyField}' does not exist in schema: ${fields.mkString(", ")}")
    val missing = settings.encryptedColumns.filterNot(fields.contains)
    require(missing.isEmpty, s"encryptedColumns not found in schema: ${missing.mkString(", ")}")
  }

  private def applyFunction(df: DataFrame, fn: String, tableName: String, settings: CryptoSettings): DataFrame = {
    val keyCol = col(BusinessLogic.quoteIdent(settings.keyField))
    settings.encryptedColumns.foldLeft(df) { (acc, c) =>
      acc.withColumn(c, call_function(fn, lit(tableName), col(BusinessLogic.quoteIdent(c)), keyCol))
    }
  }

  /** Mã hoá các cột đã cấu hình. `sql`: ném `AnalysisException` nếu hàm chưa được extension nào đăng ký. */
  def encrypt(df: DataFrame, tableName: String, settings: CryptoSettings): DataFrame = {
    validate(df, settings)
    settings match {
      case s: SqlFunctionCrypto => applyFunction(df, s.encryptFunction, tableName, s)
      case c: CdrDataFrameCrypto => CdrCrypto.encryptColumns(df, tableName, c.keyField, c.encryptedColumns)
    }
  }

  def decrypt(df: DataFrame, tableName: String, settings: CryptoSettings): DataFrame = {
    validate(df, settings)
    settings match {
      case s: SqlFunctionCrypto => applyFunction(df, s.decryptFunction, tableName, s)
      case c: CdrDataFrameCrypto => CdrCrypto.decryptColumns(df, tableName, c.keyField, c.encryptedColumns)
    }
  }
}
