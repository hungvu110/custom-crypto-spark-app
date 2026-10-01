package org.example

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
  keyField: String
) {
  /** Mô tả ngắn để log. */
  def description: String = s"SQL functions $encryptFunction/$decryptFunction"
}

/**
 * Hợp đồng với mọi hàm mã hoá được hỗ trợ: `fn(tableName, value, keyValue)` — 3 tham số, tham số đầu là
 * hằng chuỗi tên dataset (nơi extension tra keyPrefix), tham số 2 là cột cần xử lý, tham số 3 là giá trị
 * cột `keyField` (giữ nguyên plaintext) của chính dòng đó. `column_encrypt` và `cdr_encrypt` đều theo hợp đồng này.
 */
object CryptoStep {

  private val FunctionName = "^[A-Za-z_][A-Za-z0-9_]*$".r

  /**
   * `None` nếu cả 2 hàm đều rỗng (không mã hoá). Ném `IllegalArgumentException` nếu cấu hình
   * dở dang hoặc sai (chỉ 1 hàm, tên hàm không phải identifier, thiếu cột/keyField, keyField nằm trong cột mã hoá).
   */
  def parse(
    encryptFunction: String,
    decryptFunction: String,
    encryptedColumns: String,
    keyField: String
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
      Some(CryptoSettings(enc, dec, columns, key))
    }
  }

  private def validate(df: DataFrame, settings: CryptoSettings): Unit = {
    val fields = df.schema.fieldNames.toSet
    require(fields.contains(settings.keyField),
      s"keyField '${settings.keyField}' does not exist in schema: ${fields.mkString(", ")}")
    val missing = settings.encryptedColumns.filterNot(fields.contains)
    require(missing.isEmpty, s"encryptedColumns not found in schema: ${missing.mkString(", ")}")
  }

  private def applyFunction(df: DataFrame, fn: String, tableName: String, settings: CryptoSettings): DataFrame = {
    validate(df, settings)
    val keyCol = col(BusinessLogic.quoteIdent(settings.keyField))
    settings.encryptedColumns.foldLeft(df) { (acc, c) =>
      acc.withColumn(c, call_function(fn, lit(tableName), col(BusinessLogic.quoteIdent(c)), keyCol))
    }
  }

  /** Mã hoá các cột đã cấu hình. Ném `AnalysisException` nếu hàm chưa được extension nào đăng ký. */
  def encrypt(df: DataFrame, tableName: String, settings: CryptoSettings): DataFrame =
    applyFunction(df, settings.encryptFunction, tableName, settings)

  def decrypt(df: DataFrame, tableName: String, settings: CryptoSettings): DataFrame =
    applyFunction(df, settings.decryptFunction, tableName, settings)
}
