package vai.lakehouse.columncrypto

import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * Mã hoá/giải mã cột qua DataFrame API. Công thức nằm ở [[CryptoExpressions]] (dùng chung với
 * SQL function), key sinh RIÊNG CHO TỪNG DÒNG từ keyPrefix (cố định theo dataset) và giá trị cột
 * keyField của chính dòng đó.
 */
object ColumnCrypto {

  private def validate(df: DataFrame, cfg: ColumnCryptoConfig): Unit = {
    val fields = df.schema.fieldNames.toSet
    require(fields.contains(cfg.keyField),
      s"keyField '${cfg.keyField}' does not exist in schema: ${fields.mkString(", ")}")
    require(!cfg.encryptedColumns.contains(cfg.keyField),
      s"keyField '${cfg.keyField}' must not be in encryptedColumns — it must stay plaintext to be used as key material")
    val missing = cfg.encryptedColumns.filterNot(fields.contains)
    require(missing.isEmpty, s"encryptedColumns not found in schema: ${missing.mkString(", ")}")
  }

  // Tên cột là 1 phần duy nhất (không tách theo dấu '.'), giống việc bọc backtick như bản SQL cũ.
  private def attr(name: String): Expression = UnresolvedAttribute.quoted(name)

  def encryptColumns(df: DataFrame, cfg: ColumnCryptoConfig): DataFrame = {
    validate(df, cfg)
    cfg.encryptedColumns.foldLeft(df) { (acc, c) =>
      acc.withColumn(c, new Column(CryptoExpressions.encrypt(attr(c), attr(cfg.keyField), cfg.keyPrefix)))
    }
  }

  def decryptColumns(df: DataFrame, cfg: ColumnCryptoConfig): DataFrame = {
    validate(df, cfg)
    cfg.encryptedColumns.foldLeft(df) { (acc, c) =>
      acc.withColumn(c, new Column(CryptoExpressions.decrypt(attr(c), attr(cfg.keyField), cfg.keyPrefix)))
    }
  }

  private val RedactionKey = "spark.sql.redaction.string.regex"

  /**
   * Regex che dữ liệu mới = regex đã cấu hình sẵn (nếu có) + prefix. Trả lại nguyên `existing` nếu
   * prefix đã có trong đó, để gọi lặp lại (mỗi câu query SQL) không làm regex phình ra mãi.
   */
  private[columncrypto] def redactionRegex(existing: Option[String], keyPrefix: String): String = {
    val mine = Pattern.quote(keyPrefix)
    existing.filter(_.nonEmpty) match {
      case Some(e) if e.contains(mine) => e
      case Some(e)                     => s"($e)|$mine"
      case None                        => mine
    }
  }

  /**
   * keyPrefix nằm dưới dạng literal trong biểu thức nên sẽ hiện ra trong explain /
   * queryExecution.toString (tab SQL của Spark UI, event log). Gọi 1 lần sau khi có keyPrefix
   * để Spark che nó; giữ nguyên regex che dữ liệu mà người dùng đã cấu hình sẵn.
   */
  def redactKeyPrefixInPlans(spark: SparkSession, cfg: ColumnCryptoConfig): Unit =
    spark.conf.set(RedactionKey, redactionRegex(spark.conf.getOption(RedactionKey), cfg.keyPrefix))

  /** Như trên nhưng cho đường SQL: gọi được trong lúc analyze, tác động lên conf của session đang chạy. */
  private[columncrypto] def redactKeyPrefixInPlans(conf: SQLConf, keyPrefix: String): Unit = {
    val existing = Option(conf.getConfString(RedactionKey, ""))
    val updated = redactionRegex(existing, keyPrefix)
    if (!existing.contains(updated)) conf.setConfString(RedactionKey, updated)
  }
}
