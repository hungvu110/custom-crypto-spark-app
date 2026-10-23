package vai.lakehouse.columncrypto.cdr

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType
import vai.lakehouse.columncrypto.cdr.audit.{AuditMeter, CdrAudit, CdrAuditRuntime}
import vai.lakehouse.keyprefix.{ChainedConfigSource, ColumnKeySource, ColumnKeySourceFactory, EnvConfigSource, KeyIntent,
  SparkConfSource}

/**
 * Mã hoá/giải mã cột CDR theo thuật toán của đối tác qua **DataFrame API** — đối ứng của SQL function
 * `cdr_encrypt`/`cdr_decrypt` ([[CdrCryptoExtension]]). Cả 2 đường dùng CHUNG [[transform]] nên cho kết quả
 * giống hệt nhau (dữ liệu ghi bằng đường này giải mã được bằng đường kia). Là UDF thật, nên EXECUTOR cũng cần
 * `DataLakeSecurity_jv8.jar` + `cdr-crypto-udf.jar` trên classpath (`spark.jars`).
 *
 * Mỗi cột một key, xin qua DAK `by-column` (docs/cdr-crypto/CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md). Tên cột của DataFrame là tên
 * dùng để xin key: đổi tên cột trước khi gọi thì phải đổi lại cho khớp tên lưu ở DAK.
 * {{{
 * val enc = CdrCrypto.encryptColumns(df, "demo_db.sub_rel_product", keyField = "start_datetime", Seq("isdn"))
 * val dec = CdrCrypto.decryptColumns(enc, "demo_db.sub_rel_product", keyField = "start_datetime", Seq("isdn"))
 * }}}
 *
 * Key được tra ở DRIVER (từ `spark.cdrcrypto.*` rồi tới biến môi trường `DAK_*`/`CRYPTO_*`) và nằm trong closure của UDF,
 * không phải literal của plan nên không cần che khỏi `EXPLAIN`.
 */
object CdrCrypto {

  /** Namespace conf riêng, không đụng `spark.columncrypto.*` của `column-crypto-lib`. */
  val ConfPrefix = "spark.cdrcrypto."

  /**
   * Dùng chung cho cả tiến trình (extension bị khởi tạo lại theo từng SparkSession của Thrift Server,
   * nhưng cache key phải chỉ có 1). Cấu hình đọc từ SparkConf của driver rồi tới biến môi trường; nguồn chỉ có `dak`.
   */
  lazy val sharedColumnKeySource: ColumnKeySource =
    ColumnKeySourceFactory.create(new ChainedConfigSource(Seq(
      new SparkConfSource(SparkEnv.get.conf, ConfPrefix),
      new EnvConfigSource())))

  // Fail-fast (1 lần/tiến trình) nếu jar đối tác đổi version/hành vi — xem CdrCipherCore.verifyCompatibility.
  private lazy val compatibilityVerified: Boolean = { CdrCipherCore.verifyCompatibility(); true }

  /**
   * Hàm biến đổi thực sự chạy trên executor: `(giá trị, giá trị keyField của dòng) => mã hoá/giải mã`, với
   * `keyInput = key của cột + keyField`. NULL vào thì NULL ra. AES/ECB không IV nên kết quả TẤT ĐỊNH.
   */
  def transform(prefix: String, encryptMode: Boolean): (String, String) => String =
    (value, fieldValue) =>
      if (value == null || fieldValue == null) null
      else {
        val keyInput = prefix + fieldValue
        if (encryptMode) CdrCipherCore.encrypt(value, keyInput) else CdrCipherCore.decrypt(value, keyInput)
      }

  /** Như [[transform]], kèm đếm số dòng cho audit khi có `meter` (cộng trên executor qua accumulator). */
  def transform(prefix: String, encryptMode: Boolean, meter: Option[AuditMeter]): (String, String) => String = {
    val base = transform(prefix, encryptMode)
    meter match {
      case None => base
      case Some(m) => (value, fieldValue) => { m.record(value, fieldValue); base(value, fieldValue) }
    }
  }

  /**
   * Xin key của MỌI cột trong `encryptedColumns` bằng một lời gọi tới `source` (mặc định [[sharedColumnKeySource]]) rồi dựng
   * config. `intent` = [[KeyIntent.Encrypt]] khi mã hoá (cột chưa có key thì DAK mở YC), [[KeyIntent.Decrypt]] khi giải mã.
   */
  def loadConfig(
    datasetName: String,
    keyField: String,
    encryptedColumns: Seq[String],
    intent: KeyIntent,
    source: ColumnKeySource = sharedColumnKeySource
  ): CdrCryptoConfig =
    CdrCryptoConfig(source.readColumns(datasetName, encryptedColumns, intent), keyField, encryptedColumns, datasetName)

  def encryptColumns(df: DataFrame, cfg: CdrCryptoConfig): DataFrame = apply(df, cfg, encryptMode = true)

  def decryptColumns(df: DataFrame, cfg: CdrCryptoConfig): DataFrame = apply(df, cfg, encryptMode = false)

  def encryptColumns(df: DataFrame, datasetName: String, keyField: String, encryptedColumns: Seq[String]): DataFrame =
    encryptColumns(df, loadConfig(datasetName, keyField, encryptedColumns, KeyIntent.Encrypt))

  def decryptColumns(df: DataFrame, datasetName: String, keyField: String, encryptedColumns: Seq[String]): DataFrame =
    decryptColumns(df, loadConfig(datasetName, keyField, encryptedColumns, KeyIntent.Decrypt))

  private def validate(df: DataFrame, cfg: CdrCryptoConfig): Unit = {
    val fields = df.schema.fieldNames.toSet
    require(cfg.encryptedColumns.nonEmpty, "encryptedColumns must not be empty")
    require(fields.contains(cfg.keyField),
      s"keyField '${cfg.keyField}' does not exist in schema: ${fields.mkString(", ")}")
    require(!cfg.encryptedColumns.contains(cfg.keyField),
      s"keyField '${cfg.keyField}' must not be in encryptedColumns — it must stay plaintext to be used as key material")
    val missing = cfg.encryptedColumns.filterNot(fields.contains)
    require(missing.isEmpty, s"encryptedColumns not found in schema: ${missing.mkString(", ")}")
    val withoutKey = cfg.encryptedColumns.filter(c => cfg.keyFor(c).isEmpty)
    require(withoutKey.isEmpty, s"no key loaded for encryptedColumns: ${withoutKey.mkString(", ")}")
  }

  // Tên cột là 1 phần duy nhất (không tách theo dấu '.').
  private def attr(name: String): Column = new Column(UnresolvedAttribute.quoted(name))

  private def apply(df: DataFrame, cfg: CdrCryptoConfig, encryptMode: Boolean): DataFrame =
    applyWith(df, cfg, encryptMode, CdrAudit.shared)

  /** Tách khỏi [[apply]] để test truyền runtime audit riêng thay cho runtime dùng chung của tiến trình. */
  private[cdr] def applyWith(df: DataFrame, cfg: CdrCryptoConfig, encryptMode: Boolean,
                             audit: Option[CdrAuditRuntime]): DataFrame = {
    require(compatibilityVerified)
    validate(df, cfg)
    val function = if (encryptMode) CdrCryptoExtension.EncryptFunction else CdrCryptoExtension.DecryptFunction
    val keyAttr = attr(cfg.keyField)
    cfg.encryptedColumns.foldLeft(df) { (acc, c) =>
      val prefix = cfg.keyFor(c).get // validate đã bảo đảm mọi cột có key
      def udfExpr(meter: Option[AuditMeter]): Expression =
        udf(transform(prefix, encryptMode, meter)).apply(attr(c).cast(StringType), keyAttr.cast(StringType)).expr
      val expr = audit match {
        case Some(runtime) => runtime.tag(function, cfg.datasetName, c, attr(c).expr, keyAttr.expr, udfExpr)
        case None => udfExpr(None)
      }
      acc.withColumn(c, new Column(expr))
    }
  }
}
