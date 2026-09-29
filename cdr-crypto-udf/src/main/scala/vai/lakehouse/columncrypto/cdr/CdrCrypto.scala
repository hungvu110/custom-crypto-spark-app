package vai.lakehouse.columncrypto.cdr

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType
import vai.lakehouse.keyprefix.{ChainedConfigSource, EnvConfigSource, PrefixSource, PrefixSourceFactory, SparkConfSource}

/**
 * Mã hoá/giải mã cột CDR theo thuật toán của đối tác qua **DataFrame API** — đối ứng của SQL function
 * `cdr_encrypt`/`cdr_decrypt` ([[CdrCryptoExtension]]). Cả 2 đường dùng CHUNG [[transform]] nên cho kết quả
 * giống hệt nhau (dữ liệu ghi bằng đường này giải mã được bằng đường kia). Là UDF thật, nên EXECUTOR cũng cần
 * `DataLakeSecurity_jv8.jar` + `cdr-crypto-udf.jar` trên classpath (`spark.jars`).
 *
 * {{{
 * val enc = CdrCrypto.encryptColumns(df, "sub_rel_product", keyField = "start_datetime", Seq("isdn"))
 * val dec = CdrCrypto.decryptColumns(enc, "sub_rel_product", keyField = "start_datetime", Seq("isdn"))
 * }}}
 *
 * keyPrefix được tra ở DRIVER (từ `spark.cdrcrypto.*` rồi tới biến môi trường `VAULT_*`/`CRYPTO_*`) và nằm trong
 * closure của UDF, không phải literal của plan nên không cần che khỏi `EXPLAIN`.
 */
object CdrCrypto {

  /** Namespace conf riêng, không đụng `spark.columncrypto.*` của `column-crypto-lib`. */
  val ConfPrefix = "spark.cdrcrypto."

  /**
   * Dùng chung cho cả tiến trình (extension bị khởi tạo lại theo từng SparkSession của Thrift Server,
   * nhưng cache prefix phải chỉ có 1). Cấu hình đọc từ SparkConf của driver rồi tới biến môi trường.
   */
  lazy val sharedPrefixSource: PrefixSource =
    PrefixSourceFactory.create(new ChainedConfigSource(Seq(
      new SparkConfSource(SparkEnv.get.conf, ConfPrefix),
      new EnvConfigSource())))

  // Fail-fast (1 lần/tiến trình) nếu jar đối tác đổi version/hành vi — xem CdrCipherCore.verifyCompatibility.
  private lazy val compatibilityVerified: Boolean = { CdrCipherCore.verifyCompatibility(); true }

  /**
   * Hàm biến đổi thực sự chạy trên executor: `(giá trị, giá trị keyField của dòng) => mã hoá/giải mã`, với
   * `keyInput = prefix + keyField`. NULL vào thì NULL ra. AES/ECB không IV nên kết quả TẤT ĐỊNH.
   */
  def transform(prefix: String, encryptMode: Boolean): (String, String) => String =
    (value, fieldValue) =>
      if (value == null || fieldValue == null) null
      else {
        val keyInput = prefix + fieldValue
        if (encryptMode) CdrCipherCore.encrypt(value, keyInput) else CdrCipherCore.decrypt(value, keyInput)
      }

  /** Tra keyPrefix của `datasetName` từ `prefixSource` (mặc định [[sharedPrefixSource]]) rồi dựng config. */
  def loadConfig(
    datasetName: String,
    keyField: String,
    encryptedColumns: Seq[String],
    prefixSource: PrefixSource = sharedPrefixSource
  ): CdrCryptoConfig = CdrCryptoConfig(prefixSource.read(datasetName), keyField, encryptedColumns)

  def encryptColumns(df: DataFrame, cfg: CdrCryptoConfig): DataFrame = apply(df, cfg, encryptMode = true)

  def decryptColumns(df: DataFrame, cfg: CdrCryptoConfig): DataFrame = apply(df, cfg, encryptMode = false)

  def encryptColumns(df: DataFrame, datasetName: String, keyField: String, encryptedColumns: Seq[String]): DataFrame =
    encryptColumns(df, loadConfig(datasetName, keyField, encryptedColumns))

  def decryptColumns(df: DataFrame, datasetName: String, keyField: String, encryptedColumns: Seq[String]): DataFrame =
    decryptColumns(df, loadConfig(datasetName, keyField, encryptedColumns))

  private def validate(df: DataFrame, cfg: CdrCryptoConfig): Unit = {
    val fields = df.schema.fieldNames.toSet
    require(cfg.encryptedColumns.nonEmpty, "encryptedColumns must not be empty")
    require(fields.contains(cfg.keyField),
      s"keyField '${cfg.keyField}' does not exist in schema: ${fields.mkString(", ")}")
    require(!cfg.encryptedColumns.contains(cfg.keyField),
      s"keyField '${cfg.keyField}' must not be in encryptedColumns — it must stay plaintext to be used as key material")
    val missing = cfg.encryptedColumns.filterNot(fields.contains)
    require(missing.isEmpty, s"encryptedColumns not found in schema: ${missing.mkString(", ")}")
  }

  // Tên cột là 1 phần duy nhất (không tách theo dấu '.').
  private def attr(name: String): Column = new Column(UnresolvedAttribute.quoted(name))

  private def apply(df: DataFrame, cfg: CdrCryptoConfig, encryptMode: Boolean): DataFrame = {
    require(compatibilityVerified)
    validate(df, cfg)
    val fn = udf(transform(cfg.keyPrefix, encryptMode))
    val keyCol = attr(cfg.keyField).cast(StringType)
    cfg.encryptedColumns.foldLeft(df) { (acc, c) =>
      acc.withColumn(c, fn(attr(c).cast(StringType), keyCol))
    }
  }
}
