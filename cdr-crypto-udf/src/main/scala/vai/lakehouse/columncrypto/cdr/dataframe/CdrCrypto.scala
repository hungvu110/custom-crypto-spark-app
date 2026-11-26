package vai.lakehouse.columncrypto.cdr

import scala.util.control.NonFatal

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
  def transform(prefix: String, encryptMode: Boolean): (String, String) => String = transform(prefix, encryptMode, context = "")

  /**
   * Như trên; `context` (vd `dataset x, column y, keyField z`) đi vào thông báo lỗi khi giải mã hoặc mã hoá thất bại, kèm HÌNH DẠNG của giá
   * trị keyField và của bản mã ([[shapeOf]]) và loại lỗi, để biết nguyên nhân mà không cần dữ liệu. Không bao giờ chứa giá trị, keyField
   * hay prefix.
   */
  def transform(prefix: String, encryptMode: Boolean, context: String): (String, String) => String =
    (value, fieldValue) =>
      if (value == null || fieldValue == null) null
      else {
        val keyInput = prefix + fieldValue
        try {
          if (encryptMode) CdrCipherCore.encrypt(value, keyInput) else CdrCipherCore.decrypt(value, keyInput)
        } catch {
          case NonFatal(e) => throw new IllegalStateException(failure(encryptMode, context, value, fieldValue, e), e)
        }
      }

  /** Như [[transform]], kèm đếm số dòng cho audit khi có `meter` (cộng trên executor qua accumulator). */
  def transform(prefix: String, encryptMode: Boolean, meter: Option[AuditMeter]): (String, String) => String =
    transform(prefix, encryptMode, meter, context = "")

  def transform(prefix: String, encryptMode: Boolean, meter: Option[AuditMeter], context: String): (String, String) => String = {
    val base = transform(prefix, encryptMode, context)
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

  private val MaxShapeRuns = 24

  /**
   * Hình dạng của một chuỗi, không chứa dữ liệu: độ dài và các đoạn liên tiếp cùng loại ký tự, `d` = chữ số, `a` = chữ cái, `_` = khoảng
   * trắng, dấu câu giữ nguyên. Vd `20260928123456` -> `14 chars d14`, `2026-09-28 12:34:56` -> `19 chars d4-d2-d2_1d2:d2:d2`. Đủ để thấy
   * keyField bị ép sang chuỗi khác dạng (timestamp, date, số thực), không đủ để đoán lại giá trị.
   */
  def shapeOf(text: String): String = {
    def kind(c: Char): String = if (c.isDigit) "d" else if (c.isLetter) "a" else if (c.isWhitespace) "_" else if (c.isControl) "?" else c.toString
    val runs = text.foldLeft(List.empty[(String, Int)]) {
      case ((k, n) :: rest, c) if kind(c) == k => (k, n + 1) :: rest
      case (acc, c) => (kind(c), 1) :: acc
    }.reverse.map { case (k, n) => if (k == "d" || k == "a" || k == "_" || k == "?") s"$k$n" else k * n }
    val body = if (runs.size > MaxShapeRuns) runs.take(MaxShapeRuns).mkString + "..." else runs.mkString
    if (text.isEmpty) "0 chars" else s"${text.length} chars $body"
  }

  /** Bản mã của đối tác là base64 của một bội số 16 byte (AES/ECB). Chỉ nói độ dài và có đúng dạng đó không. */
  private def ciphertextShape(value: String): String =
    scala.util.Try(java.util.Base64.getDecoder.decode(value)).toOption match {
      case Some(bytes) if bytes.nonEmpty && bytes.length % 16 == 0 =>
        s"${value.length} chars base64, ${bytes.length} bytes (a multiple of 16)"
      case _ => s"${value.length} chars, not base64 of a multiple of 16 bytes"
    }

  private def failure(encryptMode: Boolean, context: String, value: String, fieldValue: String, error: Throwable): String = {
    val root = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toSeq.last
    val kind = root.getClass.getSimpleName
    val reason = (encryptMode, root) match {
      case (false, _: javax.crypto.BadPaddingException) =>
        "wrong key: the prefix, or the keyField value as a string, differs from the one used to encrypt"
      case (false, _: javax.crypto.IllegalBlockSizeException | _: IllegalArgumentException) =>
        "not a provider ciphertext (plain value, or cut/changed)"
      case _ => "see the cause"
    }
    val where = if (context.isEmpty) "" else s" ($context)"
    val action = if (encryptMode) "encrypt" else "decrypt"
    val valueShape = if (encryptMode) s"value: ${value.length} chars" else s"ciphertext: ${ciphertextShape(value)}"
    s"CDR $action failed$where: $kind, $reason; keyField value: ${shapeOf(fieldValue)}; $valueShape"
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
        udf(transform(prefix, encryptMode, meter, s"dataset ${cfg.datasetName}, column $c, keyField ${cfg.keyField}"))
          .apply(attr(c).cast(StringType), keyAttr.cast(StringType)).expr
      val expr = audit match {
        case Some(runtime) => runtime.tag(function, cfg.datasetName, c, attr(c).expr, keyAttr.expr, udfExpr)
        case None => udfExpr(None)
      }
      acc.withColumn(c, new Column(expr))
    }
  }
}
