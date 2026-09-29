package vai.lakehouse.columncrypto.cdr

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionInfo, Literal, ScalaUDF}
import org.apache.spark.sql.types.StringType
import vai.lakehouse.columncrypto.prefix.{PrefixSource, PrefixSourceFactory, SparkConfSource}

/**
 * Đăng ký `cdr_encrypt(table, value, fieldValue)` / `cdr_decrypt(table, value, fieldValue)`, bọc
 * [[CdrCipherCore]]. KHÁC `ColumnCryptoExtension` (`column_encrypt`/`column_decrypt`) ở chỗ:
 * builder trả về `ScalaUDF` (chạy code JVM thật trên executor) thay vì cây biểu thức built-in —
 * vì thuật toán của đối tác (vòng lặp cộng dồn key + AES/ECB) không biểu diễn được bằng hàm
 * built-in của Spark. Do đó:
 *   - Executor BẮT BUỘC có `DataLakeSecurity_jv8.jar` + `column-crypto-lib.jar` +
 *     `cdr-crypto-udf.jar` trên classpath (`spark.jars`) — khác hẳn `column_encrypt`/
 *     `column_decrypt` built-in hiện tại (executor không cần jar gì).
 *   - `keyPrefix` nằm trong CLOSURE của UDF (không phải `Literal` trong plan) — không cần
 *     `redactKeyPrefixInPlans` để che khỏi `EXPLAIN`, nhưng vẫn tồn tại trong bộ nhớ/task executor.
 *
 * Cấu hình Vault RIÊNG, namespace `spark.cdrcrypto.*` (KHÔNG dùng chung `spark.columncrypto.*`
 * của `column_encrypt`) — vì đối tác cấp prefix qua kênh/đường dẫn Vault khác với keyPrefix tự
 * quản của mình (`docs/VDL - Mã hóa CDR.docx`: "prefix sẽ được gửi riêng cho từng đơn vị"). Hai
 * loại prefix cùng tồn tại trên 1 engine mà không đụng nhau, nhờ `SparkConfSource` nhận `prefix`
 * qua constructor (tái dùng nguyên `PrefixSourceFactory` của `column-crypto-lib`).
 *
 * Xem `docs/PARTNER_CDR_CRYPTO_UDF_PLAN.md`.
 *
 * @param prefixSource nơi lấy keyPrefix; mặc định dựng từ `spark.cdrcrypto.*` lần đầu cần dùng.
 */
class CdrCryptoExtension(prefixSource: () => PrefixSource) extends (SparkSessionExtensions => Unit) {

  /** Constructor không tham số mà Spark yêu cầu khi nạp qua `spark.sql.extensions`. */
  def this() = this(() => CdrCryptoExtension.sharedPrefixSource)

  import CdrCryptoExtension._

  private lazy val source: PrefixSource = prefixSource()

  private def register(ext: SparkSessionExtensions, name: String, usage: String, encryptMode: Boolean): Unit =
    ext.injectFunction((
      FunctionIdentifier(name),
      new ExpressionInfo(getClass.getName, null, name, usage, ""),
      (args: Seq[Expression]) => args match {
        case Seq(table, value, fieldValue) =>
          val prefix = source.read(datasetOf(name, table))
          val udfFn = (v: String, f: String) => {
            if (v == null || f == null) null
            else {
              val keyInput = prefix + f
              if (encryptMode) CdrCipherCore.encrypt(v, keyInput) else CdrCipherCore.decrypt(v, keyInput)
            }
          }
          // AES/ECB không IV -> mã hoá/giải mã TẤT ĐỊNH thật sự (khác GCM ngẫu nhiên của
          // column_encrypt), nên udfDeterministic = true là đúng bản chất, không phải xấp xỉ.
          ScalaUDF(udfFn, StringType, Seq(Cast(value, StringType), Cast(fieldValue, StringType)),
            udfDeterministic = true)
        case other =>
          throw new IllegalArgumentException(s"$name expects 3 arguments (table, value, fieldValue) but got ${other.size}")
      }))

  override def apply(ext: SparkSessionExtensions): Unit = {
    // Fail-fast nếu jar đối tác đổi version/hành vi — xem CdrCipherCore.verifyCompatibility.
    CdrCipherCore.verifyCompatibility()
    register(ext, EncryptFunction,
      s"$EncryptFunction(table, value, fieldValue) - encrypts `value` using the partner CDR " +
        "algorithm (AES/ECB), key derived from the dataset's Vault prefix and `fieldValue`.",
      encryptMode = true)
    register(ext, DecryptFunction,
      s"$DecryptFunction(table, value, fieldValue) - decrypts a value produced by $EncryptFunction " +
        "or by the partner's own TransformDL, with the same table and `fieldValue`.",
      encryptMode = false)
  }
}

object CdrCryptoExtension {

  val EncryptFunction = "cdr_encrypt"
  val DecryptFunction = "cdr_decrypt"

  /** Namespace conf riêng, không đụng `spark.columncrypto.*` — xem docstring của class. */
  val ConfPrefix = "spark.cdrcrypto."

  /**
   * Dùng chung cho cả tiến trình: mỗi kết nối của Thrift Server là 1 SparkSession riêng nên
   * extension bị khởi tạo lại nhiều lần, nhưng cache prefix phải chỉ có 1 — giống hệt lý do ở
   * `ColumnCryptoExtension.sharedPrefixSource`.
   */
  private lazy val sharedPrefixSource: PrefixSource =
    PrefixSourceFactory.create(new SparkConfSource(SparkEnv.get.conf, ConfPrefix))

  private def datasetOf(function: String, arg: Expression): String = arg match {
    case Literal(v, StringType) if v != null => v.toString
    case _ => throw new IllegalArgumentException(
      s"$function: the first argument must be a constant string with the table/dataset name, " +
        s"e.g. $function('sub_rel_product', <column>, <fieldColumn>)")
  }
}
