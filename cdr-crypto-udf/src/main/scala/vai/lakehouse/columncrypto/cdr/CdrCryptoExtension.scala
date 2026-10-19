package vai.lakehouse.columncrypto.cdr

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionInfo, Literal, ScalaUDF}
import org.apache.spark.sql.types.StringType
import vai.lakehouse.columncrypto.cdr.audit.{AuditMeter, CdrAudit, CdrAuditRuntime}
import vai.lakehouse.keyprefix.PrefixSource

/**
 * Đăng ký `cdr_encrypt(table, value, fieldValue)` / `cdr_decrypt(table, value, fieldValue)`, bọc
 * [[CdrCipherCore]]. KHÁC `ColumnCryptoExtension` (`column_encrypt`/`column_decrypt`) ở chỗ:
 * builder trả về `ScalaUDF` (chạy code JVM thật trên executor) thay vì cây biểu thức built-in —
 * vì thuật toán của đối tác (vòng lặp cộng dồn key + AES/ECB) không biểu diễn được bằng hàm
 * built-in của Spark. Do đó:
 *   - Executor BẮT BUỘC có `DataLakeSecurity_jv8.jar` + `key-prefix-lib.jar` +
 *     `cdr-crypto-udf.jar` trên classpath (`spark.jars`) — khác hẳn `column_encrypt`/
 *     `column_decrypt` built-in hiện tại (executor không cần jar gì).
 *   - `keyPrefix` nằm trong CLOSURE của UDF (không phải `Literal` trong plan) — không cần
 *     `redactKeyPrefixInPlans` để che khỏi `EXPLAIN`, nhưng vẫn tồn tại trong bộ nhớ/task executor.
 *
 * Cấu hình theo THỨ TỰ ƯU TIÊN (`ChainedConfigSource`, dùng giá trị đầu tiên có mặt):
 *   1. `spark.cdrcrypto.*` (Spark conf) — namespace riêng, không đụng `spark.columncrypto.*` của
 *      `column_encrypt`, dùng khi cần cấu hình khác nhau giữa 2 bộ hàm (ví dụ trên sql-engine, nơi
 *      chỉ chỉnh được qua màn hình cấu hình `spark.sql.extensions`/Spark conf).
 *   2. Bộ biến môi trường `VAULT_*`/`CRYPTO_*` CHUNG với `column-crypto-lib` (`EnvConfigSource`,
 *      dùng trên `spark-app` — 2 bộ hàm dùng chung 1 giá trị keyPrefix từ cùng field Vault, theo
 *      lựa chọn thực tế đã xác nhận). Không cần khai thêm biến/Secret nào riêng cho CDR trên
 *      SparkApplication CRD nếu dùng cách này.
 *
 * Audit (`spark.cdrcrypto.audit.*`, mặc định tắt): mỗi lần hàm được dùng, biểu thức UDF được bọc trong `AuditTag` để ghi
 * START/END theo từng SQL execution — xem `docs/cdr-crypto/CDR_CRYPTO_AUDIT_LOG_PLAN.md`.
 *
 * Xem `docs/cdr-crypto/PARTNER_CDR_CRYPTO_UDF_PLAN.md`.
 *
 * @param prefixSource nơi lấy keyPrefix; mặc định dựng từ `spark.cdrcrypto.*` lần đầu cần dùng.
 * @param audit runtime audit, `None` khi tắt; được gọi ở mỗi lần dùng hàm nên phải rẻ (mặc định là `CdrAudit.shared`).
 */
class CdrCryptoExtension(prefixSource: () => PrefixSource, audit: () => Option[CdrAuditRuntime])
    extends (SparkSessionExtensions => Unit) {

  def this(prefixSource: () => PrefixSource) = this(prefixSource, () => CdrAudit.shared)

  /** Constructor không tham số mà Spark yêu cầu khi nạp qua `spark.sql.extensions`. */
  def this() = this(() => CdrCrypto.sharedPrefixSource)

  import CdrCryptoExtension._

  private lazy val source: PrefixSource = prefixSource()

  private def register(ext: SparkSessionExtensions, name: String, usage: String, encryptMode: Boolean): Unit =
    ext.injectFunction((
      FunctionIdentifier(name),
      new ExpressionInfo(getClass.getName, null, name, usage, ""),
      (args: Seq[Expression]) => args match {
        case Seq(table, value, fieldValue) =>
          val dataset = datasetOf(name, table)
          val prefix = source.read(dataset)
          // Cùng hàm biến đổi với DataFrame API (CdrCrypto.encryptColumns/decryptColumns) -> 2 đường luôn khớp nhau.
          // AES/ECB không IV -> mã hoá/giải mã TẤT ĐỊNH thật sự (khác GCM ngẫu nhiên của
          // column_encrypt), nên udfDeterministic = true là đúng bản chất, không phải xấp xỉ.
          def udfExpr(meter: Option[AuditMeter]): Expression =
            ScalaUDF(CdrCrypto.transform(prefix, encryptMode, meter), StringType,
              Seq(Cast(value, StringType), Cast(fieldValue, StringType)), udfDeterministic = true)
          audit() match {
            case Some(runtime) => runtime.tag(name, dataset, value, fieldValue, udfExpr)
            case None => udfExpr(None)
          }
        case other =>
          throw new IllegalArgumentException(s"$name expects 3 arguments (table, value, fieldValue) but got ${other.size}")
      }))

  override def apply(ext: SparkSessionExtensions): Unit = {
    // Fail-fast nếu jar đối tác đổi version/hành vi — xem CdrCipherCore.verifyCompatibility.
    CdrCipherCore.verifyCompatibility()
    // Fail-fast nếu cấu hình audit sai: engine không khởi động thay vì lỗi ở query đầu tiên. Chỉ làm được khi nạp qua
    // spark.sql.extensions (SparkContext đã chạy); SparkSession.Builder.withExtensions gọi tới đây TRƯỚC khi có SparkContext,
    // khi đó cấu hình được kiểm tra ở lần dùng hàm đầu tiên.
    if (SparkEnv.get != null) audit()
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

  private def datasetOf(function: String, arg: Expression): String = arg match {
    case Literal(v, StringType) if v != null => v.toString
    case _ => throw new IllegalArgumentException(
      s"$function: the first argument must be a constant string with the table/dataset name, " +
        s"e.g. $function('demo_db.sub_rel_product', <column>, <fieldColumn>)")
  }
}
