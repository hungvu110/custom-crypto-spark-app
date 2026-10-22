package vai.lakehouse.columncrypto.cdr

import java.util.Locale

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionInfo, Literal, ScalaUDF}
import org.apache.spark.sql.types.StringType
import vai.lakehouse.columncrypto.cdr.audit.{AuditMeter, CdrAudit, CdrAuditRuntime}
import vai.lakehouse.keyprefix.{ColumnKeySource, KeyIntent}

/**
 * Đăng ký `cdr_encrypt(table, column, value, fieldValue)` / `cdr_decrypt(table, column, value, fieldValue)`, bọc
 * [[CdrCipherCore]]. Mỗi CỘT một key: `column` (hằng chuỗi) là tên cột dùng để xin key qua DAK `by-column`; `value` là dữ liệu
 * của từng dòng (cột, biểu thức hay hằng số). Tách hai tham số để chọn key đúng cả khi cột đã bị đổi tên (alias, view, CTE)
 * hoặc khi `value` là hằng số (tìm theo giá trị đã mã hoá) — docs/cdr-crypto/CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md.
 *
 * KHÁC `ColumnCryptoExtension` (`column_encrypt`/`column_decrypt`) ở chỗ builder trả về `ScalaUDF` (chạy code JVM thật trên
 * executor) thay vì cây biểu thức built-in — vì thuật toán của đối tác (vòng lặp cộng dồn key + AES/ECB) không biểu diễn được
 * bằng hàm built-in của Spark. Do đó:
 *   - Executor BẮT BUỘC có `DataLakeSecurity_jv8.jar` + `key-prefix-lib.jar` + `cdr-crypto-udf.jar` trên classpath
 *     (`spark.jars`).
 *   - Key nằm trong CLOSURE của UDF (không phải `Literal` trong plan) — không cần che khỏi `EXPLAIN`, nhưng vẫn tồn tại trong
 *     bộ nhớ/task executor.
 *
 * Cấu hình theo THỨ TỰ ƯU TIÊN (`ChainedConfigSource`, dùng giá trị đầu tiên có mặt): `spark.cdrcrypto.*` (Spark conf, riêng
 * với `spark.columncrypto.*`), rồi tới biến môi trường `DAK_*`/`CRYPTO_*` (SparkApplication). Nguồn key chỉ có `dak`, với client
 * nội bộ của DAK và `dak.tag=internal`.
 *
 * Audit (`spark.cdrcrypto.audit.*`, mặc định tắt): mỗi lần hàm được dùng, biểu thức UDF được bọc trong `AuditTag` để ghi
 * START/END theo từng SQL execution — xem `docs/cdr-crypto/CDR_CRYPTO_AUDIT_LOG_PLAN.md`.
 *
 * @param keySource nơi lấy key theo cột; mặc định dựng từ `spark.cdrcrypto.*` lần đầu cần dùng.
 * @param audit runtime audit, `None` khi tắt; được gọi ở mỗi lần dùng hàm nên phải rẻ (mặc định là `CdrAudit.shared`).
 */
class CdrCryptoExtension(keySource: () => ColumnKeySource, audit: () => Option[CdrAuditRuntime])
    extends (SparkSessionExtensions => Unit) {

  def this(keySource: () => ColumnKeySource) = this(keySource, () => CdrAudit.shared)

  /** Constructor không tham số mà Spark yêu cầu khi nạp qua `spark.sql.extensions`. */
  def this() = this(() => CdrCrypto.sharedColumnKeySource)

  import CdrCryptoExtension._

  private lazy val source: ColumnKeySource = keySource()

  private def register(ext: SparkSessionExtensions, name: String, usage: String, encryptMode: Boolean): Unit =
    ext.injectFunction((
      FunctionIdentifier(name),
      new ExpressionInfo(getClass.getName, null, name, usage, ""),
      (args: Seq[Expression]) => args match {
        case Seq(table, column, value, fieldValue) =>
          val dataset = constantArg(name, table, "first", "the table/dataset name")
          val columnName = constantArg(name, column, "second", "the column name (the key is chosen by this name)")
          val intent = if (encryptMode) KeyIntent.Encrypt else KeyIntent.Decrypt
          val prefix = keyOf(source.readColumns(dataset, Seq(columnName), intent), dataset, columnName)
          // Cùng hàm biến đổi với DataFrame API (CdrCrypto.encryptColumns/decryptColumns) -> 2 đường luôn khớp nhau.
          // AES/ECB không IV -> mã hoá/giải mã TẤT ĐỊNH thật sự (khác GCM ngẫu nhiên của
          // column_encrypt), nên udfDeterministic = true là đúng bản chất, không phải xấp xỉ.
          def udfExpr(meter: Option[AuditMeter]): Expression =
            ScalaUDF(CdrCrypto.transform(prefix, encryptMode, meter), StringType,
              Seq(Cast(value, StringType), Cast(fieldValue, StringType)), udfDeterministic = true)
          audit() match {
            case Some(runtime) => runtime.tag(name, dataset, columnName, value, fieldValue, udfExpr)
            case None => udfExpr(None)
          }
        case other =>
          throw new IllegalArgumentException(
            s"$name expects 4 arguments (table, column, value, fieldValue) but got ${other.size}, e.g. ${example(name)}")
      }))

  override def apply(ext: SparkSessionExtensions): Unit = {
    // Fail-fast nếu jar đối tác đổi version/hành vi — xem CdrCipherCore.verifyCompatibility.
    CdrCipherCore.verifyCompatibility()
    // Fail-fast nếu cấu hình audit sai: engine không khởi động thay vì lỗi ở query đầu tiên. Chỉ làm được khi nạp qua
    // spark.sql.extensions (SparkContext đã chạy); SparkSession.Builder.withExtensions gọi tới đây TRƯỚC khi có SparkContext,
    // khi đó cấu hình được kiểm tra ở lần dùng hàm đầu tiên.
    if (SparkEnv.get != null) audit()
    register(ext, EncryptFunction,
      s"$EncryptFunction(table, column, value, fieldValue) - encrypts `value` using the partner CDR algorithm (AES/ECB), " +
        "with the DAK key of `column` in `table` and `fieldValue` of the same row.",
      encryptMode = true)
    register(ext, DecryptFunction,
      s"$DecryptFunction(table, column, value, fieldValue) - decrypts a value produced by $EncryptFunction " +
        "or by the partner's own TransformDL, with the same table, column and `fieldValue`.",
      encryptMode = false)
  }
}

object CdrCryptoExtension {

  val EncryptFunction = "cdr_encrypt"
  val DecryptFunction = "cdr_decrypt"

  private def example(function: String): String =
    s"$function('demo_db.sub_rel_product', 'isdn', isdn, start_datetime)"

  private def constantArg(function: String, arg: Expression, position: String, meaning: String): String = arg match {
    case Literal(v, StringType) if v != null => v.toString
    case _ => throw new IllegalArgumentException(
      s"$function: the $position argument must be a constant string with $meaning, e.g. ${example(function)}")
  }

  // Nguồn key trả map khoá chữ thường; thiếu cột được hỏi là nguồn vi phạm hợp đồng, không phải lỗi người dùng.
  private def keyOf(keys: Map[String, String], dataset: String, column: String): String =
    keys.getOrElse(column.toLowerCase(Locale.ROOT),
      throw new IllegalStateException(s"Key source returned no key for column '$column' of '$dataset'"))
}
