package vai.lakehouse.columncrypto.sql

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import vai.lakehouse.columncrypto.{ColumnCrypto, CryptoExpressions}
import vai.lakehouse.columncrypto.prefix.{PrefixSource, PrefixSourceFactory, SparkConfSource}

/**
 * Đăng ký 2 SQL function để chạy mã hoá/giải mã ngay trên query console của sql-engine:
 * {{{
 * column_encrypt('<bảng>', <cột cần mã hoá>, <giá trị keyField>)
 * column_decrypt('<bảng>', <cột đã mã hoá>,  <giá trị keyField>)
 * }}}
 * Bật bằng conf tĩnh (cần restart engine):
 * `spark.sql.extensions=vai.lakehouse.columncrypto.sql.ColumnCryptoExtension`
 * (nếu đã có extension khác, ví dụ Ranger, thì nối bằng dấu phẩy). Nguồn keyPrefix cấu hình bằng
 * `spark.columncrypto.*` (xem [[PrefixSourceFactory]]).
 *
 * keyPrefix được tra ở DRIVER lúc analyze rồi nhúng vào biểu thức dưới dạng literal, nên executor
 * không cần gọi Vault và không cần jar của lib (biểu thức chỉ gồm hàm built-in của Spark).
 *
 * @param prefixSource nơi lấy keyPrefix; mặc định dựng từ `spark.columncrypto.*` lần đầu cần dùng.
 */
class ColumnCryptoExtension(prefixSource: () => PrefixSource) extends (SparkSessionExtensions => Unit) {

  /** Constructor không tham số mà Spark yêu cầu khi nạp qua `spark.sql.extensions`. */
  def this() = this(() => ColumnCryptoExtension.sharedPrefixSource)

  import ColumnCryptoExtension._

  private lazy val source: PrefixSource = prefixSource()

  private def register(
    ext: SparkSessionExtensions,
    name: String,
    usage: String,
    build: (Expression, Expression, String) => Expression
  ): Unit =
    ext.injectFunction((
      FunctionIdentifier(name),
      new ExpressionInfo(classOf[ColumnCryptoExtension].getName, null, name, usage, ""),
      (args: Seq[Expression]) => args match {
        case Seq(table, value, keyValue) =>
          val prefix = source.read(datasetOf(name, table))
          // Prefix nằm trong literal của plan: che khỏi explain / Spark UI / event log của session này.
          ColumnCrypto.redactKeyPrefixInPlans(SQLConf.get, prefix)
          build(value, keyValue, prefix)
        case _ =>
          throw new IllegalArgumentException(
            s"$name expects 3 arguments (table, value, keyValue) but got ${args.size}")
      }))

  override def apply(ext: SparkSessionExtensions): Unit = {
    register(ext, EncryptFunction,
      s"$EncryptFunction(table, value, keyValue) - AES-256-GCM encrypts `value` (Base64 output) with a key " +
        "derived from the table's keyPrefix and `keyValue`.",
      CryptoExpressions.encrypt)
    register(ext, DecryptFunction,
      s"$DecryptFunction(table, value, keyValue) - decrypts a value produced by $EncryptFunction with the same " +
        "table and `keyValue`.",
      CryptoExpressions.decrypt)
  }
}

object ColumnCryptoExtension {

  val EncryptFunction = "column_encrypt"
  val DecryptFunction = "column_decrypt"

  /**
   * Dùng chung cho cả tiến trình: mỗi kết nối của Thrift Server là 1 SparkSession riêng nên
   * extension bị khởi tạo lại nhiều lần, nhưng cache prefix phải chỉ có 1.
   * Cấu hình đọc từ SparkConf của driver, là conf tĩnh nạp lúc khởi động engine.
   */
  private lazy val sharedPrefixSource: PrefixSource =
    PrefixSourceFactory.create(new SparkConfSource(SparkEnv.get.conf))

  private def datasetOf(function: String, arg: Expression): String = arg match {
    case Literal(v, StringType) if v != null => v.toString
    case _ => throw new IllegalArgumentException(
      s"$function: the first argument must be a constant string with the table name, " +
        s"e.g. $function('customers', <column>, <keyColumn>)")
  }
}
