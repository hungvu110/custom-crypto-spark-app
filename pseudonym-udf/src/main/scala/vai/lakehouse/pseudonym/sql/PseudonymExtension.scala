package vai.lakehouse.pseudonym.sql

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionInfo, ScalaUDF}
import org.apache.spark.sql.types.StringType
import vai.lakehouse.pseudonym.{AesSivPseudonymizer, HmacPseudonymizer, KeyProvider, PseudonymConf}

/**
 * Đăng ký SQL function sinh định danh giả: `pseudo_hash(value)` (HASH) và `pseudo_encrypt(value)` (ENCRYPT).
 * Builder trả về `ScalaUDF` — giống `CdrCryptoExtension`: khoá nằm trong CLOSURE của UDF, không phải `Literal` của
 * plan, nên `EXPLAIN` chỉ hiện tên hàm. Executor phải có `key-prefix-lib.jar` + `pseudonym-udf.jar` (`spark.jars`).
 *
 * CHỈ nạp trên engine ETL/hạn chế, KHÔNG nạp lên sql-engine của analyst: ai gọi được `pseudo_hash` tuỳ ý là có
 * "oracle" — liệt kê ~10^9 SĐT là đảo ngược được toàn bộ, tương đương lộ khoá. Hàm giải mã nằm ở
 * [[PseudonymRevealExtension]] (nạp riêng).
 *
 * Nạp qua `spark.sql.extensions=vai.lakehouse.pseudonym.sql.PseudonymExtension`; khoá cấu hình qua
 * `spark.pseudonym.*` (xem [[PseudonymConf]]).
 */
class PseudonymExtension(keys: () => KeyProvider) extends (SparkSessionExtensions => Unit) {

  /** Constructor không tham số mà Spark yêu cầu khi nạp qua `spark.sql.extensions`. */
  def this() = this(() => PseudonymConf.sharedKeyProvider)

  private lazy val hmac   = new HmacPseudonymizer(keys().hmacKey)
  private lazy val aesSiv = new AesSivPseudonymizer(keys().aesSivKey)

  override def apply(ext: SparkSessionExtensions): Unit = {
    SqlFunctions.register(ext, getClass, "pseudo_hash",
      "pseudo_hash(value) - pseudonymizes `value` with HMAC-SHA256 (secret key), returns k<version>_<32 hex>.",
      () => { val h = hmac; (v: String) => h.pseudonymize(v) })
    SqlFunctions.register(ext, getClass, "pseudo_encrypt",
      "pseudo_encrypt(value) - pseudonymizes `value` with deterministic AES-256-SIV, returns e<version>_<base64url>.",
      () => { val e = aesSiv; (v: String) => e.encrypt(v) })
  }
}

/**
 * `pseudo_reveal(value)` — giải mã định danh giả của `pseudo_encrypt`. Tách extension riêng để chỉ nạp ở môi trường
 * tái định danh có thẩm quyền (mục c của tài liệu).
 */
class PseudonymRevealExtension(keys: () => KeyProvider) extends (SparkSessionExtensions => Unit) {

  def this() = this(() => PseudonymConf.sharedKeyProvider)

  private lazy val aesSiv = new AesSivPseudonymizer(keys().aesSivKey)

  override def apply(ext: SparkSessionExtensions): Unit =
    SqlFunctions.register(ext, getClass, "pseudo_reveal",
      "pseudo_reveal(pseudonym) - decrypts a value produced by pseudo_encrypt (same key version).",
      () => { val e = aesSiv; (v: String) => e.decrypt(v) })
}

private[sql] object SqlFunctions {

  /** `fn` là factory: khoá chỉ được đọc khi câu SQL đầu tiên dùng tới hàm, không phải lúc nạp extension. */
  def register(ext: SparkSessionExtensions, owner: Class[_], name: String, usage: String,
               fn: () => String => String): Unit =
    ext.injectFunction((
      FunctionIdentifier(name),
      new ExpressionInfo(owner.getName, null, name, usage, ""),
      (args: Seq[Expression]) => args match {
        case Seq(value) =>
          ScalaUDF(fn(), StringType, Seq(Cast(value, StringType)), udfName = Some(name), udfDeterministic = true)
        case other =>
          throw new IllegalArgumentException(s"$name expects 1 argument (value) but got ${other.size}")
      }))
}
