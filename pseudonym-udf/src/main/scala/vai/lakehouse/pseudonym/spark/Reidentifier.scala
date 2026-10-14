package vai.lakehouse.pseudonym.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StringType
import vai.lakehouse.pseudonym.{ConfigurationException, PseudonymMode}
import vai.lakehouse.pseudonym.core.{AesSivPseudonymizer, HmacPseudonymizer, KeyProvider}

/**
 * Tái định danh CÓ THẨM QUYỀN — luồng cá nhân hoá ưu đãi Viettel++: hệ thống phân tích chỉ ra danh sách định danh
 * giả, hệ thống gửi ưu đãi (có khoá / quyền đọc bảng ánh xạ) đổi lại thành giá trị gốc. Chỉ chạy trong môi trường
 * có quyền đọc khoá hoặc bảng ánh xạ (mục c của tài liệu).
 *
 *   - HASH (một chiều): băm XUÔI danh sách giá trị gốc đã biết (`knownValues`) rồi join với danh sách định danh giả.
 *   - ENCRYPT: giải mã trực tiếp.
 *   - MAPPING: join ngược bảng ánh xạ theo token.
 *
 * Định danh giả không tìm được giá trị gốc -> `null`.
 */
final class Reidentifier(keys: KeyProvider, mappingTablePath: Option[String] = None) {
  import Reidentifier._

  /**
   * Thay cột định danh giả `column` của `targets` bằng giá trị gốc (cột `output`, cùng vị trí).
   *
   * @param knownValues chỉ cho HASH: DataFrame 1 cột chứa các giá trị gốc ứng viên (vd toàn bộ `customer_id`).
   * @throws ConfigurationException khi thiếu `knownValues` (HASH) hoặc đường dẫn bảng ánh xạ (MAPPING)
   */
  def reidentify(
    targets: DataFrame,
    column: String,
    mode: PseudonymMode,
    output: String,
    knownValues: Option[DataFrame] = None
  ): DataFrame = mode match {
    case PseudonymMode.Hash    => viaForwardHash(targets, column, output, knownValues)
    case PseudonymMode.Encrypt => viaDecrypt(targets, column, output)
    case PseudonymMode.Mapping => viaMappingTable(targets, column, output)
  }

  private def viaForwardHash(targets: DataFrame, column: String, output: String, knownValues: Option[DataFrame]): DataFrame = {
    val values = knownValues.getOrElse(throw new ConfigurationException(
      "HASH is one-way: reidentify needs knownValues (candidate original values) to hash forward and match"))
    if (values.columns.length != 1) throw new ConfigurationException("knownValues must have exactly one column")
    val hmac = new HmacPseudonymizer(keys.hmacKey)
    val hashUdf = udf((v: String) => hmac.pseudonymize(v))
    val raw = values.select(Columns.ref(values, values.columns.head).cast(StringType).as(Original))
      .where(col(Original).isNotNull).distinct()
    val lookup = raw.select(hashUdf(col(Original)).as(Key), col(Original))
    val joined = targets.withColumn(Key, Columns.ref(targets, column).cast(StringType)).join(lookup, Seq(Key), "left")
    joined.select(targets.columns.map(c => if (c == column) col(Original).as(output) else Columns.ref(joined, c)): _*)
  }

  private def viaDecrypt(targets: DataFrame, column: String, output: String): DataFrame = {
    val aesSiv = new AesSivPseudonymizer(keys.aesSivKey)
    val revealUdf = udf((v: String) => aesSiv.decrypt(v))
    Columns.replace(targets, column, output)(c => revealUdf(c.cast(StringType)))
  }

  private def viaMappingTable(targets: DataFrame, column: String, output: String): DataFrame = {
    val path = mappingTablePath.getOrElse(throw new ConfigurationException(
      s"MAPPING reidentify needs the mapping table path (${PseudonymConf.MappingPathKey})"))
    new MappingTable(targets.sparkSession, path).reveal(targets, column, output)
  }
}

object Reidentifier {
  private val Key      = "__reidentify_key"
  private val Original = "__reidentify_original"

  /** Khoá + bảng ánh xạ dựng từ `conf` tường minh (`spark.pseudonym.*` rồi biến môi trường). */
  def fromConf(conf: SparkConf): Reidentifier =
    new Reidentifier(PseudonymConf.keyProvider(conf), PseudonymConf.mappingPath(conf))
}
