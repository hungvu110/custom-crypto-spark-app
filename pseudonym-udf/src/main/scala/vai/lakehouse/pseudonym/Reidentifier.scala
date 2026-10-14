package vai.lakehouse.pseudonym

import org.apache.spark.SparkEnv
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StringType

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
   * @param knownValues chỉ cho HASH: DataFrame 1 cột chứa các giá trị gốc ứng viên (vd toàn bộ `customer_id`).
   */
  def reidentify(
    targets: DataFrame,
    column: String,
    mode: PseudonymMode,
    output: String,
    knownValues: Option[DataFrame] = None
  ): DataFrame = mode match {
    case PseudonymMode.Hash =>
      val values = knownValues.getOrElse(throw new IllegalArgumentException(
        "HASH is one-way: reidentify needs knownValues (candidate original values) to hash forward and match"))
      require(values.columns.length == 1, "knownValues must have exactly one column")
      val h = new HmacPseudonymizer(keys.hmacKey)
      val hashUdf = udf((v: String) => h.pseudonymize(v))
      val raw = values.select(values.col(Pseudonymizer.quote(values.columns.head)).cast(StringType).as(Original))
        .where(col(Original).isNotNull).distinct()
      val lookup = raw.select(hashUdf(col(Original)).as(Key), col(Original))
      val joined = targets.withColumn(Key, targets.col(Pseudonymizer.quote(column)).cast(StringType))
        .join(lookup, Seq(Key), "left")
      joined.select(targets.columns.map(c =>
        if (c == column) col(Original).as(output) else joined.col(Pseudonymizer.quote(c))): _*)

    case PseudonymMode.Encrypt =>
      val e = new AesSivPseudonymizer(keys.aesSivKey)
      val revealUdf = udf((v: String) => e.decrypt(v))
      targets.select(targets.columns.map { c =>
        val current = targets.col(Pseudonymizer.quote(c))
        if (c == column) revealUdf(current.cast(StringType)).as(output) else current
      }: _*)

    case PseudonymMode.Mapping =>
      val path = mappingTablePath.getOrElse(throw new IllegalArgumentException(
        s"MAPPING reidentify needs the mapping table path (${PseudonymConf.MappingPathKey})"))
      new MappingTable(targets.sparkSession, path).reveal(targets, column, output)
  }
}

object Reidentifier {
  private val Key      = "__reidentify_key"
  private val Original = "__reidentify_original"

  lazy val shared: Reidentifier =
    new Reidentifier(PseudonymConf.sharedKeyProvider, PseudonymConf.mappingPath(SparkEnv.get.conf))
}
