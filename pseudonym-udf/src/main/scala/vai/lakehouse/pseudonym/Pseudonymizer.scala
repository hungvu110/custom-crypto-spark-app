package vai.lakehouse.pseudonym

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType

/**
 * Giả danh hoá qua **DataFrame API** — đối ứng của SQL function `pseudo_hash`/`pseudo_encrypt`
 * ([[sql.PseudonymExtension]]). Cả 2 đường dùng chung lõi ([[HmacPseudonymizer]], [[AesSivPseudonymizer]]) nên cho
 * kết quả giống hệt nhau.
 *
 * {{{
 * val out = Pseudonymizer(df, policies("loyalty.loyalty_txn"))   // khoá từ spark.pseudonym.* / biến môi trường
 * }}}
 *
 * Mỗi cột trong policy được thay TẠI CHỖ bằng định danh giả (cột `output`, mặc định cùng tên); nếu đổi tên thì cột
 * gốc bị xoá — output không bao giờ chứa đồng thời cột gốc và cột giả. Cột không có trong policy giữ nguyên.
 *
 * @param mappingTablePath bảng ánh xạ Delta, chỉ cần khi policy có cột `MAPPING`.
 */
final class Pseudonymizer(keys: KeyProvider, mappingTablePath: Option[String] = None) {

  private lazy val hmac   = new HmacPseudonymizer(keys.hmacKey)
  private lazy val aesSiv = new AesSivPseudonymizer(keys.aesSivKey)

  def pseudonymize(df: DataFrame, policy: TablePolicy): DataFrame = {
    validate(df, PseudonymPolicy.validated(policy))
    policy.columns.foldLeft(df)(applyRule)
  }

  private def applyRule(df: DataFrame, rule: ColumnRule): DataFrame = rule.mode match {
    case PseudonymMode.Hash =>
      val h = hmac
      replace(df, rule, udf((v: String) => h.pseudonymize(v)).withName("pseudo_hash")(_))
    case PseudonymMode.Encrypt =>
      val e = aesSiv
      replace(df, rule, udf((v: String) => e.encrypt(v)).withName("pseudo_encrypt")(_))
    case PseudonymMode.Mapping =>
      val path = mappingTablePath.getOrElse(throw new IllegalArgumentException(
        s"Column '${rule.column}' uses MAPPING but no mapping table path is configured (${PseudonymConf.MappingPathKey})"))
      new MappingTable(df.sparkSession, path).pseudonymize(df, rule.column, rule.outputName)
  }

  private def replace(df: DataFrame, rule: ColumnRule, fn: Column => Column): DataFrame =
    df.select(df.columns.map { c =>
      val column = df.col(Pseudonymizer.quote(c))
      if (c == rule.column) fn(column.cast(StringType)).as(rule.outputName) else column
    }: _*)

  private def validate(df: DataFrame, policy: TablePolicy): Unit = {
    val fields = df.columns.toSet
    val missing = policy.columns.map(_.column).filterNot(fields.contains)
    require(missing.isEmpty,
      s"Table '${policy.table}': policy columns not found in DataFrame: ${missing.mkString(", ")}")
    val clashes = policy.columns.filter(r => r.outputName != r.column && fields.contains(r.outputName))
    require(clashes.isEmpty,
      s"Table '${policy.table}': output column(s) already exist: ${clashes.map(_.outputName).mkString(", ")}")
  }
}

object Pseudonymizer {

  /** Dựng từ SparkConf của driver: khoá qua `spark.pseudonym.*`/biến môi trường, bảng ánh xạ qua `spark.pseudonym.mapping.path`. */
  lazy val shared: Pseudonymizer =
    new Pseudonymizer(PseudonymConf.sharedKeyProvider, PseudonymConf.mappingPath(SparkEnv.get.conf))

  def apply(df: DataFrame, policy: TablePolicy): DataFrame = shared.pseudonymize(df, policy)

  private[pseudonym] def quote(c: String): String = s"`${c.replace("`", "``")}`"
}
