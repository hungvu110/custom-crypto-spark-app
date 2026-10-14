package vai.lakehouse.pseudonym.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType
import vai.lakehouse.pseudonym.{ConfigurationException, InvalidPolicyException, PseudonymMode}
import vai.lakehouse.pseudonym.core.{AesSivPseudonymizer, HmacPseudonymizer, KeyProvider}
import vai.lakehouse.pseudonym.policy.{ColumnRule, PseudonymPolicy, TablePolicy}

/**
 * Giả danh hoá qua **DataFrame API** — đối ứng của SQL function `pseudo_hash`/`pseudo_encrypt`
 * ([[PseudonymExtension]]). Cả 2 đường dùng chung lõi ([[HmacPseudonymizer]], [[AesSivPseudonymizer]]) nên cho
 * kết quả giống hệt nhau.
 *
 * {{{
 * val pseudonymizer = Pseudonymizer.fromConf(spark.sparkContext.getConf)   // khoá từ spark.pseudonym.* / biến môi trường
 * val out = pseudonymizer.pseudonymize(df, policies("loyalty.loyalty_txn"))
 * }}}
 *
 * Mỗi cột trong policy được thay TẠI CHỖ bằng định danh giả (cột `output`, mặc định cùng tên); nếu đổi tên thì cột
 * gốc bị xoá — output không bao giờ chứa đồng thời cột gốc và cột giả. Cột không có trong policy giữ nguyên.
 *
 * @param keys             nguồn khoá; chỉ khoá của chế độ được dùng mới bị đọc
 * @param mappingTablePath bảng ánh xạ Delta, chỉ cần khi policy có cột `MAPPING`
 */
final class Pseudonymizer(keys: KeyProvider, mappingTablePath: Option[String] = None) {

  private lazy val hmac   = new HmacPseudonymizer(keys.hmacKey)
  private lazy val aesSiv = new AesSivPseudonymizer(keys.aesSivKey)

  /** @throws InvalidPolicyException khi policy không khớp schema của `df`. */
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
      val path = mappingTablePath.getOrElse(throw new ConfigurationException(
        s"Column '${rule.column}' uses MAPPING but no mapping table path is configured (${PseudonymConf.MappingPathKey})"))
      new MappingTable(df.sparkSession, path).pseudonymize(df, rule.column, rule.outputName)
  }

  private def replace(df: DataFrame, rule: ColumnRule, fn: Column => Column): DataFrame =
    Columns.replace(df, rule.column, rule.outputName)(c => fn(c.cast(StringType)))

  private def validate(df: DataFrame, policy: TablePolicy): Unit = {
    val fields = df.columns.toSet
    val missing = policy.columns.map(_.column).filterNot(fields.contains)
    if (missing.nonEmpty)
      throw new InvalidPolicyException(
        s"Table '${policy.table}': policy columns not found in DataFrame: ${missing.mkString(", ")}")
    val clashes = policy.columns.filter(r => r.outputName != r.column && fields.contains(r.outputName))
    if (clashes.nonEmpty)
      throw new InvalidPolicyException(
        s"Table '${policy.table}': output column(s) already exist: ${clashes.map(_.outputName).mkString(", ")}")
  }
}

object Pseudonymizer {

  /** Khoá + bảng ánh xạ dựng từ `conf` tường minh (`spark.pseudonym.*` rồi biến môi trường). */
  def fromConf(conf: SparkConf): Pseudonymizer =
    new Pseudonymizer(PseudonymConf.keyProvider(conf), PseudonymConf.mappingPath(conf))
}
