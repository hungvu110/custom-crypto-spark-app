package vai.lakehouse.pseudonymverify.check

import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.policy.ColumnRule
import vai.lakehouse.pseudonymverify.data.SyntheticData

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/** Phép tính dùng chung giữa các check. */
private[pseudonymverify] object CheckSupport {

  val Pseudo  = "__pseudo"
  val Subject = SyntheticData.SubjectId
  val IsNull  = "__is_null"

  /** Định dạng output của từng chế độ (khớp TOÀN BỘ chuỗi). */
  def formatRegex(mode: PseudonymMode): String = mode match {
    case PseudonymMode.Hash    => "^k[0-9]+_[0-9a-f]{32}$"
    case PseudonymMode.Encrypt => "^e[0-9]+_[A-Za-z0-9_-]+$"
    case PseudonymMode.Mapping => "^t_[0-9a-f]{32}$"
  }

  /** Bỏ tiền tố `k1_`/`e1_`/`t_` để so với giá trị băm/mã hoá "trần" của kẻ tấn công. */
  def stripPrefix(c: Column): Column = regexp_replace(c, "^[a-z][0-9]*_", "")

  /** Mẫu định danh trực tiếp (tìm chuỗi con, biên không phải chữ/số) — check P3. */
  val IdentifierPatterns: Map[String, String] = Map(
    "isdn"        -> "(?<![0-9A-Za-z])(?:\\+?84|0)[35789][0-9]{8}(?![0-9A-Za-z])",
    "imei"        -> "(?<![0-9A-Za-z])[0-9]{15}(?![0-9A-Za-z])",
    "customer_id" -> "(?<![0-9A-Za-z])KH[0-9]{10}(?![0-9A-Za-z])")

  def quote(c: String): String = s"`${c.replace("`", "``")}`"

  def outCol(df: DataFrame, name: String): Column = df.col(quote(name))

  /** `(pseudo, __subject_id, __is_null)` của 1 cột giả danh: output ghép với đáp án theo khoá dòng. */
  def joined(run: TableRun, rule: ColumnRule): DataFrame = {
    val key = run.table.rowKey
    run.out.select(outCol(run.out, key).as(key), outCol(run.out, rule.outputName).cast(StringType).as(Pseudo))
      .join(run.table.truth.select(col(key), col(Subject), col(SyntheticData.nullFlag(rule.column)).as(IsNull)), Seq(key))
  }

  /** Gộp `joined` của mọi cột cùng loại định danh (cùng tên cột gốc) trên mọi bảng. */
  def byIdentifier(runs: Seq[TableRun]): Map[String, DataFrame] =
    runs.flatMap(r => r.rules.map(rule => rule.column -> joined(r, rule).select(Pseudo, Subject, IsNull)))
      .groupBy(_._1).map { case (column, parts) => column -> parts.map(_._2).reduce(_ unionByName _) }

  /** Tên cột giả danh trong output của `run`, hoặc tên gốc nếu cột không có trong policy. */
  def outputName(run: TableRun, column: String): String = run.policy.ruleFor(column).map(_.outputName).getOrElse(column)

  /** Mọi giá trị (cast string, không null) của mọi cột output, kèm "bảng.cột" nơi chứa. Cột: `where`, `value`. */
  def allValues(runs: Seq[TableRun]): DataFrame =
    runs.map { r =>
      val cells = r.out.columns.map(c => struct(lit(s"${r.name}.$c").as("where"), outCol(r.out, c).cast(StringType).as("value")))
      r.out.select(explode(array(cells: _*)).as("cell")).select(col("cell.where"), col("cell.value")).where(col("value").isNotNull)
    }.reduce(_ unionByName _)

  /** Số phần tử khác nhau giữa 2 multiset (cùng schema). */
  def multisetDiff(a: DataFrame, b: DataFrame): Long = a.exceptAll(b).count() + b.exceptAll(a).count()

  def passedIfZero(metric: String, observed: Long, samples: Seq[String] = Nil, detail: Map[String, Any] = Map.empty): Measured =
    Measured(metric, observed.toDouble, "== 0", observed == 0, samples, detail)
}
