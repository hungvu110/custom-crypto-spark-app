package vai.lakehouse.pseudonymverify.report

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * In vài dòng đầu của 1 bảng dạng văn bản canh cột để quan sát bằng mắt: dữ liệu gốc và dữ liệu sau giả danh. Chỉ dùng
 * cho dữ liệu TỔNG HỢP (full) — không bao giờ gọi trong RUN_MODE=guard vì đó là dữ liệu thật.
 */
object Preview {

  /** `n` dòng đầu theo `orderBy` (khoá dòng) để 2 bên gốc/giả danh in cùng các dòng và so sánh được. */
  def table(title: String, df: DataFrame, orderBy: String, n: Int): String = {
    val columns = df.columns
    val rows = df.orderBy(col(s"`$orderBy`")).limit(n).collect().map { r =>
      columns.indices.map(i => if (r.isNullAt(i)) "NULL" else r.get(i).toString)
    }
    val widths = columns.indices.map(i => (columns(i) +: rows.map(_(i)).toSeq).map(_.length).max)
    def line(cells: Seq[String]) = cells.zip(widths).map { case (c, w) => c + " " * (w - c.length) }.mkString(" | ")
    val sep = widths.map("-" * _).mkString("-+-")
    (Seq(s"### $title (${rows.length} dòng đầu, sắp theo $orderBy)", line(columns.toSeq), sep) ++ rows.map(line))
      .mkString("\n") + "\n"
  }
}
