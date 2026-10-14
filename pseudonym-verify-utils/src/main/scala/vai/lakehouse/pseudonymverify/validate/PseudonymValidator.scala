package vai.lakehouse.pseudonymverify.validate

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.types.StringType
import vai.lakehouse.pseudonym.PseudonymMode

/** Kết quả một phép kiểm. `detail` mô tả số đo để người đọc không phải suy ra. */
final case class CheckResult(name: String, passed: Boolean, detail: String)

/** Kết quả kiểm một bảng: đạt khi MỌI phép kiểm đạt. */
final case class ValidationReport(results: Seq[CheckResult]) {
  def passed: Boolean = results.nonEmpty && results.forall(_.passed)

  def render: String = results.map(r => s"[${if (r.passed) "PASS" else "FAIL"}] ${r.name}: ${r.detail}")
    .mkString("\n") + s"\nKẾT LUẬN: ${if (passed) "PASS" else "FAIL"}"
}

/**
 * So bảng nguồn với bảng đã khử định danh theo các cột `columns`. Không cần khoá, không sửa dữ liệu.
 *
 * Với mỗi cột giả danh: giá trị đích đúng định dạng của `mode`, null giữ nguyên, số giá trị khác nhau giữ nguyên
 * (ánh xạ 1-1 → còn liên kết được), không giá trị gốc nào còn trong đích. Toàn bảng: cùng số dòng, schema và dữ liệu
 * các cột còn lại không đổi.
 *
 * Giả định bảng đích là kết quả của đúng bảng nguồn này (OUTPUT_MODE=overwrite); không dùng cho bảng đích đã append.
 */
object PseudonymValidator {

  /** Định dạng token theo từng chế độ (xem docs/pseudonymization/ALGORITHMS.md). */
  def tokenPattern(mode: PseudonymMode): String = mode match {
    case PseudonymMode.Hash    => "^k[0-9]+_[0-9a-f]{32}$"
    case PseudonymMode.Encrypt => "^e[0-9]+_[A-Za-z0-9_-]+$"
    case PseudonymMode.Mapping => "^t_[0-9a-f]{32}$"
  }

  def validate(source: DataFrame, target: DataFrame, columns: Seq[String], mode: PseudonymMode): ValidationReport = {
    require(columns.nonEmpty, "columns must not be empty")
    val structural = structure(source, target, columns)
    // Thiếu cột / lệch schema thì các phép đo theo cột vô nghĩa (và ném AnalysisException) -> dừng ở đây.
    if (!structural.forall(_.passed)) ValidationReport(structural)
    else ValidationReport(structural ++ rows(source, target, columns) ++ columns.flatMap(column(source, target, _, mode)))
  }

  private def structure(source: DataFrame, target: DataFrame, columns: Seq[String]): Seq[CheckResult] = {
    val missing = columns.filterNot(c => source.columns.contains(c) && target.columns.contains(c))
    val sourceNames = source.columns.toSeq
    val targetNames = target.columns.toSeq
    Seq(
      CheckResult("cột giả danh tồn tại ở cả hai bảng", missing.isEmpty,
        if (missing.isEmpty) columns.mkString(", ") else s"thiếu: ${missing.mkString(", ")}"),
      CheckResult("tên cột nguồn == đích", sourceNames == targetNames,
        s"nguồn=${sourceNames.mkString(",")} đích=${targetNames.mkString(",")}"))
  }

  private def rows(source: DataFrame, target: DataFrame, columns: Seq[String]): Seq[CheckResult] = {
    val (s, t) = (source.count(), target.count())
    val others = source.columns.filterNot(columns.contains).toSeq
    val untouched =
      if (others.isEmpty) CheckResult("cột không giả danh giữ nguyên", passed = true, "(không có cột nào khác)")
      else {
        val (a, b) = (source.select(others.map(F.col): _*), target.select(others.map(F.col): _*))
        val diff = a.exceptAll(b).count() + b.exceptAll(a).count()
        CheckResult("cột không giả danh giữ nguyên", diff == 0, s"${others.size} cột, số dòng khác biệt=$diff")
      }
    Seq(CheckResult("số dòng nguồn == đích", s == t, s"$s / $t"), untouched)
  }

  private def column(source: DataFrame, target: DataFrame, c: String, mode: PseudonymMode): Seq[CheckResult] = {
    val src = source.select(F.col(s"`$c`").cast(StringType).as("v"))
    val tgt = target.select(F.col(s"`$c`").cast(StringType).as("v"))
    val srcValues = src.where(F.col("v").isNotNull).distinct()
    val tgtValues = tgt.where(F.col("v").isNotNull).distinct()

    val (nullsS, nullsT) = (src.where(F.col("v").isNull).count(), tgt.where(F.col("v").isNull).count())
    val (distinctS, distinctT) = (srcValues.count(), tgtValues.count())
    val badFormat = tgtValues.where(!F.col("v").rlike(tokenPattern(mode))).count()
    val leaked = tgtValues.join(srcValues, Seq("v"), "left_semi").count()

    Seq(
      CheckResult(s"$c: null giữ nguyên", nullsS == nullsT, s"$nullsS -> $nullsT"),
      CheckResult(s"$c: ánh xạ 1-1 (số giá trị khác nhau giữ nguyên)", distinctS == distinctT, s"$distinctS -> $distinctT"),
      CheckResult(s"$c: đúng định dạng ${mode.name}", badFormat == 0, s"giá trị sai định dạng=$badFormat"),
      CheckResult(s"$c: không sót giá trị gốc", leaked == 0, s"giá trị gốc còn trong đích=$leaked"))
  }
}
