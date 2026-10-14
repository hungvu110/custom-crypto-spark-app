package vai.lakehouse.pseudonymverify.check

import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.policy.{ColumnRule, TablePolicy}
import vai.lakehouse.pseudonymverify.config.VerifyConfig
import vai.lakehouse.pseudonymverify.data.SyntheticTable
import vai.lakehouse.pseudonymverify.key.KeyMaterial

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import scala.util.control.NonFatal

sealed abstract class Level(val name: String)

object Level {
  /** Vi phạm -> FAIL. */
  case object Must extends Level("MUST")
  /** Đối chứng: vi phạm nghĩa là bộ check không kết luận được -> ERROR. */
  case object Control extends Level("CONTROL")
  /** Chỉ báo cáo. */
  case object Info extends Level("INFO")
}

sealed abstract class Status(val name: String)

object Status {
  case object Pass extends Status("PASS")
  case object Fail extends Status("FAIL")
  case object Error extends Status("ERROR")
  case object Skipped extends Status("SKIPPED")
  case object Reported extends Status("INFO")
}

/** Kết quả đo của 1 check. `samples` chỉ chứa khoá dòng / subject_id / tên bảng.cột — KHÔNG BAO GIỜ giá trị gốc hay khoá. */
final case class Measured(
    metric: String,
    observed: Double,
    threshold: String,
    passed: Boolean,
    samples: Seq[String] = Nil,
    detail: Map[String, Any] = Map.empty)

final case class CheckResult(
    id: String,
    level: Level,
    status: Status,
    description: String,
    metric: String,
    observed: Double,
    threshold: String,
    samples: Seq[String],
    detail: Map[String, Any],
    error: Option[String])

final case class Check(
    id: String,
    level: Level,
    description: String,
    appliesTo: PseudonymMode => Boolean,
    measure: VerifyContext => Measured) {

  def run(ctx: VerifyContext): CheckResult =
    if (!appliesTo(ctx.mode)) CheckResult(id, level, Status.Skipped, description, "", 0, "", Nil, Map.empty, None)
    else Check.evaluate(id, level, description)(measure(ctx))
}

object Check {

  /** Đo rồi xếp trạng thái; exception trong lúc đo -> ERROR (không kết luận được), không làm dừng các check khác. */
  def evaluate(id: String, level: Level, description: String)(measure: => Measured): CheckResult =
    try {
      val m = measure
      val status = level match {
        case Level.Info => Status.Reported
        case _ if m.passed => Status.Pass
        case _ => Status.Fail
      }
      CheckResult(id, level, status, description, m.metric, m.observed, m.threshold, m.samples.take(5), m.detail, None)
    } catch {
      case NonFatal(e) =>
        CheckResult(id, level, Status.Error, description, "", Double.NaN, "", Nil, Map.empty,
          Some(s"${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("")}".take(500)))
    }
}

/**
 * Kết quả giả danh 1 bảng trong 1 lần chạy: output (đọc lại), lần chạy lại (U8), đường SQL (U9 — `Failure` nếu không dựng
 * được câu SQL, vd bảng ánh xạ không tồn tại; U9 khi đó ra ERROR thay vì làm dừng cả lần chạy).
 */
final case class TableRun(table: SyntheticTable, policy: TablePolicy, out: DataFrame, rerun: DataFrame, viaSql: Try[DataFrame]) {
  def rules: Seq[ColumnRule] = policy.columns
  def name: String = table.name
}

/** Mọi thứ check cần để đánh giá 1 chế độ (hoặc 1 mutation). */
final case class VerifyContext(
    spark: SparkSession,
    cfg: VerifyConfig,
    mode: PseudonymMode,
    label: String,
    runs: Seq[TableRun],
    keys: KeyMaterial,
    mappingPath: Option[String],
    scratchDir: String,
    outputDir: String,
    planTexts: Seq[String],
    timings: Map[String, Double],
    dictionary: DataFrame,
    attackCandidates: DataFrame) {

  def run(table: String): TableRun =
    runs.find(_.name == table).getOrElse(throw new IllegalStateException(s"Table '$table' was not pseudonymized in this run"))
}

object Checks {
  val all: Seq[Check] = LinkabilityChecks.all ++ PrivacyChecks.all

  def runAll(ctx: VerifyContext): Seq[CheckResult] = all.map(_.run(ctx))
}
