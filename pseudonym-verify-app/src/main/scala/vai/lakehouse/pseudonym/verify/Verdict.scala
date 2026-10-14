package vai.lakehouse.pseudonym.verify

import vai.lakehouse.pseudonym.verify.checks.{CheckResult, Level, Status}

sealed abstract class Verdict(val name: String, val exitCode: Int, val rank: Int)

/**
 * docs/pseudonym-verify-app/PLAN.md mục 5.3:
 *   check ném exception hoặc CONTROL (P6) không đạt -> ERROR (exit 2, không kết luận được)
 *   check MUST không đạt                            -> FAIL  (exit 1)
 *   còn lại                                         -> PASS  (exit 0)
 */
object Verdict {
  case object Pass extends Verdict("PASS", 0, 0)
  case object Fail extends Verdict("FAIL", 1, 1)
  case object Error extends Verdict("ERROR", 2, 2)

  def of(results: Seq[CheckResult]): Verdict =
    if (results.exists(r => r.status == Status.Error || (r.level == Level.Control && r.status == Status.Fail))) Error
    else if (results.exists(r => r.level == Level.Must && r.status == Status.Fail)) Fail
    else Pass

  /** Verdict chung của nhiều chế độ = verdict xấu nhất. */
  def worst(verdicts: Seq[Verdict]): Verdict = if (verdicts.isEmpty) Error else verdicts.maxBy(_.rank)

  /** ID các check không đạt (FAIL hoặc ERROR). */
  def failedIds(results: Seq[CheckResult]): Seq[String] =
    results.filter(r => r.status == Status.Fail || r.status == Status.Error).map(_.id)
}
