package vai.lakehouse.pseudonymverify.job

import vai.lakehouse.pseudonymverify.check.Checks
import vai.lakehouse.pseudonymverify.logging.Log
import vai.lakehouse.pseudonymverify.report.Verdict

/**
 * RUN_MODE=selftest — kiểm chứng CHÍNH bộ check: chạy từng mutation (bản giả danh cố ý sai) của các chế độ trong
 * PSEUDO_MODES. PASS ⇔ mọi mutation đã chạy đều bị bắt (verdict ≠ PASS và các check kỳ vọng nằm trong danh sách fail).
 */
object SelftestJob extends VerifyJob {

  override def run(s: VerifySession): Verdict = {
    val cfg = s.cfg
    val mutations = Mutations.all.filter(m => cfg.pseudoModes.contains(m.mode))
    val outcomes = mutations.map { m =>
      Log.section(s"MUTATION ${m.id} (${m.mode.name}) — ${m.description}")
      Log.resetCapture()
      val base = s"${cfg.runPath}/selftest/${m.id}"
      val restricted = s"${cfg.restrictedDir}/selftest/${m.id}"
      val mapping = s.mappingPathFor(m.mode, s"$restricted/pseudonym_map")
      val strategy = m.strategy(s.keys)
      val checks =
        try Checks.runAll(s.pipeline.run(m.mode, strategy, s"selftest/${m.id}", s"$base/output", s"$restricted/scratch",
          mapping, measureTimings = false))
        finally strategy.cleanup(s.spark)
      val verdict = Verdict.of(checks)
      val failed = Verdict.failedIds(checks)
      val caught = verdict != Verdict.Pass && m.expected.forall(failed.contains)
      val line = s"${m.id}: verdict=${verdict.name} fail=[${failed.mkString(",")}] expected=[${m.expected.mkString(",")}]"
      if (caught) Log.ok(s"$line -> CAUGHT") else Log.fail(s"$line -> MISSED")
      Map("id" -> m.id, "mode" -> m.mode.name, "description" -> m.description, "verdict" -> verdict.name,
        "failedChecks" -> failed, "expected" -> m.expected, "caught" -> caught)
    }
    val caughtCount = outcomes.count(_("caught") == true)
    val overall = if (outcomes.nonEmpty && caughtCount == outcomes.size) Verdict.Pass else Verdict.Fail
    Log.kv("mutations caught", s"$caughtCount/${outcomes.size}")
    s.writeReport("selftest-report.json", Map(
      "runId" -> cfg.runId, "runMode" -> cfg.runMode.name, "verdict" -> overall.name,
      "caught" -> s"$caughtCount/${outcomes.size}", "config" -> cfg.describe.toMap, "mutations" -> outcomes))
    overall
  }
}
