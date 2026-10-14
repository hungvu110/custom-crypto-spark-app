package vai.lakehouse.pseudonym.verify

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import vai.lakehouse.pseudonym.{PseudonymMode, PseudonymPolicy, Pseudonymizer, TablePolicy}
import vai.lakehouse.pseudonym.verify.checks.{AttackDictionary, CheckResult, Checks}

/** Những thứ dùng chung giữa các chế độ/mutation của 1 lần chạy app: dữ liệu tổng hợp, từ điển tấn công, pipeline. */
final class VerifySession(val spark: SparkSession, val cfg: VerifyConfig, val keys: KeyMaterial) {

  lazy val policies: Map[String, TablePolicy] = PseudonymPolicy.load(cfg.policyPath)

  lazy val tables: Seq[SyntheticTable] = SyntheticData.generate(spark, cfg)

  lazy val dictionary: DataFrame =
    SyntheticData.identifierDictionary(spark, cfg.nSubjects, cfg.partitions).persist(StorageLevel.MEMORY_AND_DISK)

  lazy val attackCandidates: DataFrame = AttackDictionary.candidates(dictionary).persist(StorageLevel.MEMORY_AND_DISK)

  lazy val pipeline = new Pipeline(spark, cfg, keys, tables, policies, dictionary, attackCandidates)

  def mappingPathFor(mode: PseudonymMode, path: String): Option[String] =
    if (mode == PseudonymMode.Mapping) Some(path) else None

  def rowCounts: Map[String, Any] = tables.map(t => t.name -> (t.raw.count(): Any)).toMap
}

/** RUN_MODE=full — nghiệm thu lib: chạy toàn bộ check cho từng chế độ trong PSEUDO_MODES. */
object FullRun {

  final case class ModeResult(mode: PseudonymMode, verdict: Verdict, checks: Seq[CheckResult], timings: Map[String, Double])

  def run(s: VerifySession): Verdict = {
    val cfg = s.cfg
    val results = cfg.pseudoModes.map { mode =>
      Log.section(s"MODE ${mode.name}")
      val mapping = s.mappingPathFor(mode, cfg.mappingTablePath)
      val ctx = s.pipeline.run(mode, Strategy.library(new Pseudonymizer(s.keys.provider, mapping)), mode.name,
        s"${cfg.outputDir}/${mode.name}", s"${cfg.restrictedDir}/scratch/${mode.name}", mapping, measureTimings = true)
      val checks = Checks.runAll(ctx)
      val verdict = Verdict.of(checks)
      ReportWriter.logChecks(mode.name, checks)
      Log.kv(s"verdict ${mode.name}", verdict.name)
      ModeResult(mode, verdict, checks, ctx.timings)
    }
    val overall = Verdict.worst(results.map(_.verdict))
    val report = Map(
      "runId" -> cfg.runId,
      "runMode" -> cfg.runMode.name,
      "verdict" -> overall.name,
      "config" -> cfg.describe.toMap,
      "keySource" -> s.keys.description,
      "rows" -> s.rowCounts,
      "modes" -> results.map(r => r.mode.name -> Map(
        "verdict" -> r.verdict.name,
        "checks" -> r.checks.map(ReportWriter.checkJson),
        "timings" -> r.timings)).toMap)
    writeReport(s, "report.json", report)
    overall
  }

  def writeReport(s: VerifySession, name: String, report: Map[String, Any]): Unit = {
    val path = s"${s.cfg.runPath}/$name"
    ReportWriter.write(s.spark, path, ReportWriter.toJson(report))
    Log.kv("report", path)
  }
}

/**
 * RUN_MODE=selftest — kiểm chứng CHÍNH bộ check: chạy từng mutation (bản giả danh cố ý sai) của các chế độ trong
 * PSEUDO_MODES. PASS ⇔ mọi mutation đã chạy đều bị bắt (verdict ≠ PASS và các check kỳ vọng nằm trong danh sách fail).
 */
object SelftestRun {

  def run(s: VerifySession): Verdict = {
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
    FullRun.writeReport(s, "selftest-report.json", Map(
      "runId" -> cfg.runId, "runMode" -> cfg.runMode.name, "verdict" -> overall.name,
      "caught" -> s"$caughtCount/${outcomes.size}", "config" -> cfg.describe.toMap, "mutations" -> outcomes))
    overall
  }
}
