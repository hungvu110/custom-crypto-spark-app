package vai.lakehouse.pseudonymverify.job

import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.spark.Pseudonymizer
import vai.lakehouse.pseudonymverify.check.{CheckResult, Checks}
import vai.lakehouse.pseudonymverify.logging.Log
import vai.lakehouse.pseudonymverify.report.{Preview, ReportWriter, Verdict}
import org.apache.spark.sql.DataFrame


/** RUN_MODE=full — nghiệm thu lib: chạy toàn bộ check cho từng chế độ trong PSEUDO_MODES. */
object FullJob extends VerifyJob {

  final case class ModeResult(mode: PseudonymMode, verdict: Verdict, checks: Seq[CheckResult], timings: Map[String, Double])

  override def run(s: VerifySession): Verdict = {
    val cfg = s.cfg
    val preview = new StringBuilder
    // Dữ liệu gốc in TRƯỚC mọi thứ khác để so sánh với dữ liệu sau giả danh và kết quả check.
    if (cfg.previewRows > 0) {
      Log.section("DỮ LIỆU GỐC (trước khi giả danh)")
      s.tables.foreach(t => show(preview, s"GỐC · ${t.name}", t.raw, t.rowKey, cfg.previewRows))
    }
    val results = cfg.pseudoModes.map { mode =>
      Log.section(s"MODE ${mode.name}")
      val mapping = s.mappingPathFor(mode, cfg.mappingTablePath)
      val ctx = s.pipeline.run(mode, Strategy.library(new Pseudonymizer(s.keys.provider, mapping)), mode.name,
        s"${cfg.outputDir}/${mode.name}", s"${cfg.restrictedDir}/scratch/${mode.name}", mapping, measureTimings = true)
      if (cfg.previewRows > 0) {
        Log.info(s"DỮ LIỆU SAU KHI GIẢ DANH — ${mode.name}")
        ctx.runs.foreach(r => show(preview, s"${mode.name} · ${r.name}", r.out, r.table.rowKey, cfg.previewRows))
      }
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
    s.writeReport("report.json", report)
    if (cfg.previewRows > 0) {
      val path = s"${cfg.runPath}/preview.txt"
      ReportWriter.write(s.spark, path, preview.toString)
      Log.kv("preview (gốc + sau giả danh)", path)
    }
    overall
  }

  /** In 1 bảng ra log và gom vào `sink` để ghi preview.txt. */
  private def show(sink: StringBuilder, title: String, df: DataFrame, orderBy: String, n: Int): Unit = {
    val text = Preview.table(title, df, orderBy, n)
    sink.append(text).append('\n')
    text.split("\n").foreach(l => Log.info(l))
  }

}
