package vai.lakehouse.pseudonymverify.job

import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.policy.TablePolicy
import vai.lakehouse.pseudonym.spark.Pseudonymizer
import vai.lakehouse.pseudonymverify.check.{Check, CheckResult, CheckSupport, Level, Measured}
import vai.lakehouse.pseudonymverify.logging.Log
import vai.lakehouse.pseudonymverify.report.{ReportWriter, Verdict}

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.FormattedMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * RUN_MODE=guard — cổng chặn trên dữ liệu THẬT (docs/pseudonym-verify-app/PLAN.md mục 7): giả danh `INPUT_TABLE` theo
 * policy của `GUARD_TABLE`, chạy các check KHÔNG cần đáp án, chỉ ghi output khi verdict PASS.
 */
object GuardJob extends VerifyJob {
  import CheckSupport._

  override def run(s: VerifySession): Verdict = {
    val cfg = s.cfg
    val table = cfg.guardTable.get
    val policy = s.policies.getOrElse(table, throw new IllegalArgumentException(
      s"Policy ${cfg.policyPath} has no table '$table' (has: ${s.policies.keys.mkString(", ")})"))
    val input = s.spark.read.parquet(cfg.inputTable.get)
    val pseudonymizer = new Pseudonymizer(s.keys.provider, Some(cfg.mappingTablePath))
    val produced = pseudonymizer.pseudonymize(input, policy)
    val plans = Seq(produced.queryExecution.toString, produced.queryExecution.explainString(FormattedMode))
    val out = produced.localCheckpoint()

    val checks = guardChecks(s, input, out, policy, pseudonymizer, plans)
    val verdict = Verdict.of(checks)
    ReportWriter.logChecks(s"guard/$table", checks)
    Log.kv("verdict", verdict.name)

    val target = s"${cfg.outputDir}/$table"
    if (verdict == Verdict.Pass) {
      out.write.mode(SaveMode.Overwrite).parquet(target)
      Log.ok(s"đã ghi output: $target")
    } else Log.fail(s"KHÔNG ghi output (verdict ${verdict.name})")

    s.writeReport("guard-report.json", Map(
      "runId" -> cfg.runId, "runMode" -> cfg.runMode.name, "verdict" -> verdict.name, "table" -> table,
      "input" -> cfg.inputTable.get, "output" -> (if (verdict == Verdict.Pass) target else null),
      "checks" -> checks.map(ReportWriter.checkJson)))
    verdict
  }

  private def guardChecks(s: VerifySession, input: DataFrame, out: DataFrame, policy: TablePolicy,
                          pseudonymizer: Pseudonymizer, plans: Seq[String]): Seq[CheckResult] = {
    val rules = policy.columns
    Seq(
      Check.evaluate("U1", Level.Must, "Không mất/thừa dòng") {
        passedIfZero("row_count_diff", math.abs(out.count() - input.count()))
      },
      Check.evaluate("U2", Level.Must, "Số null mỗi cột giữ nguyên") {
        val diffs = rules.map(r => r.column ->
          math.abs(input.where(outCol(input, r.column).isNull).count() - out.where(outCol(out, r.outputName).isNull).count()))
        passedIfZero("null_count_diff", diffs.map(_._2).sum, diffs.collect { case (c, d) if d > 0 => c })
      },
      Check.evaluate("U3", Level.Must, "Đúng định dạng theo chế độ") {
        val bad = rules.map { r =>
          val c = outCol(out, r.outputName)
          r.outputName -> out.where(c.isNotNull && !c.rlike(formatRegex(r.mode))).count()
        }
        passedIfZero("bad_format_values", bad.map(_._2).sum, bad.collect { case (c, n) if n > 0 => c })
      },
      Check.evaluate("U4+U5", Level.Must, "Song ánh giá trị gốc <-> định danh giả (a == b == c == d)") {
        val broken = rules.flatMap { r =>
          val distinctRaw = input.select(outCol(input, r.column).cast(StringType).as(r.column)).where(col(r.column).isNotNull).distinct()
          val pairs = pseudonymizer.pseudonymize(distinctRaw.withColumn("__raw", col(r.column)),
            TablePolicy(policy.table, Seq(r)))
          val a = distinctRaw.count()
          val b = pairs.select(r.outputName).distinct().count()
          val c = pairs.select(r.outputName, "__raw").distinct().count()
          val d = out.where(outCol(out, r.outputName).isNotNull).select(r.outputName).distinct().count()
          if (a == b && b == c && c == d) None else Some(s"${r.column}: a=$a b=$b c=$c d=$d")
        }
        passedIfZero("columns_not_bijective", broken.size.toLong, broken)
      },
      Check.evaluate("U10", Level.Must, "Không trộn thế hệ khoá trong 1 cột") {
        val mixed = rules.filter(_.mode != PseudonymMode.Mapping).filter { r =>
          val c = outCol(out, r.outputName)
          out.where(c.isNotNull).select(regexp_extract(c, "^([a-z][0-9]+)_", 1)).distinct().count() > 1
        }
        passedIfZero("columns_with_mixed_key_versions", mixed.size.toLong, mixed.map(_.outputName))
      },
      Check.evaluate("P1", Level.Must, "Schema đầu ra đúng policy") {
        val expected = input.columns.toSeq.map(c => policy.ruleFor(c).map(_.outputName).getOrElse(c))
        passedIfZero("schema_mismatch", if (expected == out.columns.toSeq) 0 else 1,
          Seq(s"extra=[${out.columns.diff(expected).mkString(",")}] missing=[${expected.diff(out.columns).mkString(",")}]"))
      },
      Check.evaluate("P2", Level.Must, "Không sót giá trị định danh nguyên vẹn (mẫu từ cột gốc)") {
        val dictionary = input.sample(s.cfg.identifierSampleFraction, 42L)
          .select(explode(array(rules.map(r => outCol(input, r.column).cast(StringType)): _*)).as("value"))
          .where(col("value").isNotNull).distinct()
        val values = out.select(explode(array(out.columns.map(c => outCol(out, c).cast(StringType)): _*)).as("value"))
        passedIfZero("raw_identifier_values_in_output", values.join(dictionary, Seq("value"), "left_semi").count())
      },
      Check.evaluate("P3", Level.Must, "Không sót định danh lẫn trong text") {
        val columns = out.schema.fields.filter(_.dataType == StringType).map(_.name).filterNot(c => rules.exists(_.outputName == c))
        val per = columns.toSeq.map(c => c -> out.where(IdentifierPatterns.values.map(outCol(out, c).rlike).reduce(_ || _)).count())
        passedIfZero("identifier_pattern_hits", per.map(_._2).sum, per.collect { case (c, n) if n > 0 => c })
      },
      Check.evaluate("P7", Level.Must, "Khoá không lọt vào plan / conf / log") {
        val secrets = rules.map(_.mode).distinct.flatMap(s.keys.secretStrings)
        val conf = (s.spark.conf.getAll.toSeq ++ s.spark.sparkContext.getConf.getAll.toSeq).map { case (k, v) => s"$k=$v" }
        val sources = Seq("query plans" -> plans, "spark conf" -> conf, "app log" -> Log.capturedLines)
        val leaks = sources.collect { case (n, texts) if texts.exists(t => secrets.exists(t.contains)) => n }
        Measured("places_containing_key", leaks.size.toDouble, "== 0", leaks.isEmpty, leaks)
      })
  }
}
