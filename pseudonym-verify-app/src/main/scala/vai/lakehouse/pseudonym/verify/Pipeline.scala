package vai.lakehouse.pseudonym.verify

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.FormattedMode
import org.apache.spark.sql.functions.{col, sha2}
import org.apache.spark.sql.types.StringType
import vai.lakehouse.pseudonym._
import vai.lakehouse.pseudonym.verify.checks.{TableRun, VerifyContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * Cách giả danh 1 bảng: lib thật, hoặc 1 mutation cố ý sai (selftest). `cleanup` chạy SAU khi check xong (vd mutation M5
 * gỡ khoá khỏi Spark conf) — để P7 còn thấy chỗ rò rỉ.
 */
final case class Strategy(
    name: String,
    pseudonymize: (DataFrame, TablePolicy) => DataFrame,
    prepare: SparkSession => Unit = _ => (),
    cleanup: SparkSession => Unit = _ => ())

object Strategy {
  def library(pseudonymizer: Pseudonymizer): Strategy = Strategy("library", pseudonymizer.pseudonymize)
}

/**
 * Chạy giả danh cho mọi bảng tổng hợp theo 1 chế độ, ghi output rồi ĐỌC LẠI (kiểm đúng cái nằm trên Data Lake), kèm
 * lần chạy lại (U8) và đường SQL (U9), rồi dựng [[VerifyContext]] cho bộ check.
 */
final class Pipeline(spark: SparkSession, cfg: VerifyConfig, keys: KeyMaterial, tables: Seq[SyntheticTable],
                     policies: Map[String, TablePolicy], dictionary: DataFrame, attackCandidates: DataFrame) {

  def run(mode: PseudonymMode, strategy: Strategy, label: String, outputDir: String, scratchDir: String,
          mappingPath: Option[String], measureTimings: Boolean): VerifyContext = {
    strategy.prepare(spark)
    val plans = ArrayBuffer.empty[String]
    var seconds = 0d
    val runs = tables.map { table =>
      val policy = policyFor(table).withMode(mode)
      val produced = strategy.pseudonymize(table.raw, policy)
      plans += produced.queryExecution.toString
      plans += produced.queryExecution.explainString(FormattedMode)
      val start = System.nanoTime()
      val out = materialize(produced, s"$outputDir/${table.name}")
      seconds += (System.nanoTime() - start) / 1e9
      val rerun = strategy.pseudonymize(table.raw.repartition(cfg.partitions + 7), policy).localCheckpoint()
      val viaSql = Try(sqlPath(table, policy, mode, mappingPath))
      viaSql.foreach(df => plans += df.queryExecution.toString)
      TableRun(table, policy, out, rerun, viaSql)
    }
    val rows = tables.map(_.raw.count()).sum.toDouble
    val timings = Map("rows" -> rows, "pseudonymizeSec" -> seconds) ++
      (if (measureTimings) Map("sha2BaselineSec" -> baselineSeconds()) else Map.empty)
    VerifyContext(spark, cfg, mode, label, runs, keys, mappingPath, scratchDir, outputDir, plans.toList, timings,
      dictionary, attackCandidates)
  }

  private def policyFor(table: SyntheticTable): TablePolicy =
    policies.getOrElse(table.name, throw new IllegalArgumentException(
      s"Policy ${cfg.policyPath} has no table '${table.name}' (has: ${policies.keys.mkString(", ")})"))

  /** WRITE_OUTPUT=true: ghi parquet rồi đọc lại. false: chốt kết quả trong bộ nhớ (để check không tính lại lần nữa). */
  private def materialize(df: DataFrame, path: String): DataFrame =
    if (cfg.writeOutput) {
      df.write.mode(SaveMode.Overwrite).parquet(path)
      spark.read.parquet(path)
    } else df.localCheckpoint()

  /** Cùng phép biến đổi qua SQL: `pseudo_hash`/`pseudo_encrypt` (PseudonymExtension), MAPPING = JOIN bảng ánh xạ. */
  private def sqlPath(table: SyntheticTable, policy: TablePolicy, mode: PseudonymMode, mappingPath: Option[String]): DataFrame = {
    val view = s"__verify_raw_${table.name}"
    table.raw.createOrReplaceTempView(view)
    val q = (c: String) => s"`${c.replace("`", "``")}`"
    mode match {
      case PseudonymMode.Mapping =>
        val path = mappingPath.getOrElse(throw new IllegalStateException("MAPPING needs a mapping table path"))
        val aliases = policy.columns.zipWithIndex.map { case (rule, i) => rule.column -> s"m$i" }.toMap
        val select = table.raw.columns.map(c => policy.ruleFor(c).fold(s"r.${q(c)}")(rule =>
          s"${aliases(c)}.${MappingTable.Token} AS ${q(rule.outputName)}"))
        val joins = policy.columns.map { rule =>
          val m = aliases(rule.column)
          s"LEFT JOIN delta.`$path` $m ON CAST(r.${q(rule.column)} AS STRING) = $m.${MappingTable.RawValue}"
        }
        spark.sql(s"SELECT ${select.mkString(", ")} FROM $view r ${joins.mkString(" ")}")
      case _ =>
        val fn = if (mode == PseudonymMode.Hash) "pseudo_hash" else "pseudo_encrypt"
        val select = table.raw.columns.map(c => policy.ruleFor(c).fold(q(c))(rule =>
          s"$fn(CAST(${q(c)} AS STRING)) AS ${q(rule.outputName)}"))
        spark.sql(s"SELECT ${select.mkString(", ")} FROM $view")
    }
  }

  /** Baseline U11: cùng các cột nhưng băm bằng `sha2` built-in (không khoá), ghi noop. */
  private def baselineSeconds(): Double = {
    val start = System.nanoTime()
    tables.foreach { t =>
      val policy = policyFor(t)
      t.raw.select(t.raw.columns.map(c =>
        if (policy.ruleFor(c).isDefined) sha2(col(c).cast(StringType), 256).as(c) else col(c)): _*)
        .write.format("noop").mode(SaveMode.Overwrite).save()
    }
    (System.nanoTime() - start) / 1e9
  }
}
