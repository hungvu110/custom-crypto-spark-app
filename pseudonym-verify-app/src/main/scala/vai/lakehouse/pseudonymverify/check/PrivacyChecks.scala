package vai.lakehouse.pseudonymverify.check

import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.core.{AesSivPseudonymizer, HmacPseudonymizer, PseudonymKey}
import vai.lakehouse.pseudonym.spark.{MappingTable, Pseudonymizer}
import vai.lakehouse.pseudonymverify.config.VerifyConfig
import vai.lakehouse.pseudonymverify.key.KeyMaterial
import vai.lakehouse.pseudonymverify.logging.Log
import vai.lakehouse.pseudonymverify.data.SyntheticData._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.util.Try

/**
 * Nhóm P — đã thay định danh (C1), không suy ngược được nếu không có salt/khoá/bảng ánh xạ (C2, mục a + c + chú thích ¹).
 * Xem docs/pseudonymization/VERIFY_APP_PLAN.md mục 5.2.
 */
private[pseudonymverify] object PrivacyChecks {
  import CheckSupport._

  private val every: PseudonymMode => Boolean = _ => true

  val all: Seq[Check] = Seq(
    Check("P1", Level.Must, "Schema đầu ra đúng policy", every, ctx => {
      val problems = ctx.runs.flatMap { r =>
        val expected = r.table.raw.columns.toSeq.map(c => r.policy.ruleFor(c).map(_.outputName).getOrElse(c))
        val actual = r.out.columns.toSeq
        val wrongType = r.rules.map(_.outputName).filter(o => actual.contains(o) && r.out.schema(o).dataType != StringType)
        if (expected == actual && wrongType.isEmpty) None
        else Some(s"${r.name}: missing=[${expected.diff(actual).mkString(",")}] extra=[${actual.diff(expected).mkString(",")}]" +
          s" nonString=[${wrongType.mkString(",")}]")
      }
      passedIfZero("tables_with_wrong_schema", problems.size.toLong, problems)
    }),

    Check("P2", Level.Must, "Không sót giá trị định danh nguyên vẹn", every, ctx => {
      val hits = allValues(ctx.runs).join(ctx.dictionary, Seq("value"), "left_semi")
      val where = hits.select("where").distinct().limit(5).collect().map(_.getString(0)).toSeq
      passedIfZero("raw_identifier_values_in_output", hits.count(), where)
    }),

    Check("P3", Level.Must, "Không sót định danh lẫn trong text", every, ctx => {
      val per = for {
        r <- ctx.runs
        c <- r.out.schema.fields.filter(_.dataType == StringType).map(_.name).toSeq if !r.rules.exists(_.outputName == c)
      } yield {
        val v = outCol(r.out, c)
        s"${r.name}.$c" -> r.out.where(IdentifierPatterns.values.map(v.rlike).reduce(_ || _)).count()
      }
      passedIfZero("identifier_pattern_hits", per.map(_._2).sum, per.collect { case (w, n) if n > 0 => w })
    }),

    Check("P4", Level.Must, "Suy ngược không cần khoá (tấn công từ điển)", every, ctx => {
      val outputs = pseudonymValues(ctx.runs).select(stripPrefix(col(Pseudo)).as("candidate")).distinct()
      val matched = outputs.join(ctx.attackCandidates, Seq("candidate"), "left_semi").count()
      passedIfZero("unkeyed_dictionary_matches", matched, detail = Map("dictionaryValues" -> ctx.dictionary.count()))
    }),

    Check("P5", Level.Must, "Sai khoá / bảng ánh xạ khác -> không khớp", every, ctx => {
      val sub = ctx.run(Subscriber)
      val real = pseudonymValues(Seq(sub))
      val (metric, observed) = ctx.mode match {
        case PseudonymMode.Hash =>
          val wrong = new Pseudonymizer(KeyMaterial.random()).pseudonymize(sub.table.raw, sub.policy)
          "pseudonyms_matching_with_wrong_key" -> overlap(real, wrong, sub)
        case PseudonymMode.Encrypt =>
          val wrongKey = new AesSivPseudonymizer(KeyMaterial.random().aesSivKey)
          val decrypts = udf((p: String) => Try(wrongKey.decrypt(p)).isSuccess)
          "values_decrypted_with_wrong_key" -> real.where(decrypts(col(Pseudo))).count()
        case PseudonymMode.Mapping =>
          val other = new Pseudonymizer(ctx.keys.provider, Some(s"${ctx.scratchDir}/p5_independent_map"))
            .pseudonymize(sub.table.raw, sub.policy)
          "tokens_matching_independent_mapping" -> overlap(real, other, sub)
      }
      passedIfZero(metric, observed, detail = Map("table" -> Subscriber))
    }),

    Check("P6", Level.Control, "Đối chứng: có khoá / bảng ánh xạ -> suy ngược được hết", every, ctx => {
      val (matched, total) = ctx.mode match {
        case PseudonymMode.Hash =>
          val h = new HmacPseudonymizer(ctx.keys.provider.hmacKey)
          val hash = udf((v: String) => h.pseudonymize(v))
          val recomputed = ctx.dictionary.select(hash(col("value")).as(Pseudo))
          val outs = pseudonymValues(ctx.runs).distinct()
          (outs.join(recomputed, Seq(Pseudo), "left_semi").count(), outs.count())
        case PseudonymMode.Encrypt =>
          val e = new AesSivPseudonymizer(ctx.keys.provider.aesSivKey)
          val decrypt = udf((p: String) => Try(e.decrypt(p)).toOption.orNull)
          reverseRate(ctx, pairs => pairs.withColumn("__back", decrypt(col(Pseudo))))
        case PseudonymMode.Mapping =>
          val map = new MappingTable(ctx.spark, ctx.mappingPath.getOrElse(throw new IllegalStateException("no mapping path")))
            .table.select(col(MappingTable.Token).as(Pseudo), col(MappingTable.RawValue).as("__back"))
          reverseRate(ctx, pairs => pairs.join(map, Seq(Pseudo), "left"))
      }
      val rate = if (total == 0) 1.0 else matched.toDouble / total
      Measured("keyed_reverse_rate", rate, "== 1.0", rate == 1.0, detail = Map("matched" -> matched, "total" -> total))
    }),

    Check("P7", Level.Must, "Khoá / bảng ánh xạ được giữ riêng", every, ctx => ctx.mode match {
      case PseudonymMode.Mapping =>
        val under = ctx.mappingPath.exists(p => VerifyConfig.isUnder(p, ctx.outputDir))
        passedIfZero("mapping_table_under_output_dir", if (under) 1 else 0)
      case mode =>
        val secrets = ctx.keys.secretStrings(mode)
        val sources = leakSources(ctx, mode)
        val leaks = sources.collect { case (name, texts) if texts.exists(t => secrets.exists(t.contains)) => name }
        val dataHits = allValues(ctx.runs).where(secrets.map(s => instr(col("value"), s) > 0).reduce(_ || _)).count()
        val found = leaks ++ (if (dataHits > 0) Seq("output data") else Nil)
        passedIfZero("places_containing_key", found.size.toLong, found, Map("scanned" -> (sources.map(_._1) :+ "output data")))
    })
  )

  /** Mọi giá trị định danh giả (không null) của các cột trong policy. Cột `__pseudo`. */
  private def pseudonymValues(runs: Seq[TableRun]): DataFrame =
    runs.flatMap(r => r.rules.map(rule => r.out.select(outCol(r.out, rule.outputName).cast(StringType).as(Pseudo))))
      .reduce(_ unionByName _).where(col(Pseudo).isNotNull)

  private def overlap(real: DataFrame, other: DataFrame, run: TableRun): Long = {
    val otherValues = run.rules.map(rule => other.select(outCol(other, rule.outputName).cast(StringType).as(Pseudo)))
      .reduce(_ unionByName _).where(col(Pseudo).isNotNull).distinct()
    real.distinct().join(otherValues, Seq(Pseudo), "left_semi").count()
  }

  /** (số định danh giả suy ngược ra ĐÚNG giá trị gốc của dòng đó, tổng số định danh giả) trên mọi bảng/cột. */
  private def reverseRate(ctx: VerifyContext, back: DataFrame => DataFrame): (Long, Long) = {
    val parts = for (r <- ctx.runs; rule <- r.rules) yield {
      val key = r.table.rowKey
      val pairs = r.out.select(outCol(r.out, key).as(key), outCol(r.out, rule.outputName).cast(StringType).as(Pseudo))
        .join(r.table.raw.select(col(key), col(quote(rule.column)).cast(StringType).as("__original")), Seq(key))
        .where(col(Pseudo).isNotNull)
      back(pairs).select((col("__back") === col("__original")).as("ok"))
    }
    val all = parts.reduce(_ unionByName _)
    (all.where(col("ok") === true).count(), all.count())
  }

  /** Các nơi khoá có thể lọt ra: plan, DESCRIBE FUNCTION, Spark conf, message lỗi, log của app. */
  private def leakSources(ctx: VerifyContext, mode: PseudonymMode): Seq[(String, Seq[String])] = {
    val describe = Seq("pseudo_hash", "pseudo_encrypt").flatMap(f =>
      Try(ctx.spark.sql(s"DESCRIBE FUNCTION EXTENDED $f").collect().map(_.getString(0)).toSeq).getOrElse(Nil))
    val conf = (ctx.spark.conf.getAll.toSeq ++ ctx.spark.sparkContext.getConf.getAll.toSeq).map { case (k, v) => s"$k=$v" }
    val size = if (mode == PseudonymMode.Hash) HmacPseudonymizer.KeyBytes else AesSivPseudonymizer.KeyBytes
    val errors = ctx.keys.secretStrings(mode).take(1).flatMap { secret =>
      Seq(
        Try(PseudonymKey.parse("probe", 1, s"base64:$secret%", size)).failed.toOption,
        Try(PseudonymKey.parse("probe", 1, s"hex:$secret", size + 1)).failed.toOption,
        Try(new AesSivPseudonymizer(ctx.keys.provider.aesSivKey).decrypt("e1_AAAA")).failed.toOption
      ).flatten.map(e => String.valueOf(e.getMessage))
    }
    Seq(
      "query plans (EXPLAIN)" -> ctx.planTexts,
      "DESCRIBE FUNCTION EXTENDED" -> describe,
      "spark conf" -> conf,
      "error messages" -> errors,
      "app log" -> Log.capturedLines)
  }
}
