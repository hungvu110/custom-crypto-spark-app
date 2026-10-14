package vai.lakehouse.pseudonymverify.check

import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.spark.Reidentifier
import vai.lakehouse.pseudonymverify.data.{IdentityFormats, SyntheticData}
import vai.lakehouse.pseudonymverify.data.SyntheticData._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * Nhóm U — liên kết được các bản ghi của cùng một chủ thể, phân tích dọc vẫn đúng (điều kiện C3, mục b + d của tài liệu).
 * Xem docs/pseudonym-verify-app/PLAN.md mục 5.1.
 */
private[pseudonymverify] object LinkabilityChecks {
  import CheckSupport._

  private val every: PseudonymMode => Boolean = _ => true

  val all: Seq[Check] = Seq(
    Check("U1", Level.Must, "Không mất/thừa dòng", every, ctx => {
      val diffs = ctx.runs.map(r => r.name -> (r.out.count() - r.table.raw.count()))
      passedIfZero("row_count_diff", diffs.map(d => math.abs(d._2)).sum,
        diffs.collect { case (t, d) if d != 0 => s"$t:$d" }, diffs.toMap)
    }),

    Check("U2", Level.Must, "null vào -> null ra", every, ctx => {
      val bad = for (r <- ctx.runs; rule <- r.rules)
        yield s"${r.name}.${rule.column}" -> joined(r, rule).where(col(IsNull) =!= col(Pseudo).isNull).count()
      passedIfZero("null_semantics_violations", bad.map(_._2).sum, bad.collect { case (w, n) if n > 0 => w })
    }),

    Check("U3", Level.Must, "Đúng định dạng theo chế độ", every, ctx => {
      val regex = formatRegex(ctx.mode)
      val bad = for (r <- ctx.runs; rule <- r.rules) yield {
        val c = outCol(r.out, rule.outputName).cast(StringType)
        s"${r.name}.${rule.outputName}" -> r.out.where(c.isNotNull && !c.rlike(regex)).count()
      }
      passedIfZero("bad_format_values", bad.map(_._2).sum, bad.collect { case (w, n) if n > 0 => w }, Map("regex" -> regex))
    }),

    Check("U4", Level.Must, "1 chủ thể -> 1 định danh giả (xuyên bảng)", every, ctx => {
      val per = byIdentifier(ctx.runs).toSeq.map { case (id, j) =>
        val split = j.where(!col(IsNull)).groupBy(Subject).agg(countDistinct(Pseudo).as("n")).where(col("n") > 1)
        (id, split.count(), split.select(Subject).limit(5).collect().map(r => s"$id:subject=${r.get(0)}").toSeq)
      }
      passedIfZero("subjects_with_multiple_pseudonyms", per.map(_._2).sum, per.flatMap(_._3), per.map(p => p._1 -> p._2).toMap)
    }),

    Check("U5", Level.Must, "1 định danh giả -> 1 chủ thể (không va chạm)", every, ctx => {
      val per = byIdentifier(ctx.runs).toSeq.map { case (id, j) =>
        val shared = j.where(!col(IsNull) && col(Pseudo).isNotNull).groupBy(Pseudo)
          .agg(countDistinct(Subject).as("n"), min(Subject).as("subject")).where(col("n") > 1)
        (id, shared.count(), shared.select("subject").limit(5).collect().map(r => s"$id:subject=${r.get(0)}").toSeq)
      }
      passedIfZero("pseudonyms_shared_by_subjects", per.map(_._2).sum, per.flatMap(_._3), per.map(p => p._1 -> p._2).toMap)
    }),

    Check("U6", Level.Must, "Join giữa các bảng giữ nguyên", every, ctx => {
      val sub = ctx.run(Subscriber)
      val pairs = Seq(AppEvent -> Isdn, BtsConnection -> Imei, LoyaltyTxn -> CustomerId).map { case (table, column) =>
        val r = ctx.run(table)
        val outJoin = keyOf(r.out, outputName(r, column)).join(keyOf(sub.out, outputName(sub, column)), "k").count()
        val truthJoin = subjectsOf(r, column).join(subjectsOf(sub, column), "k").count()
        (s"$table⋈$Subscriber ON $column", outJoin, truthJoin)
      }
      val diff = pairs.map(p => math.abs(p._2 - p._3)).sum
      passedIfZero("join_count_diff", diff, pairs.collect { case (n, a, b) if a != b => n },
        pairs.map(p => p._1 -> Map("pseudonymized" -> p._2, "original" -> p._3)).toMap)
    }),

    Check("U7", Level.Must, "Chỉ số của 3 ví dụ (MyViettel, BTS, Viettel++) giữ nguyên", every, ctx => {
      val cs = UseCaseMetrics.compareAll(ctx)
      passedIfZero("use_case_metric_mismatches", cs.map(_.mismatches).sum, cs.filter(_.mismatches > 0).map(_.name),
        cs.map(c => c.name -> (if (c.detail.isEmpty) c.mismatches else c.detail + ("mismatches" -> c.mismatches))).toMap)
    }),

    Check("U8", Level.Must, "Ổn định giữa các lần chạy (partition khác, UDF mới)", every, ctx => {
      val diffs = ctx.runs.map(r => r.name -> multisetDiff(r.out, r.rerun.select(r.out.columns.map(quote).map(col): _*)))
      passedIfZero("rows_differing_between_runs", diffs.map(_._2).sum, diffs.collect { case (t, n) if n > 0 => t }, diffs.toMap)
    }),

    Check("U9", Level.Must, "DataFrame API ≡ SQL", every, ctx => {
      val diffs = ctx.runs.map(r => r.name -> multisetDiff(r.out, r.viaSql.get.select(r.out.columns.map(quote).map(col): _*)))
      passedIfZero("rows_differing_dataframe_vs_sql", diffs.map(_._2).sum, diffs.collect { case (t, n) if n > 0 => t }, diffs.toMap)
    }),

    Check("U10", Level.Must, "Không trộn thế hệ khoá trong 1 cột", m => m != PseudonymMode.Mapping, ctx => {
      val versions = for (r <- ctx.runs; rule <- r.rules) yield {
        val c = outCol(r.out, rule.outputName)
        s"${r.name}.${rule.outputName}" -> r.out.where(c.isNotNull).select(regexp_extract(c, "^([a-z][0-9]+)_", 1)).distinct().count()
      }
      val mixed = versions.filter(_._2 > 1)
      passedIfZero("columns_with_mixed_key_versions", mixed.size.toLong, mixed.map(_._1), versions.toMap)
    }),

    Check("U11", Level.Info, "Hiệu năng bước giả danh", every, ctx => {
      val rows = ctx.timings.getOrElse("rows", 0d)
      val seconds = ctx.timings.getOrElse("pseudonymizeSec", 0d)
      Measured("pseudonymize_rows_per_sec", if (seconds > 0) rows / seconds else 0d, "-", passed = true, detail = ctx.timings)
    }),

    Check("U12", Level.Must, "Luồng ưu đãi Viettel++: pseudonym_id -> đúng customer_id", every, ctx => {
      val loyalty = ctx.run(LoyaltyTxn)
      val rule = loyalty.policy.ruleFor(CustomerId).getOrElse(throw new IllegalStateException("loyalty_txn.customer_id is not in policy"))
      val expected = joined(loyalty, rule).where(!col(IsNull))
        .select(col(Pseudo).as("ref"), IdentityFormats.customerId(col(Subject)).as("expected")).distinct()
      val targets = expected.select("ref").distinct()
        .orderBy(xxhash64(col("ref"), lit(ctx.cfg.seed))).limit(ctx.cfg.offerSample)
        .select(col("ref").as("pseudonym_id"), col("ref"))
      // Hệ thống gửi ưu đãi có ĐỦ danh sách khách hàng (không phải cột customer_id đã bị cài null của dữ liệu thô).
      val known = SyntheticData.allCustomerIds(ctx.spark, ctx.cfg.nSubjects, ctx.cfg.partitions)
      val back = new Reidentifier(ctx.keys.provider, ctx.mappingPath)
        .reidentify(targets, "pseudonym_id", ctx.mode, "customer_id", Some(known))
      val checked = back.join(expected, Seq("ref"), "left")
      val wrong = checked.where(col("customer_id").isNull || col("customer_id") =!= col("expected")).select("ref").distinct().count()
      passedIfZero("offers_reidentified_wrongly", wrong, detail = Map("offers" -> targets.count()))
    })
  )

  private def keyOf(df: DataFrame, column: String): DataFrame =
    df.select(outCol(df, column).cast(StringType).as("k")).where(col("k").isNotNull)

  private def subjectsOf(run: TableRun, column: String): DataFrame =
    run.table.truth.where(!col(nullFlag(column))).select(col(Subject).as("k"))
}
