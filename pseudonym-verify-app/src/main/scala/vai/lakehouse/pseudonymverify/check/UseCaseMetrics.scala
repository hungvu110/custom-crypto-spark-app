package vai.lakehouse.pseudonymverify.check

import vai.lakehouse.pseudonymverify.data.SyntheticData._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Chỉ số của 3 ví dụ trong tài liệu (check U7), tính 2 lần: trên OUTPUT theo định danh giả và trên dữ liệu gốc theo đáp án
 * (`__subject_id`). Giả danh đúng thì 2 bên phải bằng tuyệt đối (so dạng multiset, không cần biết ai là ai).
 */
private[pseudonymverify] object UseCaseMetrics {
  import CheckSupport._

  final case class Comparison(name: String, mismatches: Long, detail: Map[String, Any] = Map.empty)

  private val Key = "__metric_key"

  def compareAll(ctx: VerifyContext): Seq[Comparison] =
    myViettel(ctx) ++ bts(ctx) ++ viettelPlus(ctx)

  /** Hành trình MyViettel: chuỗi event_type theo thời gian của từng người + tỉ lệ chuyển đổi phễu. */
  private def myViettel(ctx: VerifyContext): Seq[Comparison] = {
    val (outSide, truthSide) = sides(ctx.run(AppEvent), Isdn)
    def journeys(df: DataFrame): DataFrame = df.groupBy(Key)
      .agg(sort_array(collect_list(struct(col("event_ts"), col("event_id"), col("event_type")))).as("evs"))
      .select(concat_ws(",", col("evs.event_type")).as("journey"))
    val (a, b) = (journeys(outSide).cache(), journeys(truthSide).cache())
    val funnel = (df: DataFrame) => df.where(col("journey").rlike("login.*check_balance.*buy_data")).count()
    val (fa, fb, people) = (funnel(a), funnel(b), b.count())
    Seq(
      Comparison("myviettel.journey", multisetDiff(a, b)),
      Comparison("myviettel.funnel_login_check_balance_buy_data", math.abs(fa - fb),
        Map("converted_pseudonymized" -> fa, "converted_original" -> fb, "people" -> people)))
  }

  /** Trạm BTS: mẫu di chuyển (chuỗi cell theo thời gian), tổng thời gian kết nối, số cell/thiết bị, số thiết bị/cell. */
  private def bts(ctx: VerifyContext): Seq[Comparison] = {
    val (outSide, truthSide) = sides(ctx.run(BtsConnection), Imei)
    def devices(df: DataFrame): DataFrame = df.groupBy(Key)
      .agg(sort_array(collect_list(struct(col("connect_ts"), col("conn_id"), col("cell_id")))).as("cs"),
        sum("duration_s").as("total_duration"), countDistinct("cell_id").as("cells"))
      .select(concat_ws(",", col("cs.cell_id")).as("movement"), col("total_duration"), col("cells"))
    def cells(df: DataFrame): DataFrame = df.groupBy("cell_id").agg(countDistinct(Key).as("devices"))
    Seq(
      Comparison("bts.device_movement_duration", multisetDiff(devices(outSide), devices(truthSide))),
      Comparison("bts.devices_per_cell", multisetDiff(cells(outSide), cells(truthSide))))
  }

  /** Viettel++: tổng điểm tích/đổi và chuỗi giao dịch theo thời gian của từng khách hàng. */
  private def viettelPlus(ctx: VerifyContext): Seq[Comparison] = {
    val (outSide, truthSide) = sides(ctx.run(LoyaltyTxn), CustomerId)
    def history(df: DataFrame): DataFrame = df.groupBy(Key)
      .agg(sum(when(col("txn_type") === "earn", col("points")).otherwise(0)).as("earned"),
        sum(when(col("txn_type") === "redeem", col("points")).otherwise(0)).as("redeemed"),
        sort_array(collect_list(struct(col("txn_ts"), col("txn_id"), col("txn_type")))).as("txns"))
      .select(col("earned"), col("redeemed"), concat_ws(",", col("txns.txn_type")).as("history"))
    Seq(Comparison("viettel_plus.points_history", multisetDiff(history(outSide), history(truthSide))))
  }

  /**
   * (OUTPUT theo định danh giả, dữ liệu gốc theo đáp án). Bỏ dòng có định danh null ở cả 2 bên — dòng đó không thuộc
   * về ai trong phân tích dọc.
   */
  private def sides(run: TableRun, column: String): (DataFrame, DataFrame) = {
    val outSide = run.out.withColumn(Key, outCol(run.out, outputName(run, column))).where(col(Key).isNotNull)
    val truthSide = run.table.all.where(!col(nullFlag(column))).withColumn(Key, col(SubjectId))
    (outSide, truthSide)
  }
}
