package vai.lakehouse.pseudonymverify.data

import vai.lakehouse.pseudonymverify.config.VerifyConfig

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * 1 bảng tổng hợp: `all` gồm dữ liệu thô + cột ẩn đáp án (`__subject_id`, `__is_null_<cột>`).
 * `raw` là thứ đưa vào lib (lib không thấy đáp án); `truth` chỉ dùng trong check, không bao giờ ghi ra output.
 */
final case class SyntheticTable(name: String, rowKey: String, identifierColumns: Seq[String], all: DataFrame) {
  import SyntheticData._

  def raw: DataFrame = all.drop(SubjectId +: identifierColumns.map(nullFlag): _*)

  def truth: DataFrame = all.select((rowKey +: SubjectId +: identifierColumns.map(nullFlag)).map(col): _*)
}

/**
 * Sinh 4 bảng theo 3 ví dụ của tài liệu (hồ sơ thuê bao, hành trình MyViettel, kết nối trạm BTS, giao dịch Viettel++)
 * ngay trên executor từ `spark.range`. Tất định theo `seed`: cùng seed -> cùng dữ liệu.
 */
object SyntheticData {

  val SubjectId  = "__subject_id"
  val Isdn       = "isdn"
  val Imei       = "imei"
  val CustomerId = "customer_id"

  val Subscriber    = "subscriber"
  val AppEvent      = "app_event"
  val BtsConnection = "bts_connection"
  val LoyaltyTxn    = "loyalty_txn"

  val EventTypes = Seq("login", "check_balance", "buy_data", "view_offer", "logout")
  val Cells      = 500
  val Days       = 30
  /** 2026-10-01 00:00:00 UTC — mốc thời gian của dữ liệu tổng hợp. */
  val BaseEpochSeconds = 1790812800L

  private val Provinces = Seq("Ha Noi", "TP HCM", "Da Nang", "Hai Phong", "Can Tho", "Nghe An", "Thanh Hoa", "Quang Ninh")
  private val Plans     = Seq("prepaid", "postpaid", "data_only", "family")

  def nullFlag(column: String): String = s"__is_null_$column"

  def generate(spark: SparkSession, cfg: VerifyConfig): Seq[SyntheticTable] = {
    val gen = new Generator(spark, cfg.seed, cfg.nSubjects, cfg.partitions, cfg.nullRate)
    Seq(
      gen.subscriber(),
      gen.appEvent(cfg.appEventsPerSubject),
      gen.btsConnection(cfg.btsConnPerSubject),
      gen.loyaltyTxn(cfg.loyaltyTxnPerSubject))
  }

  /** Mọi định danh hợp lệ của mọi chủ thể — từ điển `D` của check P2/P4/P6. Cột `value`. */
  def identifierDictionary(spark: SparkSession, nSubjects: Long, partitions: Int): DataFrame = {
    val s = col("id")
    spark.range(0, nSubjects, 1, partitions)
      .select(explode(array(IdentityFormats.isdn(s), IdentityFormats.imei(s), IdentityFormats.customerId(s))).as("value"))
  }

  /** Danh sách đầy đủ mã khách hàng (cột `customer_id`) — thứ hệ thống gửi ưu đãi Viettel++ có trong tay (check U12). */
  def allCustomerIds(spark: SparkSession, nSubjects: Long, partitions: Int): DataFrame =
    spark.range(0, nSubjects, 1, partitions).select(IdentityFormats.customerId(col("id")).as(CustomerId))

  private final class Generator(spark: SparkSession, seed: String, n: Long, partitions: Int, nullRate: Double) {

    private val seedLit = lit(seed)

    private def h(parts: Column*): Column = xxhash64(parts :+ seedLit: _*)

    private def pick(values: Seq[String], parts: Column*): Column =
      element_at(array(values.map(lit): _*), (pmod(h(parts: _*), lit(values.size.toLong)) + 1).cast("int"))

    /** Định danh của chủ thể, thay bằng null với tỉ lệ `nullRate` (tất định theo khoá dòng + tên cột). */
    private def identifier(name: String, rowKey: Column, subject: Column): Seq[Column] = {
      val isNull = pmod(h(rowKey, lit(s"null_$name")), lit(1000000L)) < lit((nullRate * 1000000).toLong)
      Seq(when(isNull, lit(null).cast("string")).otherwise(IdentityFormats.forColumn(name)(subject)).as(name),
        isNull.as(nullFlag(name)))
    }

    private def subjectOf(id: Column): Column = pmod(h(id), lit(n))

    private def timestampWithin(day: Column, parts: Column*): Column =
      timestamp_seconds(lit(BaseEpochSeconds) + day * 86400L + pmod(h(parts: _*), lit(86400L)))

    private def persist(t: SyntheticTable): SyntheticTable = t.copy(all = t.all.persist(StorageLevel.MEMORY_AND_DISK))

    def subscriber(): SyntheticTable = {
      val s = col("id")
      val ids = Seq(CustomerId, Isdn, Imei)
      val df = spark.range(0, n, 1, partitions).select(
        Seq(s.as("record_id"), s.as(SubjectId)) ++
          ids.flatMap(identifier(_, s, s)) ++
          Seq(pick(Provinces, s, lit("province")).as("province"), pick(Plans, s, lit("plan")).as("plan")): _*)
      persist(SyntheticTable(Subscriber, "record_id", ids,
        ordered(df, Seq("record_id", CustomerId, Isdn, Imei, "province", "plan"), ids)))
    }

    def appEvent(perSubject: Int): SyntheticTable = {
      val id = col("id")
      val subject = subjectOf(id)
      val day = pmod(h(id, lit("day")), lit(Days.toLong))
      val df = spark.range(0, n * perSubject, 1, partitions).select(
        Seq(id.as("event_id"), subject.as(SubjectId)) ++ identifier(Isdn, id, subject) ++
          Seq(pick(EventTypes, id, lit("type")).as("event_type"), timestampWithin(day, id, lit("ts")).as("event_ts")): _*)
      persist(SyntheticTable(AppEvent, "event_id", Seq(Isdn),
        ordered(df, Seq("event_id", Isdn, "event_type", "event_ts"), Seq(Isdn))))
    }

    /** Mỗi thiết bị mỗi ngày bám 1 cell -> có "mẫu di chuyển" thật để U7 đo lại. */
    def btsConnection(perSubject: Int): SyntheticTable = {
      val id = col("id")
      val subject = subjectOf(id)
      val day = pmod(h(id, lit("day")), lit(Days.toLong))
      val cell = concat(lit("CELL_"), lpad(pmod(h(subject, day, lit("cell")), lit(Cells.toLong)).cast("string"), 3, "0"))
      val df = spark.range(0, n * perSubject, 1, partitions).select(
        Seq(id.as("conn_id"), subject.as(SubjectId)) ++ identifier(Imei, id, subject) ++
          Seq(cell.as("cell_id"), timestampWithin(day, id, lit("ts")).as("connect_ts"),
            (pmod(h(id, lit("dur")), lit(3600L)) + 1).as("duration_s")): _*)
      persist(SyntheticTable(BtsConnection, "conn_id", Seq(Imei),
        ordered(df, Seq("conn_id", Imei, "cell_id", "connect_ts", "duration_s"), Seq(Imei))))
    }

    def loyaltyTxn(perSubject: Int): SyntheticTable = {
      val id = col("id")
      val subject = subjectOf(id)
      val day = pmod(h(id, lit("day")), lit(Days.toLong))
      val txnType = when(pmod(h(id, lit("type")), lit(10L)) < 8, lit("earn")).otherwise(lit("redeem"))
      val df = spark.range(0, n * perSubject, 1, partitions).select(
        Seq(id.as("txn_id"), subject.as(SubjectId)) ++ identifier(CustomerId, id, subject) ++
          Seq(txnType.as("txn_type"), (pmod(h(id, lit("pts")), lit(500L)) + 1).as("points"),
            timestampWithin(day, id, lit("ts")).as("txn_ts")): _*)
      persist(SyntheticTable(LoyaltyTxn, "txn_id", Seq(CustomerId),
        ordered(df, Seq("txn_id", CustomerId, "txn_type", "points", "txn_ts"), Seq(CustomerId))))
    }

    /** Cột dữ liệu theo thứ tự hiển thị, cột ẩn đáp án ở cuối. */
    private def ordered(df: DataFrame, visible: Seq[String], ids: Seq[String]): DataFrame =
      df.select((visible ++ (SubjectId +: ids.map(nullFlag))).map(col): _*)
  }
}
