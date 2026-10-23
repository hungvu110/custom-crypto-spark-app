package vai.lakehouse.columncrypto.cdr.audit

import java.nio.file.Files

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.LoneElement._
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import vai.lakehouse.columncrypto.cdr.{CdrCrypto, CdrCryptoExtension}
import vai.lakehouse.keyprefix.{ColumnKeySource, KeyIntent}

/** Audit qua SQL và DataFrame API thật (`local[2]`, listener và accumulator thật), sink trong bộ nhớ. */
class CdrAuditIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with Eventually {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(10, Seconds), Span(50, Millis))

  private val Prefix = "231x#@Vie3"
  private val Dataset = "demo_db.sub_rel_product"
  private val Plaintexts = Seq("978356099", "912345678")

  private val stubSource = new ColumnKeySource {
    override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] =
      columns.map(_.toLowerCase -> Prefix).toMap
  }

  private val sink = new InMemoryAuditSink
  @volatile private var runtime: CdrAuditRuntime = _
  @volatile private var auditOn = false
  private var ciphertexts: Seq[String] = Nil

  private val warehouse = Files.createTempDirectory("cdr-audit-wh")
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder()
      .appName("CdrAuditIntegrationSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.toString)
      .withExtensions(new CdrCryptoExtension(() => stubSource, () => if (auditOn) Option(runtime) else None))
      .getOrCreate()
    runtime = CdrAuditRuntime.start(AuditTestSupport.enabled(), spark.sparkContext, sink, installShutdownHook = false)

    val schema = StructType(Seq(StructField("isdn", StringType), StructField("start_datetime", StringType)))
    val rows = Seq(Row(Plaintexts(0), "20260928123456"), Row(Plaintexts(1), "20260101000000"), Row(null, "20260101000001"))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema).createOrReplaceTempView("cdr_sample")
    spark.sql(s"CREATE TABLE enc_tbl USING parquet AS " +
      s"SELECT cdr_encrypt('$Dataset', 'isdn', isdn, start_datetime) AS isdn, start_datetime FROM cdr_sample")
    ciphertexts = spark.table("enc_tbl").collect().flatMap(r => Option(r.getString(0))).toSeq
    auditOn = true
  }

  override def afterAll(): Unit = {
    if (runtime != null) runtime.close()
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private val DecryptSql = s"SELECT cdr_decrypt('$Dataset', 'isdn', isdn, start_datetime) FROM enc_tbl"

  /** Chạy `body`, chờ đến khi mọi START đều có END, trả về các event phát sinh. */
  private def audited(body: => Unit): Seq[AuditEvent] = {
    sink.clear()
    body
    eventually {
      val starts = sink.events.count(_.event == EventType.Start)
      starts should be > 0
      sink.events.count(_.event == EventType.End) shouldBe starts
    }
    sink.events
  }

  "cdr_decrypt qua SQL" should "sinh một cặp START/END với dataset, cột, trạng thái và số dòng" in {
    var result: Seq[String] = Nil
    val events = audited { result = spark.sql(DecryptSql).collect().map(_.getString(0)).toSeq }

    result should contain theSameElementsAs (Plaintexts :+ null)
    val Seq(start, end) = events
    start.event shouldBe EventType.Start
    start.executionId shouldBe end.executionId
    start.appId shouldBe spark.sparkContext.applicationId
    val info = start.usages.loneElement.info.get
    (info.function, info.dataset, info.column) shouldBe ((CdrCryptoExtension.DecryptFunction, Dataset, "isdn"))
    info.valueExpr should endWith("isdn")
    info.keyFieldExpr should endWith("start_datetime")
    end.status shouldBe Some(Status.Succeeded)
    end.usages.loneElement.counts shouldBe Some(UsageCounts(3, 1))
  }

  it should "giữ nguyên tên cột kết quả; chỉ plan có thêm cdr_audit" in {
    auditOn = false
    val (offColumns, offPlan) = { val df = spark.sql(DecryptSql); (df.columns.toSeq, df.queryExecution.executedPlan.toString) }
    auditOn = true
    val (onColumns, onPlan) = { val df = spark.sql(DecryptSql); (df.columns.toSeq, df.queryExecution.executedPlan.toString) }

    onColumns shouldBe offColumns
    offPlan should not include "cdr_audit:"
    onPlan should include("cdr_audit:")
  }

  it should "ghi FAILED khi giải mã lỗi" in {
    val events = audited {
      intercept[Exception](spark.sql(
        s"SELECT cdr_decrypt('$Dataset', 'isdn', isdn, '00000000000000') FROM enc_tbl WHERE isdn IS NOT NULL").collect())
    }

    events.filter(_.event == EventType.End).map(_.status) shouldBe Seq(Some(Status.Failed))
  }

  it should "lấy user từ event Thrift qua spark.jobGroup.id của câu lệnh" in {
    runtime.onStatementUser("stmt-42", "alice")
    val events = audited {
      spark.sparkContext.setJobGroup("stmt-42", "statement")
      try spark.sql(DecryptSql).collect()
      finally spark.sparkContext.clearJobGroup()
    }

    val start = events.find(_.event == EventType.Start).get
    (start.user, start.userSource, start.statementId) shouldBe ((Some("alice"), Some(UserSource.Thrift), Some("stmt-42")))
  }

  it should "tách đúng START/END, số dòng và user khi hai query chạy song song" in {
    val users = Map("stmt-a" -> "alice", "stmt-b" -> "bob")
    users.foreach { case (statement, user) => runtime.onStatementUser(statement, user) }
    val ready = new java.util.concurrent.CountDownLatch(users.size)
    val events = audited {
      val threads = users.keys.map { statement =>
        new Thread(() => {
          spark.sparkContext.setJobGroup(statement, statement)
          ready.countDown()
          ready.await()
          try spark.sql(DecryptSql).collect()
          finally spark.sparkContext.clearJobGroup()
        })
      }
      threads.foreach(_.start())
      threads.foreach(_.join())
    }

    val starts = events.filter(_.event == EventType.Start)
    val ends = events.filter(_.event == EventType.End)
    starts.map(s => s.statementId -> s.user).toSet shouldBe users.map { case (s, u) => Some(s) -> Some(u) }.toSet
    starts.flatMap(_.executionId).distinct should have size 2
    ends.flatMap(_.executionId).toSet shouldBe starts.flatMap(_.executionId).toSet
    all(ends.map(_.usages.loneElement.counts)) shouldBe Some(UsageCounts(3, 1))
    // Mỗi END nói về đúng usage mà START cùng executionId đã liệt kê.
    starts.foreach { s =>
      ends.find(_.executionId == s.executionId).get.usages.map(_.usageId) shouldBe s.usages.map(_.usageId)
    }
  }

  "cdr_encrypt trong CTAS" should "chỉ sinh một cặp START/END dù Spark tạo execution con" in {
    val events = audited {
      spark.sql(s"CREATE TABLE enc_copy USING parquet AS SELECT cdr_encrypt('$Dataset', 'isdn', isdn, start_datetime) AS isdn FROM cdr_sample")
    }

    events.map(_.event) shouldBe Seq(EventType.Start, EventType.End)
    events.head.usages.loneElement.info.map(_.function) shouldBe Some(CdrCryptoExtension.EncryptFunction)
    events.last.usages.loneElement.counts shouldBe Some(UsageCounts(3, 1))
  }

  "CdrCrypto.decryptColumns (DataFrame API)" should "được audit giống SQL" in {
    val cfg = CdrCrypto.loadConfig(Dataset, "start_datetime", Seq("isdn"), KeyIntent.Decrypt, stubSource)
    var result: Seq[String] = Nil
    val events = audited {
      result = CdrCrypto.applyWith(spark.table("enc_tbl"), cfg, encryptMode = false, Some(runtime))
        .collect().map(_.getAs[String]("isdn")).toSeq
    }

    result should contain theSameElementsAs (Plaintexts :+ null)
    val info = events.head.usages.loneElement.info.get
    (info.function, info.dataset, info.column) shouldBe ((CdrCryptoExtension.DecryptFunction, Dataset, "isdn"))
    events.last.usages.loneElement.counts shouldBe Some(UsageCounts(3, 1))
  }

  "Audit tắt" should "không sinh event nào" in {
    auditOn = false
    try {
      sink.clear()
      spark.sql(DecryptSql).collect()
      Thread.sleep(300)
      sink.events shouldBe empty
    } finally auditOn = true
  }

  "Mọi event đã ghi" should "không chứa keyPrefix, giá trị gốc hay ciphertext" in {
    val json = sink.history.map(_.toJson)

    json should not be empty
    all(json) should not include Prefix
    (Plaintexts ++ ciphertexts).foreach(v => all(json) should not include v)
  }
}
