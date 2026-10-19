package vai.lakehouse.columncrypto.cdr.audit

import java.nio.file.Files
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ScalaUDF}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

/**
 * Phase 0 của docs/cdr-crypto/CDR_CRYPTO_AUDIT_LOG_PLAN.md: kiểm chứng các hành vi của Spark 3.5.1 mà thiết kế audit dựa vào
 * (ghép lần dùng hàm ↔ SQL execution qua chuỗi plan, event START/END, user qua jobGroup). Giữ lại làm test hồi quy: đổi phiên bản
 * Spark mà test này đỏ thì cơ chế audit cần xem lại trước.
 */
class SparkAuditContractSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with Eventually {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  private val starts = new ConcurrentLinkedQueue[SparkListenerSQLExecutionStart]()
  private val ends = new ConcurrentLinkedQueue[SparkListenerSQLExecutionEnd]()
  private val builderCalls = new AtomicInteger()
  private val builderJobGroups = new ConcurrentLinkedQueue[String]()
  private val usageSeq = new AtomicInteger()

  private val warehouse = Files.createTempDirectory("audit-contract-wh")
  private var spark: SparkSession = _

  private def nextUsageId(): String = f"u${usageSeq.incrementAndGet()}%012x"

  private val probeFn: String => String = v => if (v == "boom") throw new IllegalStateException("boom") else s"x$v"

  /** Cách đã chọn: UDF giữ tên ổn định `cdr_decrypt`, usageId nằm trong [[AuditTag]] bọc ngoài. */
  private def taggedBuilder(args: Seq[Expression]): Expression = {
    builderCalls.incrementAndGet()
    builderJobGroups.add(String.valueOf(
      SparkSession.getActiveSession.map(_.sparkContext.getLocalProperty("spark.jobGroup.id")).orNull))
    AuditTag(ScalaUDF(probeFn, StringType, args, udfName = Some("cdr_decrypt"), udfDeterministic = true), nextUsageId())
  }

  /** Cách bị loại: nhét usageId vào tên UDF. Chỉ giữ lại để ghi nhận vì sao bị loại (tên cột tự sinh đổi theo từng lần chạy). */
  private def namedBuilder(args: Seq[Expression]): Expression =
    ScalaUDF(probeFn, StringType, args, udfName = Some(s"cdr_decrypt#${nextUsageId()}"), udfDeterministic = true)

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder()
      .appName("SparkAuditContractSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.toString)
      .withExtensions { ext =>
        ext.injectFunction((FunctionIdentifier("probe"), new ExpressionInfo(getClass.getName, "probe"), taggedBuilder _))
        ext.injectFunction((FunctionIdentifier("probe_named"), new ExpressionInfo(getClass.getName, "probe_named"), namedBuilder _))
      }
      .getOrCreate()
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case e: SparkListenerSQLExecutionStart => starts.add(e)
        case e: SparkListenerSQLExecutionEnd => ends.add(e)
        case _ =>
      }
    })
    val schema = StructType(Seq(StructField("name", StringType), StructField("grp", StringType)))
    val rows = Seq(Row("a", "g1"), Row("b", "g1"), Row("c", "g2"))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema).createOrReplaceTempView("t")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def usageIdsIn(text: String): Set[String] = AuditTag.usageIdsIn(text).toSet

  /** Chạy `action`, chờ mọi END tương ứng, trả về các START phát sinh trong lúc đó. */
  private def capture(action: => Unit): Seq[SparkListenerSQLExecutionStart] = {
    val before = starts.asScala.map(_.executionId).toSet
    action
    eventually(ends.asScala.map(_.executionId).toSet should contain allElementsOf starts.asScala.map(_.executionId).toSet)
    starts.asScala.filterNot(s => before.contains(s.executionId)).toSeq
  }

  /** Chỉ execution gốc: execution con (ví dụ phần SELECT bên trong CTAS) có rootExecutionId trỏ về execution khác. */
  private def roots(news: Seq[SparkListenerSQLExecutionStart]): Seq[SparkListenerSQLExecutionStart] =
    news.filter(s => s.rootExecutionId.forall(_ == s.executionId))

  "usageId trong AuditTag" should "xuất hiện trong physicalPlanDescription của SQL, AQE bật/tắt, whole-stage codegen bật/tắt" in {
    for (aqe <- Seq("true", "false"); wsc <- Seq("true", "false")) {
      spark.conf.set("spark.sql.adaptive.enabled", aqe)
      spark.conf.set("spark.sql.codegen.wholeStage", wsc)
      var result: Seq[Row] = Nil
      val news = capture { result = spark.sql("SELECT grp, max(probe(name)) FROM t GROUP BY grp ORDER BY grp").collect().toSeq }
      info(s"AQE=$aqe WSC=$wsc: usageIds=${news.map(s => usageIdsIn(s.physicalPlanDescription))}, result=$result")
      roots(news).flatMap(s => usageIdsIn(s.physicalPlanDescription)) should have size 1
      result shouldBe Seq(Row("g1", "xb"), Row("g2", "xc"))
    }
    spark.conf.unset("spark.sql.adaptive.enabled")
    spark.conf.unset("spark.sql.codegen.wholeStage")
  }

  it should "xuất hiện với DataFrame API (Column bọc AuditTag)" in {
    val id = nextUsageId()
    val f = udf(probeFn).withName("cdr_decrypt")
    val tagged = new Column(AuditTag(f(col("name")).expr, id))
    var out: Seq[Row] = Nil
    val news = capture { out = spark.table("t").select(tagged.as("v")).collect().toSeq }
    news.flatMap(s => usageIdsIn(s.physicalPlanDescription)) should contain(id)
    out.map(_.getString(0)) should contain theSameElementsAs Seq("xa", "xb", "xc")
  }

  it should "xuất hiện với INSERT ... SELECT; CTAS sinh execution con nhưng chỉ có một execution gốc" in {
    spark.sql("CREATE TABLE sink_tbl (v STRING) USING parquet")
    val insert = capture(spark.sql("INSERT INTO sink_tbl SELECT probe(name) FROM t"))
    info(s"INSERT: ${insert.map(s => (s.executionId, s.rootExecutionId, usageIdsIn(s.physicalPlanDescription)))}")
    roots(insert).flatMap(s => usageIdsIn(s.physicalPlanDescription)) should have size 1

    val ctas = capture(spark.sql("CREATE TABLE ctas_tbl USING parquet AS SELECT probe(name) AS v FROM t"))
    info(s"CTAS: ${ctas.map(s => (s.executionId, s.rootExecutionId, usageIdsIn(s.physicalPlanDescription)))}")
    roots(ctas) should have size 1
    roots(ctas).flatMap(s => usageIdsIn(s.physicalPlanDescription)) should have size 1
    spark.table("sink_tbl").collect().map(_.getString(0)) should contain theSameElementsAs Seq("xa", "xb", "xc")
  }

  "Tên cột tự sinh khi không có AS" should "ổn định (không chứa usageId) với AuditTag, khác với cách nhét usageId vào tên UDF" in {
    val tagged1 = spark.sql("SELECT probe(name) FROM t").columns.toSeq
    val tagged2 = spark.sql("SELECT probe(name) FROM t").columns.toSeq
    val named = spark.sql("SELECT probe_named(name) FROM t").columns.toSeq
    info(s"AuditTag -> $tagged1 / $tagged2; usageId trong tên UDF -> $named")
    tagged1 shouldBe Seq("cdr_decrypt(name)")
    tagged2 shouldBe tagged1
    named.head should include("#u")
  }

  "Builder hàm" should "chạy một lần mỗi câu SQL, trong thread gọi sql(), và đọc được spark.jobGroup.id" in {
    builderJobGroups.clear()
    val before = builderCalls.get()
    spark.sparkContext.setJobGroup("stmt-123", "test statement")
    try spark.sql("SELECT probe(name) FROM t").collect()
    finally spark.sparkContext.clearJobGroup()
    builderCalls.get() - before shouldBe 1
    builderJobGroups.asScala.toList shouldBe List("stmt-123")
  }

  "SparkListenerSQLExecutionEnd.errorMessage" should "rỗng khi thành công và có nội dung khi query lỗi" in {
    val ok = roots(capture(spark.sql("SELECT probe(name) FROM t").collect())).map(_.executionId).toSet
    val failed = roots(capture(intercept[Exception](spark.sql("SELECT probe('boom') FROM t").collect()))).map(_.executionId).toSet
    val endById = ends.asScala.map(e => e.executionId -> e).toMap
    info(s"ok -> ${ok.map(id => endById(id).errorMessage)}; failed -> ${failed.map(id => endById(id).errorMessage.map(_.take(60)))}")
    ok.foreach(id => endById(id).errorMessage.forall(_.isEmpty) shouldBe true)
    failed.foreach(id => endById(id).errorMessage.exists(_.nonEmpty) shouldBe true)
  }

  "df.rdd" should "không phát SQLExecutionStart (usage sẽ rơi vào EXPIRED)" in {
    capture(spark.sql("SELECT probe(name) AS v FROM t").rdd.count()) shouldBe empty
  }

  "DataFrame dùng lại cho nhiều action" should "giữ cùng usageId; action không cần cột UDF thì không có usage trong plan" in {
    val df = spark.sql("SELECT probe(name) AS v FROM t")
    val first = capture(df.collect()).flatMap(s => usageIdsIn(s.physicalPlanDescription)).toSet
    val second = capture(df.collect()).flatMap(s => usageIdsIn(s.physicalPlanDescription)).toSet
    val counted = capture(df.count()).flatMap(s => usageIdsIn(s.physicalPlanDescription)).toSet
    first should have size 1
    second shouldBe first
    counted shouldBe empty
  }
}

