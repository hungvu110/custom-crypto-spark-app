package vai.lakehouse.columncrypto.cdr.audit

import java.time.Instant
import java.util.concurrent.TimeUnit

import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Concat, Literal}
import org.apache.spark.sql.types.StringType
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

/** Logic ghép usage ↔ execution, user, START/END/EXPIRED — không cần SparkContext (không có accumulator thật). */
class CdrAuditRuntimeSpec extends AnyFlatSpec with Matchers with Eventually {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(5, Seconds), Span(20, Millis))

  private val Clock = Instant.parse("2026-10-19T09:00:00Z")

  private class Fixture(extra: Seq[(String, String)]) {
    var now = 0L
    val sink = new InMemoryAuditSink
    val runtime = new CdrAuditRuntime(AuditTestSupport.enabled(extra: _*), "app-1", "presto", sink, None,
      () => now, () => Clock)

    def register(statementId: Option[String] = None, ugiUser: Option[String] = Some("presto"),
                 counts: Option[CountSource] = None): Usage =
      runtime.registerUsage("cdr_decrypt", "demo_db.t", "isdn", "isdn", "start_datetime", statementId, ugiUser, counts)

    def plan(usages: Usage*): String =
      usages.map(u => s"Project [UDF(isdn#1, start_datetime#2)[cdr_audit:${u.usageId}] AS isdn#3]").mkString("\n")

    def events: Seq[AuditEvent] = sink.events
  }

  private def withFixture(extra: (String, String)*)(body: Fixture => Unit): Unit = {
    val f = new Fixture(extra)
    try body(f) finally f.runtime.close()
  }

  "START" should "liệt kê usage có trong plan và lấy user từ event Thrift của câu lệnh" in withFixture() { f =>
    val u = f.register(statementId = Some("stmt-1"))
    f.runtime.onStatementUser("stmt-1", "alice")

    f.runtime.onExecutionStart(7, Some(7), f.plan(u), 1000L)

    eventually(f.events should have size 1)
    val e = f.events.head
    (e.event, e.executionId, e.user, e.userSource, e.statementId) shouldBe
      ((EventType.Start, Some(7L), Some("alice"), Some(UserSource.Thrift), Some("stmt-1")))
    e.ts shouldBe Instant.ofEpochMilli(1000L)
    e.usages shouldBe Seq(UsageRecord(u.usageId, Some(u.info), None))
    e.identity.engineId shouldBe "en-test"
  }

  it should "dùng user của UGI khi khác sparkUser (doAs), nếu không thì dùng sparkUser" in withFixture() { f =>
    val proxied = f.register(ugiUser = Some("bob"))
    val service = f.register(ugiUser = Some("presto"))

    f.runtime.onExecutionStart(1, None, f.plan(proxied), 0L)
    f.runtime.onExecutionStart(2, None, f.plan(service), 0L)

    eventually(f.events should have size 2)
    f.events.map(e => e.executionId -> (e.user, e.userSource)).toMap shouldBe Map(
      Some(1L) -> (Some("bob"), Some(UserSource.Ugi)),
      Some(2L) -> (Some("presto"), Some(UserSource.SparkUser)))
  }

  it should "bỏ qua execution con và plan không có usage" in withFixture() { f =>
    val u = f.register()

    f.runtime.onExecutionStart(8, Some(7), f.plan(u), 0L)
    f.runtime.onExecutionStart(9, Some(9), "Project [isdn#1]", 0L)
    Thread.sleep(200)

    f.events shouldBe empty
  }

  it should "vẫn ghi START cho usageId không còn trong registry, chỉ với usageId" in withFixture() { f =>
    f.runtime.onExecutionStart(10, None, "Project [UDF(isdn#1)[cdr_audit:u0000000000ff] AS x#2]", 0L)

    eventually(f.events.map(_.usages) shouldBe Seq(Seq(UsageRecord("u0000000000ff", None, None))))
  }

  it should "tách thành nhiều phần khi quá maxUsagesPerEvent" in withFixture("audit.maxUsagesPerEvent" -> "2") { f =>
    val usages = (1 to 5).map(_ => f.register())

    f.runtime.onExecutionStart(3, None, f.plan(usages: _*), 0L)

    eventually(f.events.map(_.part) shouldBe Seq(Some(1 -> 3), Some(2 -> 3), Some(3 -> 3)))
    f.events.flatMap(_.usages).map(_.usageId) shouldBe usages.map(_.usageId)
  }

  "END" should "ghi trạng thái, thời lượng và số dòng phát sinh trong execution" in withFixture() { f =>
    val counts = new FakeCounts
    val u = f.register(counts = Some(counts))
    f.runtime.onExecutionStart(7, Some(7), f.plan(u), 1000L)
    counts.rows = 3
    counts.nullRows = 1

    f.runtime.onExecutionEnd(7, 2500L, Some(""))

    eventually(f.events.map(_.event) shouldBe Seq(EventType.Start, EventType.End))
    val end = f.events.last
    (end.status, end.durationMs, end.executionId) shouldBe ((Some(Status.Succeeded), Some(1500L), Some(7L)))
    end.usages shouldBe Seq(UsageRecord(u.usageId, None, Some(UsageCounts(3, 1))))
  }

  it should "ghi FAILED khi errorMessage có nội dung, và bỏ qua END không có START" in withFixture() { f =>
    val u = f.register()
    f.runtime.onExecutionStart(7, None, f.plan(u), 0L)

    f.runtime.onExecutionEnd(7, 10L, Some("Job aborted"))
    f.runtime.onExecutionEnd(99, 10L, None)

    eventually(f.events.map(e => e.event -> e.status) shouldBe
      Seq(EventType.Start -> None, EventType.End -> Some(Status.Failed)))
  }

  "EXPIRED" should "ghi usage chưa từng chạy khi hết TTL" in withFixture() { f =>
    val u = f.register()

    f.now = TimeUnit.SECONDS.toNanos(3601)
    f.runtime.sweep()

    eventually(f.events.map(e => e.event -> e.usages) shouldBe
      Seq(EventType.Expired -> Seq(UsageRecord(u.usageId, Some(u.info), None))))
    f.events.head.ts shouldBe Clock
  }

  it should "chỉ ghi usage đã chạy khi có dòng được xử lý ngoài SQL execution" in withFixture() { f =>
    val quiet = new FakeCounts
    val outside = new FakeCounts
    val uQuiet = f.register(counts = Some(quiet))
    val uOutside = f.register(counts = Some(outside))
    f.runtime.onExecutionStart(1, None, f.plan(uQuiet, uOutside), 0L)
    f.runtime.onExecutionEnd(1, 1L, Some(""))
    outside.rows = 4 // ví dụ df.rdd.count sau khi query xong

    f.now = TimeUnit.SECONDS.toNanos(3601)
    f.runtime.sweep()

    eventually(f.events.count(_.event == EventType.Expired) shouldBe 1)
    f.events.filter(_.event == EventType.Expired).flatMap(_.usages) shouldBe
      Seq(UsageRecord(uOutside.usageId, Some(uOutside.info), Some(UsageCounts(4, 0))))
  }

  "CdrAuditRuntime.describe" should "che mọi literal (cả số), không chỉ literal chuỗi" in {
    val isdn = AttributeReference("isdn", StringType)()
    val expr = Concat(Seq(isdn, Literal("84903217465"), Cast(Literal(84903217465L), StringType)))

    val described = CdrAuditRuntime.describe(expr)

    described should include("isdn")
    described should not include "84903217465"
  }

  "CdrAuditListener" should "đọc user từ event Thrift bằng reflection và quên khi câu lệnh đóng" in withFixture() { f =>
    val listener = new CdrAuditListener(f.runtime)
    listener.onOtherEvent(SparkListenerThriftServerOperationStart("stmt-9", "sess", "SELECT 1", "stmt-9", 0L, "carol"))
    val first = f.register(statementId = Some("stmt-9"))
    f.runtime.onExecutionStart(1, None, f.plan(first), 0L)

    listener.onOtherEvent(SparkListenerThriftServerOperationClosed("stmt-9", 1L))
    val second = f.register(statementId = Some("stmt-9"))
    f.runtime.onExecutionStart(2, None, f.plan(second), 0L)

    eventually(f.events should have size 2)
    f.events.map(e => e.executionId -> e.user).toMap shouldBe Map(Some(1L) -> Some("carol"), Some(2L) -> Some("presto"))
  }
}

/** Cùng tên ngắn và field với event thật của Spark Thrift Server (module `spark-hive-thriftserver`, không có trên classpath test). */
case class SparkListenerThriftServerOperationStart(
  id: String, sessionId: String, statement: String, groupId: String, startTime: Long, userName: String)
  extends SparkListenerEvent

case class SparkListenerThriftServerOperationClosed(id: String, closeTime: Long) extends SparkListenerEvent
