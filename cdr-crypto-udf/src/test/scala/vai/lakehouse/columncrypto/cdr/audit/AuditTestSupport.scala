package vai.lakehouse.columncrypto.cdr.audit

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import vai.lakehouse.keyprefix.ConfigSource

/** Dựng cấu hình audit cho test từ cặp key/value logic (`audit.*`), giống cách `SparkConfSource` đọc `spark.cdrcrypto.*`. */
object AuditTestSupport {

  val Identity: Seq[(String, String)] = Seq(
    "audit.tenant" -> "tenantw1xjixm",
    "audit.workspace" -> "wsw7vtwvi",
    "audit.team" -> "teamtscauiy",
    "audit.engineId" -> "en-test")

  def source(kv: (String, String)*): ConfigSource = new ConfigSource {
    private val values = kv.toMap
    override def get(key: String): Option[String] = values.get(key).map(_.trim).filter(_.nonEmpty)
    override def describe(key: String): String = s"spark.cdrcrypto.$key"
  }

  /** Cấu hình hợp lệ tối thiểu (sink `log`), `extra` ghi đè hoặc bổ sung. */
  def enabled(extra: (String, String)*): AuditConfig =
    AuditConfig.load(source(Seq("audit.enabled" -> "true", "audit.sinks" -> "log") ++ Identity ++ extra: _*))
      .getOrElse(throw new IllegalStateException("audit should be enabled"))
}

/** Sink ghi nhớ event trong bộ nhớ. Không `final` để test ghi đè `write` mô phỏng sink chậm/lỗi. */
class InMemoryAuditSink extends AuditSink {
  private val received = new ConcurrentLinkedQueue[AuditEvent]()
  private val everything = new ConcurrentLinkedQueue[AuditEvent]()
  private val sizes = new ConcurrentLinkedQueue[Int]()
  @volatile var closed = false

  override def write(events: Seq[AuditEvent]): Unit = {
    sizes.add(events.size)
    events.foreach { e => received.add(e); everything.add(e) }
  }

  override def close(): Unit = closed = true

  def events: Seq[AuditEvent] = received.asScala.toSeq
  def batchSizes: Seq[Int] = sizes.asScala.toSeq
  /** Mọi event từng nhận, kể cả trước các lần `clear()`. */
  def history: Seq[AuditEvent] = everything.asScala.toSeq
  def clear(): Unit = { received.clear(); sizes.clear() }
}

/** Nguồn số đếm do test điều khiển, thay cho accumulator của Spark. */
final class FakeCounts extends CountSource {
  @volatile var rows = 0L
  @volatile var nullRows = 0L
  override def snapshot: UsageCounts = UsageCounts(rows, nullRows)
}
