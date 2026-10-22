package vai.lakehouse.columncrypto.cdr.audit

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

/** Định danh cố định của engine/job, ghi vào mọi event (mỗi engine thuộc đúng một team). */
final case class EngineIdentity(tenant: String, workspace: String, team: String, engineId: String)

/**
 * Thông tin tĩnh của một lần dùng hàm, biết ngay lúc builder chạy. Không chứa giá trị dữ liệu hay keyPrefix.
 *
 * @param column tên cột dùng để chọn key (tham số 2 của hàm, chữ thường) — có từ `auditVersion` 2
 */
final case class UsageInfo(
  usageId: String,
  function: String,
  dataset: String,
  column: String,
  valueExpr: String,
  keyFieldExpr: String)

/** Số dòng hàm đã xử lý (chỉ tính task thành công) và số dòng có đầu vào NULL. */
final case class UsageCounts(rows: Long, nullRows: Long) {
  def minus(other: UsageCounts): UsageCounts = UsageCounts(rows - other.rows, nullRows - other.nullRows)
}

object UsageCounts {
  val Zero: UsageCounts = UsageCounts(0, 0)
}

/** Một phần tử của `usages`: START mang `info`, END mang `counts`, EXPIRED mang cả hai. */
final case class UsageRecord(usageId: String, info: Option[UsageInfo], counts: Option[UsageCounts])

object EventType {
  val Start = "START"
  val End = "END"
  val Expired = "EXPIRED"
}

object Status {
  val Succeeded = "SUCCEEDED"
  val Failed = "FAILED"
}

/** Nguồn của trường `user`, theo thứ tự tin cậy giảm dần. */
object UserSource {
  val Thrift = "thrift"
  val Ugi = "ugi"
  val SparkUser = "sparkUser"
}

/**
 * Một event audit, ghi thành đúng một dòng JSON (schema: docs/cdr-crypto/CDR_CRYPTO_AUDIT_LOG_PLAN.md mục 3). Trường không có giá trị
 * thì bỏ hẳn khỏi JSON. `eventId` cố định cho từng event để bên đọc dedupe khi giao hàng at-least-once.
 */
final case class AuditEvent(
  event: String,
  ts: Instant,
  appId: String,
  identity: EngineIdentity,
  usages: Seq[UsageRecord],
  executionId: Option[Long] = None,
  user: Option[String] = None,
  userSource: Option[String] = None,
  statementId: Option[String] = None,
  status: Option[String] = None,
  durationMs: Option[Long] = None,
  part: Option[(Int, Int)] = None,
  dropped: Long = 0L,
  eventId: String = UUID.randomUUID().toString) {

  def toJson: String = {
    val json = new JsonObject
    json.num("auditVersion", AuditEvent.Version)
    json.str("eventId", eventId)
    json.str("event", event)
    json.str("ts", ts.truncatedTo(ChronoUnit.MILLIS).toString)
    json.str("appId", appId)
    executionId.foreach(json.num("executionId", _))
    user.foreach(json.str("user", _))
    userSource.foreach(json.str("userSource", _))
    statementId.foreach(json.str("statementId", _))
    json.str("tenant", identity.tenant)
    json.str("workspace", identity.workspace)
    json.str("team", identity.team)
    json.str("engineId", identity.engineId)
    status.foreach(json.str("status", _))
    durationMs.foreach(json.num("durationMs", _))
    part.foreach { case (index, total) => json.num("part", index); json.num("parts", total) }
    if (dropped > 0) json.num("dropped", dropped)
    json.raw("usages", usages.map(usageJson).mkString("[", ",", "]"))
    json.result
  }

  private def usageJson(usage: UsageRecord): String = {
    val json = new JsonObject
    json.str("usageId", usage.usageId)
    usage.info.foreach { i =>
      json.str("function", i.function)
      json.str("dataset", i.dataset)
      json.str("column", i.column)
      json.str("valueExpr", i.valueExpr)
      json.str("keyFieldExpr", i.keyFieldExpr)
    }
    usage.counts.foreach { c =>
      json.num("rows", c.rows)
      json.num("nullRows", c.nullRows)
    }
    json.result
  }
}

object AuditEvent {

  /** 2: mỗi usage có thêm `column` (key theo cột). */
  val Version = 2

  /** Tách event có quá `maxUsages` phần tử `usages` thành nhiều phần (`part`/`parts`), mỗi phần một `eventId`, để một dòng log luôn nhỏ. */
  def split(event: AuditEvent, maxUsages: Int): Seq[AuditEvent] =
    if (event.usages.size <= maxUsages) Seq(event)
    else {
      val groups = event.usages.grouped(maxUsages).toSeq
      groups.zipWithIndex.map { case (group, i) =>
        event.copy(usages = group, part = Some((i + 1, groups.size)), eventId = UUID.randomUUID().toString)
      }
    }
}

/** Dựng object JSON theo đúng thứ tự thêm trường (không thêm dependency JSON vào jar thuần). */
private[audit] final class JsonObject {
  private val sb = new StringBuilder("{")

  private def key(name: String): StringBuilder = {
    if (sb.length > 1) sb.append(',')
    sb.append(AuditText.quote(name)).append(':')
  }

  def str(name: String, value: String): Unit = key(name).append(AuditText.quote(value))
  def num(name: String, value: Long): Unit = key(name).append(value)
  def raw(name: String, json: String): Unit = key(name).append(json)
  def result: String = sb.append('}').toString
}
