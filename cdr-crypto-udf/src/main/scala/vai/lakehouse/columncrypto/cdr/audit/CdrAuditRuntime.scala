package vai.lakehouse.columncrypto.cdr.audit

import java.time.Instant
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.slf4j.LoggerFactory

/**
 * Lõi audit trên driver (docs/cdr-crypto/CDR_CRYPTO_AUDIT_LOG_PLAN.md mục 4): đăng ký mỗi lần dùng hàm (usage) lúc builder chạy, ghép
 * usage với SQL execution qua [[AuditTag]] trong chuỗi plan, dựng START/END/EXPIRED và đẩy sang [[AuditDispatcher]]. Mọi phương thức
 * được gọi từ thread của query hoặc thread listener bus và không bao giờ chặn chờ ghi audit.
 *
 * @param sparkContext `None` khi test không có Spark: usage không được đếm và không đọc `spark.jobGroup.id`.
 */
final class CdrAuditRuntime private[audit] (
  val config: AuditConfig,
  appId: String,
  sparkUser: String,
  sink: AuditSink,
  sparkContext: Option[SparkContext],
  nanoTime: () => Long = () => System.nanoTime(),
  clock: () => Instant = () => Instant.now()) {

  import CdrAuditRuntime._

  private val log = LoggerFactory.getLogger(classOf[CdrAuditRuntime])
  private val registry = new UsageRegistry(TimeUnit.SECONDS.toNanos(config.usageTtlSec), config.maxUsages, nanoTime)
  private val users = new StatementUserTracker(TimeUnit.SECONDS.toNanos(config.statementUserTtlSec), config.maxUsages, nanoTime)
  private val dispatcher = new AuditDispatcher(sink, config.maxQueueSize, config.coalesceMs)
  private val executions = new ConcurrentHashMap[java.lang.Long, Execution]()
  private var sweeper: Option[ScheduledExecutorService] = None

  /**
   * Gọi trong builder của hàm: đăng ký usage rồi bọc biểu thức UDF bằng [[AuditTag]]. `build` nhận meter đếm dòng (nếu có Spark) để
   * gắn vào hàm biến đổi của UDF.
   */
  def tag(function: String, dataset: String, value: Expression, fieldValue: Expression,
          build: Option[AuditMeter] => Expression): Expression = {
    val meter = sparkContext.map(AuditMeter.register)
    val statementId = sparkContext.flatMap(sc => Option(sc.getLocalProperty(JobGroupProperty)))
    val usage = registerUsage(function, dataset, describe(value), describe(fieldValue), statementId, currentUgiUser(), meter)
    AuditTag(build(meter), usage.usageId)
  }

  def registerUsage(function: String, dataset: String, valueExpr: String, keyFieldExpr: String,
                    statementId: Option[String], ugiUser: Option[String], counts: Option[CountSource]): Usage = {
    val info = UsageInfo(AuditTag.newUsageId(), function, dataset,
      AuditText.describeExpr(valueExpr), AuditText.describeExpr(keyFieldExpr), config.dakRequestCode)
    val usage = new Usage(info, statementId, ugiUser, counts, nanoTime())
    expire(registry.register(usage))
    usage
  }

  /** `SparkListenerSQLExecutionStart`: chỉ xét execution gốc (execution con của CTAS... trỏ `rootExecutionId` về execution khác). */
  def onExecutionStart(executionId: Long, rootExecutionId: Option[Long], planDescription: String, timeMs: Long): Unit = {
    val tagged = !rootExecutionId.exists(_ != executionId) && planDescription.contains(AuditTag.Marker)
    val ids = if (tagged) AuditTag.usageIdsIn(planDescription) else Nil
    if (ids.nonEmpty) {
      val found = ids.flatMap(registry.get)
      val now = nanoTime()
      found.foreach(_.begin(now))
      executions.put(executionId, Execution(found, ids.filterNot(id => found.exists(_.usageId == id)), timeMs))
      val statementId = found.flatMap(_.statementId).headOption
      val (user, source) = resolveUser(found, statementId)
      // usageId không còn trong registry (đã hết TTL) vẫn được ghi để không mất dấu vết, chỉ thiếu thông tin chi tiết.
      val records = ids.map(id => UsageRecord(id, found.find(_.usageId == id).map(_.info), None))
      submit(AuditEvent(EventType.Start, Instant.ofEpochMilli(timeMs), appId, config.identity, records,
        executionId = Some(executionId), user = Some(AuditText.limit(user)), userSource = Some(source),
        statementId = statementId.map(AuditText.limit)))
    }
  }

  /** `SparkListenerSQLExecutionEnd`: Spark 3.5 trả `errorMessage = Some("")` khi thành công, có nội dung khi lỗi. */
  def onExecutionEnd(executionId: Long, timeMs: Long, errorMessage: Option[String]): Unit =
    Option(executions.remove(executionId)).foreach { execution =>
      val now = nanoTime()
      val records = execution.usages.map(u => UsageRecord(u.usageId, None, u.end(now))) ++
        execution.unknownIds.map(UsageRecord(_, None, None))
      val status = if (errorMessage.exists(_.nonEmpty)) Status.Failed else Status.Succeeded
      submit(AuditEvent(EventType.End, Instant.ofEpochMilli(timeMs), appId, config.identity, records,
        executionId = Some(executionId), status = Some(status), durationMs = Some(math.max(0L, timeMs - execution.startMs))))
    }

  def onStatementUser(statementId: String, user: String): Unit = users.put(statementId, user)

  def onStatementClosed(statementId: String): Unit = users.remove(statementId)

  /** Dọn usage và user hết hạn. Usage chưa từng chạy, hoặc có dòng được xử lý ngoài SQL execution (vd `df.rdd`), sinh EXPIRED. */
  def sweep(): Unit = {
    expire(registry.sweep())
    users.sweep()
  }

  /** Dừng bộ dọn định kỳ, ghi nốt hàng đợi (tối đa `shutdownFlushTimeoutSec`) rồi đóng sink. Gọi nhiều lần không sao. */
  def close(): Unit = {
    synchronized {
      sweeper.foreach(_.shutdownNow())
      sweeper = None
    }
    dispatcher.close(TimeUnit.SECONDS.toMillis(config.shutdownFlushTimeoutSec))
  }

  private[audit] def startSweeper(): Unit = synchronized {
    val periodSec = math.max(1L, math.min(60L, config.usageTtlSec))
    val executor = Executors.newSingleThreadScheduledExecutor { (r: Runnable) =>
      val thread = new Thread(r, "cdr-audit-sweeper")
      thread.setDaemon(true)
      thread
    }
    executor.scheduleWithFixedDelay(() => {
      try sweep() catch { case NonFatal(e) => log.warn("CDR audit: periodic sweep failed", e) }
    }, periodSec, periodSec, TimeUnit.SECONDS)
    sweeper = Some(executor)
  }

  private def expire(usages: Seq[Usage]): Unit = usages.foreach { usage =>
    val delta = usage.takeDelta()
    if (!usage.wasExecuted || delta.exists(_.rows > 0))
      submit(AuditEvent(EventType.Expired, clock(), appId, config.identity,
        Seq(UsageRecord(usage.usageId, Some(usage.info), delta)), statementId = usage.statementId.map(AuditText.limit)))
  }

  /** User Thrift của câu lệnh, rồi user UGI nếu khác user dịch vụ (doAs), cuối cùng là `sparkUser`. */
  private def resolveUser(usages: Seq[Usage], statementId: Option[String]): (String, String) =
    statementId.flatMap(users.get).map(_ -> UserSource.Thrift)
      .orElse(usages.flatMap(_.ugiUser).find(_ != sparkUser).map(_ -> UserSource.Ugi))
      .getOrElse(sparkUser -> UserSource.SparkUser)

  private def submit(event: AuditEvent): Unit = AuditEvent.split(event, config.maxUsagesPerEvent).foreach(dispatcher.submit)
}

object CdrAuditRuntime {

  val JobGroupProperty = "spark.jobGroup.id"

  private final case class Execution(usages: Seq[Usage], unknownIds: Seq[String], startMs: Long)

  /** Khởi động audit cho `sc`: listener, bộ dọn định kỳ và (tuỳ chọn) shutdown hook để ghi nốt hàng đợi khi JVM tắt. */
  def start(config: AuditConfig, sc: SparkContext, sink: AuditSink, installShutdownHook: Boolean = true): CdrAuditRuntime = {
    val runtime = new CdrAuditRuntime(config, sc.applicationId, sc.sparkUser, sink, Some(sc))
    sc.addSparkListener(new CdrAuditListener(runtime))
    runtime.startSweeper()
    if (installShutdownHook) sys.addShutdownHook(runtime.close())
    runtime
  }

  def sinkFor(config: AuditConfig): AuditSink = {
    val sinks = config.sinks.map {
      case AuditConfig.LogSink => new LogAuditSink(config.log)
      case other => throw new IllegalArgumentException(s"Unsupported CDR audit sink '$other'")
    }
    if (sinks.size == 1) sinks.head else new CompositeAuditSink(sinks)
  }

  private def currentUgiUser(): Option[String] = Try(UserGroupInformation.getCurrentUser.getShortUserName).toOption

  /** Mô tả biểu thức tham số để ghi audit: mọi literal (chuỗi, số, ngày...) bị thay bằng `'***'` trước khi in `sql`. */
  private[audit] def describe(expression: Expression): String = {
    val masked = expression.transform { case _: Literal => Literal(MaskedLiteral) }
    Try(masked.sql).getOrElse(masked.prettyName)
  }

  private val MaskedLiteral = "***"
}
