package vai.lakehouse.columncrypto.cdr.audit

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.slf4j.LoggerFactory

/** Chuyển event của Spark cho [[CdrAuditRuntime]]. Không bao giờ ném lỗi ra listener bus. */
final class CdrAuditListener(runtime: CdrAuditRuntime) extends SparkListener {

  private val log = LoggerFactory.getLogger(classOf[CdrAuditListener])

  override def onOtherEvent(event: SparkListenerEvent): Unit =
    try event match {
      case e: SparkListenerSQLExecutionStart =>
        runtime.onExecutionStart(e.executionId, e.rootExecutionId, e.physicalPlanDescription, e.time)
      case e: SparkListenerSQLExecutionEnd =>
        runtime.onExecutionEnd(e.executionId, e.time, e.errorMessage)
      case other =>
        ThriftServerEvents.dispatch(other, runtime)
    } catch {
      case NonFatal(e) => log.warn(s"CDR audit: failed to handle ${event.getClass.getSimpleName}", e)
    }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = runtime.close()
}

/**
 * Event của Spark Thrift Server nằm trong module `spark-hive-thriftserver` (không phải dependency của module này) nên được nhận theo
 * tên class ngắn và đọc field bằng reflection. Thrift Server đặt `groupId` bằng id câu lệnh, cũng là `spark.jobGroup.id` của thread
 * chạy câu lệnh; `userName` là user đăng nhập JDBC.
 */
private[audit] object ThriftServerEvents {

  val OperationStart = "SparkListenerThriftServerOperationStart"
  val OperationClosed = "SparkListenerThriftServerOperationClosed"

  def dispatch(event: SparkListenerEvent, runtime: CdrAuditRuntime): Unit = event.getClass.getSimpleName match {
    case OperationStart =>
      for (groupId <- field(event, "groupId"); user <- field(event, "userName")) runtime.onStatementUser(groupId, user)
    case OperationClosed =>
      field(event, "id").foreach(runtime.onStatementClosed)
    case _ =>
  }

  private def field(event: AnyRef, name: String): Option[String] =
    Try(event.getClass.getMethod(name).invoke(event)).toOption.collect { case s: String if s.nonEmpty => s }
}
