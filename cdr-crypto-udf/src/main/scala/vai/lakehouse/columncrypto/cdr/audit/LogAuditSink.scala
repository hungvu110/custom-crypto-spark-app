package vai.lakehouse.columncrypto.cdr.audit

import java.io.PrintStream
import java.nio.charset.StandardCharsets.UTF_8

import org.slf4j.{Logger, LoggerFactory}

/**
 * Sink H1: mỗi event thành một message `<prefix> <json>` trong log của pod driver.
 *  - `logger` (mặc định): qua SLF4J/log4j2 ở level INFO. Trên sql-engine message chứa `AUDIT` nên đi vào cả log container (appender
 *    `console`, stderr) lẫn file `/opt/spark/logs/audit/spark-audit.log` (appender `audit`, lọc `.*AUDIT.*`).
 *  - `direct`: ghi thẳng `out` (mặc định `System.out`), không phụ thuộc cấu hình log4j2; một batch được ghi trong một lần có khoá.
 */
final class LogAuditSink(config: LogSinkConfig, out: PrintStream) extends AuditSink {

  def this(config: LogSinkConfig) = this(config, System.out)

  private val logger: Option[Logger] =
    if (config.mode == LogSinkConfig.LoggerMode) Some(LoggerFactory.getLogger(config.loggerName)) else None

  override def write(events: Seq[AuditEvent]): Unit = logger match {
    case Some(l) =>
      if (!l.isInfoEnabled) throw new IllegalStateException(
        s"Logger '${config.loggerName}' has INFO disabled, so CDR audit events would be lost. " +
          "Enable INFO for that logger or set spark.cdrcrypto.audit.log.mode=direct")
      events.foreach(e => l.info("{}", line(e)))
    case None =>
      val bytes = events.map(e => line(e) + "\n").mkString.getBytes(UTF_8)
      out.synchronized { out.write(bytes); out.flush() }
  }

  private def line(event: AuditEvent): String = s"${config.prefix} ${event.toJson}"
}
