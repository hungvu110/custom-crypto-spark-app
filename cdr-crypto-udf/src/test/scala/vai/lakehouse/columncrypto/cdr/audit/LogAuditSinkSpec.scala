package vai.lakehouse.columncrypto.cdr.audit

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LogAuditSinkSpec extends AnyFlatSpec with Matchers {

  private def ev(id: String) = AuditEvent(EventType.Start, Instant.parse("2026-10-19T08:15:30Z"), "app",
    EngineIdentity("t", "w", "tm", "e"), Nil, eventId = id)

  private def directSink(out: PrintStream) =
    new LogAuditSink(LogSinkConfig(LogSinkConfig.DirectMode, LogSinkConfig.DefaultLoggerName, "CDR_AUDIT"), out)

  private def linesOf(buf: ByteArrayOutputStream): Seq[String] = new String(buf.toByteArray, UTF_8).split("\n").toSeq

  "Chế độ direct" should "ghi mỗi event đúng một dòng: tiền tố, dấu cách, JSON" in {
    val buf = new ByteArrayOutputStream()

    directSink(new PrintStream(buf, true, "UTF-8")).write(Seq(ev("a"), ev("b")))

    linesOf(buf) shouldBe Seq(s"CDR_AUDIT ${ev("a").toJson}", s"CDR_AUDIT ${ev("b").toJson}")
  }

  it should "không để các dòng xen vào nhau khi nhiều thread cùng ghi" in {
    val buf = new ByteArrayOutputStream()
    val sink = directSink(new PrintStream(buf, false, "UTF-8"))
    val threads = (1 to 8).map(t => new Thread(() => (1 to 200).foreach(i => sink.write(Seq(ev(s"$t-$i"))))))

    threads.foreach(_.start())
    threads.foreach(_.join())

    val lines = linesOf(buf)
    lines should have size 1600
    all(lines) should fullyMatch regex """CDR_AUDIT \{"auditVersion":2,.*\}"""
  }

  "Chế độ logger" should "ghi một message INFO khớp filter .*AUDIT.* của appender audit trên nền tảng" in {
    val name = "vai.lakehouse.cdrcrypto.audit.test.info"

    val captured = capture(name, Level.INFO) {
      new LogAuditSink(LogSinkConfig(LogSinkConfig.LoggerMode, name, "CDR_AUDIT")).write(Seq(ev("a")))
    }

    captured shouldBe Seq(Level.INFO -> s"CDR_AUDIT ${ev("a").toJson}")
    captured.head._2.matches(".*AUDIT.*") shouldBe true
  }

  it should "báo lỗi khi logger tắt INFO thay vì làm mất event âm thầm" in {
    val name = "vai.lakehouse.cdrcrypto.audit.test.warn"

    capture(name, Level.WARN) {
      val ex = intercept[IllegalStateException](
        new LogAuditSink(LogSinkConfig(LogSinkConfig.LoggerMode, name, "CDR_AUDIT")).write(Seq(ev("a"))))
      ex.getMessage should include("log.mode=direct")
    } shouldBe empty
  }

  /** Gắn appender bộ nhớ vào logger `name` của log4j2 (backend mà Spark dùng), chạy `body`, trả về các message đã ghi. */
  private def capture(name: String, level: Level)(body: => Unit): Seq[(Level, String)] = {
    val logger = LogManager.getContext(false).asInstanceOf[LoggerContext].getLogger(name)
    val buf = new ConcurrentLinkedQueue[(Level, String)]()
    val appender = new AbstractAppender(s"memory-$name", null, null, true, Property.EMPTY_ARRAY) {
      override def append(e: LogEvent): Unit = buf.add(e.getLevel -> e.getMessage.getFormattedMessage)
    }
    appender.start()
    logger.addAppender(appender)
    logger.setAdditive(false)
    logger.setLevel(level)
    try body
    finally { logger.removeAppender(appender); appender.stop() }
    buf.asScala.toSeq
  }
}
