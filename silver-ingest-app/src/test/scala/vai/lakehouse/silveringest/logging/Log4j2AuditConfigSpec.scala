package vai.lakehouse.silveringest.logging

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.OutputStreamAppender
import org.apache.logging.log4j.core.config.Configurator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import vai.lakehouse.columncrypto.cdr.audit.LogSinkConfig

/** Kiểm tra log4j2.properties của app giữ lại dòng audit của cdr-crypto-udf (mặc định `LogSinkConfig`). */
class Log4j2AuditConfigSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val ConfigUri = getClass.getClassLoader.getResource("log4j2.properties").toURI
  private val context = Configurator.initialize("silver-audit-test", getClass.getClassLoader, ConfigUri).asInstanceOf[LoggerContext]

  override def afterAll(): Unit = Configurator.shutdown(context)

  "log4j2.properties" should "route the default audit logger at INFO to the console, not through the root logger" in {
    val loggerConfig = context.getConfiguration.getLoggerConfig(LogSinkConfig.DefaultLoggerName)

    loggerConfig.getName shouldBe LogSinkConfig.DefaultLoggerName
    loggerConfig.getLevel shouldBe Level.INFO
    loggerConfig.isAdditive shouldBe false
    loggerConfig.getAppenderRefs.asScala.map(_.getRef) should contain("console")
  }

  it should "keep the root logger at WARN so that other INFO logs stay hidden" in {
    context.getConfiguration.getRootLogger.getLevel shouldBe Level.WARN
    context.getLogger("org.apache.spark.SomeClass").isInfoEnabled shouldBe false
  }

  it should "write a CDR_AUDIT line in the console layout and drop the same line from an unrelated logger" in {
    val captured = new ByteArrayOutputStream()
    val console = context.getConfiguration.getAppender[org.apache.logging.log4j.core.Appender]("console")
    val appender = OutputStreamAppender.createAppender(console.getLayout, null, captured, "capture", false, true)
    appender.start()
    val loggerConfig = context.getConfiguration.getLoggerConfig(LogSinkConfig.DefaultLoggerName)
    loggerConfig.addAppender(appender, null, null)
    context.updateLoggers()

    val line = s"""${LogSinkConfig.DefaultPrefix} {"auditVersion":2,"event":"START"}"""
    context.getLogger(LogSinkConfig.DefaultLoggerName).info(line)
    context.getLogger("some.other.Logger").info(line)
    loggerConfig.removeAppender("capture")

    val out = new String(captured.toByteArray, StandardCharsets.UTF_8).trim
    out should include(s"INFO audit: $line")
    out.linesIterator.size shouldBe 1
  }
}
