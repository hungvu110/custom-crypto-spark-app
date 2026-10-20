package vai.lakehouse.columncrypto.cdr.audit

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import AuditTestSupport.{source, Identity}

class AuditConfigSpec extends AnyFlatSpec with Matchers {

  private val base = Seq("audit.enabled" -> "true", "audit.sinks" -> "log") ++ Identity

  private def load(kv: (String, String)*) = AuditConfig.load(source(kv: _*))

  private def error(kv: (String, String)*): String = intercept[IllegalArgumentException](load(kv: _*)).getMessage

  "AuditConfig.load" should "tắt audit khi không đặt hoặc đặt false" in {
    load() shouldBe None
    load("audit.enabled" -> "false", "audit.sinks" -> "nonsense") shouldBe None
  }

  it should "dùng giá trị mặc định khi chỉ đặt các key bắt buộc" in {
    val cfg = load(base: _*).get

    cfg.sinks shouldBe Seq(AuditConfig.LogSink)
    cfg.identity shouldBe EngineIdentity("tenantw1xjixm", "wsw7vtwvi", "teamtscauiy", "en-test")
    cfg.coalesceMs shouldBe 0
    cfg.maxQueueSize shouldBe 10000
    cfg.maxUsagesPerEvent shouldBe 5
    cfg.usageTtlSec shouldBe 3600
    cfg.maxUsages shouldBe 10000
    cfg.statementUserTtlSec shouldBe 3600
    cfg.shutdownFlushTimeoutSec shouldBe 10
    cfg.log shouldBe LogSinkConfig(LogSinkConfig.LoggerMode, "vai.lakehouse.cdrcrypto.audit", "CDR_AUDIT")
  }

  it should "đọc các giá trị tuỳ chỉnh" in {
    val cfg = load(base ++ Seq("audit.coalesceMs" -> "200", "audit.maxQueueSize" -> "50", "audit.log.mode" -> "direct",
      "audit.log.prefix" -> "MY_AUDIT", "audit.log.loggerName" -> "x.y") : _*).get

    cfg.coalesceMs shouldBe 200
    cfg.maxQueueSize shouldBe 50
    cfg.log shouldBe LogSinkConfig(LogSinkConfig.DirectMode, "x.y", "MY_AUDIT")
  }

  it should "báo lỗi rõ ràng khi enabled không phải true/false" in {
    error("audit.enabled" -> "yes") should include("spark.cdrcrypto.audit.enabled")
  }

  it should "báo lỗi khi thiếu sinks hoặc định danh engine" in {
    error(base.filterNot(_._1 == "audit.sinks"): _*) should include("spark.cdrcrypto.audit.sinks")
    error(base.filterNot(_._1 == "audit.team"): _*) should include("spark.cdrcrypto.audit.team")
  }

  it should "từ chối sink không hỗ trợ, và sink http (H2) chưa có" in {
    error(base :+ ("audit.sinks" -> "log,foo"): _*) should include("foo")
    error(base :+ ("audit.sinks" -> "http"): _*) should include("not supported yet")
  }

  it should "từ chối định danh engine có ký tự lạ" in {
    error(base :+ ("audit.engineId" -> "en 1/../x"): _*) should include("spark.cdrcrypto.audit.engineId")
  }

  it should "từ chối số không hợp lệ" in {
    error(base :+ ("audit.maxQueueSize" -> "0"): _*) should include("spark.cdrcrypto.audit.maxQueueSize")
    error(base :+ ("audit.coalesceMs" -> "-1"): _*) should include("spark.cdrcrypto.audit.coalesceMs")
    error(base :+ ("audit.usageTtlSec" -> "abc"): _*) should include("spark.cdrcrypto.audit.usageTtlSec")
  }

  it should "từ chối log.mode không hỗ trợ" in {
    error(base :+ ("audit.log.mode" -> "file"): _*) should include("spark.cdrcrypto.audit.log.mode")
  }
}
