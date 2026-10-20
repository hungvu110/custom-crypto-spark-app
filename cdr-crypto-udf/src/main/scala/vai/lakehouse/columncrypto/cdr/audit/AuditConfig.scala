package vai.lakehouse.columncrypto.cdr.audit

import java.util.Locale

import scala.util.Try

import vai.lakehouse.keyprefix.ConfigSource

/** Cấu hình sink H1: ghi qua logger log4j2 (`logger`) hay thẳng `System.out` (`direct`). */
final case class LogSinkConfig(mode: String, loggerName: String, prefix: String)

object LogSinkConfig {
  val LoggerMode = "logger"
  val DirectMode = "direct"
  val DefaultLoggerName = "vai.lakehouse.cdrcrypto.audit"
  /** Chứa chữ `AUDIT` để message tự đi vào appender `audit` (lọc `.*AUDIT.*`) của log4j2 trên sql-engine. */
  val DefaultPrefix = "CDR_AUDIT"
}

final case class AuditConfig(
  sinks: Seq[String],
  identity: EngineIdentity,
  coalesceMs: Long,
  maxQueueSize: Int,
  maxUsagesPerEvent: Int,
  usageTtlSec: Long,
  maxUsages: Int,
  statementUserTtlSec: Long,
  shutdownFlushTimeoutSec: Long,
  log: LogSinkConfig,
  dakRequestCode: Option[String])

/**
 * Đọc cấu hình audit theo tên logic (trên sql-engine/SparkApplication là `spark.cdrcrypto.<key>`):
 * {{{
 * audit.enabled                  true | false (mặc định false)
 * audit.sinks                    (bắt buộc khi bật) log; http (H2) chưa hỗ trợ
 * audit.tenant / audit.workspace / audit.team / audit.engineId   (bắt buộc khi bật) định danh engine
 * audit.coalesceMs               mặc định 0 (ghi ngay)
 * audit.maxQueueSize             mặc định 10000
 * audit.maxUsagesPerEvent        mặc định 5
 * audit.usageTtlSec              mặc định 3600
 * audit.maxUsages                mặc định 10000
 * audit.statementUserTtlSec      mặc định 3600
 * audit.shutdownFlushTimeoutSec  mặc định 10
 * audit.log.mode                 logger | direct (mặc định logger)
 * audit.log.loggerName           mặc định vai.lakehouse.cdrcrypto.audit
 * audit.log.prefix               mặc định CDR_AUDIT (một dấu cách được thêm trước JSON)
 * }}}
 * `dak.requestCode` (cấu hình sẵn có của nguồn keyPrefix) được ghi kèm usage khi `source` là `dak`.
 */
object AuditConfig {

  object Keys {
    val Enabled = "audit.enabled"
    val Sinks = "audit.sinks"
    val Tenant = "audit.tenant"
    val Workspace = "audit.workspace"
    val Team = "audit.team"
    val EngineId = "audit.engineId"
    val CoalesceMs = "audit.coalesceMs"
    val MaxQueueSize = "audit.maxQueueSize"
    val MaxUsagesPerEvent = "audit.maxUsagesPerEvent"
    val UsageTtlSec = "audit.usageTtlSec"
    val MaxUsages = "audit.maxUsages"
    val StatementUserTtlSec = "audit.statementUserTtlSec"
    val ShutdownFlushTimeoutSec = "audit.shutdownFlushTimeoutSec"
    val LogMode = "audit.log.mode"
    val LogLoggerName = "audit.log.loggerName"
    val LogPrefix = "audit.log.prefix"
    val PrefixSource = "source"
    val DakRequestCode = "dak.requestCode"
  }

  val LogSink = "log"
  val HttpSink = "http"
  val DefaultMaxUsagesPerEvent = 5

  private val IdentityPattern = "^[A-Za-z0-9._-]{1,128}$".r

  /** `None` khi audit tắt; ném `IllegalArgumentException` nêu đúng tên key khi cấu hình sai. */
  def load(cfg: ConfigSource): Option[AuditConfig] =
    if (!enabled(cfg)) None
    else Some(AuditConfig(
      sinks = sinks(cfg),
      identity = EngineIdentity(
        identityPart(cfg, Keys.Tenant), identityPart(cfg, Keys.Workspace),
        identityPart(cfg, Keys.Team), identityPart(cfg, Keys.EngineId)),
      coalesceMs = number(cfg, Keys.CoalesceMs, default = 0, min = 0),
      maxQueueSize = number(cfg, Keys.MaxQueueSize, default = 10000, min = 1).toInt,
      maxUsagesPerEvent = number(cfg, Keys.MaxUsagesPerEvent, default = DefaultMaxUsagesPerEvent, min = 1).toInt,
      usageTtlSec = number(cfg, Keys.UsageTtlSec, default = 3600, min = 1),
      maxUsages = number(cfg, Keys.MaxUsages, default = 10000, min = 1).toInt,
      statementUserTtlSec = number(cfg, Keys.StatementUserTtlSec, default = 3600, min = 1),
      shutdownFlushTimeoutSec = number(cfg, Keys.ShutdownFlushTimeoutSec, default = 10, min = 0),
      log = LogSinkConfig(
        logMode(cfg),
        cfg.get(Keys.LogLoggerName).getOrElse(LogSinkConfig.DefaultLoggerName),
        cfg.get(Keys.LogPrefix).getOrElse(LogSinkConfig.DefaultPrefix)),
      dakRequestCode = if (prefixFromDak(cfg)) cfg.get(Keys.DakRequestCode) else None))

  private def enabled(cfg: ConfigSource): Boolean = cfg.get(Keys.Enabled).map(_.toLowerCase(Locale.ROOT)) match {
    case None | Some("false") => false
    case Some("true") => true
    case Some(_) => throw new IllegalArgumentException(s"Invalid ${cfg.describe(Keys.Enabled)}: must be true or false")
  }

  private def sinks(cfg: ConfigSource): Seq[String] = {
    val names = required(cfg, Keys.Sinks).toLowerCase(Locale.ROOT).split(",").map(_.trim).filter(_.nonEmpty).toSeq.distinct
    val unknown = names.filterNot(Set(LogSink, HttpSink))
    require(names.nonEmpty && unknown.isEmpty,
      s"Invalid ${cfg.describe(Keys.Sinks)}: must be a comma-separated list of $LogSink, $HttpSink" +
        (if (unknown.nonEmpty) s" (got: ${unknown.mkString(", ")})" else ""))
    require(!names.contains(HttpSink),
      s"Invalid ${cfg.describe(Keys.Sinks)}: sink '$HttpSink' (Elasticsearch/log collector) is not supported yet; use '$LogSink'")
    names
  }

  private def identityPart(cfg: ConfigSource, key: String): String = {
    val value = required(cfg, key)
    require(IdentityPattern.pattern.matcher(value).matches(),
      s"Invalid ${cfg.describe(key)}: must be 1-128 characters of letters, digits, '.', '_' or '-'")
    value
  }

  private def logMode(cfg: ConfigSource): String = {
    val mode = cfg.get(Keys.LogMode).map(_.toLowerCase(Locale.ROOT)).getOrElse(LogSinkConfig.LoggerMode)
    require(mode == LogSinkConfig.LoggerMode || mode == LogSinkConfig.DirectMode,
      s"Invalid ${cfg.describe(Keys.LogMode)}: must be ${LogSinkConfig.LoggerMode} or ${LogSinkConfig.DirectMode}")
    mode
  }

  private def prefixFromDak(cfg: ConfigSource): Boolean =
    cfg.get(Keys.PrefixSource).exists(_.toLowerCase(Locale.ROOT).split(",").map(_.trim).contains("dak"))

  private def number(cfg: ConfigSource, key: String, default: Long, min: Long): Long =
    cfg.get(key).map { raw =>
      Try(raw.toLong).toOption.filter(v => v >= min && v <= Int.MaxValue).getOrElse(throw new IllegalArgumentException(
        s"Invalid ${cfg.describe(key)}: must be a whole number between $min and ${Int.MaxValue}"))
    }.getOrElse(default)

  private def required(cfg: ConfigSource, key: String): String =
    cfg.get(key).getOrElse(throw new IllegalArgumentException(s"Missing required setting: ${cfg.describe(key)}"))
}
