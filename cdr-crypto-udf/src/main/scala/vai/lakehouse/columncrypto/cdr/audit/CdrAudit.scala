package vai.lakehouse.columncrypto.cdr.audit

import org.apache.spark.{SparkContext, SparkEnv}
import org.slf4j.LoggerFactory
import vai.lakehouse.columncrypto.cdr.CdrCrypto
import vai.lakehouse.keyprefix.{ChainedConfigSource, EnvConfigSource, SparkConfSource}

/**
 * Runtime audit dùng chung cho cả tiến trình (giống `CdrCrypto.sharedPrefixSource`: Thrift Server khởi tạo extension lại theo từng
 * SparkSession, nhưng chỉ được có một listener và một hàng đợi). Đọc `spark.cdrcrypto.audit.*` lần đầu cần dùng; `None` khi audit tắt.
 * Cấu hình sai thì ném `IllegalArgumentException` — `CdrCryptoExtension` gọi tới ngay khi được nạp nên engine không khởi động được.
 */
object CdrAudit {

  lazy val shared: Option[CdrAuditRuntime] = {
    val conf = SparkEnv.get.conf
    val source = new ChainedConfigSource(Seq(new SparkConfSource(conf, CdrCrypto.ConfPrefix), new EnvConfigSource()))
    AuditConfig.load(source).map { config =>
      val runtime = CdrAuditRuntime.start(config, SparkContext.getOrCreate(conf), CdrAuditRuntime.sinkFor(config))
      LoggerFactory.getLogger(getClass).info(
        s"CDR audit enabled: sinks=${config.sinks.mkString(",")}, log.mode=${config.log.mode}, engineId=${config.identity.engineId}")
      runtime
    }
  }
}
