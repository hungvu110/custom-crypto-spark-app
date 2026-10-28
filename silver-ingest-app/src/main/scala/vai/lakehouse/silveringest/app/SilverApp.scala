package vai.lakehouse.silveringest.app

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import vai.lakehouse.columncrypto.cdr.CdrCrypto
import vai.lakehouse.keyprefix.{ChainedConfigSource, ColumnKeySource, ColumnKeySourceFactory, ColumnKeysUnavailableException,
  EnvConfigSource, PathKeySource, PathKeySourceFactory, SparkConfSource}
import vai.lakehouse.silveringest.config.{IngestConfig, Mode}
import vai.lakehouse.silveringest.job.{IngestResult, DecryptEncryptJob, PassthroughJob}
import vai.lakehouse.silveringest.logging.Log

/** Nguồn key của mode decrypt-encrypt: `by-path` và `by-column` của DAK. */
final case class KeySources(pathKeys: PathKeySource, columnKeys: ColumnKeySource)

/**
 * Phần chung của hai entry point [[PassthroughSilverApp]] và [[DecryptEncryptSilverApp]]: cấu hình -> session -> job -> exit code.
 *
 * Exit code: 0 = xong (decrypt-encrypt: có hay không có key `by-path` đều là xong), 1 = lỗi khi chạy, 2 = sai cấu hình (chưa dựng Spark),
 * 5 = decrypt-encrypt: DAK chưa có key cho một số cột cần mã hoá — chưa ghi gì, chạy lại sau khi DAK tạo key.
 */
object SilverApp {

  val ExitOk = 0
  val ExitRuntimeError = 1
  val ExitConfigError = 2
  val ExitColumnKeysPending = 5

  /**
   * Không ném exception, không `sys.exit` (gọi được từ test).
   *
   * @param existingSession session dựng sẵn (test), KHÔNG bị `stop`; `None` = tự dựng và tự `stop`
   * @param keys            nguồn key dựng sẵn (test); `None` = dựng từ biến môi trường `CRYPTO_PREFIX_SOURCE`/`DAK_*` và
   *                        `spark.cdrcrypto.*` (chỉ VTNET)
   */
  def execute(mode: Mode, env: Map[String, String], existingSession: Option[SparkSession] = None,
              keys: Option[KeySources] = None): Int = {
    val start = System.nanoTime()
    Log.banner(s"SILVER INGEST — ${mode.name.toUpperCase}")
    Log.section("1. CONFIG")
    val exitCode = IngestConfig.fromEnv(mode, env) match {
      case Left(errors) =>
        errors.foreach(e => Log.fail(s"CONFIG $e"))
        ExitConfigError
      case Right(cfg) =>
        cfg.describe.foreach { case (k, v) => Log.kv(k, v) }
        keySources(cfg, env, keys) match {
          case Left(error) =>
            Log.fail(s"CONFIG $error")
            ExitConfigError
          case Right(sources) => run(cfg, existingSession, sources)
        }
    }
    Log.section("FINISHED")
    Log.kv("exit code", exitCode)
    Log.elapsed("total duration", start)
    exitCode
  }

  // Dựng nguồn key TRƯỚC khi có Spark: thiếu cấu hình DAK là lỗi cấu hình (exit 2), không phải lỗi khi chạy.
  private def keySources(cfg: IngestConfig, env: Map[String, String],
                         given: Option[KeySources]): Either[String, Option[KeySources]] =
    if (cfg.decryptEncrypt.isEmpty) Right(None)
    else given.map(k => Right(Some(k))).getOrElse {
      try {
        val envSource = new EnvConfigSource(env)
        val columnConfig = new ChainedConfigSource(Seq(new SparkConfSource(new SparkConf(), CdrCrypto.ConfPrefix), envSource))
        Right(Some(KeySources(PathKeySourceFactory.create(envSource), ColumnKeySourceFactory.create(columnConfig))))
      } catch {
        case e: IllegalArgumentException => Left(e.getMessage)
      }
    }

  private def run(cfg: IngestConfig, existingSession: Option[SparkSession], keys: Option[KeySources]): Int =
    try {
      Log.section("2. SPARK SESSION")
      val owned = existingSession.isEmpty
      val spark = existingSession.getOrElse(SparkSessions.create(cfg.appName, cfg.enableHiveSupport, cfg.warehousePath))
      Log.kv("spark version", spark.version)
      Log.kv("application id", spark.sparkContext.applicationId)
      try {
        val result = (cfg.decryptEncrypt, keys) match {
          case (Some(_), Some(k)) => DecryptEncryptJob.run(spark, cfg, k.pathKeys, k.columnKeys)
          case _ => PassthroughJob.run(spark, cfg)
        }
        result match {
          case IngestResult.Written(files, decrypted) =>
            Log.banner(s"JOB SUCCEEDED — $files file(s)${if (decrypted) " decrypted and" else ""} written to ${cfg.target}")
        }
        ExitOk
      } finally if (owned) spark.stop()
    } catch {
      case e: ColumnKeysUnavailableException =>
        Log.banner("COLUMN KEYS NOT READY — nothing written, rerun after DAK creates the keys")
        Log.fail(e.getMessage)
        ExitColumnKeysPending
      case NonFatal(e) =>
        Log.banner("JOB FAILED")
        Log.causeChain(e)
        ExitRuntimeError
    }
}

/** Entry point của mode passthrough: đọc file và ghi bảng nguyên trạng, không gọi DAK. */
object PassthroughSilverApp {
  def main(args: Array[String]): Unit = sys.exit(SilverApp.execute(Mode.Passthrough, sys.env))
}

/** Entry point của mode decrypt-encrypt: giải mã file (khi DAK có key), mã hoá cột (nếu có), ghi bảng. */
object DecryptEncryptSilverApp {
  def main(args: Array[String]): Unit = sys.exit(SilverApp.execute(Mode.DecryptEncrypt, sys.env))
}
