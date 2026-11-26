package vai.lakehouse.silveringest.app

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import vai.lakehouse.columncrypto.cdr.CdrCrypto
import vai.lakehouse.keyprefix.{ChainedConfigSource, ColumnKeySource, ColumnKeySourceFactory, ColumnKeysUnavailableException,
  EnvConfigSource, GlobalColumnKeySource, GlobalKeySourceFactory, PathKeySource, PathKeySourceFactory, SparkConfSource}
import vai.lakehouse.silveringest.config.{BulkLoad, IngestConfig, MissingDay, Mode}
import vai.lakehouse.silveringest.job.{IngestResult, DecryptEncryptJob, PassthroughJob, SourceFiles}
import vai.lakehouse.silveringest.logging.Log
import vai.lakehouse.silveringest.security.InputToken

/**
 * Nguồn key của mode decrypt-encrypt: `by-path` (giải mã file) và key mã hoá cột — `by-column` của DAK (mỗi cột một key), hoặc, khi có
 * `GLOBAL_KEY_ZONE`, nguồn key global của vùng (mọi cột chung một key, không gọi `by-column`).
 */
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
              keys: Option[KeySources] = None): Int =
    installInputToken(mode, env).getOrElse(executeLoad(mode, env, existingSession, keys))

  /**
   * `INPUT_TOKEN_FILE` (INPUT_PATH ở một cluster khác, [[InputToken]]): nạp token vào user hiện tại TRƯỚC khi dựng Spark và trước mọi lần đọc
   * file. Không có biến: không làm gì. `Some(exit code)` = dừng (file thiếu, sai, không có token cho cluster của INPUT_PATH, hoặc hết hạn).
   */
  private def installInputToken(mode: Mode, env: Map[String, String]): Option[Int] = {
    val now = System.currentTimeMillis()
    InputToken.install(env, env.getOrElse("INPUT_PATH", "").trim, now) match {
      case Right(None) => None
      case Right(Some(installed)) =>
        Log.section("0. INPUT TOKEN (INPUT_PATH is on another cluster)")
        Log.kv("token file", installed.file)
        installed.tokens.foreach(t => Log.info(InputToken.describe(t, now)))
        installed.warnings.foreach(Log.warn)
        Log.ok(s"${installed.tokens.size} token(s) added to the current user")
        None
      case Left(error) =>
        Log.banner(s"SILVER INGEST — ${mode.name.toUpperCase} — INPUT TOKEN")
        Log.fail(s"CONFIG $error")
        Some(ExitConfigError)
    }
  }

  private def executeLoad(mode: Mode, env: Map[String, String], existingSession: Option[SparkSession], keys: Option[KeySources]): Int =
    BulkLoad.fromEnv(env) match {
      case Left(errors) =>
        Log.banner(s"SILVER INGEST — ${mode.name.toUpperCase} — BULK LOAD")
        errors.foreach(e => Log.fail(s"CONFIG $e"))
        ExitConfigError
      case Right(None) => executeDay(mode, env, existingSession, keys)
      case Right(Some(bulk)) => executeBulk(mode, env, bulk, existingSession, keys)
    }

  /**
   * Bulk load (`BULK_FROM`..`BULK_TO`): lần lượt từng ngày trong CÙNG MỘT Spark session, mỗi ngày là một lần chạy bình thường ([[executeDay]]) với
   * `INPUT_PATH` của ngày đó. Cấu hình của ngày đầu (và nguồn key) được kiểm trước khi dựng Spark. Dừng ở ngày đầu tiên thất bại với exit code của ngày đó:
   * các ngày trước đã nạp xong (nạp tiếp bằng `BULK_FROM` = ngày lỗi), các ngày sau chưa nạp. `BULK_MISSING_DAY=skip` bỏ ngày không có file; không ngày nào
   * nạp được là lỗi.
   */
  private def executeBulk(mode: Mode, env: Map[String, String], bulk: BulkLoad, existingSession: Option[SparkSession],
                          keys: Option[KeySources]): Int = {
    val start = System.nanoTime()
    val days = bulk.days
    Log.banner(s"SILVER INGEST — ${mode.name.toUpperCase} — BULK LOAD ${bulk.from}..${bulk.to} (${days.size} day(s), missing day: ${bulk.onMissingDay.name})")
    val code = IngestConfig.fromEnv(mode, BulkLoad.dayEnv(env, days.head)) match {
      case Left(errors) =>
        errors.foreach(e => Log.fail(s"CONFIG $e"))
        ExitConfigError
      case Right(cfg) =>
        keySources(cfg, env, keys) match {
          case Left(error) =>
            Log.fail(s"CONFIG $error")
            ExitConfigError
          case Right(_) => loadDays(mode, env, bulk, cfg, existingSession, keys)
        }
    }
    Log.section("BULK LOAD FINISHED")
    Log.kv("exit code", code)
    Log.elapsed("total duration", start)
    code
  }

  private def loadDays(mode: Mode, env: Map[String, String], bulk: BulkLoad, cfg: IngestConfig, existingSession: Option[SparkSession],
                       keys: Option[KeySources]): Int = {
    val owned = existingSession.isEmpty
    try {
      val spark = existingSession.getOrElse(SparkSessions.create(cfg.appName, cfg.enableHiveSupport, cfg.warehousePath))
      try {
        val days = bulk.days
        var loaded = Vector.empty[String]
        var skipped = Vector.empty[String]
        var code = ExitOk
        val remaining = days.iterator
        while (remaining.hasNext && code == ExitOk) {
          val day = remaining.next()
          val dayEnv = BulkLoad.dayEnv(env, day)
          Log.section(s"BULK DAY $day (${loaded.size + skipped.size + 1}/${days.size})")
          if (bulk.onMissingDay == MissingDay.Skip && !SourceFiles.hasFiles(spark, dayEnv("INPUT_PATH"))) {
            Log.warn(s"no file for $day under ${dayEnv("INPUT_PATH")}: skipped (BULK_MISSING_DAY=skip)")
            skipped :+= day
          } else {
            val dayStart = System.nanoTime()
            code = executeDay(mode, dayEnv, Some(spark), keys)
            if (code == ExitOk) {
              loaded :+= day
              Log.ok(BulkLoad.dayDone(day, loaded.size + skipped.size, days.size, (System.nanoTime() - dayStart) / 1e9, cfg.target,
                cfg.batch.map(_.column).getOrElse(IngestConfig.DefaultBatchColumn)))
            } else Log.fail(s"BULK LOAD STOPPED at day $day (exit code $code): loaded ${loaded.size} day(s) before it " +
              s"(${loaded.mkString(", ")}); to continue run again with BULK_FROM=$day")
          }
        }
        Log.kv("days loaded", s"${loaded.size} ${loaded.mkString("(", ", ", ")")}")
        if (skipped.nonEmpty) Log.kv("days skipped (no file)", skipped.mkString(", "))
        if (code == ExitOk && loaded.isEmpty) {
          Log.fail("BULK LOAD loaded no day: every day of the range has no file")
          ExitRuntimeError
        } else code
      } finally if (owned) spark.stop()
    } catch {
      case NonFatal(e) =>
        Log.banner("BULK LOAD FAILED")
        Log.causeChain(e)
        ExitRuntimeError
    }
  }

  /** Một ngày (hoặc một lô): cấu hình -> session -> job -> exit code. Dùng cho chạy thường và cho từng ngày của bulk load. */
  def executeDay(mode: Mode, env: Map[String, String], existingSession: Option[SparkSession] = None,
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
        // Key global: mọi cột chung key của vùng (cùng client DAK với by-path), không có key theo cột.
        val columnKeys = cfg.decryptEncrypt.flatMap(_.globalZone) match {
          case Some(zone) => new GlobalColumnKeySource(GlobalKeySourceFactory.create(envSource), zone)
          case None => ColumnKeySourceFactory.create(columnConfig)
        }
        Right(Some(KeySources(PathKeySourceFactory.create(envSource), columnKeys)))
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
