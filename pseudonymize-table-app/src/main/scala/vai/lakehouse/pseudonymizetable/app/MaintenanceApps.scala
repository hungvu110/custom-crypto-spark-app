package vai.lakehouse.pseudonymizetable.app

import org.apache.spark.sql.SparkSession
import vai.lakehouse.pseudonymizetable.config.{MaintenanceConfig, PurgeConfig, VacuumConfig}
import vai.lakehouse.pseudonymizetable.job.Maintenance
import vai.lakehouse.pseudonymizetable.logging.Log

/** Exit code chung của hai app bảo trì: 0 = xong (kể cả không còn gì để làm), 1 = lỗi khi chạy, 2 = sai cấu hình. */
object MaintenanceExit {
  val Ok = 0
  val RuntimeError = 1
  val ConfigError = 2

  /** Cấu hình -> session -> việc -> exit code. Không ném exception, không `sys.exit`; session truyền vào (test) không bị `stop`. */
  def run[C](title: String, parsed: Either[Seq[String], C], describe: C => Seq[(String, String)],
             session: C => Option[SparkSession] => (SparkSession, Boolean), work: (SparkSession, C) => Int,
             existing: Option[SparkSession]): Int = {
    Log.banner(title)
    Log.section("1. CONFIG")
    parsed match {
      case Left(errors) => errors.foreach(e => Log.fail(s"CONFIG $e")); ConfigError
      case Right(cfg) =>
        describe(cfg).foreach { case (k, v) => Log.kv(k, v) }
        try {
          Log.section("2. SPARK SESSION")
          val (spark, owned) = session(cfg)(existing)
          try work(spark, cfg) finally if (owned) spark.stop()
        } catch {
          case t: Throwable => Log.banner("JOB FAILED"); Log.causeChain(t); RuntimeError
        }
    }
  }
}

/**
 * Xoá vùng khai thác của một phiếu hết hạn: xoá các partition (`INPUT_DATE_KEY`, `INPUT_HOUR_KEY`) trong `INPUT_FROM`..`INPUT_TO`
 * (+ `INPUT_HOURS`) của `OUTPUT_DB`.`OUTPUT_TABLE`. Idempotent: chạy lại, hoặc bảng không còn, vẫn là exit 0.
 */
object PurgeRangeApp {
  def main(args: Array[String]): Unit = sys.exit(execute(sys.env))

  def execute(env: Map[String, String], existing: Option[SparkSession] = None): Int =
    MaintenanceExit.run[PurgeConfig]("PURGE RANGE — DELETE THE PARTITIONS OF AN EXPIRED REQUEST", MaintenanceConfig.purgeFromEnv(env),
      _.describe, cfg => ex => ex.map(s => (s, false)).getOrElse((SparkSessions.create("PurgeRange", cfg.enableHiveSupport, cfg.warehousePath), true)),
      (spark, cfg) => { Maintenance.purge(spark, cfg); Log.banner("PURGE SUCCEEDED"); MaintenanceExit.Ok }, existing)
}

/** VACUUM định kỳ các bảng đích: xoá vật lý file đã bị bỏ khỏi log. Có bảng lỗi thì exit 1 (sau khi đã thử hết các bảng). */
object VacuumApp {
  def main(args: Array[String]): Unit = sys.exit(execute(sys.env))

  def execute(env: Map[String, String], existing: Option[SparkSession] = None): Int =
    MaintenanceExit.run[VacuumConfig]("VACUUM — REMOVE THE FILES DELTA NO LONGER REFERENCES", MaintenanceConfig.vacuumFromEnv(env),
      _.describe, cfg => ex => ex.map(s => (s, false)).getOrElse((SparkSessions.create("DeidVacuum", cfg.enableHiveSupport, cfg.warehousePath), true)),
      (spark, cfg) => {
        val result = Maintenance.vacuum(spark, cfg)
        Log.kv("vacuumed / skipped / failed", s"${result.vacuumed.size} / ${result.skipped.size} / ${result.failed.size}")
        if (result.failed.isEmpty) { Log.banner("VACUUM SUCCEEDED"); MaintenanceExit.Ok }
        else { Log.banner("VACUUM FAILED FOR: " + result.failed.map(_._1).mkString(", ")); MaintenanceExit.RuntimeError }
      }, existing)
}
