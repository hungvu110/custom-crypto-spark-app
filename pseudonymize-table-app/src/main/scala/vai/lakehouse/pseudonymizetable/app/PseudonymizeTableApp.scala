package vai.lakehouse.pseudonymizetable.app

import org.apache.spark.sql.SparkSession
import vai.lakehouse.pseudonymizetable.config.JobConfig
import vai.lakehouse.pseudonymizetable.job.PseudonymizeTableJob
import vai.lakehouse.pseudonymizetable.logging.Log

/**
 * Entry point: khử định danh 1 bảng (`DB_NAME`, `TABLE_NAME`, `PSEUDO_COLUMNS`) -> bảng đích.
 *
 * Exit code: 0 = xong và đối chiếu đạt, 1 = lỗi khi chạy, 2 = sai cấu hình, 3 = đã ghi bảng đích nhưng đối chiếu
 * nguồn/đích KHÔNG đạt (không dùng bảng đích). `main` chỉ đổi kết quả của [[execute]] thành exit code.
 */
object PseudonymizeTableApp {

  val ExitOk = 0
  val ExitRuntimeError = 1
  val ExitConfigError = 2
  val ExitVerificationFailed = 3

  def main(args: Array[String]): Unit = sys.exit(execute(sys.env))

  /**
   * Cấu hình -> session -> job -> exit code. Không ném exception, không `sys.exit` (gọi được từ test).
   *
   * @param existingSession session dựng sẵn (test); `None` = tự dựng theo cấu hình và tự `stop` khi xong. Session truyền
   *                        vào KHÔNG bị `stop`.
   */
  def execute(env: Map[String, String], existingSession: Option[SparkSession] = None): Int = {
    val start = System.nanoTime()
    Log.banner("PSEUDONYMIZE TABLE — KHỬ ĐỊNH DANH 1 BẢNG BẰNG pseudonym-udf")

    Log.section("1. CONFIG")
    val exitCode = JobConfig.fromEnv(env) match {
      case Left(errors) =>
        errors.foreach(e => Log.fail(s"CONFIG $e"))
        ExitConfigError
      case Right(cfg) =>
        cfg.describe.foreach { case (k, v) => Log.kv(k, v) }
        runJob(cfg, existingSession)
    }

    Log.section("KẾT THÚC")
    Log.kv("exit code", exitCode)
    Log.elapsed("tổng thời gian", start)
    exitCode
  }

  private def runJob(cfg: JobConfig, existingSession: Option[SparkSession]): Int =
    try {
      Log.section("2. SPARK SESSION")
      val owned = existingSession.isEmpty
      val spark = existingSession.getOrElse(SparkSessions.create(cfg.appName, cfg.enableHiveSupport, cfg.warehousePath))
      Log.kv("spark version", spark.version)
      Log.kv("application id", spark.sparkContext.applicationId)
      Log.kv("master", spark.sparkContext.master)
      try {
        val result = PseudonymizeTableJob.run(spark, cfg)
        if (!cfg.verify || result.verified) {
          Log.banner("JOB SUCCEEDED")
          ExitOk
        } else {
          Log.banner("JOB WRITTEN BUT VERIFICATION FAILED — KHÔNG dùng bảng đích " + cfg.target)
          ExitVerificationFailed
        }
      } finally if (owned) spark.stop()
    } catch {
      case t: Throwable =>
        Log.banner("JOB FAILED")
        Log.causeChain(t)
        ExitRuntimeError
    }
}
