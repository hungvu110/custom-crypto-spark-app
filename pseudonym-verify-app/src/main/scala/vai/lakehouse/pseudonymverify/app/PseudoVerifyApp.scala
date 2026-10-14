package vai.lakehouse.pseudonymverify.app

import vai.lakehouse.pseudonymverify.config.VerifyConfig
import vai.lakehouse.pseudonymverify.job.{VerifyJob, VerifySession}
import vai.lakehouse.pseudonymverify.key.KeyMaterial
import vai.lakehouse.pseudonymverify.logging.Log
import vai.lakehouse.pseudonymverify.report.Verdict

import org.apache.spark.SparkConf

/**
 * Entry point. Exit code: 0 = PASS, 1 = FAIL (check MUST không đạt / selftest có mutation lọt), 2 = ERROR (không kết
 * luận được: check lỗi, đối chứng P6 không đạt, sai cấu hình, job lỗi). Xem docs/pseudonym-verify-app/PLAN.md.
 *
 * `main` chỉ đổi kết quả của [[execute]] thành exit code — toàn bộ logic nằm trong `execute` (gọi được từ test, không
 * `sys.exit`).
 */
object PseudoVerifyApp {

  def main(args: Array[String]): Unit = sys.exit(execute(sys.env))

  /** Cấu hình -> session -> job theo `RUN_MODE` -> exit code. Không ném exception, không `sys.exit`. */
  def execute(env: Map[String, String]): Int = {
    val start = System.nanoTime()
    Log.banner("PSEUDONYM VERIFY — SINH DỮ LIỆU, GIẢ DANH HOÁ, KIỂM CHỨNG KHỬ ĐỊNH DANH")

    val exitCode = VerifyConfig.fromEnv(env) match {
      case Left(errors) =>
        Log.section("1. CONFIG")
        errors.foreach(e => Log.fail(s"CONFIG $e"))
        Verdict.Error.exitCode
      case Right(cfg) =>
        Log.section("1. CONFIG")
        cfg.describe.foreach { case (k, v) => Log.kv(k, v) }
        runJob(cfg)
    }

    Log.section("KẾT THÚC")
    Log.kv("exit code", exitCode)
    Log.elapsed("tổng thời gian", start)
    exitCode
  }

  private def runJob(cfg: VerifyConfig): Int =
    try {
      val keys = if (cfg.isEphemeralKeys) KeyMaterial.ephemeral() else KeyMaterial.fromConf(new SparkConf(), cfg.keySource)
      Log.kv("khoá", keys.description)

      Log.section("2. SPARK SESSION")
      val spark = SparkSessions.create(cfg.appName, keys)
      Log.kv("spark version", spark.version)
      Log.kv("application id", spark.sparkContext.applicationId)
      Log.kv("master", spark.sparkContext.master)
      try {
        Log.section(s"3. RUN_MODE=${cfg.runMode.name}")
        val verdict = VerifyJob.forMode(cfg.runMode).run(new VerifySession(spark, cfg, keys))
        Log.kv("VERDICT", verdict.name)
        verdict.exitCode
      } finally spark.stop()
    } catch {
      case t: Throwable =>
        Log.fail("VERIFY_FAILED job dừng vì lỗi", t)
        Log.causeChain(t)
        Verdict.Error.exitCode
    }
}
