package vai.lakehouse.pseudonym.verify

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import vai.lakehouse.pseudonym.sql.PseudonymExtension

/**
 * Entry point. Exit code: 0 = PASS, 1 = FAIL (check MUST không đạt / selftest có mutation lọt), 2 = ERROR (không kết
 * luận được: check lỗi, đối chứng P6 không đạt, sai cấu hình, job lỗi). Xem docs/pseudonym-verify-app/PLAN.md.
 */
object PseudoVerifyApp {

  val BlockTokenPluginClass = "vai.lakehouse.hdfs.BlockTokenFixPlugin"
  val DeltaExtension        = "io.delta.sql.DeltaSparkSessionExtension"
  val DeltaCatalog          = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

  def main(args: Array[String]): Unit = {
    val jobStart = System.nanoTime()
    Log.banner("PSEUDONYM VERIFY — SINH DỮ LIỆU, GIẢ DANH HOÁ, KIỂM CHỨNG KHỬ ĐỊNH DANH")

    Log.section("1. CONFIG")
    val cfg = VerifyConfig.fromEnv(sys.env) match {
      case Right(c) => c
      case Left(errors) =>
        errors.foreach(e => Log.fail(s"CONFIG $e"))
        sys.exit(Verdict.Error.exitCode)
    }
    cfg.describe.foreach { case (k, v) => Log.kv(k, v) }

    val exitCode =
      try {
        val keys = if (cfg.isEphemeralKeys) KeyMaterial.ephemeral() else KeyMaterial.fromConf(new SparkConf(), cfg.keySource)
        Log.kv("khoá", keys.description)

        Log.section("2. SPARK SESSION")
        val spark = createSession(cfg.appName, keys)
        Log.kv("spark version", spark.version)
        Log.kv("application id", spark.sparkContext.applicationId)
        Log.kv("master", spark.sparkContext.master)
        try run(new VerifySession(spark, cfg, keys)).exitCode
        finally spark.stop()
      } catch {
        case t: Throwable =>
          Log.fail("VERIFY_FAILED job dừng vì lỗi", t)
          Log.causeChain(t)
          Verdict.Error.exitCode
      }

    Log.section("KẾT THÚC")
    Log.kv("exit code", exitCode)
    Log.elapsed("tổng thời gian", jobStart)
    sys.exit(exitCode)
  }

  def run(s: VerifySession): Verdict = {
    Log.section(s"3. RUN_MODE=${s.cfg.runMode.name}")
    val verdict = s.cfg.runMode match {
      case RunMode.Full     => FullRun.run(s)
      case RunMode.Selftest => SelftestRun.run(s)
      case RunMode.Guard    => GuardRun.run(s)
    }
    Log.kv("VERDICT", verdict.name)
    verdict
  }

  /** Thêm vào danh sách đã khai trong manifest (không ghi đè): patch block token Isilon + Delta (cho MAPPING). */
  def merged(existing: Option[String], extra: String): String =
    (existing.toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty) :+ extra).distinct.mkString(",")

  def createSession(appName: String, keys: KeyMaterial): SparkSession = {
    val conf = new SparkConf()
    SparkSession.builder().appName(appName)
      .config("spark.plugins", merged(conf.getOption("spark.plugins"), BlockTokenPluginClass))
      .config("spark.sql.extensions", merged(conf.getOption("spark.sql.extensions"), DeltaExtension))
      .config("spark.sql.catalog.spark_catalog", DeltaCatalog)
      // pseudo_hash/pseudo_encrypt cho check U9 (DataFrame API ≡ SQL), dùng đúng khoá của lần chạy.
      .withExtensions(new PseudonymExtension(() => keys.provider))
      .getOrCreate()
  }
}
