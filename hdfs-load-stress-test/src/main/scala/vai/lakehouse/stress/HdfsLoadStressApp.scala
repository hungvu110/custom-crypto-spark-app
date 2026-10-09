package vai.lakehouse.stress

import java.net.InetAddress

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import vai.lakehouse.hdfs.BlockTokenDiagnostics

/**
 * Entry point. Exit code: 0 = chạy xong (xem STRESS_SUMMARY), 1 = lỗi khi chạy, 2 = sai cấu hình,
 * 3 = patch block token không hoạt động trên driver hoặc executor (REQUIRE_BLOCK_TOKEN_PATCH=true).
 */
object HdfsLoadStressApp {

  val BlockTokenPluginClass = "vai.lakehouse.hdfs.BlockTokenFixPlugin"

  def main(args: Array[String]): Unit = {
    val jobStart = System.nanoTime()

    Log.banner("HDFS LOAD STRESS TEST — GHI DỮ LIỆU TỔNG HỢP VÀO HDFS (Kerberos + wire encryption + block token)")

    Log.section("1. CONFIG")
    val cfg = StressConfig.fromEnv(sys.env) match {
      case Right(c) => c
      case Left(errors) =>
        errors.foreach(e => Log.fail(s"CONFIG $e"))
        sys.exit(2)
    }
    cfg.describe.foreach { case (k, v) => Log.kv(k, v) }

    Log.section("2. SPARK SESSION")
    val spark = createSession(cfg.appName)
    Log.kv("spark version", spark.version)
    Log.kv("application id", spark.sparkContext.applicationId)
    Log.kv("master", spark.sparkContext.master)
    Log.kv("default parallelism", spark.sparkContext.defaultParallelism)
    Log.kv("fs.defaultFS", spark.sparkContext.hadoopConfiguration.get("fs.defaultFS", "<unset>"))

    val exitCode =
      try {
        if (!blockTokenPatchReady(spark, cfg.requirePatch)) 3
        else {
          val runPath = new Path(cfg.runPath)
          val fs      = runPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
          new StressRunner(spark, cfg, new RunStore(fs, fs.makeQualified(runPath))).run()
          0
        }
      } catch {
        case t: Throwable =>
          Log.fail("STRESS_FAILED job dừng vì lỗi", t)
          Log.causeChain(t)
          1
      } finally spark.stop()

    Log.section("KẾT THÚC")
    Log.kv("exit code", exitCode)
    Log.elapsed("tổng thời gian", jobStart)
    sys.exit(exitCode)
  }

  /**
   * Thêm BlockTokenFixPlugin vào spark.plugins (giữ plugin đã khai trong manifest): plugin ghi đè
   * tokenKindMap ngay khi driver/executor khởi động, phòng trường hợp ServiceLoader thua cuộc đua classpath.
   */
  def mergedPlugins(existing: Option[String]): String =
    (existing.toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty) :+ BlockTokenPluginClass).distinct.mkString(",")

  private def createSession(appName: String): SparkSession = {
    val plugins = mergedPlugins(new SparkConf().getOption("spark.plugins"))
    SparkSession.builder().appName(appName).config("spark.plugins", plugins).getOrCreate()
  }

  /** Kiểm tra class đang xử lý HDFS_BLOCK_TOKEN trên driver và trên từng executor trước khi ghi dữ liệu. */
  private def blockTokenPatchReady(spark: SparkSession, required: Boolean): Boolean = {
    Log.section("3. BLOCK TOKEN PATCH")
    val driver = BlockTokenDiagnostics.isPatchActive()
    val slots  = math.max(1, spark.sparkContext.defaultParallelism)
    val executors = spark.sparkContext
      .parallelize(1 to slots, slots)
      .mapPartitions(_ => Iterator(InetAddress.getLocalHost.getHostName -> BlockTokenDiagnostics.isPatchActive()))
      .collect()
      .distinct
      .toSeq

    Log.kv("class xử lý HDFS_BLOCK_TOKEN", BlockTokenDiagnostics.registeredBlockTokenClass().getOrElse("<không đọc được>"))
    report(driver, "BLOCK_TOKEN_PATCH driver")
    executors.foreach { case (host, active) => report(active, s"BLOCK_TOKEN_PATCH executor $host") }

    val ready = driver && executors.forall(_._2)
    if (!ready && required) {
      Log.fail("BLOCK_TOKEN_PATCH chưa hoạt động -> dừng trước khi ghi (đặt REQUIRE_BLOCK_TOKEN_PATCH=false để bỏ qua)")
    } else if (!ready) {
      Log.warn("BLOCK_TOKEN_PATCH chưa hoạt động nhưng REQUIRE_BLOCK_TOKEN_PATCH=false -> vẫn chạy tiếp")
    }
    ready || !required
  }

  private def report(active: Boolean, what: String): Unit =
    if (active) Log.ok(s"$what: active") else Log.fail(s"$what: NOT active")
}
