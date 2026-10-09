package vai.lakehouse.stress

import java.nio.file.Files

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Chạy thật với Spark local và filesystem local (file://) thay cho HDFS, kích thước tính bằng KB. */
class StressRunnerSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark = SparkSession.builder()
    .master("local[2]")
    .appName("StressRunnerSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private def gib(bytes: Long): String = (bytes / Units.GiB).toString

  private def config(output: String, overrides: (String, String)*): StressConfig = {
    val env = Map(
      "RUN_ID"          -> "test",
      "OUTPUT_PATH"     -> output,
      "MODE"            -> "burst",
      "PAYLOAD_BYTES"   -> "64",
      "TARGET_FILE_MIB" -> "1",
      "TOTAL_GIB"       -> gib(200000),
      "BATCH_GIB"       -> gib(60000)) ++ overrides
    StressConfig.fromEnv(env).fold(errors => fail(errors.mkString("; ")), identity)
  }

  private def store(cfg: StressConfig): RunStore = {
    val path = new Path(cfg.runPath)
    val fs   = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    new RunStore(fs, fs.makeQualified(path))
  }

  private def newOutput(): String = Files.createTempDirectory("stress").toUri.toString

  private def rowsWritten(cfg: StressConfig): (Long, Long) = {
    val df = spark.read.parquet(cfg.runPath)
    (df.count(), df.select("id").distinct().count())
  }

  test("burst writes batches until the total is reached and records each one") {
    val cfg     = config(newOutput())
    val runs    = store(cfg)
    val summary = new StressRunner(spark, cfg, runs).run()

    assert(summary.batches >= 3)
    assert(summary.completed)
    assert(summary.status == "PASS")
    assert(runs.recover() == Progress(summary.batches, summary.bytesTotal))

    val (rows, distinctIds) = rowsWritten(cfg)
    assert(rows == distinctIds)
    assert(rows > 0)
  }

  test("resumes after the last recorded batch and rewrites a batch left half-written") {
    val output = newOutput()
    val first  = new StressRunner(spark, config(output), store(config(output))).run()

    // Pod chết khi đang ghi batch kế tiếp: thư mục có dữ liệu nhưng chưa có metric.
    val runs     = store(config(output))
    val dangling = new Path(runs.batchPath(first.batches), "part-garbage")
    val fs       = dangling.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.create(dangling).close()

    val bigger = config(output, "TOTAL_GIB" -> gib(400000))
    val second = new StressRunner(spark, bigger, store(bigger)).run()

    assert(!fs.exists(dangling))
    assert(second.batches > 0)
    assert(second.bytesTotal == first.bytesTotal + second.bytesThisRun)
    assert(second.completed)
    val (rows, distinctIds) = rowsWritten(bigger)
    assert(rows == distinctIds)
  }

  test("sustained mode starts one batch per interval and stops when the duration is used up") {
    var now    = 1000000L
    val sleeps = ArrayBuffer.empty[Long]
    val perBatchBytes = 20000L
    val cfg = config(newOutput(),
      "MODE"                   -> "sustained",
      "BATCH_INTERVAL_SECONDS" -> "10",
      "DURATION_HOURS"         -> (25.0 / 3600).toString,
      "STRESS_GIB_PER_DAY"     -> gib(perBatchBytes * 8640),
      "TOTAL_GIB"              -> gib(10000000))

    val summary = new StressRunner(spark, cfg, store(cfg), () => now, ms => { sleeps += ms; now += ms }).run()

    assert(summary.batches == 3) // t = 0s, 10s, 20s; batch ở 30s vượt 25s nên không chạy
    assert(sleeps.toList == List(10000L, 10000L))
    assert(summary.maxLagSeconds == 0.0)
    assert(summary.keptUp)
    assert(!summary.completed) // tổng 10 MB không thể đạt trong 25s -> WARN
    assert(summary.status == "WARN")
  }

  test("keeps only the newest batches when RETAIN_BATCHES is set but still counts every batch") {
    val cfg     = config(newOutput(), "RETAIN_BATCHES" -> "1")
    val runs    = store(cfg)
    val summary = new StressRunner(spark, cfg, runs).run()

    val fs = runs.runPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val batchDirs = fs.listStatus(runs.runPath).map(_.getPath.getName).filter(_.startsWith("batch="))
    assert(batchDirs.toSeq == Seq(f"batch=${summary.batches - 1}%06d"))
    assert(runs.recover().bytesDone == summary.bytesTotal)
  }

  test("verifies the written row count by reading each batch back") {
    val cfg     = config(newOutput(), "VERIFY_READ" -> "true", "TOTAL_GIB" -> gib(60000))
    val summary = new StressRunner(spark, cfg, store(cfg)).run()
    // VERIFY_READ ném lỗi nếu số dòng đọc lại lệch, nên chạy hết là đã kiểm xong.
    assert(summary.batches >= 1)
    assert(summary.completed)
  }
}
