package vai.lakehouse.stress

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession

/**
 * Vòng lặp ghi batch. `clock`/`sleep` tiêm được để test nhịp sustained mà không phải chờ thật.
 *
 * Mỗi batch: sinh DataFrame -> ghi Parquet vào batch=NNNNNN -> đo byte/file trên HDFS -> (tuỳ chọn) đọc lại đếm
 * dòng -> ghi metric (đánh dấu batch xong) -> xoá batch cũ nếu đặt RETAIN_BATCHES. Lỗi ghi/đọc ném thẳng ra ngoài:
 * job fail, Spark Operator restart (sustained) và lần chạy sau resume từ batch dở.
 */
final class StressRunner(
    spark: SparkSession,
    cfg: StressConfig,
    store: RunStore,
    clock: () => Long = () => System.currentTimeMillis(),
    sleep: Long => Unit = (millis: Long) => Thread.sleep(millis)) {

  def run(): RunSummary = {
    Log.section("4. LOAD")
    Log.kv("run path", store.runPath)
    val recovered = store.recover()
    store.removeBatchesFrom(recovered.nextIndex).foreach(p => Log.warn(s"Xoá batch ghi dở / chưa có metric: $p"))
    Log.kv("bắt đầu từ batch", f"#${recovered.nextIndex}%06d" + (if (recovered.nextIndex > 0) " (resume)" else ""))
    Log.kv("đã ghi trước đó", Units.gib(recovered.bytesDone))
    Log.kv("mục tiêu", Units.gib(cfg.totalBytes))
    Log.kv("dung lượng / batch", Units.mib(cfg.bytesPerBatch))
    if (cfg.mode == Mode.Sustained) Log.kv("chu kỳ batch", s"${cfg.batchIntervalSeconds} s")

    val runStart    = clock()
    val metrics     = ArrayBuffer.empty[BatchMetric]
    var index       = recovered.nextIndex
    var bytesDone   = recovered.bytesDone
    var bytesPerRow = BatchPlanner.initialBytesPerRow(cfg.payloadBytes)
    var running     = true

    while (running) {
      val target    = BatchPlanner.nextBatchBytes(cfg.bytesPerBatch, cfg.totalBytes, bytesDone)
      val scheduled = scheduledStart(runStart, metrics.size)
      if (target <= 0 || timeUp(runStart, scheduled)) {
        running = false
      } else {
        val waitMillis = scheduled - clock()
        if (waitMillis > 0) sleep(waitMillis)
        val lagMillis = math.max(0L, clock() - scheduled)

        val metric = writeBatch(index, target, bytesPerRow, lagMillis)
        store.writeMetric(store.metricName(index), metric.toJson)
        Log.ok(metric.toLogLine)
        if (cfg.mode == Mode.Sustained && metric.lagSeconds >= cfg.batchIntervalSeconds) {
          Log.warn(f"STRESS_BEHIND batch #$index%06d bắt đầu trễ ${Units.secs(metric.lagSeconds)} (>= 1 chu kỳ)")
        }
        if (cfg.retainBatches > 0 && index - cfg.retainBatches >= 0 && store.deleteBatch(index - cfg.retainBatches)) {
          Log.info(f"RETAIN_BATCHES=${cfg.retainBatches}: đã xoá batch #${index - cfg.retainBatches}%06d")
        }

        metrics += metric
        bytesDone += metric.bytes
        bytesPerRow = BatchPlanner.updateEstimate(bytesPerRow, metric.bytes, metric.rows)
        index += 1
      }
    }

    val summary = RunSummary.from(cfg, metrics.toList, bytesDone, (clock() - runStart) / 1000.0)
    store.writeMetric(s"summary-$runStart.json", summary.toJson)
    logSummary(summary)
    summary
  }

  private def logSummary(s: RunSummary): Unit = {
    Log.section("5. SUMMARY")
    Log.kv("RUN_ID / mode", s"${s.runId} / ${s.mode}")
    Log.kv("số batch (lần chạy này)", s.batches)
    Log.kv("đã ghi (lần chạy này)", Units.gib(s.bytesThisRun))
    Log.kv("tổng đã ghi / mục tiêu", s"${Units.gib(s.bytesTotal)} / ${Units.gib(s.targetBytes)}")
    Log.kv("thời gian chạy", Units.secs(s.elapsedSeconds))
    Log.kv("tốc độ ghi (thuần)", Units.mibps(s.writeMiBps))
    Log.kv("tốc độ hiệu dụng (gồm chờ)", Units.mibps(s.effectiveMiBps))
    Log.kv("thời gian ghi p50 / p95 / max",
      s"${Units.secs(s.writeSecondsP50)} / ${Units.secs(s.writeSecondsP95)} / ${Units.secs(s.writeSecondsMax)}")
    s.intervalSeconds.foreach(_ => Log.kv("độ trễ lớn nhất so với lịch", Units.secs(s.maxLagSeconds)))

    val verdict = s"STRESS_SUMMARY status=${s.status} (completed=${s.completed}, keptUp=${s.keptUp})"
    if (s.status == "PASS") Log.ok(verdict) else Log.warn(verdict)
    // Một dòng JSON để copy vào báo cáo / grep; cùng nội dung với _metrics/summary-*.json.
    Log.info(s"STRESS_SUMMARY ${s.toJson}")
  }

  /** Burst không có lịch: batch kế tiếp chạy ngay. */
  private def scheduledStart(runStart: Long, n: Int): Long =
    if (cfg.mode == Mode.Sustained) BatchPlanner.scheduledStart(runStart, n, cfg.batchIntervalSeconds) else clock()

  /** Dừng khi batch kế tiếp sẽ bắt đầu sau khi hết DURATION_HOURS (không ngủ quá hạn chỉ để rồi thoát). */
  private def timeUp(runStart: Long, scheduled: Long): Boolean =
    cfg.durationMillis.exists(d => math.max(scheduled, clock()) - runStart >= d)

  private def writeBatch(index: Int, targetBytes: Long, bytesPerRow: Double, lagMillis: Long): BatchMetric = {
    val rows       = BatchPlanner.rowsFor(targetBytes, bytesPerRow)
    val partitions = BatchPlanner.partitionsFor(targetBytes, cfg.targetFileBytes)
    val path       = store.batchPath(index).toString

    spark.sparkContext.setJobDescription(s"stress batch $index ($rows rows, $partitions files)")
    val df = DataGenerator.generate(spark, BatchPlanner.firstId(index), rows, partitions, cfg.payloadBytes, cfg.runId)

    val writeStart = System.nanoTime()
    df.write.option("compression", cfg.compression).parquet(path)
    val writeSeconds = (System.nanoTime() - writeStart) / 1e9

    val (bytes, files) = store.batchSize(index)
    val readSeconds    = if (cfg.verifyRead) Some(verifyRead(path, rows)) else None

    BatchMetric(index, rows, bytes, files, partitions, writeSeconds, readSeconds, lagMillis / 1000.0, clock())
  }

  /** Đọc lại toàn bộ batch (đi qua block token + wire encryption ở chiều đọc) và so số dòng. */
  private def verifyRead(path: String, expectedRows: Long): Double = {
    val start = System.nanoTime()
    val count = spark.read.parquet(path).count()
    if (count != expectedRows) {
      throw new IllegalStateException(s"VERIFY_READ $path: đọc được $count dòng, kỳ vọng $expectedRows")
    }
    (System.nanoTime() - start) / 1e9
  }
}
