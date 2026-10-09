package vai.lakehouse.stress

import java.time.Instant
import java.util.Locale

final case class BatchMetric(
    batch: Int,
    rows: Long,
    bytes: Long,
    files: Long,
    partitions: Int,
    writeSeconds: Double,
    readSeconds: Option[Double],
    lagSeconds: Double,
    finishedAtMillis: Long) {

  def writeMiBps: Double = if (writeSeconds > 0) bytes / Units.MiB / writeSeconds else 0.0

  /** Dòng tóm tắt cho log (grep STRESS_BATCH); bản JSON đầy đủ nằm ở _metrics/batch=N.json trên HDFS. */
  def toLogLine: String = {
    val read = readSeconds.map(s => s"  read=${Units.secs(s)}").getOrElse("")
    String.format(Locale.ROOT, "STRESS_BATCH #%06d  %,11d rows  %12s  %3d files  write=%s (%s)%s  lag=%s",
      Int.box(batch), Long.box(rows), Units.mib(bytes), Long.box(files), Units.secs(writeSeconds),
      Units.mibps(writeMiBps), read, Units.secs(lagSeconds))
  }

  /** Một dòng JSON: file metric trên HDFS (sổ tiến độ để resume, nguồn số liệu cho báo cáo). */
  def toJson: String = {
    val read = readSeconds.map(Units.fmt).getOrElse("null")
    s"""{"batch":$batch,"rows":$rows,"bytes":$bytes,"files":$files,"partitions":$partitions,""" +
      s""""writeSeconds":${Units.fmt(writeSeconds)},"writeMiBps":${Units.fmt(writeMiBps)},""" +
      s""""readSeconds":$read,"lagSeconds":${Units.fmt(lagSeconds)},""" +
      s""""finishedAt":"${Instant.ofEpochMilli(finishedAtMillis)}"}"""
  }
}

final case class RunSummary(
    runId: String,
    mode: String,
    batches: Int,
    bytesThisRun: Long,
    bytesTotal: Long,
    targetBytes: Long,
    elapsedSeconds: Double,
    writeSecondsTotal: Double,
    writeSecondsP50: Double,
    writeSecondsP95: Double,
    writeSecondsMax: Double,
    maxLagSeconds: Double,
    intervalSeconds: Option[Int]) {

  /** Cho phép thiếu 2%: dung lượng mỗi batch dựa trên ước lượng byte/dòng nên tổng không trúng từng byte. */
  def completed: Boolean = bytesTotal >= (targetBytes * RunSummary.CompletionRatio).toLong

  /** Sustained: chưa batch nào bắt đầu trễ quá một chu kỳ, tức hệ thống theo kịp tải danh định. */
  def keptUp: Boolean = intervalSeconds.forall(maxLagSeconds < _)

  def status: String = if (completed && keptUp) "PASS" else "WARN"

  def writeMiBps: Double = if (writeSecondsTotal > 0) bytesThisRun / Units.MiB / writeSecondsTotal else 0.0

  def effectiveMiBps: Double = if (elapsedSeconds > 0) bytesThisRun / Units.MiB / elapsedSeconds else 0.0

  def toJson: String =
    s"""{"runId":"$runId","mode":"$mode","status":"$status","completed":$completed,"keptUp":$keptUp,""" +
      s""""batches":$batches,"bytesThisRun":$bytesThisRun,"bytesTotal":$bytesTotal,"targetBytes":$targetBytes,""" +
      s""""gibTotal":${Units.fmt(bytesTotal / Units.GiB)},"elapsedSeconds":${Units.fmt(elapsedSeconds)},""" +
      s""""writeMiBps":${Units.fmt(writeMiBps)},"effectiveMiBps":${Units.fmt(effectiveMiBps)},""" +
      s""""writeSecondsP50":${Units.fmt(writeSecondsP50)},"writeSecondsP95":${Units.fmt(writeSecondsP95)},""" +
      s""""writeSecondsMax":${Units.fmt(writeSecondsMax)},"maxLagSeconds":${Units.fmt(maxLagSeconds)}}"""
}

object RunSummary {
  val CompletionRatio = 0.98

  def from(cfg: StressConfig, metrics: Seq[BatchMetric], bytesTotal: Long, elapsedSeconds: Double): RunSummary = {
    val writes = metrics.map(_.writeSeconds).sorted
    RunSummary(
      runId             = cfg.runId,
      mode              = cfg.mode.name,
      batches           = metrics.size,
      bytesThisRun      = metrics.map(_.bytes).sum,
      bytesTotal        = bytesTotal,
      targetBytes       = cfg.totalBytes,
      elapsedSeconds    = elapsedSeconds,
      writeSecondsTotal = writes.sum,
      writeSecondsP50   = percentile(writes, 0.50),
      writeSecondsP95   = percentile(writes, 0.95),
      writeSecondsMax   = writes.lastOption.getOrElse(0.0),
      maxLagSeconds     = if (metrics.isEmpty) 0.0 else metrics.map(_.lagSeconds).max,
      intervalSeconds   = if (cfg.mode == Mode.Sustained) Some(cfg.batchIntervalSeconds) else None)
  }

  /** Nearest-rank percentile trên dãy đã sắp xếp. */
  def percentile(sorted: Seq[Double], p: Double): Double =
    if (sorted.isEmpty) 0.0
    else sorted(math.min(sorted.size - 1, math.max(0, math.ceil(p * sorted.size).toInt - 1)))
}
