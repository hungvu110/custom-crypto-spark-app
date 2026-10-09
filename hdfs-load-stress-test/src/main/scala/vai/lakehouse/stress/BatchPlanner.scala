package vai.lakehouse.stress

/** Tính toán thuần (không I/O) cho từng batch: số dòng, số file, lịch chạy. */
object BatchPlanner {

  /** Mỗi batch dùng một dải id riêng, nên id không trùng giữa các batch và giữa các lần resume. */
  val IdsPerBatch: Long = 10000000000L

  /** Byte ngoài payload của một dòng sau khi ghi Parquet (id, ts, user_id, event_type, amount) — ước lượng ban đầu. */
  val NonPayloadBytesPerRow: Double = 48.0

  def initialBytesPerRow(payloadBytes: Int): Double = payloadBytes + NonPayloadBytesPerRow

  def firstId(batchIndex: Int): Long = batchIndex.toLong * IdsPerBatch

  def rowsFor(targetBytes: Long, bytesPerRow: Double): Long =
    math.max(1L, math.ceil(targetBytes / bytesPerRow).toLong)

  /** Mỗi partition của spark.range ghi đúng 1 file, nên số partition = số file của batch. */
  def partitionsFor(targetBytes: Long, targetFileBytes: Long): Int =
    math.max(1L, math.ceil(targetBytes.toDouble / targetFileBytes).toLong).toInt

  /** Hiệu chỉnh byte/dòng theo số đo thật; lấy trung bình với ước lượng cũ để một batch lệch không gây dao động. */
  def updateEstimate(previous: Double, actualBytes: Long, rows: Long): Double =
    if (rows <= 0 || actualBytes <= 0) previous else (previous + actualBytes.toDouble / rows) / 2

  def nextBatchBytes(bytesPerBatch: Long, totalBytes: Long, bytesDone: Long): Long =
    math.max(0L, math.min(bytesPerBatch, totalBytes - bytesDone))

  /** Mốc (epoch ms) batch thứ `n` của lần chạy này được phép bắt đầu (sustained). */
  def scheduledStart(runStartMillis: Long, n: Long, intervalSeconds: Int): Long =
    runStartMillis + n * intervalSeconds * 1000L
}
