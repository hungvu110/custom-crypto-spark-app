package vai.lakehouse.stress

import java.nio.charset.StandardCharsets

import scala.util.Try

import org.apache.hadoop.fs.{FileSystem, Path}

final case class Progress(nextIndex: Int, bytesDone: Long)

/**
 * Bố cục trên HDFS của một lần chạy:
 * {{{
 * <OUTPUT_PATH>/<RUN_ID>/batch=000000/part-*.parquet
 *                        batch=000001/...
 *                        _metrics/batch=000000.json   <- ghi SAU khi batch commit xong: là "sổ" tiến độ
 *                        _metrics/summary-<epoch ms>.json
 * }}}
 * Tiến độ chỉ dựa vào file metric (mỗi file ghi một lần), không có file trạng thái bị ghi đè, nên pod chết
 * giữa chừng không làm hỏng tiến độ.
 */
final class RunStore(fs: FileSystem, val runPath: Path) {

  private val metricsDir   = new Path(runPath, "_metrics")
  private val BatchDir     = "^batch=(\\d{6})$".r
  private val BatchMetric  = "^batch=(\\d{6})\\.json$".r
  private val BytesField   = "\"bytes\":(\\d+)".r

  def batchPath(index: Int): Path = new Path(runPath, f"batch=$index%06d")

  def metricName(index: Int): String = f"batch=$index%06d.json"

  /** Batch đã commit nhưng chưa kịp ghi metric bị coi là chưa xong và sẽ được ghi lại. */
  def recover(): Progress = {
    val recorded = listNames(metricsDir).flatMap {
      case BatchMetric(index) =>
        readText(new Path(metricsDir, metricName(index.toInt)))
          .flatMap(BytesField.findFirstMatchIn(_))
          .map(m => index.toInt -> m.group(1).toLong)
      case _ => None
    }
    if (recorded.isEmpty) Progress(0, 0L)
    else Progress(recorded.map(_._1).max + 1, recorded.map(_._2).sum)
  }

  /** Xoá thư mục batch từ `fromIndex` trở đi (ghi dở khi pod chết). Chỉ chạm thư mục dạng batch=NNNNNN trong run. */
  def removeBatchesFrom(fromIndex: Int): Seq[Path] =
    listNames(runPath)
      .collect { case BatchDir(index) if index.toInt >= fromIndex => batchPath(index.toInt) }
      .filter(path => fs.delete(path, true))

  /** (byte, số file dữ liệu) của batch, đo trên HDFS sau khi ghi. */
  def batchSize(index: Int): (Long, Long) = {
    val path  = batchPath(index)
    val bytes = fs.getContentSummary(path).getLength
    val files = fs.listStatus(path).count(_.getPath.getName.startsWith("part-")).toLong
    (bytes, files)
  }

  def deleteBatch(index: Int): Boolean = fs.delete(batchPath(index), true)

  def writeMetric(name: String, json: String): Unit = {
    val out = fs.create(new Path(metricsDir, name), true)
    try out.write(json.getBytes(StandardCharsets.UTF_8))
    finally out.close()
  }

  private def listNames(dir: Path): Seq[String] =
    if (!fs.exists(dir)) Seq.empty else fs.listStatus(dir).toSeq.map(_.getPath.getName)

  private def readText(path: Path): Option[String] =
    Try {
      val in = fs.open(path)
      try new String(in.readAllBytes(), StandardCharsets.UTF_8)
      finally in.close()
    }.toOption
}
