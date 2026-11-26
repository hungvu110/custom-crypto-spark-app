package vai.lakehouse.silveringest.job

import org.apache.hadoop.fs.{FileStatus, GlobPattern, Path}
import org.apache.spark.sql.SparkSession

/** Một file nguồn cần nạp. */
final case class SourceFile(path: String, length: Long)

/**
 * Liệt kê file cần nạp dưới `INPUT_PATH`: chính nó nếu là file, mọi file trong cây nếu là thư mục. `INPUT_PATH` có thể là mẫu Hadoop
 * (`*`, `?`, `{a,b}`, vd `dt=20261013/hour={10,11,12}`): mỗi đường dẫn khớp mẫu được xử lý như một `INPUT_PATH`. Bỏ file/thư mục ẩn
 * (`_*`, `.*`, vd `_SUCCESS`, `.crc`) như Spark. `fileGlob` (`INPUT_OPTION_pathGlobFilter`, vd `*.csv`): chỉ giữ file có TÊN khớp mẫu, như Spark
 * khi đọc, để danh sách file (nhóm giờ, đếm file, đo trùng, ngày thiếu) khớp đúng các file Spark đọc (VTNET: bỏ `manifest.json` cạnh file CSV).
 * Không có file nào là lỗi: DAK trigger job khi đã có lô file, lô rỗng là sai đường dẫn.
 */
object SourceFiles {

  def list(spark: SparkSession, path: String, fileGlob: Option[String] = None): Seq[SourceFile] = {
    val pattern = new Path(path)
    val fs = pattern.getFileSystem(spark.sparkContext.hadoopConfiguration)
    // globStatus: null when a path without a pattern does not exist, an empty array when a pattern matches nothing.
    val roots = Option(fs.globStatus(pattern)).getOrElse(Array.empty[FileStatus]).toSeq
    if (roots.isEmpty) throw new IllegalArgumentException(s"INPUT_PATH does not exist: $path")

    def walk(status: FileStatus): Seq[FileStatus] =
      if (status.isDirectory) fs.listStatus(status.getPath).toSeq.filterNot(s => hidden(s.getPath.getName)).flatMap(walk)
      else Seq(status)

    val keep = fileGlob.map(g => new GlobPattern(g))
    val files = roots.flatMap(walk).filter(s => keep.forall(_.matches(s.getPath.getName)))
      .map(s => SourceFile(s.getPath.toString, s.getLen)).distinct.sortBy(_.path)
    if (files.isEmpty) throw new IllegalArgumentException(s"No file under INPUT_PATH: $path${fileGlob.fold("")(g => s" (matching $g)")}")
    files
  }

  /** `INPUT_PATH` có ít nhất một file để nạp không (đường dẫn không tồn tại hoặc thư mục rỗng là không). Dùng cho ngày thiếu của bulk load. */
  def hasFiles(spark: SparkSession, path: String, fileGlob: Option[String] = None): Boolean =
    try { list(spark, path, fileGlob); true }
    catch { case _: IllegalArgumentException => false }

  private def hidden(name: String): Boolean = name.startsWith("_") || name.startsWith(".")
}
