package vai.lakehouse.silveringest.job

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession

/** Một file nguồn cần nạp. */
final case class SourceFile(path: String, length: Long)

/**
 * Liệt kê file cần nạp dưới `INPUT_PATH`: chính nó nếu là file, mọi file trong cây nếu là thư mục. Bỏ file/thư mục ẩn (`_*`, `.*`,
 * vd `_SUCCESS`, `.crc`) như Spark. Không có file nào là lỗi: DAK trigger job khi đã có lô file, lô rỗng là sai đường dẫn.
 */
object SourceFiles {

  def list(spark: SparkSession, path: String): Seq[SourceFile] = {
    val root = new Path(path)
    val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(root)) throw new IllegalArgumentException(s"INPUT_PATH does not exist: $path")

    def walk(status: FileStatus): Seq[FileStatus] =
      if (status.isDirectory) fs.listStatus(status.getPath).toSeq.filterNot(s => hidden(s.getPath.getName)).flatMap(walk)
      else Seq(status)

    val files = walk(fs.getFileStatus(root)).map(s => SourceFile(s.getPath.toString, s.getLen)).sortBy(_.path)
    if (files.isEmpty) throw new IllegalArgumentException(s"No file under INPUT_PATH: $path")
    files
  }

  private def hidden(name: String): Boolean = name.startsWith("_") || name.startsWith(".")
}
