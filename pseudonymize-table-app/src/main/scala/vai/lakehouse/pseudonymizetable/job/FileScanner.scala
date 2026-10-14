package vai.lakehouse.pseudonymizetable.job

import java.util.concurrent.{Callable, Executors}

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import vai.lakehouse.pseudonymizetable.config.FileInput
import vai.lakehouse.pseudonymizetable.logging.Log

/** Một file không đọc được theo `INPUT_FORMAT`/`INPUT_OPTION_*`, kèm nguyên nhân gốc đã rút gọn. */
final case class BadFile(path: String, reason: String)

/** Kết quả quét: `good` đọc được toàn bộ, `bad` lỗi. */
final case class FileScan(good: Seq[String], bad: Seq[BadFile])

/**
 * Tìm file lỗi trong thư mục `INPUT_PATH`: đọc TỪNG file bằng đúng cấu hình reader của job, ép parse mọi cột. File đọc
 * lỗi (CSV vượt maxColumns, parquet hỏng, lỗi HDFS...) được ghi vào `bad`; không file nào làm dừng việc quét các file khác.
 */
object FileScanner {

  /** Reader theo cấu hình đầu vào (dùng chung cho đọc thật và quét). */
  def reader(spark: SparkSession, input: FileInput) = {
    val r = spark.read.format(input.format.sparkFormat).options(input.format.defaults ++ input.options)
    input.schema.foreach(r.schema)
    r
  }

  /** File dữ liệu dưới `path` (đệ quy), bỏ file ẩn/tạm (`_`, `.`) như Spark. `path` có thể là file, thư mục hoặc glob. */
  def listFiles(spark: SparkSession, path: String): Seq[String] = {
    val p = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val roots = Option(fs.globStatus(p)).map(_.toSeq).getOrElse(Nil)
    roots.flatMap { status =>
      if (status.isFile) Seq(status.getPath)
      else {
        val it = fs.listFiles(status.getPath, true)
        Iterator.continually(it).takeWhile(_.hasNext).map(_.next().getPath).toList
      }
    }.filterNot(f => f.getName.startsWith("_") || f.getName.startsWith("."))
      .map(_.toString).sorted
  }

  def scan(spark: SparkSession, input: FileInput, parallelism: Int): FileScan = {
    val files = listFiles(spark, input.path)
    Log.kv("files to check", files.size)
    val pool = Executors.newFixedThreadPool(math.max(1, math.min(parallelism, math.max(files.size, 1))))
    try {
      val futures = files.map { f =>
        f -> pool.submit(new Callable[Option[String]] { override def call(): Option[String] = check(spark, input, f) })
      }
      val results = futures.map { case (f, fut) => f -> fut.get() }
      FileScan(results.collect { case (f, None) => f }, results.collect { case (f, Some(r)) => BadFile(f, r) })
    } finally pool.shutdown()
  }

  /** `None` = đọc được. `.rdd` giữ nguyên mọi cột nên parser không bị cắt cột (count() trên DataFrame có thể bỏ qua parse). */
  private def check(spark: SparkSession, input: FileInput, file: String): Option[String] =
    try {
      val df: DataFrame = reader(spark, input).load(file)
      df.rdd.map(_.length).count()
      None
    } catch {
      case NonFatal(e) => Some(rootCause(e))
    }

  private def rootCause(t: Throwable): String = {
    var cur = t
    val seen = scala.collection.mutable.Set.empty[Throwable]
    while (cur.getCause != null && seen.add(cur)) cur = cur.getCause
    // Thông báo của univocity kèm cả bảng cấu hình parser rất dài: chỉ giữ dòng đầu, tối đa 300 ký tự.
    val first = Option(cur.getMessage).getOrElse("").split("\n").headOption.getOrElse("")
    val msg = s"${cur.getClass.getSimpleName}: $first"
    if (msg.length > 300) msg.take(300) + "..." else msg
  }

  def log(scan: FileScan): Unit = {
    Log.kv("files OK / lỗi", s"${scan.good.size} / ${scan.bad.size}")
    scan.bad.foreach(b => Log.fail(s"FILE LỖI ${b.path} — ${b.reason}"))
  }
}
