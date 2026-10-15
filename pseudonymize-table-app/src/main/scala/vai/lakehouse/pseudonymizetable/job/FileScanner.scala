package vai.lakehouse.pseudonymizetable.job

import java.util.concurrent.{Callable, Executors}

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import vai.lakehouse.pseudonymizetable.config.{FileInput, InputFormat}
import vai.lakehouse.pseudonymizetable.logging.Log

/** Một file không đọc được theo `INPUT_FORMAT`/`INPUT_OPTION_*`, kèm nguyên nhân gốc đã rút gọn. */
final case class BadFile(path: String, reason: String)

/** Kết quả quét: `good` đọc được toàn bộ, `bad` lỗi. */
final case class FileScan(good: Seq[String], bad: Seq[BadFile])

/**
 * Tìm file lỗi trong thư mục `INPUT_PATH`; không file nào làm dừng việc quét các file khác.
 *
 * Hai mức kiểm (tham số `deep`):
 *  - nhanh (mặc định, chỉ csv/tsv/txt): đọc nguyên dòng và đếm ký tự phân tách, so với `maxColumns` (mặc định 20 480 của
 *    parser CSV). Không tách 447 trường mỗi dòng nên rẻ hơn nhiều lần parse đầy đủ; chỉ bắt lỗi "dòng quá nhiều cột".
 *  - đầy đủ (`deep`, và mọi định dạng khác): đọc TỪNG file bằng đúng cấu hình reader của job, ép parse mọi cột; bắt mọi
 *    lỗi đọc (CSV vượt maxColumns, parquet hỏng, lỗi HDFS...).
 */
object FileScanner {

  /** Reader theo cấu hình đầu vào (dùng chung cho đọc thật và quét). */
  def reader(spark: SparkSession, input: FileInput) = {
    val r = spark.read.format(input.format.sparkFormat).options(input.format.defaults ++ input.options)
    input.schema.foreach(r.schema)
    r
  }

  /** File dữ liệu dưới mọi `roots` (đệ quy), bỏ file ẩn/tạm (`_`, `.`) như Spark. Mỗi root là file, thư mục hoặc glob. */
  def listFiles(spark: SparkSession, roots: Seq[String]): Seq[String] =
    roots.flatMap(listFiles(spark, _)).distinct.sorted

  private def listFiles(spark: SparkSession, path: String): Seq[String] = {
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

  def scan(spark: SparkSession, input: FileInput, roots: Seq[String], parallelism: Int, deep: Boolean = false): FileScan = {
    val files = listFiles(spark, roots)
    Log.kv("files to check", files.size)
    val pool = Executors.newFixedThreadPool(math.max(1, math.min(parallelism, math.max(files.size, 1))))
    try {
      val futures = files.map { f =>
        f -> pool.submit(new Callable[Option[String]] { override def call(): Option[String] = check(spark, input, f, deep) })
      }
      val results = futures.map { case (f, fut) => f -> fut.get() }
      FileScan(results.collect { case (f, None) => f }, results.collect { case (f, Some(r)) => BadFile(f, r) })
    } finally pool.shutdown()
  }

  private val DefaultMaxColumns = 20480

  private def isDelimited(input: FileInput): Boolean =
    input.format == InputFormat.Csv || input.format == InputFormat.Tsv

  /** Tuỳ chọn reader không phân biệt hoa thường (như Spark): defaults của định dạng rồi `INPUT_OPTION_*`. */
  private def option(input: FileInput, names: String*): Option[String] = {
    val all = (input.format.defaults ++ input.options).map { case (k, v) => k.toLowerCase -> v }
    names.flatMap(n => all.get(n.toLowerCase)).headOption
  }

  private[job] def countOccurrences(line: String, token: String): Int = {
    var count = 0
    var from = line.indexOf(token)
    while (from >= 0) {
      count += 1
      from = line.indexOf(token, from + token.length)
    }
    count
  }

  /** `None` = đọc được. Mức nhanh chỉ cho csv/tsv/txt (xem mô tả object); còn lại luôn đọc đầy đủ. */
  private def check(spark: SparkSession, input: FileInput, file: String, deep: Boolean): Option[String] =
    try {
      if (!deep && isDelimited(input)) fastCheck(spark, input, file) else fullCheck(spark, input, file)
    } catch {
      case NonFatal(e) => Some(rootCause(e))
    }

  /** Số trường lớn nhất của một dòng = số ký tự phân tách + 1; vượt `maxColumns` thì parser CSV sẽ ném lỗi. */
  private def fastCheck(spark: SparkSession, input: FileInput, file: String): Option[String] = {
    val delimiter = option(input, "delimiter", "sep").filter(_.nonEmpty).getOrElse(",")
    val maxColumns = option(input, "maxColumns").flatMap(v => scala.util.Try(v.trim.toInt).toOption)
      .getOrElse(DefaultMaxColumns)
    val widest = spark.read.text(file).rdd.map(r => countOccurrences(r.getString(0), delimiter) + 1).fold(0)(math.max)
    if (widest > maxColumns) Some(s"dòng có $widest trường, vượt maxColumns=$maxColumns (delimiter '$delimiter')") else None
  }

  /** `.rdd` giữ nguyên mọi cột nên parser không bị cắt cột (count() trên DataFrame có thể bỏ qua parse). */
  private def fullCheck(spark: SparkSession, input: FileInput, file: String): Option[String] = {
    val df: DataFrame = reader(spark, input).load(file)
    df.rdd.map(_.length).count()
    None
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
