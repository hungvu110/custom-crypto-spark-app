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

  private val CorruptKey = "spark.sql.files.ignoreCorruptFiles"

  /** Quét/giải thích phải thấy lỗi đọc: tắt tạm ignoreCorruptFiles (chế độ skip bật nó cho việc đọc thật). */
  private def strict[T](spark: SparkSession)(body: => T): T = {
    val previous = spark.conf.getOption(CorruptKey)
    spark.conf.set(CorruptKey, "false")
    try body
    finally previous match {
      case Some(v) => spark.conf.set(CorruptKey, v)
      case None => spark.conf.unset(CorruptKey)
    }
  }

  def scan(spark: SparkSession, input: FileInput, roots: Seq[String], parallelism: Int, deep: Boolean = false): FileScan =
    strict(spark)(doScan(spark, input, roots, parallelism, deep))

  private def doScan(spark: SparkSession, input: FileInput, roots: Seq[String], parallelism: Int, deep: Boolean): FileScan = {
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

  /**
   * csv/tsv/txt không khai `INPUT_SCHEMA` và không có header: Spark sẽ suy số cột bằng cách parse dòng đầu của FILE ĐẦU TIÊN
   * ngay lúc `load()` (trên driver, trước khi `ignoreCorruptFiles` có tác dụng) nên một file hỏng đứng đầu làm cả job fail.
   * Ta tự dựng schema `_c0.._cN` (STRING) từ dòng đầu của file tốt đầu tiên: bỏ qua file không có dòng nào trong
   * `ProbeBytes` đầu (rỗng, hoặc cả file dính thành một dòng). Cũng tiết kiệm một lượt suy schema.
   */
  def ensureSchema(spark: SparkSession, input: FileInput, roots: Seq[String]): FileInput =
    if (input.schema.isDefined || !isDelimited(input) || option(input, "header").exists(_.trim.equalsIgnoreCase("true"))) input
    else {
      val delimiter = option(input, "delimiter", "sep").filter(_.nonEmpty).getOrElse(",")
      val maxColumns = option(input, "maxColumns").flatMap(v => scala.util.Try(v.trim.toInt).toOption).getOrElse(DefaultMaxColumns)
      val width = listFiles(spark, roots).iterator.flatMap(f => firstLineWidth(spark, f, delimiter))
        .find(_ <= maxColumns)
      width.fold(input)(n => input.copy(schema = Some((0 until n).map(i => s"_c$i STRING").mkString(", "))))
    }

  private val ProbeBytes = 4 * 1024 * 1024

  /** Số trường của dòng đầu (trong `ProbeBytes` đầu); `None` nếu file rỗng hoặc không có ký tự xuống dòng trong đó. */
  private def firstLineWidth(spark: SparkSession, file: String, delimiter: String): Option[Int] = {
    val path = new Path(file)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val in = fs.open(path)
    try {
      val buf = new Array[Byte](ProbeBytes)
      var n = 0
      var read = in.read(buf, 0, buf.length)
      while (read > 0 && n < buf.length) {
        n += read
        read = if (n < buf.length) in.read(buf, n, buf.length - n) else -1
      }
      val end = (0 until n).indexWhere(i => buf(i) == '\n'.toByte || buf(i) == '\r'.toByte)
      if (end <= 0) None
      else Some(countOccurrences(new String(buf, 0, end, java.nio.charset.StandardCharsets.UTF_8), delimiter) + 1)
    } finally in.close()
  }

  /** Lý do một file không đọc được (đọc đầy đủ bằng đúng cấu hình reader); `None` nếu đọc được bình thường. */
  def explain(spark: SparkSession, input: FileInput, file: String): Option[String] =
    strict(spark) {
      // Thông báo gốc của parser thường rỗng ("ArrayIndexOutOfBounds"): thử kiểm tra nhanh trước để có lý do dễ hiểu.
      val readable = if (isDelimited(input)) check(spark, input, file, deep = false) else None
      readable.orElse(check(spark, input, file, deep = true))
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
    if (widest > maxColumns) Some(s"a line has $widest fields, exceeding maxColumns=$maxColumns (delimiter '$delimiter')") else None
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
    Log.kv("files OK / bad", s"${scan.good.size} / ${scan.bad.size}")
    scan.bad.foreach(b => Log.fail(s"BAD FILE ${b.path} — ${b.reason}"))
  }
}
