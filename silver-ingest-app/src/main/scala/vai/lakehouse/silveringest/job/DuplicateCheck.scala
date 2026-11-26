package vai.lakehouse.silveringest.job

import java.security.MessageDigest

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, count, input_file_name, lit, max, min, sum, when, xxhash64}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import vai.lakehouse.silveringest.logging.Log

/** File trùng nội dung của một lần nạp: cùng kích thước và cùng checksum. `duplicateFiles` = số file thừa (bỏ đi thì còn một bản mỗi nhóm). */
final case class FileDuplicates(files: Int, withChecksum: Int, method: String, duplicateGroups: Int, duplicateFiles: Int)

/**
 * Dòng trùng (giống hệt ở mọi cột) của một lần nạp. `extraRows` = số dòng mà một lần loại trùng sẽ bỏ. `withinFile*` = phần của các nhóm mà
 * mọi bản đều nằm trong CÙNG một file (loại trùng được trong từng task, không cần shuffle).
 */
final case class RowDuplicates(rows: Long, duplicateGroups: Long, extraRows: Long, withinFileGroups: Long, withinFileExtraRows: Long)

/**
 * `DEDUP=check`: ĐO dòng trùng và file trùng của lần nạp rồi ghi vào log, KHÔNG xoá gì, để chọn cách loại trùng cho từng bảng
 * (docs/silver-ingest/TRANSFORM-PHIEU-KHAI-THAC-VTT.md mục 12). Đo trên dữ liệu ĐỌC TỪ LANDING, trước khi giải mã và gom nhóm.
 *
 *  - File: theo kích thước và checksum HDFS (`getFileChecksum`, chế độ COMPOSITE_CRC: không phụ thuộc block size), không đọc dữ liệu qua
 *    Spark. File system không có checksum (vd ổ cục bộ) thì băm nội dung file (SHA-256).
 *  - Dòng: băm mọi cột (hai `xxhash64` khác seed, 128 bit, kèm cờ null của từng cột vì `xxhash64` bỏ qua null) rồi đếm theo hash. Chỉ
 *    shuffle hash, không shuffle cả dòng. Hash chỉ để ĐẾM: va chạm 128 bit không đáng kể và không xoá gì.
 *
 * Lỗi khi đo không bao giờ làm hỏng lần nạp: chỉ ghi cảnh báo.
 */
object DuplicateCheck {

  def run(spark: SparkSession, files: Seq[SourceFile], source: DataFrame): Unit = {
    Log.section("DEDUP CHECK (measure only, nothing is removed)")
    val start = System.nanoTime()
    val fileReport = attempt("files")(this.files(spark, files))
    val rowReport = attempt("rows")(rows(source))
    fileReport.foreach { f =>
      Log.kv("files", s"${f.files} (with checksum ${f.withChecksum}, ${f.method})")
      Log.kv("duplicate files", s"${f.duplicateFiles} extra in ${f.duplicateGroups} group(s)")
    }
    rowReport.foreach { r =>
      Log.kv("rows", r.rows)
      Log.kv("duplicate rows", s"${r.extraRows} extra in ${r.duplicateGroups} group(s) (${percent(r.extraRows, r.rows)})")
      Log.kv("  of which within one file", s"${r.withinFileExtraRows} extra in ${r.withinFileGroups} group(s)")
    }
    // One line per run, easy to collect across days and tables (grep DEDUP_CHECK_RESULT).
    Log.info("DEDUP_CHECK_RESULT " + Seq(
      "files" -> fileReport.map(_.files), "duplicate_files" -> fileReport.map(_.duplicateFiles),
      "rows" -> rowReport.map(_.rows), "extra_rows" -> rowReport.map(_.extraRows), "duplicate_groups" -> rowReport.map(_.duplicateGroups),
      "within_file_extra_rows" -> rowReport.map(_.withinFileExtraRows)
    ).map { case (k, v) => s"$k=${v.fold("error")(_.toString)}" }.mkString(" "))
    Log.elapsed("dedup check duration", start)
  }

  def files(spark: SparkSession, files: Seq[SourceFile]): FileDuplicates = {
    val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
    conf.set("dfs.checksum.combine.mode", "COMPOSITE_CRC")
    val methods = scala.collection.mutable.LinkedHashSet.empty[String]
    val sums = files.groupBy(f => Option(new Path(f.path).toUri.getAuthority).getOrElse("")).values.toSeq.flatMap { group =>
      val fs = FileSystem.newInstance(new Path(group.head.path).toUri, conf) // own instance: the combine mode is read when it is created
      try group.map { f =>
        val (method, sum) = checksum(fs, new Path(f.path))
        methods += method
        (f.path, f.length, sum)
      } finally fs.close()
    }
    val groups = groupFiles(sums)
    FileDuplicates(files.size, sums.count(_._3.isDefined), if (methods.isEmpty) "none" else methods.mkString(", "),
      groups.size, groups.map(_.size - 1).sum)
  }

  /** Nhóm các file cùng kích thước và cùng checksum (chỉ nhóm có từ 2 file); file không có checksum không bao giờ bị gộp. */
  private[job] def groupFiles(entries: Seq[(String, Long, Option[String])]): Seq[Seq[String]] =
    entries.collect { case (path, length, Some(sum)) => ((length, sum), path) }
      .groupBy(_._1).values.map(_.map(_._2).sorted).filter(_.size > 1).toSeq.sortBy(_.head)

  def rows(source: DataFrame): RowDuplicates = {
    source.schema.fields.find(f => hasMap(f.dataType)).foreach { f =>
      throw new IllegalArgumentException(s"column ${f.name} is a map: xxhash64 cannot hash it")
    }
    val columns = source.columns.toSeq.map(c => col(s"`${c.replace("`", "``")}`"))
    val nulls = array(columns.map(_.isNull): _*) // xxhash64 skips nulls: without this (null, x) and (x, null) would hash alike
    val first = xxhash64(nulls +: columns: _*)
    val second = xxhash64(lit("silver-ingest-dedup") +: nulls +: columns: _*)
    val perHash = source.select(first.as("h1"), second.as("h2"), xxhash64(input_file_name()).as("f"))
      .groupBy("h1", "h2").agg(count(lit(1)).as("n"), min("f").as("f_min"), max("f").as("f_max"))
    val n = col("n")
    val duplicated = n > 1
    val withinFile = duplicated && col("f_min") === col("f_max")
    def total(c: Column): Column = sum(c).cast("long")
    val r = perHash.agg(
      total(n), total(when(duplicated, 1L).otherwise(0L)), total(when(duplicated, n - 1).otherwise(0L)),
      total(when(withinFile, 1L).otherwise(0L)), total(when(withinFile, n - 1).otherwise(0L))).head()
    def at(i: Int): Long = if (r.isNullAt(i)) 0L else r.getLong(i)
    RowDuplicates(at(0), at(1), at(2), at(3), at(4))
  }

  private def checksum(fs: FileSystem, path: Path): (String, Option[String]) =
    Try(Option(fs.getFileChecksum(path))).toOption.flatten match {
      case Some(c) => (s"hdfs ${c.getAlgorithmName}", Some(s"${c.getAlgorithmName}:${hex(c.getBytes)}"))
      case None => ("content SHA-256 (no file system checksum)", Some(s"sha256:${contentDigest(fs, path)}"))
    }

  private def contentDigest(fs: FileSystem, path: Path): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val in = fs.open(path)
    try {
      val buffer = new Array[Byte](1 << 16)
      Iterator.continually(in.read(buffer)).takeWhile(_ >= 0).foreach(n => digest.update(buffer, 0, n))
    } finally in.close()
    hex(digest.digest())
  }

  private def hex(bytes: Array[Byte]): String = bytes.map(b => f"${b & 0xff}%02x").mkString

  private def hasMap(t: DataType): Boolean = t match {
    case _: MapType => true
    case s: StructType => s.fields.exists(f => hasMap(f.dataType))
    case a: ArrayType => hasMap(a.elementType)
    case _ => false
  }

  private def percent(part: Long, whole: Long): String = if (whole == 0) "0%" else f"${100.0 * part / whole}%.4f%%"

  private def attempt[T](what: String)(body: => T): Option[T] =
    try Some(body)
    catch {
      case NonFatal(e) =>
        Log.warn(s"dedup check of $what failed, the load goes on: ${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("")}")
        None
    }
}
