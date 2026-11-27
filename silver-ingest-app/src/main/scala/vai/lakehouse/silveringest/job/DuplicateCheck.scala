package vai.lakehouse.silveringest.job

import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, col, collect_list, input_file_name, lit, struct, sum, when, xxhash64}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import vai.lakehouse.silveringest.logging.Log

/**
 * File trùng nội dung của một lần nạp: cùng số dòng và cùng tổng hash các dòng (dấu vân tay không phụ thuộc thứ tự dòng). `duplicateFiles` =
 * số file thừa (bỏ đi thì còn một bản mỗi nhóm). `withRows` = số file cho ra ít nhất một dòng (file rỗng không được so).
 */
final case class FileDuplicates(files: Int, withRows: Int, duplicateGroups: Int, duplicateFiles: Int)

/**
 * Dòng trùng (giống hệt ở mọi cột) của một lần nạp. `extraRows` = số dòng mà một lần loại trùng sẽ bỏ. `withinFile*` = phần của các nhóm mà
 * mọi bản đều nằm trong CÙNG một file (loại trùng được trong từng task, không cần shuffle).
 */
final case class RowDuplicates(rows: Long, duplicateGroups: Long, extraRows: Long, withinFileGroups: Long, withinFileExtraRows: Long,
                               sample: Int = 1) {
  /** `true` khi các số trùng là ước lượng từ mẫu 1/`sample` giá trị dòng (số dòng `rows` luôn chính xác). */
  def estimated: Boolean = sample > 1
}

/**
 * `DEDUP=check`: ĐO dòng trùng và file trùng của lần nạp rồi ghi vào log, KHÔNG xoá gì, để chọn cách loại trùng cho từng bảng
 * (docs/silver-ingest/TRANSFORM-PHIEU-KHAI-THAC-VTT.md mục 12). Đo trên dữ liệu ĐỌC TỪ LANDING, trước khi giải mã và gom nhóm.
 *
 * MỘT lượt đọc nguồn, MỘT shuffle nhỏ, mọi việc trên executor (không đọc file nào trên driver):
 *  - mỗi dòng: hai `xxhash64` khác seed (128 bit, kèm cờ null của từng cột vì `xxhash64` bỏ qua null) và file của dòng;
 *  - `GROUPING SETS ((h1, h2), (file))`: theo hash dòng thì đếm số bản và file nhỏ/lớn nhất (dòng trùng, trùng trong một file); theo file thì
 *    số dòng và tổng hash (dấu vân tay của file: hai file cùng nội dung, kể cả khác thứ tự dòng, cho cùng dấu vân tay). Phần theo file gộp
 *    gần hết ngay ở phía map (một task đọc một vài file) nên hầu như không thêm shuffle.
 * Hash chỉ để ĐẾM: va chạm 128 bit (dòng) hay 64 bit kèm số dòng (file) không đáng kể và không xoá gì. Lỗi khi đo không bao giờ làm hỏng lần
 * nạp: chỉ ghi cảnh báo.
 *
 * LẤY MẪU THEO HASH khi input lớn ([[sampleFor]], `DEDUP_SAMPLE`): gần như mọi dòng là một nhóm riêng, nên đo chính xác phải shuffle và gộp
 * một key cho mỗi dòng (EBS: 29 tỷ dòng, 691 GB shuffle, khoảng 57 triệu key mỗi task reduce: spill nặng, task và executor chết). Chỉ giá trị
 * dòng có `pmod(h1, N) = 0` được gom theo hash; mọi bản trùng của một dòng có cùng hash nên vào mẫu cùng nhau, và số nhóm trùng, số dòng thừa
 * của mẫu nhân N là ước lượng không chệch. Dòng ngoài mẫu gộp vào MỘT nhóm rỗng (gần như không shuffle). Số dòng và file trùng vẫn chính xác.
 */
object DuplicateCheck {

  /** grouping_id() của nhóm theo hash dòng (cột `file` vắng mặt: bit thấp nhất bật) trong `GROUPING SETS ((h1, h2), (file))`. */
  private val ByRow = 1

  /** Đo chính xác tới chừng này byte input; lớn hơn thì lấy mẫu sao cho mỗi phần mẫu khoảng chừng này. */
  val ExactUpToBytes: Long = 64L * 1024 * 1024 * 1024

  /** 1 = chính xác; lớn hơn: luỹ thừa 2 nhỏ nhất để input / N <= [[ExactUpToBytes]] (EBS 1,5 TB -> 32). */
  def sampleFor(inputBytes: Long): Int = {
    var n = 1
    while (inputBytes / n > ExactUpToBytes && n < 1024) n *= 2
    n
  }

  def run(spark: SparkSession, files: Seq[SourceFile], source: DataFrame, sample: Option[Int] = None): Unit = {
    Log.section("DEDUP CHECK (measure only, nothing is removed)")
    val start = System.nanoTime()
    val inputBytes = files.map(_.length).sum
    val n = sample.getOrElse(sampleFor(inputBytes))
    Log.kv("row sample", if (n == 1) "none (exact)" else s"1 in $n row values by hash (input ${inputBytes / (1024L * 1024 * 1024)} GiB): duplicate counts are estimates")
    val result = try Some(measure(spark, files, source, n))
    catch {
      case NonFatal(e) =>
        Log.warn(s"dedup check failed, the load goes on: ${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("")}")
        None
    }
    result.foreach { case (f, r) =>
      Log.kv("files", s"${f.files} (with rows ${f.withRows}, compared by row count and sum of row hashes)")
      Log.kv("duplicate files", s"${f.duplicateFiles} extra in ${f.duplicateGroups} group(s)")
      val about = if (r.estimated) "~" else ""
      Log.kv("rows", r.rows)
      Log.kv("duplicate rows", s"$about${r.extraRows} extra in $about${r.duplicateGroups} group(s) (${percent(r.extraRows, r.rows)})")
      Log.kv("  of which within one file", s"$about${r.withinFileExtraRows} extra in $about${r.withinFileGroups} group(s)")
    }
    // One line per run, easy to collect across days and tables (grep DEDUP_CHECK_RESULT).
    Log.info("DEDUP_CHECK_RESULT " + Seq(
      "files" -> result.map(_._1.files), "duplicate_files" -> result.map(_._1.duplicateFiles),
      "rows" -> result.map(_._2.rows), "extra_rows" -> result.map(_._2.extraRows), "duplicate_groups" -> result.map(_._2.duplicateGroups),
      "within_file_extra_rows" -> result.map(_._2.withinFileExtraRows), "sample" -> result.map(_._2.sample)
    ).map { case (k, v) => s"$k=${v.fold("error")(_.toString)}" }.mkString(" "))
    Log.elapsed("dedup check duration", start)
  }

  /** File trùng và dòng trùng của `source` (đọc từ `files`), trong một lượt đọc; `sample` = N của mẫu dòng (1 = chính xác). */
  def measure(spark: SparkSession, files: Seq[SourceFile], source: DataFrame, sample: Int = 1): (FileDuplicates, RowDuplicates) = {
    require(sample >= 1, "sample must be >= 1")
    source.schema.fields.find(f => hasMap(f.dataType)).foreach { f =>
      throw new IllegalArgumentException(s"column ${f.name} is a map: xxhash64 cannot hash it")
    }
    val columns = source.columns.toSeq.map(c => col(s"`${c.replace("`", "``")}`"))
    val nulls = array(columns.map(_.isNull): _*) // xxhash64 skips nulls: without this (null, x) and (x, null) would hash alike
    val view = s"silver_ingest_dedup_${UUID.randomUUID().toString.replace("-", "")}"
    // input_file_name() is nondeterministic: it is computed in a projection, never inside the aggregation.
    source.select(xxhash64(nulls +: columns: _*).as("h1"), xxhash64(lit("silver-ingest-dedup") +: nulls +: columns: _*).as("h2"),
      input_file_name().as("file")).createOrReplaceTempView(view)
    try {
      // Rows outside the sample get NULL keys: they all fall into ONE group (merged on the map side). h1 itself is never NULL.
      // Sums of the two 32-bit halves of h1 never overflow a long (fewer than 2^31 rows per file).
      val keep = if (sample == 1) "true" else s"pmod(h1, $sample) = 0"
      val perGroup = spark.sql(
        s"""SELECT grouping_id() AS g, s1, file, count(1) AS n, min(fid) AS f_min, max(fid) AS f_max,
           |       sum(h1 & 4294967295) AS lo, sum(shiftrightunsigned(h1, 32)) AS hi
           |FROM (SELECT CASE WHEN $keep THEN h1 END AS s1, CASE WHEN $keep THEN h2 END AS s2, h1, file, xxhash64(file) AS fid FROM `$view`)
           |GROUP BY GROUPING SETS ((s1, s2), (file))""".stripMargin)
      val row = col("g") === ByRow
      val n = col("n")
      val duplicated = row && col("s1").isNotNull && n > 1
      val withinFile = duplicated && col("f_min") === col("f_max")
      def total(c: Column): Column = sum(c).cast("long")
      val r = perGroup.agg(
        total(when(row, n)), total(when(duplicated, 1L)), total(when(duplicated, n - 1)),
        total(when(withinFile, 1L)), total(when(withinFile, n - 1)),
        collect_list(when(!row, struct(col("file"), n, col("lo"), col("hi"))))).head()
      def at(i: Int): Long = if (r.isNullAt(i)) 0L else r.getLong(i)
      val fingerprints = r.getList[Row](5).asScala.map(f => f.getString(0) -> ((f.getLong(1), f.getLong(2), f.getLong(3)))).toSeq
      val groups = groupFiles(fingerprints)
      (FileDuplicates(files.size, fingerprints.size, groups.size, groups.map(_.size - 1).sum),
        RowDuplicates(at(0), at(1) * sample, at(2) * sample, at(3) * sample, at(4) * sample, sample))
    } finally spark.catalog.dropTempView(view)
  }

  /** Nhóm các file cùng dấu vân tay (số dòng, tổng hash), chỉ nhóm có từ 2 file. */
  private[job] def groupFiles(fingerprints: Seq[(String, (Long, Long, Long))]): Seq[Seq[String]] =
    fingerprints.groupBy(_._2).values.map(_.map(_._1).sorted).filter(_.size > 1).toSeq.sortBy(_.head)

  private def hasMap(t: DataType): Boolean = t match {
    case _: MapType => true
    case s: StructType => s.fields.exists(f => hasMap(f.dataType))
    case a: ArrayType => hasMap(a.elementType)
    case _ => false
  }

  private def percent(part: Long, whole: Long): String = if (whole == 0) "0%" else f"${100.0 * part / whole}%.4f%%"
}
