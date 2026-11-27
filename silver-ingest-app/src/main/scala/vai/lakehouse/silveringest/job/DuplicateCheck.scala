package vai.lakehouse.silveringest.job

import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, col, collect_list, count, desc, input_file_name, lit, struct, sum, when, xxhash64}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.storage.StorageLevel
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

/** Một dòng của bảng phân tích nhóm trùng: số nhóm trùng và số dòng thừa (đã nhân N của mẫu). */
final case class DuplicateShare(label: String, groups: Long, extraRows: Long)

/** Một giá trị của các cột phân tích: dòng thừa trong nhóm trùng và tổng số dòng mang giá trị đó (cả hai đã nhân N của mẫu). */
final case class DuplicateValue(values: Seq[String], groups: Long, extraRows: Long, rows: Long)

/**
 * `DEDUP_PROFILE_COLUMNS`: dòng trùng là lỗi nạp (nguồn gửi chồng) hay sự kiện thật lặp lại.
 *  - `copies`: số bản mỗi nhóm (2, 3, 4+);
 *  - `location`: các bản nằm trong cùng file, cùng thư mục (khác file) hay khác thư mục (EBS: thư mục = lô `batch=HHMM`);
 *  - `folderPairs`: các cặp thư mục hay chứa bản trùng nhất (EBS: hai lô liền nhau = gửi chồng ở ranh giới lô);
 *  - `values`: giá trị của các cột phân tích hay trùng nhất, so với tổng số dòng có giá trị đó (tỷ lệ trùng của từng loại sự kiện).
 */
final case class DuplicateProfile(columns: Seq[String], copies: Seq[DuplicateShare], location: Seq[DuplicateShare],
                                  folderPairs: Seq[DuplicateShare], values: Seq[DuplicateValue])

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
 *
 * PHÂN TÍCH NHÓM TRÙNG ([[DuplicateProfile]], `DEDUP_PROFILE_COLUMNS`) trong CÙNG lượt đọc: thêm thư mục của file và giá trị các cột phân tích
 * vào mỗi nhóm, và một grouping set thứ ba theo các cột đó (chỉ trên mẫu). Thư mục và giá trị được gom dưới dạng hash `long`, tên thật tra lại
 * trên driver: `min`/`max` trên string buộc Spark bỏ HashAggregate sang SortAggregate (sắp xếp mọi dòng). Các nhóm cần cho kết quả (file, nhóm
 * trùng, giá trị) được giữ lại (persist, nhỏ) cho các phép đếm sau, không đọc lại nguồn.
 */
object DuplicateCheck {

  /** Đo chính xác tới chừng này byte input; lớn hơn thì lấy mẫu sao cho mỗi phần mẫu khoảng chừng này. */
  val ExactUpToBytes: Long = 64L * 1024 * 1024 * 1024

  /** Số dòng của mỗi bảng "top" trong phân tích. */
  val TopN = 10

  /** Số cột phân tích tối đa. */
  val MaxProfileColumns = 4

  private val MaxValueLength = 64

  /** Thư mục chứa file (EBS: `batch=1020`): đoạn đường dẫn ngay trước tên file. */
  private val FolderOf = "([^/]+)/[^/]*$"

  private val SameFile = "one file"
  private val SameFolder = "one folder, other files"
  private val OtherFolders = "other folders"

  /** 1 = chính xác; lớn hơn: luỹ thừa 2 nhỏ nhất để input / N <= [[ExactUpToBytes]] (EBS 1,5 TB -> 32). */
  def sampleFor(inputBytes: Long): Int = {
    var n = 1
    while (inputBytes / n > ExactUpToBytes && n < 1024) n *= 2
    n
  }

  final case class Measurement(files: FileDuplicates, rows: RowDuplicates, profile: Option[DuplicateProfile])

  def run(spark: SparkSession, files: Seq[SourceFile], source: DataFrame, sample: Option[Int] = None,
          profileColumns: Seq[String] = Nil): Unit = {
    Log.section("DEDUP CHECK (measure only, nothing is removed)")
    val start = System.nanoTime()
    val inputBytes = files.map(_.length).sum
    val n = sample.getOrElse(sampleFor(inputBytes))
    Log.kv("row sample", if (n == 1) "none (exact)" else s"1 in $n row values by hash (input ${inputBytes / (1024L * 1024 * 1024)} GiB): duplicate counts are estimates")
    if (profileColumns.nonEmpty) Log.kv("profile of duplicates by", profileColumns.mkString(", "))
    val result = try Some(measureAll(spark, files, source, n, profileColumns))
    catch {
      case NonFatal(e) =>
        Log.warn(s"dedup check failed, the load goes on: ${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("")}")
        None
    }
    result.foreach { m =>
      val (f, r) = (m.files, m.rows)
      Log.kv("files", s"${f.files} (with rows ${f.withRows}, compared by row count and sum of row hashes)")
      Log.kv("duplicate files", s"${f.duplicateFiles} extra in ${f.duplicateGroups} group(s)")
      val about = if (r.estimated) "~" else ""
      Log.kv("rows", r.rows)
      Log.kv("duplicate rows", s"$about${r.extraRows} extra in $about${r.duplicateGroups} group(s) (${percent(r.extraRows, r.rows)})")
      Log.kv("  of which within one file", s"$about${r.withinFileExtraRows} extra in $about${r.withinFileGroups} group(s)")
      m.profile.foreach(p => logProfile(p, r, about))
    }
    // One line per run, easy to collect across days and tables (grep DEDUP_CHECK_RESULT).
    Log.info("DEDUP_CHECK_RESULT " + Seq(
      "files" -> result.map(_.files.files), "duplicate_files" -> result.map(_.files.duplicateFiles),
      "rows" -> result.map(_.rows.rows), "extra_rows" -> result.map(_.rows.extraRows), "duplicate_groups" -> result.map(_.rows.duplicateGroups),
      "within_file_extra_rows" -> result.map(_.rows.withinFileExtraRows), "sample" -> result.map(_.rows.sample)
    ).map { case (k, v) => s"$k=${v.fold("error")(_.toString)}" }.mkString(" "))
    Log.elapsed("dedup check duration", start)
  }

  private def logProfile(p: DuplicateProfile, r: RowDuplicates, about: String): Unit = {
    def share(s: DuplicateShare): String = s"$about${s.extraRows} extra in $about${s.groups} group(s) (${percent(s.extraRows, r.extraRows)} of extra)"
    Log.kv("copies per group", p.copies.map(s => s"${s.label}: ${share(s)}").mkString("; "))
    p.location.foreach(s => Log.kv(s"copies in ${s.label}", share(s)))
    if (p.folderPairs.nonEmpty) Log.info(s"top ${p.folderPairs.size} folder pair(s) holding copies of the same row:")
    p.folderPairs.foreach(s => Log.info(s"  ${s.label}: ${share(s)}"))
    if (p.values.nonEmpty) Log.info(s"top ${p.values.size} value(s) of ${p.columns.mkString(", ")} among duplicates (rate = extra / rows with the value):")
    p.values.foreach { v =>
      val shown = p.columns.zip(v.values).map { case (c, x) => s"$c=$x" }.mkString(", ")
      Log.info(s"  $shown: $about${v.extraRows} extra of $about${v.rows} rows (rate ${percent(v.extraRows, v.rows)}, ${percent(v.extraRows, r.extraRows)} of extra)")
    }
    def extraOf(s: Seq[DuplicateShare], label: String): Long = s.find(_.label == label).fold(0L)(_.extraRows)
    Log.info("DEDUP_PROFILE_RESULT " + Seq(
      "copies_2" -> extraOf(p.copies, "2"), "copies_3" -> extraOf(p.copies, "3"), "copies_4plus" -> extraOf(p.copies, "4+"),
      "same_file" -> extraOf(p.location, SameFile), "same_folder" -> extraOf(p.location, SameFolder),
      "other_folders" -> extraOf(p.location, OtherFolders), "sample" -> r.sample
    ).map { case (k, v) => s"$k=$v" }.mkString(" "))
  }

  /** File trùng và dòng trùng của `source` (đọc từ `files`), trong một lượt đọc; `sample` = N của mẫu dòng (1 = chính xác). */
  def measure(spark: SparkSession, files: Seq[SourceFile], source: DataFrame, sample: Int = 1): (FileDuplicates, RowDuplicates) = {
    val m = measureAll(spark, files, source, sample, Nil)
    (m.files, m.rows)
  }

  /** Như [[measure]], thêm phân tích nhóm trùng theo `profileColumns` (rỗng = không phân tích), vẫn một lượt đọc. */
  def measureAll(spark: SparkSession, files: Seq[SourceFile], source: DataFrame, sample: Int, profileColumns: Seq[String]): Measurement = {
    require(sample >= 1, "sample must be >= 1")
    source.schema.fields.find(f => hasMap(f.dataType)).foreach { f =>
      throw new IllegalArgumentException(s"column ${f.name} is a map: xxhash64 cannot hash it")
    }
    val profiled = profileColumns.map { name =>
      source.columns.find(_.equalsIgnoreCase(name)).getOrElse {
        throw new IllegalArgumentException(s"DEDUP_PROFILE_COLUMNS: '$name' is not a column of the data (columns: ${source.columns.mkString(", ")})")
      }
    }
    val withProfile = profiled.nonEmpty
    val columns = source.columns.toSeq.map(c => col(quote(c)))
    val nulls = array(columns.map(_.isNull): _*) // xxhash64 skips nulls: without this (null, x) and (x, null) would hash alike
    val p = profiled.indices.map(i => s"p$i")
    val view = s"silver_ingest_dedup_${UUID.randomUUID().toString.replace("-", "")}"
    // input_file_name() is nondeterministic: it is computed in a projection, never inside the aggregation.
    source.select(Seq(xxhash64(nulls +: columns: _*).as("h1"), xxhash64(lit("silver-ingest-dedup") +: nulls +: columns: _*).as("h2"),
      input_file_name().as("file")) ++ profiled.zip(p).map { case (c, a) => col(quote(c)).cast("string").as(a) }: _*)
      .createOrReplaceTempView(view)
    try {
      // Rows outside the sample get NULL keys: they all fall into ONE group (merged on the map side). h1 itself is never NULL.
      // Sums of the two 32-bit halves of h1 never overflow a long (fewer than 2^31 rows per file).
      // Every aggregation buffer is a fixed-width number: HashAggregate (a min/max on strings would sort every row instead).
      val keep = if (sample == 1) "true" else s"pmod(h1, $sample) = 0"
      val pList = p.mkString(", ")
      // The profile columns, the folder and the value hash are computed for the sampled rows only (1 in N): the other rows put NULL in every
      // key, so they fall into one group of each set. A file with a duplicate group has a sampled row, so its folder is known.
      def sampled(e: String, as: String): String = s"CASE WHEN $keep THEN $e END AS $as"
      val inner = Seq(sampled("h1", "s1"), sampled("h2", "s2"), "h1", "file", "xxhash64(file) AS fid") ++
        (if (withProfile) Seq(s"($keep) AS ps") ++ p.map(c => sampled(c, c)) ++
          Seq(sampled(s"xxhash64(regexp_extract(file, '$FolderOf', 1))", "dh"),
            sampled(s"xxhash64(array(${p.map(c => s"$c IS NULL").mkString(", ")}), $pList)", "vh")) else Nil)
      val outer = Seq("grouping(s1) AS gr", "grouping(file) AS gf", "s1", "file", "count(1) AS n", "min(fid) AS f_min", "max(fid) AS f_max",
        "sum(h1 & 4294967295) AS lo", "sum(shiftrightunsigned(h1, 32)) AS hi") ++
        (if (withProfile) Seq("grouping(ps) AS gp", "ps", pList, "min(dh) AS d_min", "max(dh) AS d_max", "min(vh) AS vh") else Nil)
      val sets = Seq("(s1, s2)", "(file)") ++ (if (withProfile) Seq(s"(ps, $pList)") else Nil)
      val perGroup = spark.sql(
        s"""SELECT ${outer.mkString(", ")}
           |FROM (SELECT ${inner.mkString(", ")} FROM `$view`)
           |GROUP BY GROUPING SETS (${sets.mkString(", ")})""".stripMargin)
      val duplicated = col("gr") === 0 && col("s1").isNotNull && col("n") > 1
      val byValue = if (withProfile) col("gp") === 0 && col("ps") else lit(false)
      // Only the groups that a result needs: the files (a few thousand), the sampled duplicate groups, the values of the profile columns.
      val kept = perGroup.where(col("gf") === 0 || duplicated || byValue)
      if (withProfile) kept.persist(StorageLevel.MEMORY_AND_DISK)
      try {
        val (fileReport, rowReport, folders) = totals(kept, files, sample, withProfile)
        val profile = if (withProfile) Some(profileOf(kept, profiled, p, sample, folders)) else None
        Measurement(fileReport, rowReport, profile)
      } finally if (withProfile) kept.unpersist()
    } finally spark.catalog.dropTempView(view)
  }

  /**
   * Số dòng (từ các nhóm theo file: mọi dòng thuộc đúng một file), dòng trùng, file trùng; và thư mục của từng file (hash -> tên). Trong
   * `kept`, mọi nhóm theo hash dòng (`gr = 0`) đều là nhóm trùng đã lấy mẫu.
   */
  private def totals(kept: DataFrame, files: Seq[SourceFile], sample: Int,
                     withProfile: Boolean): (FileDuplicates, RowDuplicates, Map[Long, String]) = {
    val byFile = col("gf") === 0
    val duplicated = col("gr") === 0
    val n = col("n")
    val withinFile = duplicated && col("f_min") === col("f_max")
    def total(c: Column): Column = sum(c).cast("long")
    val fileFields = Seq(col("file"), n, col("lo"), col("hi")) ++ (if (withProfile) Seq(col("d_min")) else Nil)
    val r = kept.agg(
      total(when(byFile, n)), total(when(duplicated, 1L)), total(when(duplicated, n - 1)),
      total(when(withinFile, 1L)), total(when(withinFile, n - 1)),
      collect_list(when(byFile, struct(fileFields: _*)))).head()
    def at(i: Int): Long = if (r.isNullAt(i)) 0L else r.getLong(i)
    val fileRows = r.getList[Row](5).asScala.toSeq
    val fingerprints = fileRows.map(f => f.getString(0) -> ((f.getLong(1), f.getLong(2), f.getLong(3))))
    val folders = if (withProfile) fileRows.map(f => f.getLong(4) -> folderName(f.getString(0))).toMap else Map.empty[Long, String]
    val groups = groupFiles(fingerprints)
    (FileDuplicates(files.size, fingerprints.size, groups.size, groups.map(_.size - 1).sum),
      RowDuplicates(at(0), at(1) * sample, at(2) * sample, at(3) * sample, at(4) * sample, sample), folders)
  }

  private def profileOf(kept: DataFrame, columns: Seq[String], p: Seq[String], sample: Int, folders: Map[Long, String]): DuplicateProfile = {
    val dups = kept.where(col("gr") === 0)
    val n = col("n")
    def shares(label: Column): Seq[DuplicateShare] =
      dups.groupBy(label.as("label")).agg(count(lit(1)).as("g"), sum(n - 1).cast("long").as("e")).collect().toSeq
        .map(r => DuplicateShare(r.getString(0), r.getLong(1) * sample, r.getLong(2) * sample))
    val copies = shares(when(n === 2, "2").when(n === 3, "3").otherwise("4+")).sortBy(_.label)
    val location = shares(when(col("f_min") === col("f_max"), SameFile).when(col("d_min") === col("d_max"), SameFolder).otherwise(OtherFolders))
      .sortBy(-_.extraRows)
    val pairs = dups.where(col("d_min") =!= col("d_max")).groupBy("d_min", "d_max")
      .agg(count(lit(1)).as("g"), sum(n - 1).cast("long").as("e")).orderBy(desc("e")).limit(TopN).collect().toSeq
      .map { r =>
        val names = Seq(r.getLong(0), r.getLong(1)).map(h => folders.getOrElse(h, s"folder#$h")).sorted
        DuplicateShare(names.mkString(" + "), r.getLong(2) * sample, r.getLong(3) * sample)
      }
    val dupByValue = dups.groupBy("vh").agg(count(lit(1)).as("g"), sum(n - 1).cast("long").as("e"))
    val rowsByValue = kept.where(col("gp") === 0 && col("ps")).select(col("vh") +: p.map(col) :+ n.as("rows"): _*)
    val values = dupByValue.join(rowsByValue, "vh").orderBy(desc("e")).limit(TopN).collect().toSeq.map { r =>
      DuplicateValue(p.map(c => Option(r.getAs[String](c)).fold("(null)")(shorten)), r.getAs[Long]("g") * sample,
        r.getAs[Long]("e") * sample, r.getAs[Long]("rows") * sample)
    }
    DuplicateProfile(columns, copies, location, pairs, values)
  }

  /** Nhóm các file cùng dấu vân tay (số dòng, tổng hash), chỉ nhóm có từ 2 file. */
  private[job] def groupFiles(fingerprints: Seq[(String, (Long, Long, Long))]): Seq[Seq[String]] =
    fingerprints.groupBy(_._2).values.map(_.map(_._1).sorted).filter(_.size > 1).toSeq.sortBy(_.head)

  private[job] def folderName(file: String): String = FolderOf.r.findFirstMatchIn(file).fold("(none)")(_.group(1))

  private def shorten(v: String): String = if (v.length <= MaxValueLength) v else v.take(MaxValueLength) + "..."

  private def quote(c: String): String = s"`${c.replace("`", "``")}`"

  private def hasMap(t: DataType): Boolean = t match {
    case _: MapType => true
    case s: StructType => s.fields.exists(f => hasMap(f.dataType))
    case a: ArrayType => hasMap(a.elementType)
    case _ => false
  }

  private def percent(part: Long, whole: Long): String = if (whole == 0) "0%" else f"${100.0 * part / whole}%.4f%%"
}
