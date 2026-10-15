package vai.lakehouse.pseudonymizetable.job

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import vai.lakehouse.pseudonym.{ConfigurationException, PseudonymMode}
import vai.lakehouse.pseudonym.policy.{ColumnRule, TablePolicy}
import vai.lakehouse.pseudonym.spark.{PseudonymConf, Pseudonymizer}
import vai.lakehouse.pseudonymizetable.config.{BadFilePolicy, FileInput, JobConfig, OutputFormat, TableInput}
import vai.lakehouse.pseudonymizetable.logging.Log

/** Kết quả đối chiếu 1 cột đã giả danh giữa bảng nguồn và bảng đích. */
final case class ColumnVerification(
    column: String,
    nullsSource: Long,
    nullsTarget: Long,
    distinctSource: Long,
    distinctTarget: Long,
    originalValuesLeft: Long) {

  /**
   * `exact` (OUTPUT_MODE=overwrite: đích == kết quả của đúng lần chạy này): null vào -> null ra; số giá trị khác nhau
   * giữ nguyên (ánh xạ 1-1). Không `exact` (append: đích còn dữ liệu các lần trước) thì chỉ còn kiểm điều luôn đúng.
   * Cả hai: không còn giá trị gốc nào trong đích.
   */
  def passed(exact: Boolean): Boolean =
    originalValuesLeft == 0 && (!exact || (nullsSource == nullsTarget && distinctSource == distinctTarget))
}

/** `exact` = bảng đích được thay thế hoàn toàn (overwrite) nên so khớp tuyệt đối với nguồn. */
final case class JobResult(rowsSource: Long, rowsTarget: Long, exact: Boolean, verifications: Seq[ColumnVerification],
                           badFiles: Seq[BadFile] = Nil, written: Boolean = true) {
  def verified: Boolean =
    (if (exact) rowsSource == rowsTarget else rowsTarget >= rowsSource) && verifications.forall(_.passed(exact))
}

/**
 * Khử định danh 1 bảng: đọc `DB_NAME.TABLE_NAME` -> giả danh các cột trong `PSEUDO_COLUMNS` bằng `pseudonym-udf` -> ghi ra
 * bảng đích -> (tuỳ chọn) đọc lại bảng đích và đối chiếu với nguồn.
 *
 * Bảng nguồn chỉ được ĐỌC. Cấu hình lib (nguồn khoá, version khoá, đường dẫn bảng ánh xạ) lấy từ `conf`
 * (`spark.pseudonym.*`) rồi biến môi trường — xem README mục "Env của pseudonym-udf".
 */
object PseudonymizeTableJob {

  /** Dựng `Pseudonymizer` từ SparkConf + env; `MAPPING_TABLE_PATH` (nếu có) ưu tiên hơn `spark.pseudonym.mapping.path`. */
  def pseudonymizerFor(cfg: JobConfig, conf: SparkConf): Pseudonymizer = {
    val mappingPath = cfg.mappingTablePath.orElse(PseudonymConf.mappingPath(conf))
    if (cfg.modes.contains(PseudonymMode.Mapping) && mappingPath.isEmpty) {
      throw new ConfigurationException(
        s"Có cột dùng MAPPING cần đường dẫn bảng ánh xạ: đặt MAPPING_TABLE_PATH hoặc ${PseudonymConf.MappingPathKey}")
    }
    new Pseudonymizer(PseudonymConf.keyProvider(conf), mappingPath)
  }

  /**
   * Chạy job. `INPUT_BAD_FILES=fail` (mặc định) mà job lỗi thì quét từng file của `INPUT_PATH` để chỉ ra file lỗi rồi ném
   * lại lỗi gốc; `skip`/`report` quét TRƯỚC (xem [[readSource]]).
   */
  def run(spark: SparkSession, cfg: JobConfig): JobResult =
    try doRun(spark, cfg)
    catch {
      case NonFatal(e) =>
        (cfg.input, cfg.badFiles) match {
          case (in: FileInput, BadFilePolicy.Fail | BadFilePolicy.Skip) => diagnose(spark, in, cfg.checkParallelism)
          case _ =>
        }
        throw e
    }

  /** Nguồn file: thư mục ngày/giờ trong khoảng (nếu có), input đã gắn `basePath` để vẫn có cột partition, và các root cần đọc. */
  private final case class FileSource(input: FileInput, roots: Seq[String], partitions: Option[Seq[InputPartition]])

  private def resolveFiles(spark: SparkSession, input: FileInput): FileSource = input.range match {
    case None => FileSource(input, Seq(input.path), None)
    case Some(range) =>
      // Giữ nguyên chuỗi "05" của thư mục (mặc định Spark suy kiểu số và làm mất số 0 đầu).
      spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      val partitions = InputPartitions.resolve(spark, input, range)
      if (partitions.isEmpty) {
        throw new IllegalStateException(s"Không có thư mục ${range.dateKey}=…/${range.hourKey}=… nào trong ${input.path} " +
          s"thuộc khoảng ${range.describe}")
      }
      // basePath = INPUT_PATH để mọi chế độ (fail/skip/report) đều có cột partition date/hour giống nhau.
      FileSource(input.copy(options = Map("basePath" -> input.path) ++ input.options), partitions.map(_.path), Some(partitions))
  }

  private def diagnose(spark: SparkSession, input: FileInput, parallelism: Int): Unit =
    try {
      Log.section("CHẨN ĐOÁN: QUÉT TỪNG FILE CỦA " + input.path)
      val fs = resolveFiles(spark, input)
      val scan = FileScanner.scan(spark, fs.input, fs.roots, parallelism, deep = true)
      FileScanner.log(scan)
      if (scan.bad.isEmpty) Log.info("Mọi file đọc được riêng lẻ: lỗi không nằm ở việc đọc file (kiểm tra bước ghi/khoá).")
      else Log.info("Chạy lại với INPUT_BAD_FILES=skip để bỏ các file này, hoặc sửa/loại chúng khỏi INPUT_PATH.")
    } catch {
      case NonFatal(e) => Log.warn(s"Không quét được file để chẩn đoán: ${e.getMessage}")
    }

  private def doRun(spark: SparkSession, cfg: JobConfig): JobResult = {
    val pseudonymizer = pseudonymizerFor(cfg, spark.sparkContext.getConf)
    val policy = TablePolicy(cfg.source, cfg.pseudoColumns.map(c => ColumnRule(c.name, c.mode)))

    Log.section("3. ĐỌC NGUỒN")
    Log.kv("source", cfg.source)
    val data = readSource(spark, cfg)
    val (source, scan) = (data.df, data.scan)
    if (cfg.badFiles == BadFilePolicy.Report) {
      Log.info("INPUT_BAD_FILES=report: chỉ quét, KHÔNG ghi bảng đích")
      return JobResult(-1, -1, exact = false, Nil, scan.map(_.bad).getOrElse(Nil), written = false)
    }
    Log.kv("schema", s"${source.columns.length} cột")

    // Dựng DataFrame giả danh TRƯỚC mọi thao tác ghi: cột sai/khoá thiếu thì fail sớm, chưa chạm bảng đích.
    val pseudonymized = pseudonymizer.pseudonymize(source, policy)
    val missing = cfg.outputPartitionBy.filterNot(pseudonymized.columns.contains)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(s"OUTPUT_PARTITION_BY: cột ${missing.mkString(", ")} không có trong dữ liệu " +
        s"(có: ${pseudonymized.columns.takeRight(5).mkString(", ")}…). Đọc theo khoảng INPUT_FROM/INPUT_TO để có cột date/hour, " +
        "hoặc đặt OUTPUT_PARTITION_BY=none")
    }

    Log.section("4. GHI BẢNG ĐÍCH")
    Log.kv("target", s"${cfg.target} [${cfg.outputFormat.name}, ${cfg.outputMode}]")
    Log.kv("partition by", if (cfg.outputPartitionBy.isEmpty) "(không)" else cfg.outputPartitionBy.mkString(", "))
    Log.kv("pseudonymized columns", cfg.pseudoColumns.map(c => s"${c.name}:${c.mode.name}").mkString(", "))
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${quote(cfg.outputDb)}")
    val start = System.nanoTime()
    write(spark, cfg, pseudonymized)
    Log.elapsed("write duration", start)

    Log.section("5. ĐỐI CHIẾU")
    val result = if (cfg.verify) verify(spark, cfg, source, data.partitions) else {
      Log.info("VERIFY=false: bỏ qua đối chiếu nguồn/đích")
      JobResult(-1, -1, exact = false, Nil)
    }
    result.copy(badFiles = scan.map(_.bad).getOrElse(Nil))
  }

  private final case class SourceData(df: DataFrame, scan: Option[FileScan], partitions: Option[Seq[InputPartition]])

  /** Bảng trong catalog, hoặc file/thư mục theo `INPUT_FORMAT` (+ `INPUT_SCHEMA`, `INPUT_OPTION_*`, khoảng ngày/giờ). Chỉ đọc. */
  private def readSource(spark: SparkSession, cfg: JobConfig): SourceData = cfg.input match {
    case TableInput(db, table) =>
      if (!spark.catalog.tableExists(db, table)) {
        throw new IllegalArgumentException(s"Bảng nguồn không tồn tại: ${cfg.source}")
      }
      SourceData(spark.table(quoted(db, table)), None, None)
    case input: FileInput =>
      val fs = resolveFiles(spark, input)
      fs.partitions.foreach { ps =>
        Log.kv("partitions", s"${ps.size}: ${ps.take(3).map(_.label).mkString(", ")}${if (ps.size > 3) ", …, " + ps.last.label else ""}")
      }
      if (cfg.badFiles == BadFilePolicy.Fail) {
        SourceData(FileScanner.reader(spark, fs.input).load(fs.roots: _*), None, fs.partitions)
      } else {
        // skip/report: quét từng file TRƯỚC; skip chỉ đọc các file tốt và log danh sách file đã bỏ qua ở cuối job.
        val scan = FileScanner.scan(spark, fs.input, fs.roots, cfg.checkParallelism, deep = cfg.checkFull)
        FileScanner.log(scan)
        if (cfg.badFiles == BadFilePolicy.Report) SourceData(spark.emptyDataFrame, Some(scan), fs.partitions)
        else {
          if (scan.good.isEmpty) throw new IllegalStateException(s"Không có file nào đọc được trong ${input.path}")
          SourceData(FileScanner.reader(spark, fs.input).load(scan.good: _*), Some(scan), fs.partitions)
        }
      }
  }

  /**
   * Ghi bảng đích. `overwrite-partitions`: bảng chưa có thì tạo (có partition); đã có thì `INSERT OVERWRITE` ở chế độ
   * dynamic partition overwrite — chỉ các partition có trong dữ liệu lần này bị thay, partition khác giữ nguyên.
   */
  private def write(spark: SparkSession, cfg: JobConfig, df: DataFrame): Unit = {
    val table = quoted(cfg.outputDb, cfg.outputTable)
    val exists = spark.catalog.tableExists(cfg.outputDb, cfg.outputTable)
    if (cfg.outputMode == "overwrite-partitions" && exists) {
      requireSamePartitioning(spark, cfg)
      insertOverwriteDynamic(spark, df, table)
    } else {
      val writer = df.write.format(cfg.outputFormat.sparkFormat).mode(if (cfg.outputMode == "append") "append" else "overwrite")
      (if (cfg.outputPartitionBy.isEmpty) writer else writer.partitionBy(cfg.outputPartitionBy: _*)).saveAsTable(table)
    }
  }

  private def requireSamePartitioning(spark: SparkSession, cfg: JobConfig): Unit = {
    val existing =
      if (cfg.outputFormat == OutputFormat.Delta) {
        spark.sql(s"DESCRIBE DETAIL ${quoted(cfg.outputDb, cfg.outputTable)}").select("partitionColumns").head().getSeq[String](0)
      } else {
        spark.catalog.listColumns(cfg.outputDb, cfg.outputTable).collect().filter(_.isPartition).map(_.name).toSeq
      }
    if (existing != cfg.outputPartitionBy) {
      throw new IllegalStateException(s"Bảng đích ${cfg.target} đã có với partition [${existing.mkString(", ")}] khác cấu hình " +
        s"[${cfg.outputPartitionBy.mkString(", ")}]: dùng OUTPUT_TABLE khác hoặc xoá bảng cũ (overwrite-partitions không đổi partition)")
    }
  }

  private def insertOverwriteDynamic(spark: SparkSession, df: DataFrame, table: String): Unit = {
    val key = "spark.sql.sources.partitionOverwriteMode"
    val previous = spark.conf.getOption(key)
    spark.conf.set(key, "dynamic")
    try {
      // insertInto khớp cột THEO VỊ TRÍ: xếp lại theo đúng thứ tự cột của bảng đích.
      val targetColumns = spark.table(table).columns.toSeq
      if (targetColumns.toSet != df.columns.toSet) {
        throw new IllegalStateException(s"Schema dữ liệu khác bảng đích: thiếu ${targetColumns.diff(df.columns).mkString(",")}, " +
          s"thừa ${df.columns.diff(targetColumns).mkString(",")}")
      }
      df.select(targetColumns.map(c => col(quote(c))): _*).write.mode("overwrite").insertInto(table)
    } finally previous match {
      case Some(v) => spark.conf.set(key, v)
      case None => spark.conf.unset(key)
    }
  }

  /** Điều kiện chỉ giữ các partition vừa xử lý (target của overwrite-partitions còn partition khác không thuộc lần chạy này). */
  private def partitionFilter(cfg: JobConfig, source: DataFrame, known: Option[Seq[InputPartition]]): Column = {
    val cols = cfg.outputPartitionBy
    val tuples: Seq[Seq[String]] = known match {
      case Some(ps) if ps.forall(_.hour.isDefined) && cols.size == 2 && cfg.input.isInstanceOf[FileInput] &&
          cfg.input.asInstanceOf[FileInput].range.exists(r => Seq(r.dateKey, r.hourKey) == cols) =>
        ps.flatMap(p => p.hour.map(h => Seq(p.date, h)))
      case _ =>
        source.select(cols.map(c => col(quote(c)).cast(StringType)): _*).distinct().collect()
          .map(r => cols.indices.map(r.getString)).toSeq
    }
    tuples.map(t => cols.zip(t).map { case (c, v) => col(quote(c)).cast(StringType) === v }.reduce(_ && _)).reduce(_ || _)
  }

  private def verify(spark: SparkSession, cfg: JobConfig, source: DataFrame,
                     partitions: Option[Seq[InputPartition]]): JobResult = {
    val whole = spark.table(quoted(cfg.outputDb, cfg.outputTable))
    // overwrite-partitions: chỉ so với các partition vừa ghi; vẫn "exact" (số dòng, null, distinct phải khớp nguồn).
    val target =
      if (cfg.outputMode == "overwrite-partitions") whole.where(partitionFilter(cfg, source, partitions)) else whole
    val rowsSource = source.count()
    val rowsTarget = target.count()
    Log.kv("rows source / target", s"$rowsSource / $rowsTarget")
    val exact = cfg.outputMode == "overwrite" || cfg.outputMode == "overwrite-partitions"
    if (!exact) Log.info("OUTPUT_MODE=append: bảng đích còn dữ liệu các lần trước, chỉ đối chiếu (đích >= nguồn, không sót giá trị gốc)")

    val verifications = cfg.columns.map { c =>
      val src = source.select(col(quote(c)).cast(StringType).as("v"))
      val tgt = target.select(col(quote(c)).cast(StringType).as("v"))
      val v = ColumnVerification(
        column = c,
        nullsSource = src.where(col("v").isNull).count(),
        nullsTarget = tgt.where(col("v").isNull).count(),
        distinctSource = src.where(col("v").isNotNull).distinct().count(),
        distinctTarget = tgt.where(col("v").isNotNull).distinct().count(),
        originalValuesLeft = tgt.where(col("v").isNotNull).distinct()
          .join(src.where(col("v").isNotNull).distinct(), Seq("v"), "left_semi").count())
      val line = s"$c: null ${v.nullsSource}->${v.nullsTarget}, distinct ${v.distinctSource}->${v.distinctTarget}, " +
        s"giá trị gốc còn sót=${v.originalValuesLeft}"
      if (v.passed(exact)) Log.ok(line) else Log.fail(line)
      v
    }
    JobResult(rowsSource, rowsTarget, exact, verifications)
  }

  private def quote(identifier: String): String = s"`${identifier.replace("`", "``")}`"

  private def quoted(db: String, table: String): String = s"${quote(db)}.${quote(table)}"
}
