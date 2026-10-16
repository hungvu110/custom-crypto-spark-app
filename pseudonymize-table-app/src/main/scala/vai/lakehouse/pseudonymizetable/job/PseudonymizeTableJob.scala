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
import vai.lakehouse.pseudonymizetable.logging.{Log, Timings}

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
        s"A column uses MAPPING and needs the mapping table path: set MAPPING_TABLE_PATH or ${PseudonymConf.MappingPathKey}")
    }
    new Pseudonymizer(PseudonymConf.keyProvider(conf), mappingPath)
  }

  /**
   * Chạy job. `INPUT_BAD_FILES=fail` (mặc định) mà job lỗi thì quét từng file của `INPUT_PATH` để chỉ ra file lỗi rồi ném
   * lại lỗi gốc. `skip` KHÔNG quét trước: đọc thẳng với ignoreCorruptFiles rồi so danh sách file với các file đã cho ra
   * dòng để biết file nào bị bỏ. `report` chỉ quét.
   */
  def run(spark: SparkSession, cfg: JobConfig): JobResult = {
    val corruptKey = "spark.sql.files.ignoreCorruptFiles"
    val previous = spark.conf.getOption(corruptKey)
    try doRun(spark, cfg)
    catch {
      case NonFatal(e) =>
        (cfg.input, cfg.badFiles) match {
          case (in: FileInput, BadFilePolicy.Fail | BadFilePolicy.Skip) =>
            if (isReadFailure(e)) diagnose(spark, in, cfg.checkParallelism)
            else Log.info(s"Failure is not a file-read error (${rootMessage(e)}): skipping the per-file diagnostic scan. " +
              "If you suspect source files, rerun with INPUT_BAD_FILES=report.")
          case _ =>
        }
        throw e
    } finally previous match { // skip bật ignoreCorruptFiles: không để rò sang lần chạy sau trong cùng session
      case Some(v) => spark.conf.set(corruptKey, v)
      case None => spark.conf.unset(corruptKey)
    }
  }

  private def causes(t: Throwable): Seq[Throwable] = {
    val seen = new java.util.IdentityHashMap[Throwable, Boolean]()
    Iterator.iterate(t)(_.getCause).takeWhile(c => c != null && seen.put(c, true) == null).toSeq
  }

  private def rootMessage(t: Throwable): String = {
    val root = causes(t).last
    s"${root.getClass.getSimpleName}: ${Option(root.getMessage).getOrElse("").split("\n").headOption.getOrElse("").take(160)}"
  }

  /**
   * Lỗi có phải do ĐỌC file nguồn không (parse CSV, Parquet hỏng, mất file, "Encountered error while reading file")?
   * Quét chẩn đoán đọc lại toàn bộ dữ liệu (hàng giờ với thư mục lớn) nên chỉ chạy cho loại lỗi này; lỗi metastore, ghi
   * HDFS, khoá, cấu hình... không cần quét.
   */
  private[job] def isReadFailure(e: Throwable): Boolean = {
    val chain = causes(e)
    def name(c: Throwable) = c.getClass.getName
    def text(c: Throwable) = Option(c.getMessage).getOrElse("")
    val metastore = chain.exists(c => name(c).contains("MetaException") || name(c).contains("HiveException"))
    !metastore && chain.exists { c =>
      name(c).contains("TextParsingException") || name(c).contains("ParquetDecodingException") ||
        name(c).contains("FileNotFoundException") || name(c).contains("MalformedInputException") ||
        text(c).contains("Encountered error while reading file") || text(c).contains("is not a Parquet file") ||
        text(c).contains("PATH_NOT_FOUND")
    }
  }

  /** Nguồn file: thư mục ngày/giờ trong khoảng (nếu có), input đã gắn `basePath` để vẫn có cột partition, và các root cần đọc. */
  private final case class FileSource(input: FileInput, roots: Seq[String], partitions: Option[Seq[InputPartition]])

  private def resolveFiles(spark: SparkSession, input: FileInput): FileSource = input.range match {
    case None => FileSource(FileScanner.ensureSchema(spark, input, Seq(input.path)), Seq(input.path), None)
    case Some(range) =>
      // Giữ nguyên chuỗi "05" của thư mục (mặc định Spark suy kiểu số và làm mất số 0 đầu).
      spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      val partitions = InputPartitions.resolve(spark, input, range)
      if (partitions.isEmpty) {
        throw new IllegalStateException(s"No ${range.dateKey}=…/${range.hourKey}=… directory under ${input.path} " +
          s"within range ${range.describe}")
      }
      // basePath = INPUT_PATH để mọi chế độ (fail/skip/report) đều có cột partition date/hour giống nhau.
      val withBase = input.copy(options = Map("basePath" -> input.path) ++ input.options)
      val roots = partitions.map(_.path)
      FileSource(FileScanner.ensureSchema(spark, withBase, roots), roots, Some(partitions))
  }

  private def diagnose(spark: SparkSession, input: FileInput, parallelism: Int): Unit =
    try {
      Log.section("DIAGNOSIS: SCANNING EVERY FILE UNDER " + input.path)
      val fs = resolveFiles(spark, input)
      val scan = FileScanner.scan(spark, fs.input, fs.roots, parallelism, deep = true)
      FileScanner.log(scan)
      if (scan.bad.isEmpty) Log.info("Every file is readable on its own: the failure is not in reading files (check the write step / keys).")
      else Log.info("Rerun with INPUT_BAD_FILES=skip to skip these files, or fix/remove them from INPUT_PATH.")
    } catch {
      case NonFatal(e) => Log.warn(s"Could not scan files for diagnosis: ${e.getMessage}")
    }

  private def doRun(spark: SparkSession, cfg: JobConfig): JobResult = {
    val stats = new SparkStats(spark.sparkContext)
    spark.sparkContext.addSparkListener(stats)
    try runSteps(spark, cfg, stats) finally spark.sparkContext.removeSparkListener(stats)
  }

  private def runSteps(spark: SparkSession, cfg: JobConfig, stats: SparkStats): JobResult = {
    val pseudonymizer = pseudonymizerFor(cfg, spark.sparkContext.getConf)
    val policy = TablePolicy(cfg.source, cfg.pseudoColumns.map(c => ColumnRule(c.name, c.mode)))

    Log.section("3. READ SOURCE")
    Log.kv("source", cfg.source)
    val data = Timings.time("read: resolve source (list dirs, schema)") { readSource(spark, cfg) }
    val (source, scan) = (data.df, data.scan)
    val tracker = cfg.input match {
      case _: FileInput if cfg.badFiles == BadFilePolicy.Skip => Some(FileTracker.newAccumulator(spark))
      case _ => None
    }
    if (cfg.badFiles == BadFilePolicy.Report) {
      Log.info("INPUT_BAD_FILES=report: scan only, the target table is NOT written")
      return JobResult(-1, -1, exact = false, Nil, scan.map(_.bad).getOrElse(Nil), written = false)
    }
    Log.kv("columns", source.columns.length)

    // Dựng DataFrame giả danh TRƯỚC mọi thao tác ghi: cột sai/khoá thiếu thì fail sớm, chưa chạm bảng đích.
    val pseudonymized = Timings.time("pseudonymize: build plan") { pseudonymizer.pseudonymize(source, policy) }
    val missing = cfg.outputPartitionBy.filterNot(pseudonymized.columns.contains)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(s"OUTPUT_PARTITION_BY: column(s) ${missing.mkString(", ")} not found in the data " +
        s"(last columns: ${pseudonymized.columns.takeRight(5).mkString(", ")}…). Read by INPUT_FROM/INPUT_TO range to get " +
        "the partition columns, or set OUTPUT_PARTITION_BY=none")
    }

    // skip: theo dõi file đã cho ra dòng (xem FileTracker) để cuối job biết file nào bị bỏ qua.
    val toWrite = tracker.fold(pseudonymized)(acc => FileTracker.attach(pseudonymized, cfg.pseudoColumns.head.name, acc))
    // Each partition job rebuilds the plan from `source.where(partition)`. The partition filter MUST sit below the
    // transformations: the tracker expression is nondeterministic, so a filter placed above it is not pushed down, partition
    // pruning is lost and every partition job would scan ALL selected partitions.
    val transform: DataFrame => DataFrame = df => {
      val p = pseudonymizer.pseudonymize(df, policy)
      tracker.fold(p)(acc => FileTracker.attach(p, cfg.pseudoColumns.head.name, acc))
    }

    Log.section("4. WRITE TARGET")
    Log.kv("target", s"${cfg.target} [${cfg.outputFormat.name}, ${cfg.outputMode}]")
    Log.kv("partition by", if (cfg.outputPartitionBy.isEmpty) "(none)" else cfg.outputPartitionBy.mkString(", "))
    Log.kv("pseudonymized columns", cfg.pseudoColumns.map(c => s"${c.name}:${c.mode.name}").mkString(", "))
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${quote(cfg.outputDb)}")
    val statsBefore = stats.settled()
    // Spark runs read + pseudonymize + write fused in the same tasks, so they cannot be timed separately here.
    val writeSummary = Timings.time("write: read + pseudonymize + write (fused)") { write(spark, cfg, toWrite, source, transform, data.partitions, stats) }
    Log.kv("write totals", (stats.settled() - statsBefore).describe)
    val skippedInfo = tracker.map(acc => Timings.time("detect skipped files") { skippedFiles(spark, cfg, acc, writeSummary.writtenRoots) })
    val skipped = skippedInfo.map(_.bad).getOrElse(scan.map(_.bad).getOrElse(Nil))
    // `count()` của csv không parse nên đếm cả dòng của file hỏng: đối chiếu chỉ so với các file đã đọc được.
    val verifySource = skippedInfo.filter(_.bad.nonEmpty).fold(source) { info =>
      if (info.good.isEmpty) source else FileScanner.reader(spark, info.input).load(info.good: _*)
    }

    Log.section("5. VERIFY")
    val verifiable = cfg.verify && writeSummary.skipped == 0
    if (cfg.verify && !verifiable) {
      Log.warn("some partitions were skipped (OUTPUT_SKIP_EXISTING): they hold data from earlier runs, so verification is skipped")
    }
    val result = if (verifiable) Timings.time("verify") { verify(spark, cfg, verifySource, data.partitions) } else {
      if (!cfg.verify) Log.info("VERIFY=false: skipping source/target verification")
      JobResult(-1, -1, exact = false, Nil)
    }
    result.copy(badFiles = skipped)
  }

  private final case class SourceData(df: DataFrame, scan: Option[FileScan], partitions: Option[Seq[InputPartition]])

  /** Bảng trong catalog, hoặc file/thư mục theo `INPUT_FORMAT` (+ `INPUT_SCHEMA`, `INPUT_OPTION_*`, khoảng ngày/giờ). Chỉ đọc. */
  private def readSource(spark: SparkSession, cfg: JobConfig): SourceData = cfg.input match {
    case TableInput(db, table) =>
      if (!spark.catalog.tableExists(db, table)) {
        throw new IllegalArgumentException(s"Source table does not exist: ${cfg.source}")
      }
      SourceData(spark.table(quoted(db, table)), None, None)
    case input: FileInput =>
      val fs = resolveFiles(spark, input)
      fs.partitions.foreach { ps =>
        Log.kv("partitions", s"${ps.size}: ${ps.take(3).map(_.label).mkString(", ")}${if (ps.size > 3) ", …, " + ps.last.label else ""}")
      }
      cfg.badFiles match {
        case BadFilePolicy.Report =>
          // Chỉ quét (không ghi gì): nhanh theo mặc định, INPUT_CHECK_FULL=true để parse đầy đủ.
          val scan = FileScanner.scan(spark, fs.input, fs.roots, cfg.checkParallelism, deep = cfg.checkFull)
          FileScanner.log(scan)
          SourceData(spark.emptyDataFrame, Some(scan), fs.partitions)
        case BadFilePolicy.Skip =>
          // Không quét trước: Spark bỏ qua file lỗi khi đọc; file nào bị bỏ được tìm ra sau khi ghi (xem skippedFiles).
          spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
          SourceData(FileScanner.reader(spark, fs.input).load(fs.roots: _*), None, fs.partitions)
        case BadFilePolicy.Fail =>
          SourceData(FileScanner.reader(spark, fs.input).load(fs.roots: _*), None, fs.partitions)
      }
  }

  /**
   * File nằm trong thư mục nguồn nhưng không cho ra dòng nào (bị bỏ vì lỗi đọc, hoặc rỗng). Lý do thật của từng file được
   * xác định bằng cách đọc lại CHỈ file đó. Lưu ý: file hỏng GIỮA chừng vẫn đã cho ra các dòng trước chỗ hỏng nên không
   * nằm trong danh sách này (Spark chỉ ghi WARN `Skipped the rest of the content in the corrupted file` ở log executor).
   */
  private final case class SkipInfo(bad: Seq[BadFile], good: Seq[String], input: FileInput)

  private def skippedFiles(spark: SparkSession, cfg: JobConfig,
                           acc: org.apache.spark.util.CollectionAccumulator[String],
                           writtenRoots: Option[Seq[String]]): SkipInfo = cfg.input match {
    case input: FileInput =>
      val fs = resolveFiles(spark, input)
      val seen = FileTracker.seenPaths(acc)
      // Only the partitions written in this run were read; the rest (OUTPUT_SKIP_EXISTING) must not be judged here.
      val listed = FileScanner.listFiles(spark, writtenRoots.getOrElse(fs.roots))
      val (read, missing) = listed.partition(f => seen.contains(FileTracker.normalize(f)))
      Log.kv("files read / skipped", s"${read.size} / ${missing.size}")
      val bad = missing.map { f =>
        BadFile(f, FileScanner.explain(spark, fs.input, f).getOrElse("file has no data rows (empty)"))
      }
      SkipInfo(bad, read, fs.input)
    case _ => SkipInfo(Nil, Nil, null)
  }

  /**
   * Ghi bảng đích. `overwrite-partitions`: bảng chưa có thì tạo (có partition); đã có thì `INSERT OVERWRITE` ở chế độ
   * dynamic partition overwrite — chỉ các partition có trong dữ liệu lần này bị thay, partition khác giữ nguyên.
   */
  /** `skipped` = partitions left untouched because they were already in the target (OUTPUT_SKIP_EXISTING). */
  private final case class WriteSummary(written: Int, skipped: Int, writtenRoots: Option[Seq[String]] = None)

  private def write(spark: SparkSession, cfg: JobConfig, df: DataFrame, source: DataFrame, transform: DataFrame => DataFrame,
                    known: Option[Seq[InputPartition]], stats: SparkStats): WriteSummary = {
    val table = quoted(cfg.outputDb, cfg.outputTable)
    val exists = spark.catalog.tableExists(cfg.outputDb, cfg.outputTable)
    val partitioned = cfg.outputPartitionBy.nonEmpty &&
      (cfg.outputMode == "overwrite-partitions" || cfg.outputMode == "append")
    if (!partitioned) {
      val writer = df.write.format(cfg.outputFormat.sparkFormat).mode(if (cfg.outputMode == "append") "append" else "overwrite")
      (if (cfg.outputPartitionBy.isEmpty) writer else writer.partitionBy(cfg.outputPartitionBy: _*)).saveAsTable(table)
      WriteSummary(written = 1, skipped = 0)
    } else {
      // Đăng ký bảng trong metastore TRƯỚC khi ghi: việc ghi có thể kéo dài hàng giờ và metastore chỉ còn phải xử lý ở
      // lúc đầu (NameNode rảnh), không phải sau khi đã ghi xong như `saveAsTable` trên bảng chưa có.
      if (!exists) createEmptyTable(spark, cfg, df, table) else requireSamePartitioning(spark, cfg)
      val tuples = partitionTuples(cfg, known)
      tuples match {
        case Some(keys) =>
          val results = keys.zipWithIndex.map { case (values, i) =>
            writePartition(spark, cfg, source, transform, table, values, i + 1, keys.size, stats)
          }
          val written = results.count(_.written)
          Log.kv("partitions written / skipped", s"$written / ${results.size - written}")
          // Directories of the partitions written in THIS run: partitions skipped via OUTPUT_SKIP_EXISTING are never read,
          // so their files must not be reported as skipped-because-bad.
          val pathByValues = known.getOrElse(Nil).flatMap(p => p.hour.map(h => Seq(p.date, h) -> p.path)).toMap
          val writtenRoots = results.filter(_.written).flatMap(r => pathByValues.get(r.values))
          val retried = results.filter(r => r.written && r.attempts > 1)
          if (retried.nonEmpty) {
            Log.warn(s"partitions that needed retries: ${retried.map(r => s"${r.label} (${r.attempts} attempts)").mkString(", ")}")
          }
          WriteSummary(written, results.size - written, Some(writtenRoots))
        case None =>
          Log.info("partition list not known in advance: writing in a single pass")
          insertInto(spark, cfg, df, table)
          WriteSummary(written = 1, skipped = 0)
      }
    }
  }

  /** Danh sách giá trị partition (theo thứ tự `OUTPUT_PARTITION_BY`) khi biết trước từ thư mục nguồn; `None` nếu không. */
  private def partitionTuples(cfg: JobConfig, known: Option[Seq[InputPartition]]): Option[Seq[Seq[String]]] = {
    val cols = cfg.outputPartitionBy
    (known, cfg.input) match {
      case (Some(ps), in: FileInput)
          if ps.nonEmpty && ps.forall(_.hour.isDefined) && in.range.exists(r => Seq(r.dateKey, r.hourKey) == cols) =>
        Some(ps.flatMap(p => p.hour.map(h => Seq(p.date, h))))
      case _ => None
    }
  }

  /** `col1 = v1 AND col2 = v2` on the partition columns (as strings, matching the on-disk directory names). */
  private[job] def partitionCondition(columns: Seq[String], values: Seq[String]): Column =
    columns.zip(values).map { case (c, v) => col(quote(c)).cast(StringType) === v }.reduce(_ && _)

  private final case class PartitionResult(label: String, values: Seq[String], written: Boolean, attempts: Int)

  /**
   * Writes (and commits) one partition at a time: progress log, failures name the partition, small Delta commits.
   * With OUTPUT_SKIP_EXISTING a partition already present in the target is left untouched (resume after a failure);
   * transient failures are retried (OUTPUT_WRITE_RETRIES) — safe because a partition overwrite is atomic and idempotent.
   */
  private def writePartition(spark: SparkSession, cfg: JobConfig, source: DataFrame, transform: DataFrame => DataFrame,
                             table: String, values: Seq[String], index: Int, total: Int, stats: SparkStats): PartitionResult = {
    val label = cfg.outputPartitionBy.zip(values).map { case (c, v) => s"$c=$v" }.mkString("/")
    val condition = partitionCondition(cfg.outputPartitionBy, values)
    if (cfg.skipExisting && !spark.table(table).where(condition).isEmpty) {
      Log.info(s"[$index/$total] SKIP $label (already in target, OUTPUT_SKIP_EXISTING=true)")
      return PartitionResult(label, values, written = false, attempts = 0)
    }
    spark.sparkContext.setJobDescription(s"write $label ($index/$total)")
    val before = stats.settled()
    val start = System.nanoTime()
    val attempts = cfg.writeRetries + 1
    var used = 1
    try {
      retrying(attempts, cfg.retryWaitSeconds * 1000L, isTransient,
        (n, e) => Log.warn(s"[$index/$total] WRITE ATTEMPT $n/$attempts FAILED $label: ${rootMessage(e)}; " +
          s"retrying in ${cfg.retryWaitSeconds} s")) { n =>
        used = n
        Log.info(if (n == 1) s"[$index/$total] WRITE START $label" else s"[$index/$total] WRITE START $label (attempt $n/$attempts)")
        insertInto(spark, cfg, transform(source.where(condition)), table)
      }
    } catch {
      case NonFatal(e) =>
        val failedAfter = (System.nanoTime() - start) / 1e9
        Log.fail(f"[$index/$total] WRITE FAILED $label after $failedAfter%,.1f s: ${rootMessage(e)}")
        throw e
    } finally spark.sparkContext.setJobDescription(null)
    val seconds = (System.nanoTime() - start) / 1e9
    val attemptNote = if (used > 1) s" (attempt $used/$attempts)" else ""
    Log.ok(f"[$index/$total] WRITE DONE $label$attemptNote in $seconds%,.1f s | ${(stats.settled() - before).describe}")
    PartitionResult(label, values, written = true, attempts = used)
  }

  /**
   * Runs `body(attempt)` up to `attempts` times, retrying (after `waitMillis`) only failures `transient` accepts.
   * `onRetry(attempt, error)` is called before each wait.
   */
  private[job] def retrying[T](attempts: Int, waitMillis: Long, transient: Throwable => Boolean,
                               onRetry: (Int, Throwable) => Unit, sleep: Long => Unit = Thread.sleep(_))(body: Int => T): T = {
    var attempt = 1
    var result: Option[T] = None
    while (result.isEmpty) {
      try result = Some(body(attempt))
      catch {
        case NonFatal(e) if attempt < attempts && transient(e) =>
          onRetry(attempt, e)
          sleep(waitMillis)
          attempt += 1
      }
    }
    result.get
  }

  /**
   * Is this failure worth retrying (flaky NameNode/metastore/executor, timeouts) rather than a permanent error (bad
   * configuration, keys, schema, unreadable source file)? Permanent markers win over transient ones.
   */
  private[job] def isTransient(e: Throwable): Boolean = {
    val chain = causes(e)
    def name(c: Throwable) = c.getClass.getName
    def text(c: Throwable) = Option(c.getMessage).getOrElse("").toLowerCase
    val permanent = isReadFailure(e) || chain.exists(c =>
      Seq("InvalidPolicyException", "ConfigurationException", "InvalidKeyException", "IllegalArgumentException",
        "IllegalStateException").exists(name(c).contains))
    val markers = Seq("timed out", "timeout", "connection reset", "connection refused", "executorlostfailure", "lost executor",
      "unable to connect", "broken pipe")
    val transientSignal = chain.exists(c =>
      c.isInstanceOf[java.io.IOException] || name(c).contains("MetaException") || name(c).contains("HiveException") ||
        markers.exists(text(c).contains))
    !permanent && transientSignal
  }

  private def insertInto(spark: SparkSession, cfg: JobConfig, df: DataFrame, table: String): Unit =
    if (cfg.outputMode == "append") {
      df.select(spark.table(table).columns.map(c => col(quote(c))): _*).write.mode("append").insertInto(table)
    } else insertOverwriteDynamic(spark, df, table)

  /** Tạo bảng rỗng (đúng schema + partition) rồi mới ghi dữ liệu; thử lại vài lần khi metastore chập chờn. */
  private def createEmptyTable(spark: SparkSession, cfg: JobConfig, df: DataFrame, table: String): Unit = {
    val attempts = 3
    (1 to attempts).foreach { attempt =>
      if (!spark.catalog.tableExists(cfg.outputDb, cfg.outputTable)) {
        try {
          Log.info(s"registering table ${cfg.target} in the metastore (attempt $attempt/$attempts)")
          val writer = df.limit(0).write.format(cfg.outputFormat.sparkFormat)
          (if (cfg.outputPartitionBy.isEmpty) writer else writer.partitionBy(cfg.outputPartitionBy: _*)).saveAsTable(table)
        } catch {
          case NonFatal(e) if attempt < attempts =>
            Log.warn(s"table registration failed (${rootMessage(e)}), retrying in 15 s")
            Thread.sleep(15000L)
        }
      }
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
      throw new IllegalStateException(s"Target table ${cfg.target} already exists with partitioning [${existing.mkString(", ")}] different from the configured " +
        s"[${cfg.outputPartitionBy.mkString(", ")}]: use another OUTPUT_TABLE or drop the old table (overwrite-partitions does not change partitioning)")
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
        throw new IllegalStateException(s"Data schema differs from the target table: missing ${targetColumns.diff(df.columns).mkString(",")}, " +
          s"unexpected ${df.columns.diff(targetColumns).mkString(",")}")
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
    val tuples: Seq[Seq[String]] = partitionTuples(cfg, known).getOrElse {
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
    if (!exact) Log.info("OUTPUT_MODE=append: the target still holds earlier runs, verifying only (target >= source, no original values left)")

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
        s"original values left=${v.originalValuesLeft}"
      if (v.passed(exact)) Log.ok(line) else Log.fail(line)
      v
    }
    JobResult(rowsSource, rowsTarget, exact, verifications)
  }

  private def quote(identifier: String): String = s"`${identifier.replace("`", "``")}`"

  private def quoted(db: String, table: String): String = s"${quote(db)}.${quote(table)}"
}
