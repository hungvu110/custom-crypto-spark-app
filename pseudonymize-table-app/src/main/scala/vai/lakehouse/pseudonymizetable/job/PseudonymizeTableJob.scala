package vai.lakehouse.pseudonymizetable.job

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import vai.lakehouse.pseudonym.{ConfigurationException, PseudonymMode}
import vai.lakehouse.pseudonym.policy.{ColumnRule, TablePolicy}
import vai.lakehouse.pseudonym.spark.{PseudonymConf, Pseudonymizer}
import scala.util.control.NonFatal

import vai.lakehouse.pseudonymizetable.config.{BadFilePolicy, FileInput, JobConfig, TableInput}
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

  private def diagnose(spark: SparkSession, input: FileInput, parallelism: Int): Unit =
    try {
      Log.section("CHẨN ĐOÁN: QUÉT TỪNG FILE CỦA " + input.path)
      val scan = FileScanner.scan(spark, input, parallelism, deep = true)
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
    val (source, scan) = readSource(spark, cfg)
    if (cfg.badFiles == BadFilePolicy.Report) {
      Log.info("INPUT_BAD_FILES=report: chỉ quét, KHÔNG ghi bảng đích")
      return JobResult(-1, -1, exact = false, Nil, scan.map(_.bad).getOrElse(Nil), written = false)
    }
    Log.kv("schema", source.schema.fieldNames.mkString(", "))

    // Dựng DataFrame giả danh TRƯỚC mọi thao tác ghi: cột sai/khoá thiếu thì fail sớm, chưa chạm bảng đích.
    val pseudonymized = pseudonymizer.pseudonymize(source, policy)

    Log.section("4. GHI BẢNG ĐÍCH")
    Log.kv("target", s"${cfg.target} [${cfg.outputFormat.name}, ${cfg.outputMode}]")
    Log.kv("pseudonymized columns", cfg.pseudoColumns.map(c => s"${c.name}:${c.mode.name}").mkString(", "))
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${quote(cfg.outputDb)}")
    val start = System.nanoTime()
    pseudonymized.write.format(cfg.outputFormat.sparkFormat).mode(cfg.outputMode)
      .saveAsTable(quoted(cfg.outputDb, cfg.outputTable))
    Log.elapsed("write duration", start)

    Log.section("5. ĐỐI CHIẾU")
    val result = if (cfg.verify) verify(spark, cfg, source) else {
      Log.info("VERIFY=false: bỏ qua đối chiếu nguồn/đích")
      JobResult(-1, -1, exact = false, Nil)
    }
    result.copy(badFiles = scan.map(_.bad).getOrElse(Nil))
  }

  /** Bảng trong catalog, hoặc file/thư mục theo `INPUT_FORMAT` (+ `INPUT_SCHEMA`, `INPUT_OPTION_*`). Chỉ đọc. */
  private def readSource(spark: SparkSession, cfg: JobConfig): (DataFrame, Option[FileScan]) = cfg.input match {
    case TableInput(db, table) =>
      if (!spark.catalog.tableExists(db, table)) {
        throw new IllegalArgumentException(s"Bảng nguồn không tồn tại: ${cfg.source}")
      }
      (spark.table(quoted(db, table)), None)
    case input: FileInput if cfg.badFiles == BadFilePolicy.Fail =>
      (FileScanner.reader(spark, input).load(input.path), None)
    case input: FileInput =>
      // skip/report: quét từng file TRƯỚC; skip chỉ đọc các file tốt, báo file lỗi ở cuối (exit 4).
      val scan = FileScanner.scan(spark, input, cfg.checkParallelism, deep = cfg.checkFull)
      FileScanner.log(scan)
      if (cfg.badFiles == BadFilePolicy.Report) (spark.emptyDataFrame, Some(scan))
      else {
        if (scan.good.isEmpty) throw new IllegalStateException(s"Không có file nào đọc được trong ${input.path}")
        (FileScanner.reader(spark, input).load(scan.good: _*), Some(scan))
      }
  }

  private def verify(spark: SparkSession, cfg: JobConfig, source: DataFrame): JobResult = {
    val target = spark.table(quoted(cfg.outputDb, cfg.outputTable))
    val rowsSource = source.count()
    val rowsTarget = target.count()
    Log.kv("rows source / target", s"$rowsSource / $rowsTarget")
    val exact = cfg.outputMode == "overwrite"
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
