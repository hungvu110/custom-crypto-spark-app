package vai.lakehouse.pseudonymizetable.config

import java.util.Locale

import vai.lakehouse.pseudonym.PseudonymMode

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/** Định dạng bảng đích. `hive` ghi Parquet (cùng quy ước `TABLE_TYPE` của spark-app). */
sealed abstract class OutputFormat(val name: String, val sparkFormat: String)

object OutputFormat {
  case object Delta   extends OutputFormat("delta", "delta")
  case object Iceberg extends OutputFormat("iceberg", "iceberg")
  case object Hive    extends OutputFormat("hive", "parquet")

  val All: Seq[OutputFormat] = Seq(Delta, Iceberg, Hive)

  def parse(raw: String): Option[OutputFormat] = All.find(_.name == raw.trim.toLowerCase(Locale.ROOT))
}

/** Định dạng file đầu vào. `csv`/`txt` = text phân tách bằng ký tự (mặc định `,`); `text` = mỗi dòng 1 cột `value`. */
sealed abstract class InputFormat(val name: String, val sparkFormat: String, val defaults: Map[String, String] = Map.empty)

object InputFormat {
  case object Parquet extends InputFormat("parquet", "parquet")
  case object Orc     extends InputFormat("orc", "orc")
  case object Avro    extends InputFormat("avro", "avro")
  case object Json    extends InputFormat("json", "json")
  case object Delta   extends InputFormat("delta", "delta")
  case object Csv     extends InputFormat("csv", "csv")
  case object Tsv     extends InputFormat("tsv", "csv", Map("delimiter" -> "\t"))
  case object Text    extends InputFormat("text", "text")

  val All: Seq[InputFormat] = Seq(Parquet, Orc, Avro, Json, Delta, Csv, Tsv, Text)

  def parse(raw: String): Option[InputFormat] = raw.trim.toLowerCase(Locale.ROOT) match {
    case "txt" => Some(Csv)
    case other => All.find(_.name == other)
  }
}

/**
 * Cách xử lý file đầu vào không đọc được (`INPUT_BAD_FILES`): `fail` = dừng, rồi liệt kê file lỗi; `skip` = bỏ file lỗi,
 * xử lý phần còn lại, kết thúc exit 4; `report` = chỉ quét và liệt kê file lỗi, không ghi gì.
 */
sealed abstract class BadFilePolicy(val name: String)

object BadFilePolicy {
  case object Fail   extends BadFilePolicy("fail")
  case object Skip   extends BadFilePolicy("skip")
  case object Report extends BadFilePolicy("report")

  val All: Seq[BadFilePolicy] = Seq(Fail, Skip, Report)

  def parse(raw: String): Option[BadFilePolicy] = All.find(_.name == raw.trim.toLowerCase(Locale.ROOT))
}

/** Nguồn dữ liệu: bảng trong catalog hoặc thư mục/file trên HDFS (hay file system bất kỳ Spark đọc được). */
sealed trait InputSource { def describe: String }

final case class TableInput(db: String, table: String) extends InputSource {
  override def describe: String = s"$db.$table"
}

/**
 * Time range to process in the directory tree `<path>/<dateKey>=<yyyyMMdd>/<timeKey>=<HH | HHMM>/`.
 *
 * `from`/`to` are continuous keys `yyyyMMddHHmm` (both ends inclusive, compared with the START time of each directory, so
 * `batch=0020` is 00:20); `hours` (optional) additionally filters the hour of day, e.g. 6-18 (every minute of those
 * hours). `hourConstrained` = the user constrained the time of day (hour in FROM/TO, or INPUT_HOURS).
 */
final case class InputRange(from: Long, to: Long, hours: Option[(Int, Int)], dateKey: String, hourKey: String,
                            hourConstrained: Boolean) {

  def acceptsDate(date: String): Boolean = date.toLong >= from / 10000 && date.toLong <= to / 10000

  /** `time` = hour * 100 + minute (1740 = 17:40; an hour-only directory `hour=05` is 500). */
  def accepts(date: String, time: Option[Int]): Boolean = time match {
    case Some(t) =>
      val key = date.toLong * 10000 + t
      key >= from && key <= to && hours.forall { case (a, b) => t / 100 >= a && t / 100 <= b }
    case None => acceptsDate(date) && !hourConstrained
  }

  def describe: String = {
    def fmt(k: Long) = if (k == Long.MaxValue) "inf" else if (k == 0) "-inf" else f"$k%012d"
    s"${fmt(from)} -> ${fmt(to)}" + hours.fold("")(h => s", hours ${h._1}-${h._2}")
  }
}

final case class FileInput(path: String, format: InputFormat, schema: Option[String], options: Map[String, String],
                           range: Option[InputRange] = None) extends InputSource {
  override def describe: String = s"$path [${format.name}]" + range.fold("")(r => s" range ${r.describe}")
}

/** Một cột cần giả danh kèm chế độ riêng của nó. */
final case class PseudoColumn(name: String, mode: PseudonymMode)

/**
 * Cấu hình của job, đọc từ biến môi trường của driver (`spec.driver.env` trong SparkApplication).
 *
 * Nguồn là bảng (`DB_NAME` + `TABLE_NAME`) HOẶC file/thư mục (`INPUT_PATH` + `INPUT_FORMAT`); cột cần giả danh ở
 * `PSEUDO_COLUMNS` (`cột[:MODE]`, mỗi cột một chế độ riêng được). Nguồn KHÔNG bao giờ bị sửa: kết quả ghi ra bảng đích
 * (`OUTPUT_DB`.`OUTPUT_TABLE`), mặc định với nguồn bảng là `<DB_NAME>.<TABLE_NAME>_pseudonymized`.
 */
final case class JobConfig(
    appName: String,
    input: InputSource,
    pseudoColumns: Seq[PseudoColumn],
    mode: PseudonymMode,
    outputDb: String,
    outputTable: String,
    outputFormat: OutputFormat,
    outputMode: String,
    mappingTablePath: Option[String],
    warehousePath: Option[String],
    enableHiveSupport: Boolean,
    verify: Boolean,
    badFiles: BadFilePolicy = BadFilePolicy.Fail,
    checkParallelism: Int = 4,
    checkFull: Boolean = false,
    outputPartitionBy: Seq[String] = Nil,
    skipExisting: Boolean = false,
    writeRetries: Int = 0,
    retryWaitSeconds: Int = 30) {

  def source: String = input.describe

  def columns: Seq[String] = pseudoColumns.map(_.name)

  /** Mọi chế độ đang dùng (để biết có cần khoá HMAC / AES / bảng ánh xạ không). */
  def modes: Set[PseudonymMode] = pseudoColumns.map(_.mode).toSet

  def target: String = s"$outputDb.$outputTable"

  def describe: Seq[(String, String)] = Seq(
    "APP_NAME"            -> appName,
    "INPUT"               -> input.describe,
    "PSEUDO_COLUMNS"      -> pseudoColumns.map(c => s"${c.name}:${c.mode.name}").mkString(", "),
    "PSEUDO_MODE (default)" -> mode.name,
    "OUTPUT_DB"           -> outputDb,
    "OUTPUT_TABLE"        -> outputTable,
    "OUTPUT_FORMAT"       -> outputFormat.name,
    "OUTPUT_MODE"         -> outputMode,
    "OUTPUT_SKIP_EXISTING" -> skipExisting.toString,
    "OUTPUT_WRITE_RETRIES" -> s"$writeRetries (wait $retryWaitSeconds s between attempts)",
    "OUTPUT_PARTITION_BY" -> (if (outputPartitionBy.isEmpty) "(no partitioning)" else outputPartitionBy.mkString(", ")),
    "MAPPING_TABLE_PATH"  -> mappingTablePath.getOrElse("(spark.pseudonym.mapping.path or not used)"),
    "WAREHOUSE_PATH"      -> warehousePath.getOrElse("(Spark/metastore default)"),
    "ENABLE_HIVE_SUPPORT" -> enableHiveSupport.toString,
    "INPUT_BAD_FILES"     -> badFiles.name,
    "INPUT_CHECK_FULL"    -> checkFull.toString,
    "VERIFY"              -> verify.toString)
}

object JobConfig {

  /** `overwrite-partitions`: chỉ thay các partition có trong dữ liệu lần này, giữ nguyên partition khác (cần OUTPUT_PARTITION_BY). */
  val OutputModes = Seq("overwrite", "append", "overwrite-partitions")

  private val Identifier = "^[A-Za-z0-9_]+$"

  /** Gom MỌI lỗi cấu hình rồi trả về một lượt, để sửa manifest một lần là đủ. */
  def fromEnv(env: Map[String, String]): Either[Seq[String], JobConfig] = {
    val errors = ArrayBuffer.empty[String]

    def raw(key: String): Option[String] = env.get(key).map(_.trim).filter(_.nonEmpty)

    def identifier(key: String, default: Option[String]): String = raw(key).orElse(default) match {
      case Some(v) if v.matches(Identifier) => v
      case Some(v) => errors += s"$key='$v': must contain only [A-Za-z0-9_]"; ""
      case None    => errors += s"$key: is required"; ""
    }

    def bool(key: String, default: Boolean): Boolean = raw(key) match {
      case None => default
      case Some(v) => v.toLowerCase(Locale.ROOT) match {
        case "true"  => true
        case "false" => false
        case _       => errors += s"$key='$v': must be true or false"; default
      }
    }

    val pathInput = raw("INPUT_PATH")
    val tableInput = (raw("DB_NAME"), raw("TABLE_NAME"))
    if (pathInput.isDefined && (tableInput._1.isDefined || tableInput._2.isDefined)) {
      errors += "INPUT_PATH and DB_NAME/TABLE_NAME: choose exactly one source (files or table)"
    }
    val input: InputSource = pathInput match {
      case Some(path) => fileInput(path, env, errors)
      case None =>
        if (raw("INPUT_FORMAT").isDefined || raw("INPUT_SCHEMA").isDefined || inputOptions(env).nonEmpty) {
          errors += "INPUT_FORMAT/INPUT_SCHEMA/INPUT_OPTION_*: only valid together with INPUT_PATH"
        }
        if (Seq("INPUT_FROM", "INPUT_TO", "INPUT_HOURS", "INPUT_DATE_KEY", "INPUT_HOUR_KEY").exists(k => raw(k).isDefined)) {
          errors += "INPUT_FROM/INPUT_TO/INPUT_HOURS/INPUT_DATE_KEY/INPUT_HOUR_KEY: only valid together with INPUT_PATH"
        }
        TableInput(identifier("DB_NAME", None), identifier("TABLE_NAME", None))
    }

    val mode = raw("PSEUDO_MODE").map(m => Try(PseudonymMode.parse(m)).toOption.getOrElse {
      errors += s"PSEUDO_MODE='$m': must be one of ${PseudonymMode.All.map(_.name).mkString(" / ")}"
      PseudonymMode.Hash
    }).getOrElse(PseudonymMode.Hash)

    val columns = pseudoColumns(raw("PSEUDO_COLUMNS"), mode, errors)

    val (outDb, outTable) = input match {
      case TableInput(db, table) =>
        val d = identifier("OUTPUT_DB", Some(db).filter(_.nonEmpty))
        val t = identifier("OUTPUT_TABLE", Some(s"${table}_pseudonymized").filter(_ => table.nonEmpty))
        if (db.nonEmpty && table.nonEmpty && d == db && t == table) {
          errors += "OUTPUT_DB.OUTPUT_TABLE: must not equal the source table — the job never overwrites the source (change OUTPUT_TABLE or OUTPUT_DB)"
        }
        (d, t)
      case _: FileInput =>
        // Nguồn file không có tên bảng để suy ra mặc định -> bắt buộc khai rõ.
        (identifier("OUTPUT_DB", None), identifier("OUTPUT_TABLE", None))
    }

    val format = raw("OUTPUT_FORMAT").map(f => OutputFormat.parse(f).getOrElse {
      errors += s"OUTPUT_FORMAT='$f': must be one of ${OutputFormat.All.map(_.name).mkString(" / ")}"
      OutputFormat.Delta
    }).getOrElse(OutputFormat.Delta)

    // Partition của bảng đích: mặc định (date, hour) khi đọc theo khoảng ngày/giờ; "none" để tắt.
    val partitionBy = raw("OUTPUT_PARTITION_BY") match {
      case Some(v) if v.equalsIgnoreCase("none") => Nil
      case Some(v) =>
        val cols = v.split(",").map(_.trim).filter(_.nonEmpty).toSeq
        cols.filterNot(_.matches("^[A-Za-z_][A-Za-z0-9_]*$")).foreach(c => errors += s"OUTPUT_PARTITION_BY: invalid column name '$c'")
        if (cols.diff(cols.distinct).nonEmpty) errors += "OUTPUT_PARTITION_BY: duplicated column"
        cols
      case None => input match {
        case FileInput(_, _, _, _, Some(r)) => Seq(r.dateKey, r.hourKey)
        case _ => Nil
      }
    }
    if (partitionBy.nonEmpty && format == OutputFormat.Iceberg) {
      errors += "OUTPUT_PARTITION_BY: not supported with OUTPUT_FORMAT=iceberg"
    }
    partitionBy.intersect(columns.map(_.name)).foreach { c =>
      errors += s"OUTPUT_PARTITION_BY: partition column '$c' must not be listed in PSEUDO_COLUMNS"
    }

    // Có partition thì mặc định chỉ thay các partition vừa xử lý (không xoá ngày/giờ khác của cùng bảng).
    val outputMode = raw("OUTPUT_MODE").map(_.toLowerCase(Locale.ROOT))
      .getOrElse(if (partitionBy.nonEmpty) "overwrite-partitions" else "overwrite")
    if (!OutputModes.contains(outputMode)) errors += s"OUTPUT_MODE='$outputMode': must be one of ${OutputModes.mkString(" / ")}"
    if (outputMode == "overwrite-partitions" && partitionBy.isEmpty) {
      errors += "OUTPUT_MODE=overwrite-partitions: requires OUTPUT_PARTITION_BY (or a date/hour range via INPUT_FROM/INPUT_TO)"
    }

    // Resume / retry controls for partition-by-partition writes.
    val skipExisting = bool("OUTPUT_SKIP_EXISTING", default = false)
    if (skipExisting && !(partitionBy.nonEmpty && (outputMode == "overwrite-partitions" || outputMode == "append"))) {
      errors += "OUTPUT_SKIP_EXISTING: requires OUTPUT_MODE=overwrite-partitions or append together with OUTPUT_PARTITION_BY"
    }
    // Retrying a partition is only safe when its write is an idempotent overwrite (append could duplicate rows).
    val writeRetries = raw("OUTPUT_WRITE_RETRIES").map { v =>
      Try(v.toInt).toOption.filter(n => n >= 0 && n <= 5).getOrElse {
        errors += s"OUTPUT_WRITE_RETRIES='$v': must be an integer in 0..5"; 0
      }
    }.getOrElse(if (outputMode == "overwrite-partitions") 2 else 0)
    if (writeRetries > 0 && outputMode != "overwrite-partitions") {
      errors += "OUTPUT_WRITE_RETRIES: retries are only safe with OUTPUT_MODE=overwrite-partitions (append could duplicate data)"
    }
    val retryWaitSeconds = raw("OUTPUT_RETRY_WAIT_SECONDS").map { v =>
      Try(v.toInt).toOption.filter(n => n >= 0 && n <= 600).getOrElse {
        errors += s"OUTPUT_RETRY_WAIT_SECONDS='$v': must be an integer in 0..600"; 30
      }
    }.getOrElse(30)

    // MAPPING_TABLE_PATH hoặc spark.pseudonym.mapping.path đều được: job kiểm tra lại sau khi có SparkConf.
    val mappingPath = raw("MAPPING_TABLE_PATH")

    // Cùng quy ước với crawler-streaming-app: URI đầy đủ để không phụ thuộc fs.defaultFS trong hadoopConfigMap.
    val warehousePath = raw("WAREHOUSE_PATH")
    warehousePath.filterNot(isAbsoluteUri).foreach { v =>
      errors += s"WAREHOUSE_PATH='$v': must be an absolute URI with a scheme, e.g. hdfs://<namenode>:8020/<path>"
    }

    val badFiles = raw("INPUT_BAD_FILES").map(v => BadFilePolicy.parse(v).getOrElse {
      errors += s"INPUT_BAD_FILES='$v': must be one of ${BadFilePolicy.All.map(_.name).mkString(" / ")}"
      BadFilePolicy.Fail
    }).getOrElse(BadFilePolicy.Fail)
    val parallelism = raw("INPUT_CHECK_PARALLELISM").map(v => Try(v.toInt).toOption.filter(n => n >= 1 && n <= 64).getOrElse {
      errors += s"INPUT_CHECK_PARALLELISM='$v': must be an integer in 1..64"
      4
    }).getOrElse(4)
    val checkFull = bool("INPUT_CHECK_FULL", default = false)
    if (!input.isInstanceOf[FileInput] &&
        (raw("INPUT_BAD_FILES").isDefined || raw("INPUT_CHECK_PARALLELISM").isDefined || raw("INPUT_CHECK_FULL").isDefined)) {
      errors += "INPUT_BAD_FILES/INPUT_CHECK_PARALLELISM/INPUT_CHECK_FULL: only valid together with INPUT_PATH"
    }

    if (errors.nonEmpty) Left(errors.toList)
    else Right(JobConfig(raw("APP_NAME").getOrElse("PseudonymizeTable"), input, columns, mode, outDb, outTable, format,
      outputMode, mappingPath, warehousePath, bool("ENABLE_HIVE_SUPPORT", default = true), bool("VERIFY", default = true),
      badFiles, parallelism, checkFull, partitionBy, skipExisting, writeRetries, retryWaitSeconds))
  }

  private val InputOptionPrefix = "INPUT_OPTION_"

  private def inputOptions(env: Map[String, String]): Map[String, String] =
    env.collect { case (k, v) if k.startsWith(InputOptionPrefix) && k.length > InputOptionPrefix.length =>
      k.substring(InputOptionPrefix.length) -> v }

  private def fileInput(path: String, env: Map[String, String], errors: ArrayBuffer[String]): FileInput = {
    if (!(path.startsWith("/") || Try(new java.net.URI(path)).toOption.exists(_.getScheme != null))) {
      errors += s"INPUT_PATH='$path': must be an absolute path or a URI with a scheme"
    }
    val format = env.get("INPUT_FORMAT").map(_.trim).filter(_.nonEmpty) match {
      case None => errors += s"INPUT_FORMAT: required when INPUT_PATH is set (${InputFormat.All.map(_.name).mkString(" / ")} / txt)"; InputFormat.Parquet
      case Some(f) => InputFormat.parse(f).getOrElse {
        errors += s"INPUT_FORMAT='$f': must be one of ${InputFormat.All.map(_.name).mkString(" / ")} (or txt = csv)"
        InputFormat.Parquet
      }
    }
    FileInput(path, format, env.get("INPUT_SCHEMA").map(_.trim).filter(_.nonEmpty), inputOptions(env),
      inputRange(env, errors))
  }

  private val DateTime = "^([0-9]{8})(?:([0-9]{2})([0-9]{2})?)?$".r

  /** `INPUT_FROM`/`INPUT_TO` = `yyyyMMdd`, `yyyyMMddHH` or `yyyyMMddHHmm` (both ends inclusive), `INPUT_HOURS` = `a-b` or `h`. */
  private def inputRange(env: Map[String, String], errors: ArrayBuffer[String]): Option[InputRange] = {
    def raw(k: String) = env.get(k).map(_.trim).filter(_.nonEmpty)
    val keys = Seq("INPUT_FROM", "INPUT_TO", "INPUT_HOURS").map(raw)
    val dateKey = raw("INPUT_DATE_KEY").getOrElse("date")
    val hourKey = raw("INPUT_HOUR_KEY").getOrElse("hour")
    Seq("INPUT_DATE_KEY" -> dateKey, "INPUT_HOUR_KEY" -> hourKey).foreach { case (k, v) =>
      if (!v.matches("^[A-Za-z_][A-Za-z0-9_]*$")) errors += s"$k='$v': invalid name"
    }
    if (keys.forall(_.isEmpty)) None
    else {
      var constrained = false
      // Missing parts default to the start of the period for FROM and the end of it for TO (hour only -> :59, none -> 23:59).
      def point(key: String, isTo: Boolean): Option[Long] = raw(key).flatMap {
        case DateTime(d, h, m) =>
          if (Try(java.time.LocalDate.parse(d, java.time.format.DateTimeFormatter.BASIC_ISO_DATE)).isFailure) {
            errors += s"$key='$d': invalid date (yyyyMMdd)"; None
          } else if (h != null && h.toInt > 23) {
            errors += s"$key: hour must be 00..23"; None
          } else if (m != null && m.toInt > 59) {
            errors += s"$key: minute must be 00..59"; None
          } else {
            if (h != null) constrained = true
            val time =
              if (h == null) (if (isTo) 2359 else 0)
              else if (m == null) h.toInt * 100 + (if (isTo) 59 else 0)
              else h.toInt * 100 + m.toInt
            Some(d.toLong * 10000 + time)
          }
        case other => errors += s"$key='$other': must be yyyyMMdd, yyyyMMddHH or yyyyMMddHHmm"; None
      }
      val from = point("INPUT_FROM", isTo = false).getOrElse(0L)
      val to = point("INPUT_TO", isTo = true).getOrElse(Long.MaxValue)
      if (from > to) errors += s"INPUT_FROM ($from) must be <= INPUT_TO ($to)"
      val hours = raw("INPUT_HOURS").flatMap { v =>
        val parts = v.split("-", -1).map(_.trim)
        val parsed = Try(parts.map(_.toInt).toSeq).toOption
        parsed match {
          case Some(Seq(h)) if h >= 0 && h <= 23 => Some((h, h))
          case Some(Seq(a, b)) if a >= 0 && b <= 23 && a <= b => Some((a, b))
          case _ => errors += s"INPUT_HOURS='$v': must be a single hour (18) or a range (6-18) within 0..23"; None
        }
      }
      if (hours.isDefined) constrained = true
      Some(InputRange(from, to, hours, dateKey, hourKey, constrained))
    }
  }

  /** `cột` hoặc `cột:MODE`; cột không ghi mode dùng `default` (PSEUDO_MODE). */
  private def pseudoColumns(rawValue: Option[String], default: PseudonymMode, errors: ArrayBuffer[String]): Seq[PseudoColumn] = {
    val tokens = rawValue.map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Nil)
    if (tokens.isEmpty) errors += "PSEUDO_COLUMNS: is required (columns to pseudonymize, comma-separated; each may carry its own mode: isdn:HASH,imei:ENCRYPT)"
    val parsed = tokens.map { token =>
      val parts = token.split(":", -1).map(_.trim)
      val name = parts(0)
      if (!name.matches("^[A-Za-z_][A-Za-z0-9_]*$")) errors += s"PSEUDO_COLUMNS: invalid column name '$name'"
      val mode =
        if (parts.length == 1) default
        else if (parts.length > 2) { errors += s"PSEUDO_COLUMNS: '$token' must be column or column:MODE"; default }
        else Try(PseudonymMode.parse(parts(1))).toOption.getOrElse {
          errors += s"PSEUDO_COLUMNS: invalid mode in '$token' (must be one of ${PseudonymMode.All.map(_.name).mkString(" / ")})"
          default
        }
      PseudoColumn(name, mode)
    }
    val duplicated = parsed.map(_.name).diff(parsed.map(_.name).distinct).distinct
    if (duplicated.nonEmpty) errors += s"PSEUDO_COLUMNS: duplicated column(s) ${duplicated.mkString(", ")}"
    parsed
  }

  private def isAbsoluteUri(value: String): Boolean =
    Try(new java.net.URI(value)).toOption.exists(u => u.getScheme != null && Option(u.getPath).exists(_.startsWith("/")))
}
