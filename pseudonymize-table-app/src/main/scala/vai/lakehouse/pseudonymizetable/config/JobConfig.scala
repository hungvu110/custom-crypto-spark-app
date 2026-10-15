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
 * Khoảng thời gian cần xử lý trong cây thư mục `<path>/<dateKey>=<yyyyMMdd>/<hourKey>=<HH>/`.
 *
 * `from`/`to` là khoá liên tục `yyyyMMddHH` (bao gồm hai đầu); `hours` (tuỳ chọn) lọc thêm giờ trong NGÀY, vd 6-18.
 * `hourConstrained` = người dùng có ràng buộc giờ (giờ trong FROM/TO hoặc INPUT_HOURS).
 */
final case class InputRange(from: Long, to: Long, hours: Option[(Int, Int)], dateKey: String, hourKey: String,
                            hourConstrained: Boolean) {

  def acceptsDate(date: String): Boolean = date.toLong >= from / 100 && date.toLong <= to / 100

  def accepts(date: String, hour: Option[Int]): Boolean = hour match {
    case Some(h) =>
      val key = date.toLong * 100 + h
      key >= from && key <= to && hours.forall { case (a, b) => h >= a && h <= b }
    case None => acceptsDate(date) && !hourConstrained
  }

  def describe: String = {
    def fmt(k: Long) = if (k == Long.MaxValue) "∞" else if (k == 0) "-∞" else f"$k%010d"
    s"${fmt(from)} → ${fmt(to)}" + hours.fold("")(h => s", giờ ${h._1}-${h._2}")
  }
}

final case class FileInput(path: String, format: InputFormat, schema: Option[String], options: Map[String, String],
                           range: Option[InputRange] = None) extends InputSource {
  override def describe: String = s"$path [${format.name}]" + range.fold("")(r => s" khoảng ${r.describe}")
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
    outputPartitionBy: Seq[String] = Nil) {

  def source: String = input.describe

  def columns: Seq[String] = pseudoColumns.map(_.name)

  /** Mọi chế độ đang dùng (để biết có cần khoá HMAC / AES / bảng ánh xạ không). */
  def modes: Set[PseudonymMode] = pseudoColumns.map(_.mode).toSet

  def target: String = s"$outputDb.$outputTable"

  def describe: Seq[(String, String)] = Seq(
    "APP_NAME"            -> appName,
    "INPUT"               -> input.describe,
    "PSEUDO_COLUMNS"      -> pseudoColumns.map(c => s"${c.name}:${c.mode.name}").mkString(", "),
    "PSEUDO_MODE (mặc định)" -> mode.name,
    "OUTPUT_DB"           -> outputDb,
    "OUTPUT_TABLE"        -> outputTable,
    "OUTPUT_FORMAT"       -> outputFormat.name,
    "OUTPUT_MODE"         -> outputMode,
    "OUTPUT_PARTITION_BY" -> (if (outputPartitionBy.isEmpty) "(không partition)" else outputPartitionBy.mkString(", ")),
    "MAPPING_TABLE_PATH"  -> mappingTablePath.getOrElse("(spark.pseudonym.mapping.path hoặc không dùng)"),
    "WAREHOUSE_PATH"      -> warehousePath.getOrElse("(mặc định của Spark/metastore)"),
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
      case Some(v) => errors += s"$key='$v': chỉ gồm [A-Za-z0-9_]"; ""
      case None    => errors += s"$key: bắt buộc"; ""
    }

    def bool(key: String, default: Boolean): Boolean = raw(key) match {
      case None => default
      case Some(v) => v.toLowerCase(Locale.ROOT) match {
        case "true"  => true
        case "false" => false
        case _       => errors += s"$key='$v': phải là true hoặc false"; default
      }
    }

    val pathInput = raw("INPUT_PATH")
    val tableInput = (raw("DB_NAME"), raw("TABLE_NAME"))
    if (pathInput.isDefined && (tableInput._1.isDefined || tableInput._2.isDefined)) {
      errors += "INPUT_PATH và DB_NAME/TABLE_NAME: chỉ chọn một nguồn (file hoặc bảng)"
    }
    val input: InputSource = pathInput match {
      case Some(path) => fileInput(path, env, errors)
      case None =>
        if (raw("INPUT_FORMAT").isDefined || raw("INPUT_SCHEMA").isDefined || inputOptions(env).nonEmpty) {
          errors += "INPUT_FORMAT/INPUT_SCHEMA/INPUT_OPTION_*: chỉ dùng cùng INPUT_PATH"
        }
        if (Seq("INPUT_FROM", "INPUT_TO", "INPUT_HOURS", "INPUT_DATE_KEY", "INPUT_HOUR_KEY").exists(k => raw(k).isDefined)) {
          errors += "INPUT_FROM/INPUT_TO/INPUT_HOURS/INPUT_DATE_KEY/INPUT_HOUR_KEY: chỉ dùng cùng INPUT_PATH"
        }
        TableInput(identifier("DB_NAME", None), identifier("TABLE_NAME", None))
    }

    val mode = raw("PSEUDO_MODE").map(m => Try(PseudonymMode.parse(m)).toOption.getOrElse {
      errors += s"PSEUDO_MODE='$m': phải là ${PseudonymMode.All.map(_.name).mkString(" / ")}"
      PseudonymMode.Hash
    }).getOrElse(PseudonymMode.Hash)

    val columns = pseudoColumns(raw("PSEUDO_COLUMNS"), mode, errors)

    val (outDb, outTable) = input match {
      case TableInput(db, table) =>
        val d = identifier("OUTPUT_DB", Some(db).filter(_.nonEmpty))
        val t = identifier("OUTPUT_TABLE", Some(s"${table}_pseudonymized").filter(_ => table.nonEmpty))
        if (db.nonEmpty && table.nonEmpty && d == db && t == table) {
          errors += "OUTPUT_DB.OUTPUT_TABLE: không được trùng bảng nguồn — job không ghi đè bảng gốc (đổi OUTPUT_TABLE hoặc OUTPUT_DB)"
        }
        (d, t)
      case _: FileInput =>
        // Nguồn file không có tên bảng để suy ra mặc định -> bắt buộc khai rõ.
        (identifier("OUTPUT_DB", None), identifier("OUTPUT_TABLE", None))
    }

    val format = raw("OUTPUT_FORMAT").map(f => OutputFormat.parse(f).getOrElse {
      errors += s"OUTPUT_FORMAT='$f': phải là ${OutputFormat.All.map(_.name).mkString(" / ")}"
      OutputFormat.Delta
    }).getOrElse(OutputFormat.Delta)

    // Partition của bảng đích: mặc định (date, hour) khi đọc theo khoảng ngày/giờ; "none" để tắt.
    val partitionBy = raw("OUTPUT_PARTITION_BY") match {
      case Some(v) if v.equalsIgnoreCase("none") => Nil
      case Some(v) =>
        val cols = v.split(",").map(_.trim).filter(_.nonEmpty).toSeq
        cols.filterNot(_.matches("^[A-Za-z_][A-Za-z0-9_]*$")).foreach(c => errors += s"OUTPUT_PARTITION_BY: tên cột không hợp lệ '$c'")
        if (cols.diff(cols.distinct).nonEmpty) errors += "OUTPUT_PARTITION_BY: cột bị lặp"
        cols
      case None => input match {
        case FileInput(_, _, _, _, Some(r)) => Seq(r.dateKey, r.hourKey)
        case _ => Nil
      }
    }
    if (partitionBy.nonEmpty && format == OutputFormat.Iceberg) {
      errors += "OUTPUT_PARTITION_BY: chưa hỗ trợ với OUTPUT_FORMAT=iceberg"
    }
    partitionBy.intersect(columns.map(_.name)).foreach { c =>
      errors += s"OUTPUT_PARTITION_BY: cột partition '$c' không được nằm trong PSEUDO_COLUMNS"
    }

    // Có partition thì mặc định chỉ thay các partition vừa xử lý (không xoá ngày/giờ khác của cùng bảng).
    val outputMode = raw("OUTPUT_MODE").map(_.toLowerCase(Locale.ROOT))
      .getOrElse(if (partitionBy.nonEmpty) "overwrite-partitions" else "overwrite")
    if (!OutputModes.contains(outputMode)) errors += s"OUTPUT_MODE='$outputMode': phải là ${OutputModes.mkString(" / ")}"
    if (outputMode == "overwrite-partitions" && partitionBy.isEmpty) {
      errors += "OUTPUT_MODE=overwrite-partitions: cần OUTPUT_PARTITION_BY (hoặc đọc theo khoảng INPUT_FROM/INPUT_TO)"
    }

    // MAPPING_TABLE_PATH hoặc spark.pseudonym.mapping.path đều được: job kiểm tra lại sau khi có SparkConf.
    val mappingPath = raw("MAPPING_TABLE_PATH")

    // Cùng quy ước với crawler-streaming-app: URI đầy đủ để không phụ thuộc fs.defaultFS trong hadoopConfigMap.
    val warehousePath = raw("WAREHOUSE_PATH")
    warehousePath.filterNot(isAbsoluteUri).foreach { v =>
      errors += s"WAREHOUSE_PATH='$v': phải là URI tuyệt đối có scheme, vd hdfs://<namenode>:8020/<path>"
    }

    val badFiles = raw("INPUT_BAD_FILES").map(v => BadFilePolicy.parse(v).getOrElse {
      errors += s"INPUT_BAD_FILES='$v': phải là ${BadFilePolicy.All.map(_.name).mkString(" / ")}"
      BadFilePolicy.Fail
    }).getOrElse(BadFilePolicy.Fail)
    val parallelism = raw("INPUT_CHECK_PARALLELISM").map(v => Try(v.toInt).toOption.filter(n => n >= 1 && n <= 64).getOrElse {
      errors += s"INPUT_CHECK_PARALLELISM='$v': phải là số nguyên 1..64"
      4
    }).getOrElse(4)
    val checkFull = bool("INPUT_CHECK_FULL", default = false)
    if (!input.isInstanceOf[FileInput] &&
        (raw("INPUT_BAD_FILES").isDefined || raw("INPUT_CHECK_PARALLELISM").isDefined || raw("INPUT_CHECK_FULL").isDefined)) {
      errors += "INPUT_BAD_FILES/INPUT_CHECK_PARALLELISM/INPUT_CHECK_FULL: chỉ dùng cùng INPUT_PATH"
    }

    if (errors.nonEmpty) Left(errors.toList)
    else Right(JobConfig(raw("APP_NAME").getOrElse("PseudonymizeTable"), input, columns, mode, outDb, outTable, format,
      outputMode, mappingPath, warehousePath, bool("ENABLE_HIVE_SUPPORT", default = true), bool("VERIFY", default = true),
      badFiles, parallelism, checkFull, partitionBy))
  }

  private val InputOptionPrefix = "INPUT_OPTION_"

  private def inputOptions(env: Map[String, String]): Map[String, String] =
    env.collect { case (k, v) if k.startsWith(InputOptionPrefix) && k.length > InputOptionPrefix.length =>
      k.substring(InputOptionPrefix.length) -> v }

  private def fileInput(path: String, env: Map[String, String], errors: ArrayBuffer[String]): FileInput = {
    if (!(path.startsWith("/") || Try(new java.net.URI(path)).toOption.exists(_.getScheme != null))) {
      errors += s"INPUT_PATH='$path': phải là đường dẫn tuyệt đối hoặc URI có scheme"
    }
    val format = env.get("INPUT_FORMAT").map(_.trim).filter(_.nonEmpty) match {
      case None => errors += s"INPUT_FORMAT: bắt buộc khi có INPUT_PATH (${InputFormat.All.map(_.name).mkString(" / ")} / txt)"; InputFormat.Parquet
      case Some(f) => InputFormat.parse(f).getOrElse {
        errors += s"INPUT_FORMAT='$f': phải là ${InputFormat.All.map(_.name).mkString(" / ")} (hoặc txt = csv)"
        InputFormat.Parquet
      }
    }
    FileInput(path, format, env.get("INPUT_SCHEMA").map(_.trim).filter(_.nonEmpty), inputOptions(env),
      inputRange(env, errors))
  }

  private val DateHour = "^([0-9]{8})([0-9]{2})?$".r

  /** `INPUT_FROM`/`INPUT_TO` = `yyyyMMdd` hoặc `yyyyMMddHH` (bao gồm hai đầu), `INPUT_HOURS` = `a-b` hoặc `h`. */
  private def inputRange(env: Map[String, String], errors: ArrayBuffer[String]): Option[InputRange] = {
    def raw(k: String) = env.get(k).map(_.trim).filter(_.nonEmpty)
    val keys = Seq("INPUT_FROM", "INPUT_TO", "INPUT_HOURS").map(raw)
    val dateKey = raw("INPUT_DATE_KEY").getOrElse("date")
    val hourKey = raw("INPUT_HOUR_KEY").getOrElse("hour")
    Seq("INPUT_DATE_KEY" -> dateKey, "INPUT_HOUR_KEY" -> hourKey).foreach { case (k, v) =>
      if (!v.matches("^[A-Za-z_][A-Za-z0-9_]*$")) errors += s"$k='$v': tên không hợp lệ"
    }
    if (keys.forall(_.isEmpty)) None
    else {
      var constrained = false
      def point(key: String, defaultHour: Int): Option[Long] = raw(key).flatMap {
        case DateHour(d, h) =>
          if (Try(java.time.LocalDate.parse(d, java.time.format.DateTimeFormatter.BASIC_ISO_DATE)).isFailure) {
            errors += s"$key='$d': ngày không hợp lệ (yyyyMMdd)"; None
          } else if (h != null && h.toInt > 23) {
            errors += s"$key: giờ phải 00..23"; None
          } else {
            if (h != null) constrained = true
            Some(d.toLong * 100 + (if (h == null) defaultHour else h.toInt))
          }
        case other => errors += s"$key='$other': phải là yyyyMMdd hoặc yyyyMMddHH"; None
      }
      val from = point("INPUT_FROM", 0).getOrElse(0L)
      val to = point("INPUT_TO", 23).getOrElse(Long.MaxValue)
      if (from > to) errors += s"INPUT_FROM ($from) phải ≤ INPUT_TO ($to)"
      val hours = raw("INPUT_HOURS").flatMap { v =>
        val parts = v.split("-", -1).map(_.trim)
        val parsed = Try(parts.map(_.toInt).toSeq).toOption
        parsed match {
          case Some(Seq(h)) if h >= 0 && h <= 23 => Some((h, h))
          case Some(Seq(a, b)) if a >= 0 && b <= 23 && a <= b => Some((a, b))
          case _ => errors += s"INPUT_HOURS='$v': phải là một giờ (18) hoặc khoảng (6-18) trong 0..23"; None
        }
      }
      if (hours.isDefined) constrained = true
      Some(InputRange(from, to, hours, dateKey, hourKey, constrained))
    }
  }

  /** `cột` hoặc `cột:MODE`; cột không ghi mode dùng `default` (PSEUDO_MODE). */
  private def pseudoColumns(rawValue: Option[String], default: PseudonymMode, errors: ArrayBuffer[String]): Seq[PseudoColumn] = {
    val tokens = rawValue.map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Nil)
    if (tokens.isEmpty) errors += "PSEUDO_COLUMNS: bắt buộc (cột cần khử định danh, phân cách bằng dấu phẩy; mỗi cột có thể kèm chế độ riêng: isdn:HASH,imei:ENCRYPT)"
    val parsed = tokens.map { token =>
      val parts = token.split(":", -1).map(_.trim)
      val name = parts(0)
      if (!name.matches("^[A-Za-z_][A-Za-z0-9_]*$")) errors += s"PSEUDO_COLUMNS: tên cột không hợp lệ '$name'"
      val mode =
        if (parts.length == 1) default
        else if (parts.length > 2) { errors += s"PSEUDO_COLUMNS: '$token' phải có dạng cột hoặc cột:MODE"; default }
        else Try(PseudonymMode.parse(parts(1))).toOption.getOrElse {
          errors += s"PSEUDO_COLUMNS: chế độ không hợp lệ ở '$token' (phải là ${PseudonymMode.All.map(_.name).mkString(" / ")})"
          default
        }
      PseudoColumn(name, mode)
    }
    val duplicated = parsed.map(_.name).diff(parsed.map(_.name).distinct).distinct
    if (duplicated.nonEmpty) errors += s"PSEUDO_COLUMNS: cột bị lặp ${duplicated.mkString(", ")}"
    parsed
  }

  private def isAbsoluteUri(value: String): Boolean =
    Try(new java.net.URI(value)).toOption.exists(u => u.getScheme != null && Option(u.getPath).exists(_.startsWith("/")))
}
