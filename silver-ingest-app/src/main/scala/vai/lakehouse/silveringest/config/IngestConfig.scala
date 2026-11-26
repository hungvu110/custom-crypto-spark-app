package vai.lakehouse.silveringest.config

import java.util.Locale
import java.util.regex.{Pattern, PatternSyntaxException}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import vai.lakehouse.keyprefix.{DakGlobalKeySource, PathKeySource}

/** Ingest mode: one entry point per mode, DAGs pick the mode that fits their dataset. */
sealed abstract class Mode(val name: String)

object Mode {
  /** Files are read and written as they are (no DAK call): already column-encrypted by the provider, or nothing to encrypt. */
  case object Passthrough extends Mode("passthrough")

  /**
   * DAK `by-path` key -> whole-file decryption (no key = file is not encrypted, read as it is) -> cdr-crypto column encryption
   * (skipped when no column is listed) -> write the table.
   */
  case object DecryptEncrypt extends Mode("decrypt-encrypt")
}

/**
 * Định dạng file nguồn (sau giải mã với VTNET). `text` = mỗi dòng một cột `value`; `csv`/`txt` = text phân tách, mặc định `,`;
 * `tsv` = như csv, phân tách bằng tab. `isText` = đọc được từ các dòng trong bộ nhớ (VTNET giải mã không cần ghi xuống đĩa).
 */
sealed abstract class InputFormat(val name: String, val sparkFormat: String, val isText: Boolean,
                                  val defaults: Map[String, String] = Map.empty)

object InputFormat {
  case object Csv     extends InputFormat("csv", "csv", isText = true)
  case object Tsv     extends InputFormat("tsv", "csv", isText = true, Map("delimiter" -> "\t"))
  case object Text    extends InputFormat("text", "text", isText = true)
  case object Json    extends InputFormat("json", "json", isText = true)
  case object Parquet extends InputFormat("parquet", "parquet", isText = false)
  case object Orc     extends InputFormat("orc", "orc", isText = false)
  case object Avro    extends InputFormat("avro", "avro", isText = false)

  val All: Seq[InputFormat] = Seq(Csv, Tsv, Text, Json, Parquet, Orc, Avro)

  def parse(raw: String): Option[InputFormat] = raw.trim.toLowerCase(Locale.ROOT) match {
    case "txt" => Some(Csv)
    case other => All.find(_.name == other)
  }
}

/** Định dạng bảng đích. `hive` ghi Parquet (cùng quy ước với pseudonymize-table-app). */
sealed abstract class OutputFormat(val name: String, val sparkFormat: String)

object OutputFormat {
  case object Delta extends OutputFormat("delta", "delta")
  case object Hive  extends OutputFormat("hive", "parquet")

  val All: Seq[OutputFormat] = Seq(Delta, Hive)

  def parse(raw: String): Option[OutputFormat] = All.find(_.name == raw.trim.toLowerCase(Locale.ROOT))
}

/** File cần nạp: `path` là một file hoặc một thư mục (đọc mọi file trong cây, bỏ file ẩn `_*`, `.*`). */
final case class InputSpec(path: String, format: InputFormat, schema: Option[String], options: Map[String, String]) {
  /** Option đọc thực sự: mặc định của định dạng, rồi `INPUT_OPTION_*` đè lên. */
  def readOptions: Map[String, String] = format.defaults ++ options
}

/**
 * Lô đang nạp: job thêm cột `column` = `value` (chuỗi) vào mọi dòng, làm partition đầu tiên của bảng đích. Với
 * `OUTPUT_MODE=overwrite-partitions` mỗi lần chạy chỉ thay đúng partition của lô này: chạy lại cùng lô không ghi trùng, các lô khác
 * giữ nguyên.
 */
final case class Batch(column: String, value: String)

/**
 * Cách chia file khi ghi. Gom (`REBALANCE` + AQE): trước khi ghi, gom dữ liệu theo các cột partition để mỗi file ghi ra có khoảng
 * `shuffleBytesPerFile` byte DỮ LIỆU SHUFFLE (AQE tách partition lớn, gộp partition nhỏ); tốn một shuffle toàn bộ dữ liệu. Không gom thì mỗi
 * task ghi một file cho mỗi partition có trong dữ liệu của nó: với VTT (mọi file landing có đủ 24 giờ) là số task x 24 file nhỏ mỗi ngày nếu
 * bảng partition theo giờ, nhưng chỉ MỘT file mỗi task (khoảng 128 MB đầu vào) nếu bảng chỉ partition theo lô.
 *
 * Dữ liệu shuffle (dòng nhị phân, nén lz4) lớn hơn parquet đã nén vài lần: để file parquet khoảng 128 MB thì đặt
 * `OUTPUT_FILE_SHUFFLE_MB` = 128 x (Shuffle Write trên Spark UI / dung lượng parquet đã ghi), đo trên một ngày.
 */
final case class OutputFiles(rebalance: Rebalance, shuffleBytesPerFile: Long)

/** `OUTPUT_REBALANCE`: `auto` (mặc định) gom khi bảng có cột partition khác cột lô, `true` luôn gom, `false` không bao giờ gom. */
sealed abstract class Rebalance(val name: String)

object Rebalance {
  case object Auto extends Rebalance("auto")
  case object Always extends Rebalance("true")
  case object Never extends Rebalance("false")
  val All: Seq[Rebalance] = Seq(Auto, Always, Never)
  def parse(value: String): Option[Rebalance] = All.find(_.name.equalsIgnoreCase(value.trim))
}

/**
 * `DEDUP`: `off` (mặc định) hoặc `check` = ĐO dòng trùng và file trùng của lần nạp rồi ghi vào log, KHÔNG xoá gì (`DuplicateCheck`). Tốn một
 * lần đọc nguồn nữa (để băm mọi cột) và một shuffle nhỏ (16 byte mỗi dòng): chỉ bật trong những ngày cần đo.
 */
sealed abstract class Dedup(val name: String, val describe: String)

object Dedup {
  case object Off extends Dedup("off", "off")
  case object Check extends Dedup("check", "check (measure duplicate rows and files, nothing is removed)")
  val All: Seq[Dedup] = Seq(Off, Check)
  def parse(value: String): Option[Dedup] = All.find(_.name.equalsIgnoreCase(value.trim))
}

object OutputFiles {
  val DefaultShuffleMb = 384 // 128 MB parquet x hệ số giả định 3, cho tới khi đo
  val MinShuffleMb = 16
  val MaxShuffleMb = 4096
  val Default: OutputFiles = OutputFiles(Rebalance.Auto, shuffleBytesPerFile = DefaultShuffleMb.toLong * 1024 * 1024)
}

/**
 * Giờ của từng dòng: job thêm cột `column` = giờ `HH`, suy ra từ ĐƯỜNG DẪN file nguồn bằng `pathRegex` (đúng một nhóm bắt, không khớp
 * thì `NA`), hoặc từ một CỘT của chính dữ liệu (`sourceColumn`, `pathRegex` để rỗng): giá trị `0`..`23` được đệm thành `HH`, giá trị khác
 * hoặc rỗng thành `NA`. Đặt cột này vào `OUTPUT_PARTITION_BY` nếu muốn bảng partition theo giờ; không thì nó là cột thường. `replace` (rỗng =
 * cả ngày): chỉ các giờ này của lô bị thay khi `overwrite-partitions`, nên chạy lại vài giờ không đụng các giờ khác của ngày và không ghi trùng;
 * chỉ dùng với giờ từ đường dẫn (file của một ngày chứa mọi giờ nên giờ từ cột luôn thay cả ngày).
 */
final case class Hours(column: String, pathRegex: String, replace: Seq[String] = Nil, sourceColumn: Option[String] = None)

/**
 * Phần riêng của chế độ decrypt-encrypt.
 *
 * @param keyPath        đường dẫn gửi cho DAK `by-path` (một key cho cả file/thư mục): TƯƠNG ĐỐI theo gốc `KEY_PATH_ROOT` của nguồn, ở
 *                       dạng chuẩn `PathKeySource.canonical`; `KEY_PATH` đặt thẳng thì dùng nó (vd tên dataset: một key cho cả dataset)
 * @param cipher         tên thuật toán giải mã cả file ([[vai.lakehouse.silveringest.crypto.FileCipher]]), dùng khi `by-path` có key
 * @param encryptColumns cột mã hoá bằng cdr-crypto trước khi ghi; RỖNG = chỉ giải mã file, không mã hoá cột nào (không gọi `by-column`)
 * @param keyField       cột giữ plaintext, giá trị của từng dòng ghép sau key cột (`cdr_encrypt` tham số 4); chỉ cần khi có cột mã hoá
 * @param stagingPath    thư mục tạm cho file giải mã định dạng nhị phân; xoá sau khi chạy
 * @param onMissing      `onMissing` gửi `by-column`: `request` (mặc định: lần đầu cột chưa có key thì DAK mở YC, admin duyệt và sinh key,
 *                       chạy lại là dùng được — như SA đã góp ý DAK) hoặc `none` (không mở YC)
 * @param globalZone     vùng của key global (docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md): có thì MỌI cột trong `encryptColumns` được mã hoá
 *                       bằng key global của vùng này (DAK `/api/v1/keys/global`), KHÔNG gọi `by-column`; `None` = key theo cột như trước.
 *                       Việc giải mã file (`by-path`) không đổi
 */
final case class DecryptEncryptSettings(keyPath: String, cipher: String, encryptColumns: Seq[String], keyField: Option[String],
                                        stagingPath: Option[String], onMissing: String = IngestConfig.DefaultOnMissing,
                                        globalZone: Option[String] = None, decryptMode: DecryptMode = DecryptMode.File,
                                        providerDecrypt: Option[ProviderDecrypt] = None, keyFieldValue: Option[String] = None,
                                        transforms: Seq[ColumnTransform] = Nil)

/**
 * Cách giải mã dữ liệu của đối tác (`DECRYPT_MODE`):
 *  - `file` (mặc định): `by-path` có key thì giải mã CẢ FILE (VTNET), không có key thì đọc nguyên trạng;
 *  - `column`: file đọc nguyên trạng, các cột đối tác đã mã hoá ([[ProviderDecrypt]]) được giải mã bằng cdr-crypto, prefix là key `by-path`
 *    (VTT mã hoá theo từng cột). `by-path` chưa có key là lỗi: không được mã hoá chồng lên bản mã của đối tác;
 *  - `none`: không gọi `by-path`, không giải mã gì (cột đối tác không mã hoá, hoặc bảng không có cột nhạy cảm).
 */
sealed abstract class DecryptMode(val name: String)

object DecryptMode {
  case object File extends DecryptMode("file")
  case object Column extends DecryptMode("column")
  case object NoDecrypt extends DecryptMode("none")

  val All: Seq[DecryptMode] = Seq(File, Column, NoDecrypt)

  def parse(raw: String): Option[DecryptMode] = All.find(_.name == raw.trim.toLowerCase(Locale.ROOT))
}

/**
 * Cột đối tác đã mã hoá theo từng cột và cột `keyField` của đối tác (giữ plaintext, giá trị của từng dòng ghép sau prefix thành `keyInput`).
 * Chỉ dùng với [[DecryptMode.Column]]; `keyField` không được nằm trong `columns`.
 */
final case class ProviderDecrypt(columns: Seq[String], keyField: String)

/** Cách gom nhóm một cột (`COLUMN_TRANSFORMS`, dạng `cột:KIND`): giá trị gốc được thay bằng nhãn khung, kiểu cột thành chuỗi. */
sealed abstract class TransformKind(val name: String)

object TransformKind {
  /** Tuổi: `Under 18`, `18–24`, `25–34`, `35–44`, `45–54`, `55–64`, `65+`, ngoài ra `Unknown`. */
  case object AgeBand extends TransformKind("AGE_BAND")

  /** Giờ 0..23: `0–5h`, `6–8h`, `9–11h`, `12–13h`, `14–16h`, `17–19h`, `20–23h`, ngoài ra `Unknown`. */
  case object HourBand extends TransformKind("HOUR_BAND")

  val All: Seq[TransformKind] = Seq(AgeBand, HourBand)

  def parse(raw: String): Option[TransformKind] = All.find(_.name == raw.trim.toUpperCase(Locale.ROOT))
}

final case class ColumnTransform(column: String, kind: TransformKind)

/**
 * Cấu hình của job, đọc từ biến môi trường của driver (`spec.driver.env`). Chung cho hai nguồn: `INPUT_*`, `OUTPUT_*`,
 * `WAREHOUSE_PATH`, `ENABLE_HIVE_SUPPORT`, `BATCH_*`; riêng VTNET: `KEY_PATH_ROOT`, `KEY_PATH`, `FILE_CIPHER`, `CDR_ENCRYPT_COLUMNS`, `CDR_KEY_FIELD`, `CDR_ON_MISSING`,
 * `DECRYPT_STAGING_PATH`. Nguồn key (`CRYPTO_PREFIX_SOURCE`, `DAK_*`) do key-prefix-lib đọc riêng.
 */
final case class IngestConfig(
    mode: Mode,
    appName: String,
    input: InputSpec,
    outputDb: String,
    outputTable: String,
    outputFormat: OutputFormat,
    outputMode: String,
    partitionBy: Seq[String],
    warehousePath: Option[String],
    enableHiveSupport: Boolean,
    decryptEncrypt: Option[DecryptEncryptSettings],
    batch: Option[Batch] = None,
    hours: Option[Hours] = None,
    outputFiles: OutputFiles = OutputFiles.Default,
    dedup: Dedup = Dedup.Off) {

  def target: String = s"$outputDb.$outputTable"

  /**
   * Có gom dữ liệu theo partition trước khi ghi không. `auto`: chỉ khi bảng có cột partition KHÁC cột lô (vd `hour`). Một lần chạy ghi đúng
   * một lô, nên bảng chỉ partition theo lô thì mỗi task đọc đã chỉ có một partition: không gom, mỗi task ghi một file cỡ phần nó đọc, khỏi
   * tốn một shuffle toàn bộ dữ liệu (f015: 145 GiB mỗi ngày).
   */
  def rebalances: Boolean = outputFiles.rebalance match {
    case Rebalance.Always => true
    case Rebalance.Never => false
    case Rebalance.Auto => partitionBy.exists(c => !batch.exists(_.column.equalsIgnoreCase(c)))
  }

  private def describeOutputFiles: String = {
    val mode = s"(OUTPUT_REBALANCE=${outputFiles.rebalance.name})"
    if (rebalances) {
      s"rebalance by ${if (partitionBy.isEmpty) "(no partition column)" else partitionBy.mkString(", ")}, " +
        s"about ${outputFiles.shuffleBytesPerFile / (1024 * 1024)} MB of shuffle data per file $mode"
    } else if (outputFiles.rebalance == Rebalance.Never) s"one file per task and partition $mode"
    else s"one file per read task: the run writes one partition ${batch.fold("")(_.column)} $mode".replace("  ", " ")
  }

  def describe: Seq[(String, String)] = Seq(
    "MODE"                -> mode.name,
    "APP_NAME"            -> appName,
    "INPUT_PATH"          -> input.path,
    "INPUT_FORMAT"        -> input.format.name,
    "INPUT_SCHEMA"        -> input.schema.getOrElse("(none)"),
    "INPUT_OPTION_*"      -> (if (input.options.isEmpty) "(none)" else input.options.keys.toSeq.sorted.mkString(", ")),
    "OUTPUT"              -> s"$target [${outputFormat.name}, $outputMode]",
    "BATCH"               -> batch.fold("(none)")(b => s"${b.column} = ${b.value}"),
    "HOURS"               -> hours.fold("(none)") { h =>
      h.sourceColumn.fold(s"${h.column} from ${h.pathRegex}")(c => s"${h.column} from column $c") +
        (if (h.replace.isEmpty) "" else s"; replace ${h.replace.mkString(",")}")
    },
    "OUTPUT_PARTITION_BY" -> (if (partitionBy.isEmpty) "(none)" else partitionBy.mkString(", ")),
    "OUTPUT_FILES"        -> describeOutputFiles,
    "DEDUP"               -> dedup.describe,
    "WAREHOUSE_PATH"      -> warehousePath.getOrElse("(Spark/metastore default)"),
    "ENABLE_HIVE_SUPPORT" -> enableHiveSupport.toString) ++ decryptEncrypt.toSeq.flatMap { v => Seq(
    "DECRYPT_MODE"             -> v.decryptMode.name,
    "KEY_PATH"                 -> (if (v.keyPath.isEmpty) "(not used)" else v.keyPath),
    "FILE_CIPHER"              -> v.cipher,
    "PROVIDER_DECRYPT_COLUMNS" -> v.providerDecrypt.fold("(none)")(_.columns.mkString(", ")),
    "PROVIDER_KEY_FIELD"       -> v.providerDecrypt.fold("(none)")(_.keyField),
    "CDR_ENCRYPT_COLUMNS"      -> (if (v.encryptColumns.isEmpty) "(none)" else v.encryptColumns.mkString(", ")),
    "CDR_KEY_FIELD"            -> v.keyField.getOrElse("(none)"),
    "CDR_KEY_FIELD_VALUE"      -> v.keyFieldValue.getOrElse("(none: the column is read from the data)"),
    "CDR_ON_MISSING"           -> v.onMissing,
    "GLOBAL_KEY_ZONE"          -> v.globalZone.getOrElse("(none: per-column keys)"),
    "COLUMN_TRANSFORMS"        -> (if (v.transforms.isEmpty) "(none)" else v.transforms.map(t => s"${t.column}:${t.kind.name}").mkString(", ")),
    "DECRYPT_STAGING_PATH"     -> v.stagingPath.getOrElse("(in memory)"))
  }
}

object IngestConfig {

  /**
   * `overwrite-partitions` (mặc định khi có `BATCH_VALUE`): chỉ thay partition của lô đang chạy. `overwrite` (mặc định khi không có lô):
   * thay CẢ bảng. `append`: ghi nối, chạy lại cùng lô là ghi trùng.
   */
  val OverwritePartitions = "overwrite-partitions"
  val OutputModes = Seq(OverwritePartitions, "overwrite", "append")

  val DefaultBatchColumn = "batch_date"
  private val BatchValue = "^[A-Za-z0-9_.:-]{1,64}$"
  val DefaultHourColumn = "hour"
  private val HourValue = "^([01]\\d|2[0-3])$"

  /** Thuật toán giải mã file được hỗ trợ; phải khớp `FileCipher.forName`. Chỉ dùng khi `by-path` có key cho đường dẫn. */
  val FileCiphers = Seq("aes-256-gcm")

  val DefaultCipher = "aes-256-gcm"

  /** `onMissing` của `by-column` (docs/key-access/TRA-LOI-DAK-BY-PATH.md mục 4): job MÃ HOÁ cột nên mặc định mở YC khi chưa có key. */
  val OnMissingValues = Seq("request", "none")
  val DefaultOnMissing = "request"

  private val Identifier = "^[A-Za-z0-9_]{1,128}$"
  private val ColumnName = "^[A-Za-z_][A-Za-z0-9_]{0,127}$"
  private val DecryptEncryptOnly = Seq("KEY_PATH_ROOT", "KEY_PATH", "FILE_CIPHER", "CDR_ENCRYPT_COLUMNS", "CDR_KEY_FIELD", "CDR_ON_MISSING",
    "GLOBAL_KEY_ZONE", "DECRYPT_STAGING_PATH", "DECRYPT_MODE", "PROVIDER_DECRYPT_COLUMNS", "PROVIDER_KEY_FIELD", "CDR_KEY_FIELD_VALUE",
    "COLUMN_TRANSFORMS")
  /** Giá trị của cột hằng số làm `keyField` mã hoá: nằm trong dữ liệu, nên chỉ gồm ký tự an toàn. */
  private val KeyFieldValue = "^[A-Za-z0-9_.:-]{1,64}$"
  private val OptionPrefix = "INPUT_OPTION_"

  /** Gom MỌI lỗi cấu hình rồi trả về một lượt, để sửa manifest/DAG một lần là đủ. */
  def fromEnv(mode: Mode, env: Map[String, String]): Either[Seq[String], IngestConfig] = {
    val errors = ArrayBuffer.empty[String]

    def raw(key: String): Option[String] = env.get(key).map(_.trim).filter(_.nonEmpty)

    def identifier(key: String): String = raw(key) match {
      case Some(v) if v.matches(Identifier) => v
      case Some(_) => errors += s"$key: must contain only [A-Za-z0-9_], up to 128 characters"; ""
      case None    => errors += s"$key: is required"; ""
    }

    def bool(key: String, default: Boolean): Boolean = raw(key).map(_.toLowerCase(Locale.ROOT)) match {
      case None          => default
      case Some("true")  => true
      case Some("false") => false
      case Some(v)       => errors += s"$key='$v': must be true or false"; default
    }

    def columns(key: String, value: String): Seq[String] = {
      val cols = value.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      cols.filterNot(_.matches(ColumnName)).foreach(c => errors += s"$key: invalid column name '$c'")
      val lower = cols.map(_.toLowerCase(Locale.ROOT))
      if (lower.distinct.size != lower.size) errors += s"$key: duplicated column (column names are case-insensitive)"
      cols
    }

    val path = raw("INPUT_PATH") match {
      case Some(p) if isAbsolute(p) => p
      case Some(_) => errors += "INPUT_PATH: must be an absolute path or a URI with a scheme"; ""
      case None    => errors += "INPUT_PATH: is required (a file or a directory)"; ""
    }
    val format = raw("INPUT_FORMAT") match {
      case None    => errors += s"INPUT_FORMAT: is required (${InputFormat.All.map(_.name).mkString(" / ")} / txt)"; InputFormat.Csv
      case Some(f) => InputFormat.parse(f).getOrElse {
        errors += s"INPUT_FORMAT='$f': must be one of ${InputFormat.All.map(_.name).mkString(" / ")} (or txt = csv)"
        InputFormat.Csv
      }
    }
    val options = env.collect { case (k, v) if k.startsWith(OptionPrefix) && k.length > OptionPrefix.length =>
      k.substring(OptionPrefix.length) -> v }
    val input = InputSpec(path, format, raw("INPUT_SCHEMA"), options)

    val outDb = identifier("OUTPUT_DB")
    val outTable = identifier("OUTPUT_TABLE")
    val outputFormat = raw("OUTPUT_FORMAT").map(f => OutputFormat.parse(f).getOrElse {
      errors += s"OUTPUT_FORMAT='$f': must be one of ${OutputFormat.All.map(_.name).mkString(" / ")}"
      OutputFormat.Delta
    }).getOrElse(OutputFormat.Delta)
    val batch = raw("BATCH_VALUE") match {
      case Some(v) =>
        if (!v.matches(BatchValue)) errors += "BATCH_VALUE: must be 1 to 64 characters of [A-Za-z0-9_.:-]"
        val column = raw("BATCH_COLUMN").getOrElse(DefaultBatchColumn)
        if (!column.matches(ColumnName)) errors += s"BATCH_COLUMN: invalid column name '$column'"
        Some(Batch(column, v))
      case None =>
        if (raw("BATCH_COLUMN").isDefined) errors += "BATCH_COLUMN: only used together with BATCH_VALUE"
        None
    }
    def hourColumn(): String = {
      val column = raw("HOUR_COLUMN").getOrElse(DefaultHourColumn)
      if (!column.matches(ColumnName)) errors += s"HOUR_COLUMN: invalid column name '$column'"
      if (batch.exists(_.column.equalsIgnoreCase(column))) errors += "HOUR_COLUMN: must differ from BATCH_COLUMN"
      column
    }
    val hourRegex = raw("HOUR_PATH_REGEX")
    val hourSource = raw("HOUR_FROM_COLUMN")
    if (hourRegex.isDefined && hourSource.isDefined) {
      errors += "HOUR_PATH_REGEX/HOUR_FROM_COLUMN: only one of them can be set (the hour comes from the path or from a column)"
    }
    val hours = (hourRegex, hourSource) match {
      case (Some(pattern), _) =>
        try {
          if (Pattern.compile(pattern).matcher("").groupCount != 1) errors += "HOUR_PATH_REGEX: must have exactly one capture group (the hour)"
        } catch {
          case _: PatternSyntaxException => errors += "HOUR_PATH_REGEX: not a valid regular expression"
        }
        val column = hourColumn()
        val replace = raw("BATCH_HOURS").map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Nil)
        replace.filterNot(_.matches(HourValue)).foreach(h => errors += s"BATCH_HOURS: invalid hour '$h' (two digits, 00 to 23)")
        if (replace.distinct.size != replace.size) errors += "BATCH_HOURS: duplicated hour"
        if (replace.nonEmpty && batch.isEmpty) errors += "BATCH_HOURS: only used together with BATCH_VALUE"
        Some(Hours(column, pattern, replace))
      case (None, Some(source)) =>
        if (!source.matches(ColumnName)) errors += s"HOUR_FROM_COLUMN: invalid column name '$source'"
        val column = hourColumn()
        if (raw("BATCH_HOURS").isDefined) {
          errors += "BATCH_HOURS: not used with HOUR_FROM_COLUMN (a file holds every hour of its day: the whole day is replaced)"
        }
        Some(Hours(column, "", Nil, Some(source)))
      case (None, None) =>
        val set = Seq("HOUR_COLUMN", "BATCH_HOURS").filter(k => raw(k).isDefined)
        if (set.nonEmpty) errors += s"${set.mkString("/")}: only used together with HOUR_PATH_REGEX or HOUR_FROM_COLUMN"
        None
    }
    val outputMode = raw("OUTPUT_MODE").map(_.toLowerCase(Locale.ROOT))
      .getOrElse(if (batch.isDefined) OverwritePartitions else "overwrite")
    if (!OutputModes.contains(outputMode)) errors += s"OUTPUT_MODE='$outputMode': must be one of ${OutputModes.mkString(" / ")}"
    if (outputMode == OverwritePartitions && batch.isEmpty) {
      errors += s"OUTPUT_MODE=$OverwritePartitions: requires BATCH_VALUE (the batch whose partition is replaced)"
    }
    // Cột lô luôn là partition đầu tiên; OUTPUT_PARTITION_BY (nếu có) đứng sau.
    val partitionBy = {
      val all = batch.map(_.column).toSeq ++ raw("OUTPUT_PARTITION_BY").map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
        .getOrElse(Nil)
      if (all.isEmpty) Nil else columns("OUTPUT_PARTITION_BY", all.mkString(","))
    }

    val outputFiles = {
      val rebalance = raw("OUTPUT_REBALANCE").map(v => Rebalance.parse(v).getOrElse {
        errors += s"OUTPUT_REBALANCE='$v': must be one of ${Rebalance.All.map(_.name).mkString(" / ")}"
        Rebalance.Auto
      }).getOrElse(Rebalance.Auto)
      val shuffleMb = raw("OUTPUT_FILE_SHUFFLE_MB").map { v =>
        if (rebalance == Rebalance.Never) errors += "OUTPUT_FILE_SHUFFLE_MB: not used with OUTPUT_REBALANCE=false"
        scala.util.Try(v.trim.toInt).toOption.filter(mb => mb >= OutputFiles.MinShuffleMb && mb <= OutputFiles.MaxShuffleMb).getOrElse {
          errors += s"OUTPUT_FILE_SHUFFLE_MB='$v': must be a whole number of MB from ${OutputFiles.MinShuffleMb} to ${OutputFiles.MaxShuffleMb}"
          OutputFiles.DefaultShuffleMb
        }
      }.getOrElse(OutputFiles.DefaultShuffleMb)
      OutputFiles(rebalance, shuffleMb.toLong * 1024 * 1024)
    }

    val dedup = raw("DEDUP").map(v => Dedup.parse(v).getOrElse {
      errors += s"DEDUP='$v': must be one of ${Dedup.All.map(_.name).mkString(" / ")}"
      Dedup.Off
    }).getOrElse(Dedup.Off)

    val warehousePath = raw("WAREHOUSE_PATH")
    warehousePath.filterNot(isAbsoluteUri).foreach { _ =>
      errors += "WAREHOUSE_PATH: must be an absolute URI with a scheme, e.g. hdfs://<namenode>:8020/<path>"
    }

    val decryptEncrypt = mode match {
      case Mode.Passthrough =>
        val set = DecryptEncryptOnly.filter(k => raw(k).isDefined)
        if (set.nonEmpty) errors += s"${set.mkString("/")}: only valid for the decrypt-encrypt mode"
        None
      case Mode.DecryptEncrypt => Some(decryptEncryptSettings(raw, input, partitionBy, columns, errors))
    }
    for (b <- batch; v <- decryptEncrypt; k <- v.keyField if b.column.equalsIgnoreCase(k)) {
      errors += s"BATCH_COLUMN: '${b.column}' must not be CDR_KEY_FIELD"
    }

    if (errors.nonEmpty) Left(errors.toList)
    else Right(IngestConfig(mode, raw("APP_NAME").getOrElse(s"SilverIngest-${mode.name}"), input, outDb, outTable,
      outputFormat, outputMode, partitionBy, warehousePath, bool("ENABLE_HIVE_SUPPORT", default = true), decryptEncrypt, batch, hours, outputFiles, dedup))
  }

  private def decryptEncryptSettings(raw: String => Option[String], input: InputSpec, partitionBy: Seq[String],
                                     columns: (String, String) => Seq[String], errors: ArrayBuffer[String]): DecryptEncryptSettings = {
    val mode = raw("DECRYPT_MODE") match {
      case None => DecryptMode.File
      case Some(m) => DecryptMode.parse(m).getOrElse {
        errors += s"DECRYPT_MODE='$m': must be one of ${DecryptMode.All.map(_.name).mkString(" / ")}"
        DecryptMode.File
      }
    }
    val cipher = raw("FILE_CIPHER").map(_.toLowerCase(Locale.ROOT)).getOrElse(DefaultCipher)
    if (!FileCiphers.contains(cipher)) errors += s"FILE_CIPHER='$cipher': must be one of ${FileCiphers.mkString(" / ")}"
    if (mode != DecryptMode.File && raw("FILE_CIPHER").isDefined) {
      errors += "FILE_CIPHER: only used with DECRYPT_MODE=file (whole-file decryption)"
    }
    // file, column: luôn hỏi by-path (file: chỉ biết file có mã hoá hay không khi chạy; column: lấy prefix của đối tác). none: không gọi by-path.
    val keyPath = mode match {
      case DecryptMode.NoDecrypt =>
        Seq("KEY_PATH", "KEY_PATH_ROOT").filter(k => raw(k).isDefined).foreach { k =>
          errors += s"$k: only used with DECRYPT_MODE=file/column (DECRYPT_MODE=none never calls by-path)"
        }
        ""
      case _ => keyPathOf(raw, input, errors)
    }
    val onMissing = raw("CDR_ON_MISSING").map(_.toLowerCase(Locale.ROOT)).getOrElse(DefaultOnMissing)
    if (!OnMissingValues.contains(onMissing)) {
      errors += s"CDR_ON_MISSING='$onMissing': must be one of ${OnMissingValues.mkString(" / ")}"
    }

    // No column listed is valid: the files are only decrypted and no column is encrypted (DAK by-column is not called).
    val encrypt = raw("CDR_ENCRYPT_COLUMNS").fold(Seq.empty[String])(columns("CDR_ENCRYPT_COLUMNS", _))
    val lowerEncrypt = encrypt.map(_.toLowerCase(Locale.ROOT)).toSet
    val keyField = raw("CDR_KEY_FIELD") match {
      case Some(k) if !k.matches(ColumnName) => errors += s"CDR_KEY_FIELD: invalid column name '$k'"; Some(k)
      case Some(k) =>
        if (lowerEncrypt.contains(k.toLowerCase(Locale.ROOT))) {
          errors += s"CDR_KEY_FIELD: '$k' must not be listed in CDR_ENCRYPT_COLUMNS — it stays plaintext as key material"
        }
        Some(k)
      case None =>
        if (encrypt.nonEmpty) {
          errors += "CDR_KEY_FIELD: is required with CDR_ENCRYPT_COLUMNS (plaintext column whose value completes the key of each row)"
        }
        None
    }
    // Key global: DAK luôn gửi kèm encryptColumns không rỗng; thiếu cột thì cấu hình sai (không âm thầm bỏ qua key global).
    val globalZone = raw("GLOBAL_KEY_ZONE").map { zone =>
      if (!DakGlobalKeySource.isValidZone(zone)) errors += s"GLOBAL_KEY_ZONE: must match ${DakGlobalKeySource.ZonePattern}"
      if (encrypt.isEmpty) errors += "GLOBAL_KEY_ZONE: needs CDR_ENCRYPT_COLUMNS (the columns that the global key encrypts)"
      zone
    }
    // Giá trị cột partition nằm trong tên thư mục, không mã hoá được.
    partitionBy.filter(c => lowerEncrypt.contains(c.toLowerCase(Locale.ROOT))).foreach { c =>
      errors += s"OUTPUT_PARTITION_BY: partition column '$c' must not be listed in CDR_ENCRYPT_COLUMNS (it would be stored in clear in the path)"
    }

    val provider = providerDecrypt(raw, mode, columns, errors)
    val keyFieldValue = constantKeyField(raw, keyField, encrypt, errors)
    val transforms = columnTransforms(raw, lowerEncrypt, errors)

    // Chỉ giải mã CẢ FILE (DECRYPT_MODE=file) mới cần thư mục tạm: file có mã hoá hay không chỉ biết khi chạy, nên điều kiện luôn được kiểm.
    val staging = raw("DECRYPT_STAGING_PATH")
    if (mode != DecryptMode.File) {
      staging.foreach(_ => errors += "DECRYPT_STAGING_PATH: only used with DECRYPT_MODE=file (whole-file decryption); the other modes read the files as they are")
    } else if (input.format.isText) {
      staging.foreach(_ => errors += "DECRYPT_STAGING_PATH: only used with INPUT_FORMAT=parquet/orc/avro; text formats are decrypted in memory")
      // Mỗi phần tử trong bộ nhớ là một dòng nên không đọc được bản ghi nhiều dòng.
      if (input.options.exists { case (k, v) => k.equalsIgnoreCase("multiLine") && v.trim.equalsIgnoreCase("true") }) {
        errors += "INPUT_OPTION_multiLine: not supported in decrypt-encrypt mode (decrypted text is read line by line)"
      }
    } else staging match {
      case None => errors += s"DECRYPT_STAGING_PATH: is required with INPUT_FORMAT=${input.format.name} (files decrypted with a by-path key are written there, then deleted)"
      case Some(s) if !isAbsolute(s) => errors += "DECRYPT_STAGING_PATH: must be an absolute path or a URI with a scheme"
      case Some(s) if inside(s, input.path) => errors += "DECRYPT_STAGING_PATH: must not be inside INPUT_PATH"
      case Some(_) =>
    }
    DecryptEncryptSettings(keyPath, cipher, encrypt, keyField, if (mode == DecryptMode.File) staging else None, onMissing, globalZone,
      mode, provider, keyFieldValue, transforms)
  }

  /** `PROVIDER_DECRYPT_COLUMNS` + `PROVIDER_KEY_FIELD`: bắt buộc với `DECRYPT_MODE=column`, không nhận với các chế độ khác. */
  private def providerDecrypt(raw: String => Option[String], mode: DecryptMode, columns: (String, String) => Seq[String],
                              errors: ArrayBuffer[String]): Option[ProviderDecrypt] = {
    val rawColumns = raw("PROVIDER_DECRYPT_COLUMNS")
    val rawKeyField = raw("PROVIDER_KEY_FIELD")
    if (mode != DecryptMode.Column) {
      rawColumns.foreach(_ => errors += "PROVIDER_DECRYPT_COLUMNS: only used with DECRYPT_MODE=column")
      rawKeyField.foreach(_ => errors += "PROVIDER_KEY_FIELD: only used with DECRYPT_MODE=column")
      None
    } else {
      val cols = rawColumns match {
        case Some(v) => columns("PROVIDER_DECRYPT_COLUMNS", v)
        case None =>
          errors += "PROVIDER_DECRYPT_COLUMNS: is required with DECRYPT_MODE=column (the columns that the provider encrypted)"
          Nil
      }
      if (rawColumns.isDefined && cols.isEmpty) errors += "PROVIDER_DECRYPT_COLUMNS: must list at least one column"
      val keyField = rawKeyField match {
        case Some(k) if !k.matches(ColumnName) => errors += s"PROVIDER_KEY_FIELD: invalid column name '$k'"; Some(k)
        case Some(k) =>
          if (cols.exists(_.equalsIgnoreCase(k))) {
            errors += s"PROVIDER_KEY_FIELD: '$k' must not be listed in PROVIDER_DECRYPT_COLUMNS — it stays plaintext as key material"
          }
          Some(k)
        case None =>
          errors += "PROVIDER_KEY_FIELD: is required with DECRYPT_MODE=column (the plaintext column that completes the provider key of each row)"
          None
      }
      keyField.map(ProviderDecrypt(cols, _))
    }
  }

  /**
   * `CDR_KEY_FIELD_VALUE`: `CDR_KEY_FIELD` là một cột HẰNG SỐ do job thêm (mọi dòng bằng giá trị này) thay vì cột có sẵn trong dữ liệu, để cùng
   * một giá trị gốc luôn ra cùng một bản mã ở mọi dòng và mọi bảng (điều kiện để nối được sau khi băm).
   */
  private def constantKeyField(raw: String => Option[String], keyField: Option[String], encrypt: Seq[String],
                               errors: ArrayBuffer[String]): Option[String] =
    raw("CDR_KEY_FIELD_VALUE").map { value =>
      if (!value.matches(KeyFieldValue)) errors += "CDR_KEY_FIELD_VALUE: must be 1 to 64 characters of [A-Za-z0-9_.:-]"
      if (keyField.isEmpty) errors += "CDR_KEY_FIELD_VALUE: needs CDR_KEY_FIELD (the name of the constant column that the job adds)"
      if (encrypt.isEmpty) errors += "CDR_KEY_FIELD_VALUE: needs CDR_ENCRYPT_COLUMNS (the columns that the constant key field encrypts)"
      value
    }

  /** `COLUMN_TRANSFORMS` = `cột:KIND,cột:KIND`. Gom nhóm chạy SAU giải mã và TRƯỚC mã hoá, nên không nhận cột đang mã hoá. */
  private def columnTransforms(raw: String => Option[String], lowerEncrypt: Set[String],
                               errors: ArrayBuffer[String]): Seq[ColumnTransform] = {
    val tokens = raw("COLUMN_TRANSFORMS").fold(Seq.empty[String])(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
    val parsed = tokens.flatMap { token =>
      token.split(":", -1).map(_.trim) match {
        case Array(column, kind) if column.nonEmpty && kind.nonEmpty =>
          if (!column.matches(ColumnName)) errors += s"COLUMN_TRANSFORMS: invalid column name '$column'"
          if (lowerEncrypt.contains(column.toLowerCase(Locale.ROOT))) {
            errors += s"COLUMN_TRANSFORMS: column '$column' must not be listed in CDR_ENCRYPT_COLUMNS (transforms run before the encryption)"
          }
          TransformKind.parse(kind) match {
            case Some(k) => Some(ColumnTransform(column, k))
            case None =>
              errors += s"COLUMN_TRANSFORMS: unknown transform '$kind' (must be one of ${TransformKind.All.map(_.name).mkString(" / ")})"
              None
          }
        case _ => errors += s"COLUMN_TRANSFORMS: '$token' must be column:KIND"; None
      }
    }
    val lower = parsed.map(_.column.toLowerCase(Locale.ROOT))
    if (lower.distinct.size != lower.size) errors += "COLUMN_TRANSFORMS: duplicated column (column names are case-insensitive)"
    parsed
  }

  /**
   * Đường dẫn gửi `by-path`: TƯƠNG ĐỐI theo gốc của nguồn (theo bảng, theo ngày), dạng chuẩn, giữ hoa thường — cách SA đã trả lời DAK
   * (docs/key-access/TRA-LOI-DAK-BY-PATH.md). `KEY_PATH` đặt thẳng phần tương đối; không có thì lấy phần của `INPUT_PATH` sau `KEY_PATH_ROOT`.
   */
  private def keyPathOf(raw: String => Option[String], input: InputSpec, errors: ArrayBuffer[String]): String = {
    val keyPath = raw("KEY_PATH") match {
      case Some(k) if k.contains("://") =>
        errors += "KEY_PATH: must be relative to the root of the source, without a scheme (hdfs://...)"; k
      case Some(k) => PathKeySource.canonical(k)
      case None => raw("KEY_PATH_ROOT") match {
        case None =>
          errors += "KEY_PATH_ROOT: is required (the root the by-path key path is relative to), or set KEY_PATH"
          ""
        case Some(root) => PathKeySource.relativeTo(input.path, root).getOrElse {
          if (input.path.nonEmpty) errors += "INPUT_PATH: must be strictly under KEY_PATH_ROOT (the key path is the part after the root)"
          ""
        }
      }
    }
    if (keyPath.contains("..") || keyPath.exists(Character.isISOControl) || keyPath.length > 1024) {
      errors += "KEY_PATH: must be at most 1024 characters, without '..' or control characters"
    }
    keyPath
  }

  private def inside(child: String, parent: String): Boolean = {
    val p = parent.stripSuffix("/")
    p.nonEmpty && (child.stripSuffix("/") == p || child.startsWith(p + "/"))
  }

  private def isAbsolute(path: String): Boolean =
    path.startsWith("/") || Try(new java.net.URI(path)).toOption.exists(_.getScheme != null)

  private def isAbsoluteUri(value: String): Boolean =
    Try(new java.net.URI(value)).toOption.exists(u => u.getScheme != null && Option(u.getPath).exists(_.startsWith("/")))
}
