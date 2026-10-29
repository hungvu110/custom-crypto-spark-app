package vai.lakehouse.silveringest.config

import java.util.Locale
import java.util.regex.{Pattern, PatternSyntaxException}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import vai.lakehouse.keyprefix.PathKeySource

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
 * Giờ của từng dòng: job thêm cột `column` = giờ `HH` suy ra từ ĐƯỜNG DẪN file nguồn bằng `pathRegex` (đúng một nhóm bắt, không khớp
 * thì `NA`). Đặt cột này vào `OUTPUT_PARTITION_BY` nếu muốn bảng partition theo giờ; không thì nó là cột thường. `replace` (rỗng = cả ngày):
 * chỉ các giờ này của lô bị thay khi `overwrite-partitions`, nên chạy lại vài giờ không đụng các giờ khác của ngày và không ghi trùng.
 */
final case class Hours(column: String, pathRegex: String, replace: Seq[String] = Nil)

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
 */
final case class DecryptEncryptSettings(keyPath: String, cipher: String, encryptColumns: Seq[String], keyField: Option[String],
                                        stagingPath: Option[String], onMissing: String = IngestConfig.DefaultOnMissing)

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
    hours: Option[Hours] = None) {

  def target: String = s"$outputDb.$outputTable"

  def describe: Seq[(String, String)] = Seq(
    "MODE"                -> mode.name,
    "APP_NAME"            -> appName,
    "INPUT_PATH"          -> input.path,
    "INPUT_FORMAT"        -> input.format.name,
    "INPUT_SCHEMA"        -> input.schema.getOrElse("(none)"),
    "INPUT_OPTION_*"      -> (if (input.options.isEmpty) "(none)" else input.options.keys.toSeq.sorted.mkString(", ")),
    "OUTPUT"              -> s"$target [${outputFormat.name}, $outputMode]",
    "BATCH"               -> batch.fold("(none)")(b => s"${b.column} = ${b.value}"),
    "HOURS"               -> hours.fold("(none)")(h => s"${h.column} from ${h.pathRegex}" + (if (h.replace.isEmpty) "" else s"; replace ${h.replace.mkString(",")}")),
    "OUTPUT_PARTITION_BY" -> (if (partitionBy.isEmpty) "(none)" else partitionBy.mkString(", ")),
    "WAREHOUSE_PATH"      -> warehousePath.getOrElse("(Spark/metastore default)"),
    "ENABLE_HIVE_SUPPORT" -> enableHiveSupport.toString) ++ decryptEncrypt.toSeq.flatMap { v => Seq(
    "KEY_PATH"             -> v.keyPath,
    "FILE_CIPHER"          -> v.cipher,
    "CDR_ENCRYPT_COLUMNS"  -> (if (v.encryptColumns.isEmpty) "(none)" else v.encryptColumns.mkString(", ")),
    "CDR_KEY_FIELD"        -> v.keyField.getOrElse("(none)"),
    "CDR_ON_MISSING"       -> v.onMissing,
    "DECRYPT_STAGING_PATH" -> v.stagingPath.getOrElse("(in memory)"))
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
    "DECRYPT_STAGING_PATH")
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
    val hours = raw("HOUR_PATH_REGEX") match {
      case Some(pattern) =>
        try {
          if (Pattern.compile(pattern).matcher("").groupCount != 1) errors += "HOUR_PATH_REGEX: must have exactly one capture group (the hour)"
        } catch {
          case _: PatternSyntaxException => errors += "HOUR_PATH_REGEX: not a valid regular expression"
        }
        val column = raw("HOUR_COLUMN").getOrElse(DefaultHourColumn)
        if (!column.matches(ColumnName)) errors += s"HOUR_COLUMN: invalid column name '$column'"
        if (batch.exists(_.column.equalsIgnoreCase(column))) errors += "HOUR_COLUMN: must differ from BATCH_COLUMN"
        val replace = raw("BATCH_HOURS").map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Nil)
        replace.filterNot(_.matches(HourValue)).foreach(h => errors += s"BATCH_HOURS: invalid hour '$h' (two digits, 00 to 23)")
        if (replace.distinct.size != replace.size) errors += "BATCH_HOURS: duplicated hour"
        if (replace.nonEmpty && batch.isEmpty) errors += "BATCH_HOURS: only used together with BATCH_VALUE"
        Some(Hours(column, pattern, replace))
      case None =>
        val set = Seq("HOUR_COLUMN", "BATCH_HOURS").filter(k => raw(k).isDefined)
        if (set.nonEmpty) errors += s"${set.mkString("/")}: only used together with HOUR_PATH_REGEX"
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
      outputFormat, outputMode, partitionBy, warehousePath, bool("ENABLE_HIVE_SUPPORT", default = true), decryptEncrypt, batch, hours))
  }

  private def decryptEncryptSettings(raw: String => Option[String], input: InputSpec, partitionBy: Seq[String],
                                     columns: (String, String) => Seq[String], errors: ArrayBuffer[String]): DecryptEncryptSettings = {
    val cipher = raw("FILE_CIPHER").map(_.toLowerCase(Locale.ROOT)).getOrElse(DefaultCipher)
    if (!FileCiphers.contains(cipher)) errors += s"FILE_CIPHER='$cipher': must be one of ${FileCiphers.mkString(" / ")}"
    // Luôn hỏi by-path: chỉ biết file có mã hoá hay không khi chạy (DAK có key hay trả rỗng).
    val keyPath = keyPathOf(raw, input, errors)
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
    // Giá trị cột partition nằm trong tên thư mục, không mã hoá được.
    partitionBy.filter(c => lowerEncrypt.contains(c.toLowerCase(Locale.ROOT))).foreach { c =>
      errors += s"OUTPUT_PARTITION_BY: partition column '$c' must not be listed in CDR_ENCRYPT_COLUMNS (it would be stored in clear in the path)"
    }

    // File có mã hoá hay không chỉ biết khi chạy, nên điều kiện của việc giải mã luôn được kiểm.
    val staging = raw("DECRYPT_STAGING_PATH")
    if (input.format.isText) {
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
    DecryptEncryptSettings(keyPath, cipher, encrypt, keyField, staging, onMissing)
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
