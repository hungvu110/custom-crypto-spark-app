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

/**
 * Cấu hình của job, đọc từ biến môi trường của driver (`spec.driver.env` trong SparkApplication).
 *
 * Ba đầu vào chính: `DB_NAME`, `TABLE_NAME`, `PSEUDO_COLUMNS`. Bảng nguồn KHÔNG bao giờ bị sửa: kết quả ghi ra bảng đích
 * (`OUTPUT_DB`.`OUTPUT_TABLE`), mặc định `<DB_NAME>.<TABLE_NAME>_pseudonymized`.
 */
final case class JobConfig(
    appName: String,
    dbName: String,
    tableName: String,
    columns: Seq[String],
    mode: PseudonymMode,
    outputDb: String,
    outputTable: String,
    outputFormat: OutputFormat,
    outputMode: String,
    mappingTablePath: Option[String],
    warehousePath: Option[String],
    enableHiveSupport: Boolean,
    verify: Boolean) {

  def source: String = s"$dbName.$tableName"

  def target: String = s"$outputDb.$outputTable"

  def describe: Seq[(String, String)] = Seq(
    "APP_NAME"            -> appName,
    "DB_NAME"             -> dbName,
    "TABLE_NAME"          -> tableName,
    "PSEUDO_COLUMNS"      -> columns.mkString(", "),
    "PSEUDO_MODE"         -> mode.name,
    "OUTPUT_DB"           -> outputDb,
    "OUTPUT_TABLE"        -> outputTable,
    "OUTPUT_FORMAT"       -> outputFormat.name,
    "OUTPUT_MODE"         -> outputMode,
    "MAPPING_TABLE_PATH"  -> mappingTablePath.getOrElse("(spark.pseudonym.mapping.path hoặc không dùng)"),
    "WAREHOUSE_PATH"      -> warehousePath.getOrElse("(mặc định của Spark/metastore)"),
    "ENABLE_HIVE_SUPPORT" -> enableHiveSupport.toString,
    "VERIFY"              -> verify.toString)
}

object JobConfig {

  val OutputModes = Seq("overwrite", "append")

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

    val db    = identifier("DB_NAME", None)
    val table = identifier("TABLE_NAME", None)

    val columns = raw("PSEUDO_COLUMNS").map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Nil)
    if (columns.isEmpty) errors += "PSEUDO_COLUMNS: bắt buộc (danh sách cột cần khử định danh, phân cách bằng dấu phẩy)"
    columns.filterNot(_.matches("^[A-Za-z_][A-Za-z0-9_]*$")).foreach(c => errors += s"PSEUDO_COLUMNS: tên cột không hợp lệ '$c'")
    val duplicated = columns.diff(columns.distinct).distinct
    if (duplicated.nonEmpty) errors += s"PSEUDO_COLUMNS: cột bị lặp ${duplicated.mkString(", ")}"

    val mode = raw("PSEUDO_MODE").map(m => Try(PseudonymMode.parse(m)).toOption.getOrElse {
      errors += s"PSEUDO_MODE='$m': phải là ${PseudonymMode.All.map(_.name).mkString(" / ")}"
      PseudonymMode.Hash
    }).getOrElse(PseudonymMode.Hash)

    val outDb    = identifier("OUTPUT_DB", Some(db).filter(_.nonEmpty))
    val outTable = identifier("OUTPUT_TABLE", Some(s"${table}_pseudonymized").filter(_ => table.nonEmpty))
    if (db.nonEmpty && table.nonEmpty && outDb == db && outTable == table) {
      errors += "OUTPUT_DB.OUTPUT_TABLE: không được trùng bảng nguồn — job không ghi đè bảng gốc (đổi OUTPUT_TABLE hoặc OUTPUT_DB)"
    }

    val format = raw("OUTPUT_FORMAT").map(f => OutputFormat.parse(f).getOrElse {
      errors += s"OUTPUT_FORMAT='$f': phải là ${OutputFormat.All.map(_.name).mkString(" / ")}"
      OutputFormat.Delta
    }).getOrElse(OutputFormat.Delta)

    val outputMode = raw("OUTPUT_MODE").map(_.toLowerCase(Locale.ROOT)).getOrElse("overwrite")
    if (!OutputModes.contains(outputMode)) errors += s"OUTPUT_MODE='$outputMode': phải là ${OutputModes.mkString(" / ")}"

    // MAPPING_TABLE_PATH hoặc spark.pseudonym.mapping.path đều được: job kiểm tra lại sau khi có SparkConf.
    val mappingPath = raw("MAPPING_TABLE_PATH")

    // Cùng quy ước với crawler-streaming-app: URI đầy đủ để không phụ thuộc fs.defaultFS trong hadoopConfigMap.
    val warehousePath = raw("WAREHOUSE_PATH")
    warehousePath.filterNot(isAbsoluteUri).foreach { v =>
      errors += s"WAREHOUSE_PATH='$v': phải là URI tuyệt đối có scheme, vd hdfs://<namenode>:8020/<path>"
    }

    if (errors.nonEmpty) Left(errors.toList)
    else Right(JobConfig(raw("APP_NAME").getOrElse("PseudonymizeTable"), db, table, columns, mode, outDb, outTable, format,
      outputMode, mappingPath, warehousePath, bool("ENABLE_HIVE_SUPPORT", default = true), bool("VERIFY", default = true)))
  }

  private def isAbsoluteUri(value: String): Boolean =
    Try(new java.net.URI(value)).toOption.exists(u => u.getScheme != null && Option(u.getPath).exists(_.startsWith("/")))
}
