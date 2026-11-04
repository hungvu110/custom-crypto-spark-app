package vai.lakehouse.pseudonymizetable.config

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/** Bảng đích (hoặc cả database) cần VACUUM: `db` = mọi bảng Delta của database, `db.table` = đúng một bảng. */
final case class VacuumTarget(db: String, table: Option[String]) {
  def describe: String = table.fold(s"$db.*")(t => s"$db.$t")
}

/**
 * Xoá vùng khai thác: xoá mọi partition `(dateKey, hourKey)` của `db.table` nằm trong `range`. `range` luôn có CẢ `INPUT_FROM` và
 * `INPUT_TO` (không có thì sẽ xoá cả bảng).
 */
final case class PurgeConfig(db: String, table: String, range: InputRange, enableHiveSupport: Boolean, warehousePath: Option[String]) {
  def target: String = s"$db.$table"
  def describe: Seq[(String, String)] = Seq("OUTPUT_DB.OUTPUT_TABLE" -> target, "RANGE" -> range.describe,
    "PARTITION KEYS" -> s"${range.dateKey} / ${range.hourKey}", "WAREHOUSE_PATH" -> warehousePath.getOrElse("(Spark/metastore default)"))
}

final case class VacuumConfig(targets: Seq[VacuumTarget], retainHours: Int, enableHiveSupport: Boolean, warehousePath: Option[String]) {
  def describe: Seq[(String, String)] = Seq("VACUUM_TARGETS" -> targets.map(_.describe).mkString(", "),
    "VACUUM_RETAIN_HOURS" -> retainHours.toString, "WAREHOUSE_PATH" -> warehousePath.getOrElse("(Spark/metastore default)"))
}

/** Cấu hình hai việc bảo trì bảng đích (đọc từ env của driver, gom MỌI lỗi một lượt như [[JobConfig]]). */
object MaintenanceConfig {

  private val Identifier = "^[A-Za-z0-9_]+$"

  /** Mặc định của Delta: VACUUM không giữ ít hơn 7 ngày trừ khi tắt kiểm tra (job tự tắt khi cấu hình ngắn hơn). */
  val DefaultRetainHours = 168
  private val MaxRetainHours = 24 * 365

  private def raw(env: Map[String, String], key: String): Option[String] = env.get(key).map(_.trim).filter(_.nonEmpty)

  private def warehouse(env: Map[String, String], errors: ArrayBuffer[String]): Option[String] = {
    val value = raw(env, "WAREHOUSE_PATH")
    value.filterNot(JobConfig.isAbsoluteUri).foreach { v =>
      errors += s"WAREHOUSE_PATH='$v': must be an absolute URI with a scheme, e.g. hdfs://<namenode>:8020/<path>"
    }
    value
  }

  private def hiveSupport(env: Map[String, String], errors: ArrayBuffer[String]): Boolean = raw(env, "ENABLE_HIVE_SUPPORT") match {
    case None => true
    case Some(v) if v.equalsIgnoreCase("true") => true
    case Some(v) if v.equalsIgnoreCase("false") => false
    case Some(v) => errors += s"ENABLE_HIVE_SUPPORT='$v': must be true or false"; true
  }

  def purgeFromEnv(env: Map[String, String]): Either[Seq[String], PurgeConfig] = {
    val errors = ArrayBuffer.empty[String]
    def identifier(key: String): String = raw(env, key) match {
      case Some(v) if v.matches(Identifier) => v
      case Some(v) => errors += s"$key='$v': must contain only [A-Za-z0-9_]"; ""
      case None => errors += s"$key: is required"; ""
    }
    val db = identifier("OUTPUT_DB")
    val table = identifier("OUTPUT_TABLE")
    // Without both ends the range would be open and the purge would remove the whole table.
    Seq("INPUT_FROM", "INPUT_TO").filter(raw(env, _).isEmpty).foreach(k => errors += s"$k: is required (a purge never works on an open range)")
    val range = JobConfig.inputRange(env, errors)
    val enableHive = hiveSupport(env, errors)
    val warehousePath = warehouse(env, errors)
    if (errors.nonEmpty) Left(errors.toList) else Right(PurgeConfig(db, table, range.get, enableHive, warehousePath))
  }

  def vacuumFromEnv(env: Map[String, String]): Either[Seq[String], VacuumConfig] = {
    val errors = ArrayBuffer.empty[String]
    val targets = raw(env, "VACUUM_TARGETS").map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Nil).map { token =>
      token.split("\\.", -1).toSeq match {
        case Seq(db) if db.matches(Identifier) => VacuumTarget(db, None)
        case Seq(db, table) if db.matches(Identifier) && table.matches(Identifier) => VacuumTarget(db, Some(table))
        case _ => errors += s"VACUUM_TARGETS: '$token' must be <db> or <db>.<table> ([A-Za-z0-9_] only)"; VacuumTarget("", None)
      }
    }
    if (targets.isEmpty) errors += "VACUUM_TARGETS: is required (comma-separated <db> or <db>.<table>)"
    val retainHours = raw(env, "VACUUM_RETAIN_HOURS").map { v =>
      Try(v.toInt).toOption.filter(n => n >= 1 && n <= MaxRetainHours).getOrElse {
        errors += s"VACUUM_RETAIN_HOURS='$v': must be an integer in 1..$MaxRetainHours"; DefaultRetainHours
      }
    }.getOrElse(DefaultRetainHours)
    val enableHive = hiveSupport(env, errors)
    val warehousePath = warehouse(env, errors)
    if (errors.nonEmpty) Left(errors.toList) else Right(VacuumConfig(targets.distinct, retainHours, enableHive, warehousePath))
  }
}
