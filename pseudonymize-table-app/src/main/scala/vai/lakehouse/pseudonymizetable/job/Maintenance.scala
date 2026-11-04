package vai.lakehouse.pseudonymizetable.job

import java.util.Locale

import scala.util.control.NonFatal

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import vai.lakehouse.pseudonymizetable.config.{PurgeConfig, VacuumConfig, VacuumTarget}
import vai.lakehouse.pseudonymizetable.logging.Log

/** Kết quả xoá vùng: `tableFound=false` nghĩa là bảng không có (đã xoá trước đó hoặc chưa từng tạo): với DAK là XONG VIỆC. */
final case class PurgeResult(tableFound: Boolean, metrics: Map[String, String] = Map.empty)

/** Kết quả VACUUM: các bảng đã dọn và các bảng lỗi (kèm lý do). */
final case class VacuumResult(vacuumed: Seq[String], skipped: Seq[String], failed: Seq[(String, String)])

/**
 * Hai việc bảo trì bảng đích khử định danh:
 *  - [[purge]]: phiếu hết hạn -> xoá các partition `(date, hour)` của phiếu khỏi bảng của team (mỗi team một bảng nên không đụng ai);
 *  - [[vacuum]]: xoá VẬT LÝ các file mà `DELETE`/overwrite đã bỏ khỏi log (Delta chỉ đánh dấu removed cho tới khi VACUUM).
 */
object Maintenance {

  private def quote(identifier: String): String = s"`${identifier.replace("`", "``")}`"
  private def quoted(db: String, table: String): String = s"${quote(db)}.${quote(table)}"

  private def detail(spark: SparkSession, table: String) =
    spark.sql(s"DESCRIBE DETAIL $table").select("format", "partitionColumns").head()

  private def isDelta(spark: SparkSession, table: String): Boolean =
    try detail(spark, table).getString(0).toLowerCase(Locale.ROOT) == "delta" catch { case NonFatal(_) => false }

  /**
   * Xoá mọi partition trong khoảng. Chạy lại là an toàn: không còn gì để xoá thì xong (không lỗi); bảng không có cũng xong.
   * Chỉ xoá theo partition (điều kiện chỉ chạm cột partition -> Delta bỏ file, không ghi lại dữ liệu). Bảng chỉ partition theo
   * ngày mà khoảng có ràng buộc giờ thì từ chối, giống lúc ghi.
   */
  def purge(spark: SparkSession, cfg: PurgeConfig): PurgeResult = {
    if (!spark.catalog.tableExists(cfg.db, cfg.table)) {
      Log.info(s"TABLE_NOT_FOUND ${cfg.target}: nothing to delete (already deleted or never created)")
      return PurgeResult(tableFound = false)
    }
    val table = quoted(cfg.db, cfg.table)
    val info = detail(spark, table)
    if (info.getString(0).toLowerCase(Locale.ROOT) != "delta") {
      throw new IllegalArgumentException(s"${cfg.target} is a ${info.getString(0)} table: purge by range is only supported for Delta tables")
    }
    val partitions = info.getSeq[String](1).map(_.toLowerCase(Locale.ROOT))
    val range = cfg.range
    if (!partitions.contains(range.dateKey.toLowerCase(Locale.ROOT))) {
      throw new IllegalArgumentException(s"${cfg.target} is not partitioned by ${range.dateKey} (partitions: ${partitions.mkString(", ")}): " +
        "refusing to delete by a column that is not a partition")
    }
    val hourPartitioned = partitions.contains(range.hourKey.toLowerCase(Locale.ROOT))
    if (!hourPartitioned && range.hourConstrained) {
      throw new IllegalArgumentException(s"${cfg.target} is partitioned by ${partitions.mkString(", ")} only: INPUT_HOURS and the hour in " +
        "INPUT_FROM/INPUT_TO cannot be applied, use whole days (yyyyMMdd)")
    }
    val effective = range.copy(hourPartitioned = hourPartitioned)
    Log.kv("delete", s"${cfg.target} where ${effective.describe} on ${effective.dateKey}${if (hourPartitioned) "/" + effective.hourKey else ""}")
    val delta = DeltaTable.forName(spark, table)
    delta.delete(TableRange.condition(effective))
    val metrics = delta.history(1).select("operationMetrics").head().getMap[String, String](0).toMap
    Log.kv("delete metrics", metrics.toSeq.sorted.map { case (k, v) => s"$k=$v" }.mkString(", "))
    PurgeResult(tableFound = true, metrics)
  }

  /** Bảng của target: `db.table` là đúng một bảng; `db` là mọi bảng (không tạm) của database. */
  private def tablesOf(spark: SparkSession, target: VacuumTarget): Seq[String] = target.table match {
    case Some(t) => Seq(s"${target.db}.$t")
    case None => spark.catalog.listTables(target.db).collect().filterNot(_.isTemporary).map(t => s"${target.db}.${t.name}").toSeq.sorted
  }

  /** VACUUM từng bảng Delta; bảng lỗi không chặn các bảng còn lại (gom lại để job báo lỗi ở cuối). */
  def vacuum(spark: SparkSession, cfg: VacuumConfig): VacuumResult = {
    if (cfg.retainHours < 168) {
      // Delta refuses a retention below its safe default unless the check is off.
      Log.warn(s"VACUUM_RETAIN_HOURS=${cfg.retainHours} is below 168: turning off the Delta retention check for this run")
      spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    }
    val vacuumed, skipped = scala.collection.mutable.ArrayBuffer.empty[String]
    val failed = scala.collection.mutable.ArrayBuffer.empty[(String, String)]
    val names = try cfg.targets.flatMap(t => tablesOf(spark, t)).distinct
      catch { case NonFatal(e) => failed += ("(listing) " -> Option(e.getMessage).getOrElse(e.toString)); Nil }
    names.foreach { name =>
      val Array(db, table) = name.split("\\.", 2)
      val quotedName = quoted(db, table)
      try {
        if (!spark.catalog.tableExists(db, table)) { Log.info(s"SKIP $name: table does not exist"); skipped += name }
        else if (!isDelta(spark, quotedName)) { Log.info(s"SKIP $name: not a Delta table"); skipped += name }
        else {
          Log.info(s"VACUUM $name RETAIN ${cfg.retainHours} HOURS")
          spark.sql(s"VACUUM $quotedName RETAIN ${cfg.retainHours} HOURS").collect()
          Log.ok(s"VACUUM DONE $name")
          vacuumed += name
        }
      } catch {
        case NonFatal(e) =>
          Log.fail(s"VACUUM FAILED $name: ${Option(e.getMessage).getOrElse(e.toString).split("\n").head.take(200)}")
          failed += (name -> Option(e.getMessage).getOrElse(e.toString))
      }
    }
    VacuumResult(vacuumed.toSeq, skipped.toSeq, failed.toSeq)
  }
}
