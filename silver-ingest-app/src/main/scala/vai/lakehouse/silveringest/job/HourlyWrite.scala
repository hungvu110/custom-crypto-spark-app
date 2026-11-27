package vai.lakehouse.silveringest.job

import scala.util.control.NonFatal

import org.apache.spark.sql.{DataFrame, SparkSession}
import vai.lakehouse.silveringest.config.{Dedup, IngestConfig}
import vai.lakehouse.silveringest.logging.Log

/**
 * `OUTPUT_WRITE_BY_HOUR=true` (VTNET EBS): ghi lần lượt TỪNG GIỜ, mỗi giờ (một nhóm file cùng giờ theo đường dẫn) là một lần ghi Delta riêng, và
 * ghi xong giờ nào thì log ngay một dòng cho giờ đó (số dòng, file, dung lượng lấy từ commit Delta, không đếm lại dữ liệu), để theo dõi một ngày
 * rất lớn khi đang chạy.
 *
 * `overwrite-partitions` cho cả ngày (không `BATCH_HOURS`): giờ ĐẦU TIÊN thay cả ngày (`replaceWhere` theo lô), các giờ sau chỉ thay giờ của
 * chúng, nên kết thúc vẫn đúng như ghi một lần (giờ không còn trong nguồn cũng bị xoá). Khác ghi một lần: trong lúc chạy người đọc thấy ngày
 * đang nạp dở, và nếu dừng ở giờ H thì các giờ trước H đã là dữ liệu mới, từ H trở đi chưa có: chạy lại ngày, hoặc `BATCH_HOURS` từ H.
 */
object HourlyWrite {

  /** `prepare` = phần của job giữa đọc và ghi (giải mã, chuẩn hoá, mã hoá), áp cho dữ liệu của MỘT giờ. */
  def run(spark: SparkSession, cfg: IngestConfig, files: Seq[SourceFile])(prepare: DataFrame => DataFrame): Unit = {
    val hours = cfg.hours.getOrElse(throw new IllegalArgumentException("OUTPUT_WRITE_BY_HOUR needs HOUR_PATH_REGEX"))
    val day = cfg.batch.map(_.value).getOrElse("")
    val groups = HourGroups.group(hours, files)
    // DEDUP=check measures the whole day once (same numbers as without OUTPUT_WRITE_BY_HOUR).
    if (cfg.dedup == Dedup.Check) DuplicateCheck.run(spark, files, PlainSource.read(spark, cfg.copy(dedup = Dedup.Off), files), cfg.dedupSample, cfg.dedupProfile)
    Log.info(s"writing ${groups.size} hour(s) of $day one by one: ${groups.map(_._1).mkString(", ")}")

    val start = System.nanoTime()
    groups.zipWithIndex.foreach { case ((hour, hourFiles), i) =>
      val position = s"${i + 1}/${groups.size}"
      val hourStart = System.nanoTime()
      // The first hour of a whole-day overwrite replaces the whole day; every other write replaces only its own hour.
      val replace = if (i == 0 && hours.replace.isEmpty) Nil else Seq(hour)
      val hourCfg = cfg.copy(hours = Some(hours.copy(replace = replace)))
      try {
        val data = HourGroups.read(hours, hourFiles)(group => SourceReader.read(spark, cfg.input, group.map(_.path)))
        TableWriter.write(spark, hourCfg, prepare(data))
      } catch {
        case NonFatal(e) =>
          Log.fail(s"HOUR $hour FAILED ($position) dt=$day after ${seconds(hourStart)} s: hours before $hour are written, " +
            s"hours from $hour are not. Rerun the day, or only these hours with BATCH_HOURS / fromHour=$hour")
          throw e
      }
      val written = TableWriter.lastCommit(spark, hourCfg).fold("") { m =>
        f" ${m.rows}%,d rows, ${m.files}%,d files, ${m.bytes / 1e9}%.1f GB"
      }
      Log.ok(s"HOUR $hour DONE ($position) dt=$day:$written in ${seconds(hourStart)} s")
    }
    Log.ok(s"ALL ${groups.size} HOUR(S) DONE dt=$day in ${seconds(start)} s")
  }

  private def seconds(startNanos: Long): String = f"${(System.nanoTime() - startNanos) / 1e9}%.0f"
}
