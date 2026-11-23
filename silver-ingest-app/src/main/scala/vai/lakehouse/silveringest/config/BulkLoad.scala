package vai.lakehouse.silveringest.config

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/** Ngày của khoảng bulk không có thư mục nguồn: `fail` dừng ở ngày đó; `skip` bỏ qua ngày đó và nạp các ngày còn lại. */
sealed abstract class MissingDay(val name: String)

object MissingDay {
  case object Fail extends MissingDay("fail")
  case object Skip extends MissingDay("skip")

  val All: Seq[MissingDay] = Seq(Fail, Skip)

  def parse(raw: String): Option[MissingDay] = All.find(_.name == raw.trim.toLowerCase(Locale.ROOT))
}

/**
 * Bulk load: MỘT lần chạy nạp nhiều ngày `from`..`to` (yyyyMMdd, gồm cả hai đầu), lần lượt từng ngày trong cùng một Spark session. Mỗi ngày là một lần
 * chạy bình thường: đọc `INPUT_PATH` của ngày đó (`%DAY%` được thay bằng ngày), ghi vào partition `BATCH_COLUMN` = ngày đó. Ghi theo `overwrite-partitions`,
 * nên ngày đã nạp trước đó (kể cả chạy thử) chỉ bị thay đúng partition của ngày đó, không ghi trùng, không đụng ngày ngoài khoảng.
 *
 * Không đặt `BULK_FROM` = không bulk (một ngày, như trước). Thất bại ở ngày nào thì dừng ở ngày đó (các ngày trước đã nạp xong, các ngày sau chưa nạp).
 */
final case class BulkLoad(from: String, to: String, onMissingDay: MissingDay) {
  def days: Seq[String] = BulkLoad.daysBetween(from, to)
}

object BulkLoad {

  /** Chỗ của ngày trong `INPUT_PATH`, vd `hdfs://.../f001-mpre-call/date=%DAY%`. */
  val DayToken = "%DAY%"

  /** Giới hạn một lần chạy (vài tháng): bulk dài hơn nên chia thành nhiều lần chạy. */
  val MaxDays = 62

  private val Keys = Seq("BULK_FROM", "BULK_TO", "BULK_MISSING_DAY")

  private def parseDay(value: String): Option[LocalDate] =
    if (value.matches("^[0-9]{8}$")) Try(LocalDate.parse(value, DateTimeFormatter.BASIC_ISO_DATE)).toOption else None

  private def format(day: LocalDate): String = day.format(DateTimeFormatter.BASIC_ISO_DATE)

  private[config] def daysBetween(from: String, to: String): Seq[String] = {
    val (a, b) = (parseDay(from).get, parseDay(to).get)
    Iterator.iterate(a)(_.plusDays(1)).takeWhile(!_.isAfter(b)).map(format).toList
  }

  /** `Right(None)` khi không có biến `BULK_*` nào (không bulk). Gom mọi lỗi một lượt. */
  def fromEnv(env: Map[String, String]): Either[Seq[String], Option[BulkLoad]] = {
    def raw(key: String): Option[String] = env.get(key).map(_.trim).filter(_.nonEmpty)

    if (Keys.forall(k => raw(k).isEmpty)) Right(None)
    else {
      val errors = ArrayBuffer.empty[String]

      def day(key: String, value: String): Option[LocalDate] = parseDay(value).orElse {
        errors += s"$key='$value': must be a real date yyyyMMdd"
        None
      }

      val from = raw("BULK_FROM") match {
        case Some(v) => day("BULK_FROM", v)
        case None => errors += "BULK_FROM: is required (the first day of the bulk load, yyyyMMdd)"; None
      }
      // BULK_TO bỏ trống = một ngày.
      val to = raw("BULK_TO").fold(from)(v => day("BULK_TO", v))
      for (f <- from; t <- to) {
        if (f.isAfter(t)) errors += "BULK_FROM must not be after BULK_TO"
        else if (ChronoUnit.DAYS.between(f, t) + 1 > MaxDays) errors += s"a bulk load covers at most $MaxDays days (BULK_FROM..BULK_TO): split it into several runs"
      }
      val missing = raw("BULK_MISSING_DAY").fold(MissingDay.Fail: MissingDay) { v =>
        MissingDay.parse(v).getOrElse {
          errors += s"BULK_MISSING_DAY='$v': must be one of ${MissingDay.All.map(_.name).mkString(" / ")}"
          MissingDay.Fail
        }
      }

      if (!raw("INPUT_PATH").exists(_.contains(DayToken))) {
        errors += s"INPUT_PATH: must contain $DayToken (where the day goes, e.g. hdfs://.../f001-mpre-call/date=$DayToken) with a bulk load"
      }
      raw("BATCH_VALUE").foreach(_ => errors += "BATCH_VALUE: not used with a bulk load (the batch of each day is the day itself)")
      raw("BATCH_HOURS").foreach(_ => errors += "BATCH_HOURS: not used with a bulk load (every day is loaded whole)")
      raw("OUTPUT_MODE").filterNot(_.equalsIgnoreCase(IngestConfig.OverwritePartitions)).foreach { _ =>
        errors += s"OUTPUT_MODE: a bulk load needs ${IngestConfig.OverwritePartitions} (a day that is already loaded is replaced, never written twice)"
      }

      if (errors.nonEmpty) Left(errors.toList)
      else Right(Some(BulkLoad(format(from.get), format(to.get), missing)))
    }
  }

  /** Env của MỘT ngày của bulk: `INPUT_PATH` của ngày đó, `BATCH_VALUE` = ngày, `OUTPUT_MODE=overwrite-partitions`, không còn biến `BULK_*`. */
  def dayEnv(env: Map[String, String], day: String): Map[String, String] =
    (env -- Keys) ++ Map("INPUT_PATH" -> env("INPUT_PATH").trim.replace(DayToken, day), "BATCH_VALUE" -> day,
      "OUTPUT_MODE" -> IngestConfig.OverwritePartitions)
}
