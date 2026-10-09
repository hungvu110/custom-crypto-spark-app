package vai.lakehouse.stress

import java.net.URI
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

sealed abstract class Mode(val name: String)

object Mode {
  /** Tải danh định: chia đều STRESS_GIB_PER_DAY theo chu kỳ BATCH_INTERVAL_SECONDS. */
  case object Sustained extends Mode("sustained")
  /** Năng lực tối đa: ghi TOTAL_GIB nhanh nhất có thể, mỗi batch BATCH_GIB. */
  case object Burst extends Mode("burst")

  val All: Seq[Mode] = Seq(Sustained, Burst)

  def parse(raw: String): Option[Mode] = All.find(_.name == raw.trim.toLowerCase(Locale.ROOT))
}

/**
 * Cấu hình của job, đọc từ biến môi trường của driver (`spec.driver.env` trong SparkApplication).
 * Dung lượng tính theo GiB (1024^3 byte), cùng đơn vị với `hdfs dfs -du -h`.
 */
final case class StressConfig(
    appName: String,
    mode: Mode,
    runId: String,
    outputPath: String,
    gibPerDay: Double,
    batchIntervalSeconds: Int,
    durationHours: Double,
    totalGib: Double,
    batchGib: Double,
    payloadBytes: Int,
    targetFileMib: Int,
    compression: String,
    verifyRead: Boolean,
    retainBatches: Int,
    requirePatch: Boolean) {

  /** Thư mục của lần chạy. Mọi batch và metric nằm dưới đây nên chạy lại cùng RUN_ID sẽ tiếp tục (resume). */
  def runPath: String = s"${outputPath.stripSuffix("/")}/$runId"

  def totalBytes: Long = (totalGib * Units.GiB).toLong

  /** Dung lượng mục tiêu của 1 batch: sustained = tốc độ/ngày chia theo chu kỳ, burst = BATCH_GIB. */
  def bytesPerBatch: Long = mode match {
    case Mode.Sustained =>
      math.max(1L, (gibPerDay * Units.GiB * batchIntervalSeconds / StressConfig.SecondsPerDay).toLong)
    case Mode.Burst => math.max(1L, (batchGib * Units.GiB).toLong)
  }

  def targetFileBytes: Long = targetFileMib.toLong * 1024L * 1024L

  /** Chỉ sustained có giới hạn thời gian; DURATION_HOURS=0 nghĩa là chạy tới khi đủ TOTAL_GIB. */
  def durationMillis: Option[Long] =
    if (mode == Mode.Sustained && durationHours > 0) Some((durationHours * 3600 * 1000).toLong) else None

  def describe: Seq[(String, String)] = Seq(
    "APP_NAME"                  -> appName,
    "MODE"                      -> mode.name,
    "RUN_ID"                    -> runId,
    "run path"                  -> runPath,
    "STRESS_GIB_PER_DAY"        -> (if (mode == Mode.Sustained) Units.fmt(gibPerDay) else "(không dùng)"),
    "BATCH_INTERVAL_SECONDS"    -> (if (mode == Mode.Sustained) batchIntervalSeconds.toString else "(không dùng)"),
    "DURATION_HOURS"            -> (if (mode == Mode.Sustained) Units.fmt(durationHours) else "(không dùng)"),
    "BATCH_GIB"                 -> (if (mode == Mode.Burst) Units.fmt(batchGib) else "(không dùng)"),
    "TOTAL_GIB"                 -> Units.fmt(totalGib),
    "bytes/batch (MiB)"         -> Units.fmt(bytesPerBatch / Units.MiB),
    "PAYLOAD_BYTES"             -> payloadBytes.toString,
    "TARGET_FILE_MIB"           -> targetFileMib.toString,
    "COMPRESSION"               -> compression,
    "VERIFY_READ"               -> verifyRead.toString,
    "RETAIN_BATCHES"            -> retainBatches.toString,
    "REQUIRE_BLOCK_TOKEN_PATCH" -> requirePatch.toString)
}

object StressConfig {
  val SecondsPerDay = 86400

  private val RunIdPattern = "^[A-Za-z0-9_-]{1,64}$"
  private val Compressions = Seq("snappy", "zstd", "gzip", "lz4", "none")

  /** Gom MỌI lỗi cấu hình rồi trả về một lượt, để sửa manifest một lần là đủ. */
  def fromEnv(env: Map[String, String]): Either[Seq[String], StressConfig] = {
    val errors = ArrayBuffer.empty[String]

    def raw(key: String): Option[String] = env.get(key).map(_.trim).filter(_.nonEmpty)

    def parsed[T](key: String, default: T, rule: String)(parse: String => Option[T]): T =
      raw(key) match {
        case None => default
        case Some(value) =>
          parse(value).getOrElse {
            errors += s"$key='$value': $rule"
            default
          }
      }

    def double(key: String, default: Double, rule: String)(ok: Double => Boolean): Double =
      parsed(key, default, rule)(v => Try(v.toDouble).toOption.filter(d => !d.isNaN && !d.isInfinite && ok(d)))

    def int(key: String, default: Int, rule: String)(ok: Int => Boolean): Int =
      parsed(key, default, rule)(v => Try(v.toInt).toOption.filter(ok))

    def bool(key: String, default: Boolean): Boolean =
      parsed(key, default, "phải là true hoặc false")(v =>
        v.toLowerCase(Locale.ROOT) match {
          case "true"  => Some(true)
          case "false" => Some(false)
          case _       => None
        })

    val mode = parsed[Mode]("MODE", Mode.Sustained, s"phải là ${Mode.All.map(_.name).mkString(" / ")}")(Mode.parse)

    val runId = raw("RUN_ID") match {
      case Some(v) if v.matches(RunIdPattern) => v
      case Some(v) => errors += s"RUN_ID='$v': chỉ gồm [A-Za-z0-9_-], tối đa 64 ký tự"; ""
      case None    => errors += "RUN_ID: bắt buộc (tên lần chạy, vd sustained-20261010)"; ""
    }

    val outputPath = raw("OUTPUT_PATH") match {
      case Some(v) if isAbsoluteUri(v) => v
      case Some(v) => errors += s"OUTPUT_PATH='$v': phải là URI tuyệt đối có scheme, vd hdfs://<namenode>:8020/<path>"; ""
      case None    => errors += "OUTPUT_PATH: bắt buộc"; ""
    }

    val gibPerDay     = double("STRESS_GIB_PER_DAY", 100.0, "số > 0")(_ > 0)
    val interval      = int("BATCH_INTERVAL_SECONDS", 300, s"số nguyên 10..$SecondsPerDay")(i => i >= 10 && i <= SecondsPerDay)
    val durationHours = double("DURATION_HOURS", 24.0, "số >= 0 (0 = không giới hạn thời gian)")(_ >= 0)
    val batchGib      = double("BATCH_GIB", 5.0, "số > 0")(_ > 0)

    val defaultTotal = mode match {
      case Mode.Sustained => gibPerDay * durationHours / 24
      case Mode.Burst     => 100.0
    }
    val totalGib = double("TOTAL_GIB", defaultTotal, "số > 0")(_ > 0)
    if (raw("TOTAL_GIB").isEmpty && totalGib <= 0) {
      errors += "TOTAL_GIB: bắt buộc khi MODE=sustained và DURATION_HOURS=0"
    }

    val payloadBytes  = int("PAYLOAD_BYTES", 1024, "số nguyên 64..1048576")(p => p >= 64 && p <= 1048576)
    val targetFileMib = int("TARGET_FILE_MIB", 128, "số nguyên 1..2048")(m => m >= 1 && m <= 2048)
    val compression = parsed("COMPRESSION", "snappy", s"phải là ${Compressions.mkString(" / ")}")(v =>
      Some(v.toLowerCase(Locale.ROOT)).filter(Compressions.contains))
    val verifyRead    = bool("VERIFY_READ", default = false)
    val retainBatches = int("RETAIN_BATCHES", 0, "số nguyên >= 0 (0 = giữ hết)")(_ >= 0)
    val requirePatch  = bool("REQUIRE_BLOCK_TOKEN_PATCH", default = true)
    val appName       = raw("APP_NAME").getOrElse("HdfsLoadStressTest")

    if (errors.nonEmpty) Left(errors.toList)
    else
      Right(StressConfig(appName, mode, runId, outputPath, gibPerDay, interval, durationHours, totalGib, batchGib,
        payloadBytes, targetFileMib, compression, verifyRead, retainBatches, requirePatch))
  }

  private def isAbsoluteUri(value: String): Boolean =
    Try(new URI(value)).toOption.exists(u => u.getScheme != null && Option(u.getPath).exists(_.startsWith("/")))
}
