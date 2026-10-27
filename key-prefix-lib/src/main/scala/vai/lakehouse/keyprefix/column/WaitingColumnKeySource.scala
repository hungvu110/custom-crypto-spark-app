package vai.lakehouse.keyprefix

import java.util.concurrent.TimeUnit

import scala.util.Try

import org.slf4j.LoggerFactory

/**
 * Cách chờ khi DAK đã mở yêu cầu key (YC) cho cột còn thiếu: gọi lại sau `initialSeconds`, mỗi lần sau nhân `multiplier`, tối đa
 * `maxIntervalSeconds` một lần, dừng sau tổng `timeoutSeconds`. `timeoutSeconds = 0` = không chờ.
 */
final case class PendingWait(timeoutSeconds: Long, initialSeconds: Long, maxIntervalSeconds: Long, multiplier: Double) {
  def enabled: Boolean = timeoutSeconds > 0
}

object PendingWait {

  val Disabled: PendingWait = PendingWait(0, 30, 300, 2.0)

  import PrefixSourceFactory.Keys

  /**
   * Đọc `dak.pendingWait.*`; mặc định TẮT (`timeoutSeconds = 0`): engine SQL không được treo câu lệnh chờ người duyệt, chỉ job batch
   * (vd chuẩn hoá silver) bật. Sai thì `IllegalArgumentException` nêu tên vật lý của key.
   */
  def load(cfg: ConfigSource): PendingWait = {
    def number[T](key: String, default: T, parse: String => T, ok: T => Boolean, rule: String): T =
      cfg.get(key).map { raw =>
        Try(parse(raw)).toOption.filter(ok).getOrElse(throw new IllegalArgumentException(s"Invalid ${cfg.describe(key)}: $rule"))
      }.getOrElse(default)

    val timeout = number(Keys.DakPendingTimeoutSeconds, Disabled.timeoutSeconds, _.toLong, (v: Long) => v >= 0 && v <= 86400,
      "must be a number of seconds in 0..86400 (0 = do not wait)")
    val initial = number(Keys.DakPendingInitialSeconds, Disabled.initialSeconds, _.toLong, (v: Long) => v >= 1 && v <= 3600,
      "must be a number of seconds in 1..3600")
    val max = number(Keys.DakPendingMaxIntervalSeconds, math.max(Disabled.maxIntervalSeconds, initial), _.toLong,
      (v: Long) => v >= initial && v <= 3600, s"must be a number of seconds in $initial..3600 (not below the initial wait)")
    val multiplier = number(Keys.DakPendingMultiplier, Disabled.multiplier, _.toDouble, (v: Double) => v >= 1.0 && v <= 10.0,
      "must be a number in 1..10")
    PendingWait(timeout, initial, max, multiplier)
  }
}

/**
 * Bọc nguồn key cột để CHỜ khi DAK đã mở yêu cầu key (docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md mục 3.2, luồng đã thống nhất với
 * DAK): lời gọi trả cột `PENDING` (`onMissing=request`) thì gọi lại theo exponential backoff cho tới khi admin DAK duyệt và mọi cột
 * `READY`, hoặc tới khi hết `timeoutSeconds` — khi đó ném lại [[ColumnKeysUnavailableException]] của lần gọi cuối.
 *
 * Chỉ chờ khi MỌI cột chưa dùng được đều `PENDING`. Có cột `REJECTED`, `DISABLED` hay `MISSING` thì không bao giờ tự thành `READY`, nên
 * ném ngay. `sleep`/`nanoTime` được tiêm vào để test không phải ngủ.
 */
class WaitingColumnKeySource(
  delegate: ColumnKeySource,
  wait: PendingWait,
  sleep: Long => Unit = millis => Thread.sleep(millis),
  nanoTime: () => Long = () => System.nanoTime()
) extends ColumnKeySource {

  private val logger = LoggerFactory.getLogger(classOf[WaitingColumnKeySource])

  override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
    val deadline = nanoTime() + TimeUnit.SECONDS.toNanos(wait.timeoutSeconds)
    var interval = wait.initialSeconds.toDouble
    var attempt = 1
    while (true) {
      try return delegate.readColumns(datasetName, columns, intent)
      catch {
        case e: ColumnKeysUnavailableException if e.issues.forall(_.state == "PENDING") =>
          val remainingSeconds = TimeUnit.NANOSECONDS.toSeconds(deadline - nanoTime())
          if (remainingSeconds <= 0) {
            logger.warn(s"DAK key requests for '$datasetName' are still PENDING after ${wait.timeoutSeconds} s: giving up")
            throw e
          }
          val waitSeconds = math.min(interval.toLong, remainingSeconds)
          val codes = e.issues.flatMap(i => i.requestCode.map(c => s"${i.column}=$c")).mkString(", ")
          // WARN để hiện trong log của job (Spark để root logger ở WARN).
          logger.warn(s"Waiting for DAK to approve the key requests of '$datasetName' ($codes): attempt $attempt, " +
            s"next check in $waitSeconds s, giving up in $remainingSeconds s")
          sleep(TimeUnit.SECONDS.toMillis(waitSeconds))
          interval = math.min(interval * wait.multiplier, wait.maxIntervalSeconds.toDouble)
          attempt += 1
      }
    }
    throw new IllegalStateException("unreachable")
  }
}
