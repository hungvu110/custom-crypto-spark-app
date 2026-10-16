package vai.lakehouse.pseudonymizetable.logging

import scala.collection.mutable.ArrayBuffer

/** Ghi lại thời gian từng bước của job (đơn vị giây) và in bảng tổng kết ở cuối. */
object Timings {

  private val steps = ArrayBuffer.empty[(String, Double)]

  def reset(): Unit = steps.synchronized(steps.clear())

  def record(label: String, seconds: Double): Unit = steps.synchronized { steps += label -> seconds }

  /** Chạy `body`, log `label : x.x s` và ghi vào bảng tổng kết (kể cả khi `body` ném exception). */
  def time[T](label: String)(body: => T): T = {
    val start = System.nanoTime()
    try body
    finally {
      val seconds = (System.nanoTime() - start) / 1e9
      record(label, seconds)
      Log.kv(s"$label [duration]", f"$seconds%,.1f s")
    }
  }

  def summary(totalSeconds: Double): Unit = {
    val snapshot = steps.synchronized(steps.toList)
    if (snapshot.nonEmpty) {
      Log.section("STEP TIMINGS (seconds)")
      snapshot.foreach { case (label, s) =>
        val pct = if (totalSeconds > 0) s / totalSeconds * 100 else 0.0
        Log.kv(label, f"$s%,.1f s  ($pct%.0f%%)")
      }
      Log.kv("total", f"$totalSeconds%,.1f s")
    }
  }
}
