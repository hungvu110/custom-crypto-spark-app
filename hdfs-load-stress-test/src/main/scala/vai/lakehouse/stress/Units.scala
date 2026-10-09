package vai.lakehouse.stress

import java.util.Locale

object Units {
  val MiB: Double = 1024.0 * 1024.0
  val GiB: Double = MiB * 1024.0

  /** 3 chữ số thập phân, luôn dùng dấu chấm (Locale.ROOT) để JSON hợp lệ bất kể locale của pod. */
  def fmt(value: Double): String = String.format(Locale.ROOT, "%.3f", Double.box(value))

  /** Định dạng cho log người đọc (Log.kv). */
  def gib(bytes: Long): String = String.format(Locale.ROOT, "%,.2f GiB", Double.box(bytes / GiB))
  def mib(bytes: Long): String = String.format(Locale.ROOT, "%,.1f MiB", Double.box(bytes / MiB))
  def secs(seconds: Double): String = String.format(Locale.ROOT, "%,.1f s", Double.box(seconds))
  def mibps(value: Double): String = String.format(Locale.ROOT, "%,.1f MiB/s", Double.box(value))
}
