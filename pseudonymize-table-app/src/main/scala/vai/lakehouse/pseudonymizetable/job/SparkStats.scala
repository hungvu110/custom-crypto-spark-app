package vai.lakehouse.pseudonymizetable.job

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.vlp.ListenerBusDrain

/** Tổng số liệu task Spark trong một khoảng thời gian (hiệu của hai snapshot). Thời gian tính bằng giây. */
final case class TaskStats(tasks: Long, rowsRead: Long, bytesRead: Long, rowsWritten: Long, bytesWritten: Long,
                           runSeconds: Double, cpuSeconds: Double, gcSeconds: Double) {

  def -(o: TaskStats): TaskStats = TaskStats(tasks - o.tasks, rowsRead - o.rowsRead, bytesRead - o.bytesRead,
    rowsWritten - o.rowsWritten, bytesWritten - o.bytesWritten, runSeconds - o.runSeconds, cpuSeconds - o.cpuSeconds,
    gcSeconds - o.gcSeconds)

  private def mb(bytes: Long): String = f"${bytes / 1048576.0}%,.1f MB"

  def describe: String =
    f"tasks=$tasks%d | rows read=$rowsRead%,d (${mb(bytesRead)}) | rows written=$rowsWritten%,d (${mb(bytesWritten)}) | " +
      f"executor run=$runSeconds%,.1f s | cpu=$cpuSeconds%,.1f s | gc=$gcSeconds%,.1f s"
}

/** Cộng dồn task metrics của mọi job trong session (đăng ký bằng `sparkContext.addSparkListener`). */
final class SparkStats(sc: SparkContext) extends SparkListener {
  private val tasks = new AtomicLong()
  private val rowsRead = new AtomicLong()
  private val bytesRead = new AtomicLong()
  private val rowsWritten = new AtomicLong()
  private val bytesWritten = new AtomicLong()
  private val runMs = new AtomicLong()
  private val cpuNs = new AtomicLong()
  private val gcMs = new AtomicLong()

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val m = event.taskMetrics
    if (m != null) {
      tasks.incrementAndGet()
      rowsRead.addAndGet(m.inputMetrics.recordsRead)
      bytesRead.addAndGet(m.inputMetrics.bytesRead)
      rowsWritten.addAndGet(m.outputMetrics.recordsWritten)
      bytesWritten.addAndGet(m.outputMetrics.bytesWritten)
      runMs.addAndGet(m.executorRunTime)
      cpuNs.addAndGet(m.executorCpuTime)
      gcMs.addAndGet(m.jvmGCTime)
    }
  }

  private def snapshot: TaskStats = TaskStats(tasks.get, rowsRead.get, bytesRead.get, rowsWritten.get, bytesWritten.get,
    runMs.get / 1000.0, cpuNs.get / 1e9, gcMs.get / 1000.0)

  /** Snapshot sau khi listener bus đã xử lý hết sự kiện của các job vừa chạy xong. */
  def settled(): TaskStats = {
    ListenerBusDrain.drain(sc)
    snapshot
  }
}
