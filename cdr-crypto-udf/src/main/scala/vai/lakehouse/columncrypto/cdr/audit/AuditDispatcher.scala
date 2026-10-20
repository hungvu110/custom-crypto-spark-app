package vai.lakehouse.columncrypto.cdr.audit

import java.util.concurrent.{LinkedBlockingDeque, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

/**
 * Tách việc ghi audit khỏi đường chạy của query: hàng đợi có giới hạn + một thread nền (daemon) đẩy event sang sink ngay khi có
 * (đợi thêm tối đa `coalesceMs` để gom event đến sát nhau). Không bao giờ chặn caller: hàng đợi đầy thì bỏ event cũ nhất và ghi số
 * bị bỏ (`dropped`) vào event kế tiếp được ghi. Lỗi của sink chỉ được cảnh báo.
 */
final class AuditDispatcher(sink: AuditSink, maxQueueSize: Int, coalesceMs: Long) {

  private val log = LoggerFactory.getLogger(classOf[AuditDispatcher])
  private val queue = new LinkedBlockingDeque[AuditEvent](maxQueueSize)
  private val dropped = new AtomicLong()
  private val failed = new AtomicLong()
  @volatile private var stopping = false

  private val worker = new Thread(() => run(), "cdr-audit-dispatcher")
  worker.setDaemon(true)
  worker.start()

  def submit(event: AuditEvent): Unit =
    if (stopping) dropped.incrementAndGet()
    else while (!queue.offerLast(event)) if (queue.pollFirst() != null) dropped.incrementAndGet()

  /** Số batch sink đã ghi lỗi. */
  def failedBatches: Long = failed.get()

  /** Ngừng nhận event, chờ tối đa `timeoutMs` để ghi nốt hàng đợi rồi đóng sink. Gọi nhiều lần không sao. */
  def close(timeoutMs: Long): Unit = synchronized {
    if (!stopping) {
      stopping = true
      worker.join(math.max(1L, timeoutMs))
      if (worker.isAlive) log.warn(s"CDR audit: ${queue.size} event(s) still queued after waiting $timeoutMs ms on close")
      try sink.close() catch { case NonFatal(e) => log.warn("CDR audit: failed to close sink", e) }
    }
  }

  private def run(): Unit =
    try {
      while (!stopping || !queue.isEmpty) {
        val first = queue.pollFirst(100, TimeUnit.MILLISECONDS)
        if (first != null) {
          if (coalesceMs > 0 && !stopping) Thread.sleep(coalesceMs)
          val batch = new java.util.ArrayList[AuditEvent]()
          batch.add(first)
          queue.drainTo(batch)
          deliver(batch.asScala.toSeq)
        }
      }
    } catch { case _: InterruptedException => () }

  private def deliver(batch: Seq[AuditEvent]): Unit = {
    val lost = dropped.getAndSet(0)
    val events = if (lost > 0) batch.head.copy(dropped = lost) +: batch.tail else batch
    try sink.write(events)
    catch {
      case NonFatal(e) =>
        val total = failed.incrementAndGet()
        log.warn(s"CDR audit: failed to write ${events.size} event(s) (failed batches so far: $total)", e)
    }
  }
}
