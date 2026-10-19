package vai.lakehouse.columncrypto.cdr.audit

import scala.util.control.NonFatal

/** Nơi ghi event audit. `write` được phép ném lỗi: [[AuditDispatcher]] bắt và chỉ cảnh báo, không để lỗi lan tới query. */
trait AuditSink {
  def write(events: Seq[AuditEvent]): Unit
  def close(): Unit = ()
}

/** Ghi lần lượt vào nhiều sink (`sinks=log,http`): sink này lỗi vẫn ghi sink kia, rồi ném lỗi đầu tiên (các lỗi sau ở `suppressed`). */
final class CompositeAuditSink(sinks: Seq[AuditSink]) extends AuditSink {

  override def write(events: Seq[AuditEvent]): Unit = {
    val errors = sinks.flatMap { sink =>
      try { sink.write(events); None }
      catch { case NonFatal(e) => Some(e) }
    }
    errors.headOption.foreach { first =>
      errors.tail.foreach(first.addSuppressed)
      throw first
    }
  }

  override def close(): Unit = sinks.foreach(sink => try sink.close() catch { case NonFatal(_) => () })
}
