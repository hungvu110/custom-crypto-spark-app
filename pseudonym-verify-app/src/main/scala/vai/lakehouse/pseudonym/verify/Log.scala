package vai.lakehouse.pseudonym.verify

import java.util.concurrent.ConcurrentLinkedQueue

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 * Logging helper cùng format với `Log` của spark-app / hdfs-load-stress-test (logger "VLP"). Khác ở 1 điểm: giữ lại
 * mọi dòng đã log của driver để check P7 quét xem khoá có bị log ra không.
 */
object Log {

  private val logger: Logger = LoggerFactory.getLogger("VLP")
  private val WIDTH = 84
  private val captured = new ConcurrentLinkedQueue[String]()

  private def emit(line: String, error: Boolean = false): Unit = {
    captured.add(line)
    if (error) logger.error(line) else logger.info(line)
  }

  private def pad(s: String, w: Int): String = if (s.length >= w) s else s + " " * (w - s.length)

  /** Mọi dòng đã log từ lần `resetCapture` gần nhất. */
  def capturedLines: Seq[String] = captured.asScala.toList

  def resetCapture(): Unit = captured.clear()

  def banner(title: String): Unit = {
    val line = "=" * WIDTH
    emit("")
    emit(line)
    emit(pad(s"  $title", WIDTH))
    emit(line)
  }

  def section(title: String): Unit = {
    emit("")
    emit("-" * WIDTH)
    emit(s"  $title")
    emit("-" * WIDTH)
  }

  def kv(key: String, value: Any): Unit = emit(s"    ${pad(key, 34)} : $value")

  def info(msg: String): Unit = emit(s"    $msg")
  def ok(msg: String): Unit   = emit(s"    [ OK ]   $msg")
  def fail(msg: String): Unit = emit(s"    [ FAIL ] $msg", error = true)

  def fail(msg: String, t: Throwable): Unit = {
    captured.add(msg)
    logger.error(s"    [ FAIL ] $msg", t)
  }

  /** In cây nguyên nhân của exception — hữu ích cho lỗi HDFS/Kerberos nhiều tầng. */
  def causeChain(t: Throwable): Unit = {
    var cur: Throwable = t
    var depth = 0
    while (cur != null && depth < 12) {
      val prefix = if (depth == 0) "    Exception   : " else "    " + ("  " * depth) + "Caused by : "
      emit(s"$prefix${cur.getClass.getName}: ${cur.getMessage}", error = true)
      cur = cur.getCause
      depth += 1
    }
  }

  def elapsed(label: String, startNanos: Long): Unit =
    emit(f"    ${pad(label, 34)} : ${(System.nanoTime() - startNanos) / 1000000L}%,d ms")
}
