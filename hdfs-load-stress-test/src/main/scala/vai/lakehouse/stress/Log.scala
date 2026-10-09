package vai.lakehouse.stress

import org.slf4j.{Logger, LoggerFactory}

/**
 * Logging helper cho output dễ đọc, dễ trace bằng mắt trong `kubectl logs` — cùng format với `org.example.Log`
 * của spark-app. Logger name cố định là "VLP": log4j2.properties để riêng logger này ở INFO, còn Spark/Hadoop bị
 * hạ xuống WARN/ERROR, nên output chỉ còn phần quan trọng của app.
 */
object Log {

  private val logger: Logger = LoggerFactory.getLogger("VLP")
  private val WIDTH = 84

  private def pad(s: String, w: Int): String =
    if (s.length >= w) s else s + " " * (w - s.length)

  /** Khung đôi — dùng cho tiêu đề toàn job. */
  def banner(title: String): Unit = {
    val line = "=" * WIDTH
    logger.info("")
    logger.info(line)
    logger.info(pad(s"  $title", WIDTH))
    logger.info(line)
  }

  /** Khung đơn — dùng cho từng giai đoạn. */
  def section(title: String): Unit = {
    logger.info("")
    logger.info("-" * WIDTH)
    logger.info(s"  $title")
    logger.info("-" * WIDTH)
  }

  /** Cặp key-value canh cột. */
  def kv(key: String, value: Any): Unit =
    logger.info(s"    ${pad(key, 34)} : $value")

  def info(msg: String): Unit = logger.info(s"    $msg")
  def ok(msg: String): Unit   = logger.info(s"    [ OK ]   $msg")
  def warn(msg: String): Unit = logger.warn(s"    [ WARN ] $msg")
  def fail(msg: String): Unit = logger.error(s"    [ FAIL ] $msg")

  def fail(msg: String, t: Throwable): Unit = logger.error(s"    [ FAIL ] $msg", t)

  /** In cây nguyên nhân của exception — rất hữu ích cho lỗi HDFS/Kerberos nhiều tầng. */
  def causeChain(t: Throwable): Unit = {
    var cur: Throwable = t
    var depth = 0
    while (cur != null && depth < 12) {
      val prefix = if (depth == 0) "    Exception   : " else "    " + ("  " * depth) + "Caused by : "
      logger.error(s"$prefix${cur.getClass.getName}: ${cur.getMessage}")
      cur = cur.getCause
      depth += 1
    }
  }

  def elapsed(label: String, startNanos: Long): Unit = {
    val ms = (System.nanoTime() - startNanos) / 1000000L
    logger.info(f"    ${pad(label, 34)} : $ms%,d ms")
  }
}
