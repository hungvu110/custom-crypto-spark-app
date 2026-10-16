package vai.lakehouse.pseudonymizetable.logging

import org.slf4j.{Logger, LoggerFactory}

/**
 * Logging helper cùng format với `Log` của spark-app (logger "VLP"): log4j2.properties để riêng
 * logger này ở INFO, còn Spark/Hadoop bị hạ xuống WARN, nên `kubectl logs` chỉ còn phần quan trọng của app.
 */
object Log {

  private val logger: Logger = LoggerFactory.getLogger("VLP")
  private val Width = 84

  private def pad(s: String, w: Int): String = if (s.length >= w) s else s + " " * (w - s.length)

  def banner(title: String): Unit = {
    val line = "=" * Width
    logger.info("")
    logger.info(line)
    logger.info(pad(s"  $title", Width))
    logger.info(line)
  }

  def section(title: String): Unit = {
    logger.info("")
    logger.info("-" * Width)
    logger.info(s"  $title")
    logger.info("-" * Width)
  }

  def kv(key: String, value: Any): Unit = logger.info(s"    ${pad(key, 34)} : $value")

  def info(msg: String): Unit = logger.info(s"    $msg")
  def ok(msg: String): Unit   = logger.info(s"    [ OK ]   $msg")
  def warn(msg: String): Unit = logger.warn(s"    [ WARN ] $msg")
  def fail(msg: String): Unit = logger.error(s"    [ FAIL ] $msg")

  /** In cây nguyên nhân của exception — hữu ích cho lỗi HDFS/Kerberos nhiều tầng. */
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

  /** Thời gian đã trôi qua kể từ `startNanos`, luôn tính bằng giây. */
  def elapsed(label: String, startNanos: Long): Unit =
    logger.info(f"    ${pad(label, 34)} : ${(System.nanoTime() - startNanos) / 1e9}%,.1f s")
}
