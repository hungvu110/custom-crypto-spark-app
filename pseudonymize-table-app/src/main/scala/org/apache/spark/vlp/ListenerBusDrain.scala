package org.apache.spark.vlp

import org.apache.spark.SparkContext

/**
 * Đợi listener bus xử lý hết sự kiện đang chờ (`waitUntilEmpty` là `private[spark]`), để số liệu task metrics đọc sau khi
 * một job kết thúc là đầy đủ chứ không thiếu các `onTaskEnd` còn nằm trong hàng đợi.
 */
object ListenerBusDrain {
  def drain(sc: SparkContext, timeoutMillis: Long = 10000L): Unit = sc.listenerBus.waitUntilEmpty(timeoutMillis)
}
