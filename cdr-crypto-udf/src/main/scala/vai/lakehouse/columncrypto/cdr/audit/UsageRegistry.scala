package vai.lakehouse.columncrypto.cdr.audit

/** Nguồn số đếm của một usage (accumulator của Spark khi chạy thật). Chỉ đọc trên driver. */
trait CountSource {
  def snapshot: UsageCounts
}

/**
 * Một lần dùng `cdr_encrypt`/`cdr_decrypt` (một lần builder chạy). Một DataFrame dùng lại cho nhiều action thì nhiều execution cùng
 * trỏ về một usage, nên usage được giữ đến khi rảnh quá TTL chứ không xoá ngay sau END. Thời gian là `System.nanoTime` (tiêm khi test).
 */
final class Usage(
  val info: UsageInfo,
  val statementId: Option[String],
  val ugiUser: Option[String],
  counts: Option[CountSource],
  createdAtNanos: Long) {

  private var lastUsedNanos = createdAtNanos
  private var inFlight = 0
  private var executions = 0L
  private var reported = UsageCounts.Zero

  def usageId: String = info.usageId

  def begin(nowNanos: Long): Unit = synchronized {
    inFlight += 1
    executions += 1
    lastUsedNanos = nowNanos
  }

  /** Kết thúc một execution; trả về số đếm phát sinh kể từ lần báo trước (`None` nếu usage không được đếm). */
  def end(nowNanos: Long): Option[UsageCounts] = synchronized {
    inFlight = math.max(0, inFlight - 1)
    lastUsedNanos = nowNanos
    takeDelta()
  }

  def takeDelta(): Option[UsageCounts] = synchronized {
    counts.map { source =>
      val current = source.snapshot
      val delta = current.minus(reported)
      reported = current
      delta
    }
  }

  def wasExecuted: Boolean = synchronized(executions > 0)

  private[audit] def running: Boolean = synchronized(inFlight > 0)

  private[audit] def idleLongerThan(nowNanos: Long, ttlNanos: Long): Boolean =
    synchronized(inFlight == 0 && nowNanos - lastUsedNanos > ttlNanos)
}

/** Các usage còn sống trên driver, giữ theo thứ tự đăng ký; giới hạn bởi TTL và `maxUsages` để Thrift Server chạy lâu không phình bộ nhớ. */
final class UsageRegistry(ttlNanos: Long, maxUsages: Int, nanoTime: () => Long) {

  private val usages = new java.util.LinkedHashMap[String, Usage]()

  /** Thêm usage; trả về các usage cũ bị đẩy ra vì vượt `maxUsages` (không đẩy usage đang chạy hay usage vừa thêm). */
  def register(usage: Usage): Seq[Usage] = synchronized {
    usages.put(usage.usageId, usage)
    val evicted = Seq.newBuilder[Usage]
    var excess = usages.size - maxUsages
    val it = usages.values.iterator
    while (excess > 0 && it.hasNext) {
      val candidate = it.next()
      if ((candidate ne usage) && !candidate.running) {
        it.remove()
        evicted += candidate
        excess -= 1
      }
    }
    evicted.result()
  }

  def get(usageId: String): Option[Usage] = synchronized(Option(usages.get(usageId)))

  def size: Int = synchronized(usages.size)

  /** Gỡ và trả về các usage đã rảnh quá TTL. */
  def sweep(): Seq[Usage] = synchronized {
    val now = nanoTime()
    val removed = Seq.newBuilder[Usage]
    val it = usages.values.iterator
    while (it.hasNext) {
      val usage = it.next()
      if (usage.idleLongerThan(now, ttlNanos)) {
        it.remove()
        removed += usage
      }
    }
    removed.result()
  }
}

/** `statementId` (= `groupId` của Thrift Server) → user đăng nhập JDBC, cập nhật từ event của Thrift Server. */
final class StatementUserTracker(ttlNanos: Long, maxEntries: Int, nanoTime: () => Long) {

  private val users = new java.util.LinkedHashMap[String, (String, Long)]()

  def put(statementId: String, user: String): Unit = synchronized {
    users.remove(statementId)
    users.put(statementId, (user, nanoTime()))
    val it = users.keySet.iterator
    while (users.size > maxEntries && it.hasNext) {
      it.next()
      it.remove()
    }
  }

  def get(statementId: String): Option[String] = synchronized(Option(users.get(statementId)).map(_._1))

  def remove(statementId: String): Unit = synchronized { users.remove(statementId); () }

  def sweep(): Unit = synchronized {
    val now = nanoTime()
    val it = users.values.iterator
    while (it.hasNext) if (now - it.next()._2 > ttlNanos) it.remove()
  }
}
