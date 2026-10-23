package vai.lakehouse.keyprefix

import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

/** Mục đích của lời gọi, quyết định `onMissing` gửi lên DAK. */
sealed abstract class KeyIntent(val onMissing: String)

object KeyIntent {
  /** Mã hoá: cột chưa có key là chuyện bình thường, DAK mở YC (`onMissing=request`). */
  case object Encrypt extends KeyIntent("request")

  /** Giải mã: cột chưa có key là lỗi cấu hình, DAK không mở YC (`onMissing=none`). */
  case object Decrypt extends KeyIntent("none")
}

/**
 * Nguồn key theo CỘT (mỗi cột một key). Khác [[PrefixSource]] (một key cho cả bảng). Chỉ driver gọi.
 *
 * Trả key của MỌI cột được hỏi, khoá là tên cột chữ thường; chỉ cần một cột chưa dùng được là ném
 * [[ColumnKeysUnavailableException]] cho cả lời gọi — không bao giờ trả một nửa số key.
 */
trait ColumnKeySource {
  def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String]
}

/**
 * Cache key theo `(bảng, cột)` trong bộ nhớ, hết hạn sau `ttlNanos`. Cache miss chỉ hỏi nguồn thật cho CÁC CỘT CÒN THIẾU,
 * gộp một lời gọi. Không cache khi lời gọi lỗi. Key cột không bao giờ đổi (DAK vòng 6), nên TTL chỉ giới hạn thời gian một
 * key đã bị khoá trên DAK vẫn còn dùng được trong engine. `nanoTime` được tiêm vào để test không phải ngủ.
 */
class CachedColumnKeySource(
  delegate: ColumnKeySource,
  ttlNanos: Long,
  nanoTime: () => Long = () => System.nanoTime()
) extends ColumnKeySource {

  private final case class Entry(key: String, expiresAt: Long) {
    override def toString: String = "Entry(***)"
  }

  private val entries = new ConcurrentHashMap[(String, String), Entry]()

  override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
    val table = lower(datasetName)
    val names = columns.map(lower).distinct
    val now = nanoTime()
    val cached = names.flatMap { c =>
      Option(entries.get((table, c))).filter(e => now - e.expiresAt < 0).map(e => c -> e.key)
    }.toMap
    val missing = names.filterNot(cached.contains)
    if (missing.isEmpty) cached
    else {
      val fetched = delegate.readColumns(datasetName, missing, intent)
      val expiresAt = nanoTime() + ttlNanos
      fetched.foreach { case (c, key) => entries.put((table, c), Entry(key, expiresAt)) }
      cached ++ fetched
    }
  }

  private def lower(s: String): String = Option(s).getOrElse("").toLowerCase(Locale.ROOT)
}
