package vai.lakehouse.keyprefix

import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal

/**
 * Nguồn cung cấp keyPrefix theo tên bảng. Chỉ driver gọi (executor nhận prefix qua plan).
 * Mọi cài đặt phải: kiểm tra tên bảng hợp lệ trước khi dùng, và KHÔNG đưa giá trị prefix,
 * token hay JWT vào thông báo lỗi.
 */
trait PrefixSource {
  def read(datasetName: String): String
}

/** Mỗi bảng 1 file `<dir>/<tên bảng>` (thường là thư mục mount từ K8s Secret). */
class FilePrefixSource(dir: String) extends PrefixSource {
  override def read(datasetName: String): String = PrefixFiles.readPrefix(dir, datasetName)
}

/**
 * Thử lần lượt nhiều nguồn, dùng kết quả của nguồn đầu tiên thành công (vd `file` rồi mới `vault`).
 * Tên bảng sai bị từ chối ngay, không thử nguồn nào. Nếu mọi nguồn đều lỗi thì gộp lý do của từng nguồn.
 */
class ChainedPrefixSource(sources: Seq[(String, PrefixSource)]) extends PrefixSource {
  require(sources.nonEmpty, "ChainedPrefixSource needs at least one source")

  override def read(datasetName: String): String = {
    PrefixFiles.requireValidDatasetName(datasetName)
    val failures = Seq.newBuilder[String]
    val it = sources.iterator
    while (it.hasNext) {
      val (name, source) = it.next()
      try return source.read(datasetName)
      catch { case NonFatal(e) => failures += s"$name: ${e.getMessage}" }
    }
    throw new IllegalStateException(
      s"Cannot get keyPrefix for dataset '$datasetName' from any source [${failures.result().mkString("; ")}]")
  }
}

/**
 * Cache keyPrefix trong bộ nhớ theo tên bảng, hết hạn sau `ttlNanos`. Trong sql-engine mỗi câu
 * query đều phải tra prefix nên nếu không cache thì mỗi query tốn 1 vòng login/read/revoke Vault.
 * Không cache kết quả lỗi. `nanoTime` được tiêm vào để test không phải ngủ.
 */
class CachedPrefixSource(
  delegate: PrefixSource,
  ttlNanos: Long,
  nanoTime: () => Long = () => System.nanoTime()
) extends PrefixSource {

  private case class Entry(prefix: String, expiresAt: Long)
  private val entries = new ConcurrentHashMap[String, Entry]()

  override def read(datasetName: String): String = {
    val cached = entries.get(datasetName)
    if (cached != null && nanoTime() - cached.expiresAt < 0) cached.prefix
    else {
      val prefix = delegate.read(datasetName)
      entries.put(datasetName, Entry(prefix, nanoTime() + ttlNanos))
      prefix
    }
  }
}
