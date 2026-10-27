package vai.lakehouse.keyprefix

import java.net.URI

import scala.util.Try

/**
 * Key giải mã toàn bộ nội dung một file (hoặc mọi file dưới một thư mục) do DAK cấp qua `by-path`, theo ĐƯỜNG DẪN
 * (docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md mục 2). `toString` che key.
 *
 * @param keyVersion version DAK trả kèm, chỉ để ghi log; `None` nếu DAK không gửi
 */
final case class PathKey(path: String, keyPrefix: String, keyVersion: Option[Long]) {
  override def toString: String = s"PathKey(path=$path, keyPrefix=***, keyVersion=${keyVersion.getOrElse("-")})"
}

/** Kết quả hỏi key của một đường dẫn. */
sealed trait PathKeyResult

object PathKeyResult {

  /** DAK có key (`200`). */
  final case class Available(key: PathKey) extends PathKeyResult

  /**
   * DAK không có key cho đường dẫn (`204` rỗng, không mở yêu cầu — đã thống nhất với DAK, docs/key-access/TRA-LOI-DAK-BY-PATH.md mục 5).
   * Không phải lỗi: file của đường dẫn đó không mã hoá.
   */
  case object NotAvailable extends PathKeyResult
}

/** Nguồn key theo ĐƯỜNG DẪN (một đường dẫn một key). Chỉ driver gọi. */
trait PathKeySource {

  /** `path` (tương đối theo gốc đã thống nhất với DAK) được chuẩn hoá bằng [[PathKeySource.canonical]] trước khi gửi. */
  def readPath(path: String): PathKeyResult
}

object PathKeySource {

  /**
   * Dạng chuẩn của đường dẫn gửi cho `by-path`: TƯƠNG ĐỐI theo một gốc của nguồn (theo bảng, theo ngày, vd `vtnet_cdr/20261027`), gộp
   * `//` thừa, bỏ `/` đầu và cuối, GIỮ NGUYÊN hoa thường. DAK so khớp nguyên chuỗi (tài liệu mục 6), nên SA luôn gửi dạng này và người
   * tạo key ở màn quản trị DAK phải nhập đúng dạng này.
   */
  def canonical(raw: String): String =
    Option(raw).getOrElse("").trim.replaceAll("/{2,}", "/").stripPrefix("/").stripSuffix("/")

  /**
   * Phần của `path` nằm sau `root`, ở dạng chuẩn; `None` nếu `path` không nằm dưới `root` hoặc chính là `root`. Scheme và authority
   * (`hdfs://nn:8020`) của cả hai bị bỏ qua, nên viết gốc có hay không có scheme đều được.
   */
  def relativeTo(path: String, root: String): Option[String] = {
    val p = canonical(withoutScheme(path))
    val r = canonical(withoutScheme(root))
    if (r.isEmpty) Some(p).filter(_.nonEmpty)
    else if (p.startsWith(r + "/")) Some(p.substring(r.length + 1)).filter(_.nonEmpty)
    else None
  }

  private def withoutScheme(raw: String): String = {
    val value = Option(raw).getOrElse("").trim
    Try(new URI(value)).toOption.filter(u => u.getScheme != null && u.getRawPath != null).map(_.getRawPath).getOrElse(value)
  }
}
