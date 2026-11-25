package vai.lakehouse.pseudonymizetable.config

import java.net.URI

import scala.util.Try

/**
 * So một đường dẫn với một thư mục gốc (vd `OUTPUT_DB_ROOT` = `hdfs://<namenode>:8020/sandbox`): đường dẫn phải nằm HẲN dưới gốc
 * (`/sandbox/x`, không phải `/sandboxes/x`). Cùng scheme; máy chủ so không phân biệt hoa thường và chỉ so khi cả hai đều có (HMS có thể
 * trả location không kèm cổng, hoặc theo nameservice). Không đọc file system.
 */
object PathRoot {

  def under(path: String, root: String): Boolean = (for {
    p <- Try(new URI(path)).toOption
    r <- Try(new URI(root)).toOption
  } yield sameScheme(p, r) && sameHost(p, r) && inside(Option(p.getPath).getOrElse(""), Option(r.getPath).getOrElse(""))).getOrElse(false)

  private def sameScheme(p: URI, r: URI): Boolean = Option(p.getScheme).map(_.toLowerCase) == Option(r.getScheme).map(_.toLowerCase)

  private def sameHost(p: URI, r: URI): Boolean = (Option(p.getHost), Option(r.getHost)) match {
    case (Some(a), Some(b)) => a.equalsIgnoreCase(b)
    case _ => true
  }

  private def inside(path: String, root: String): Boolean = {
    val base = root.stripSuffix("/")
    base.nonEmpty && path.stripSuffix("/").startsWith(base + "/")
  }
}
