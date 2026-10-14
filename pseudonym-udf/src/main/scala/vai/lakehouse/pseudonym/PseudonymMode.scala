package vai.lakehouse.pseudonym

import java.util.Locale

/**
 * 3 cách tạo định danh giả theo mục 2.3 tài liệu khử định danh: băm có salt, mã hoá, bảng ánh xạ.
 */
sealed abstract class PseudonymMode(val name: String)

object PseudonymMode {
  /** HMAC-SHA256 có khoá bí mật — một chiều. Output `k<version>_<32 hex>`. */
  case object Hash extends PseudonymMode("HASH")
  /** AES-256-SIV — mã hoá tất định, giải mã lại được bằng khoá. Output `e<version>_<base64url>`. */
  case object Encrypt extends PseudonymMode("ENCRYPT")
  /** Token ngẫu nhiên + bảng ánh xạ Delta lưu riêng. Output `t_<32 hex>`. */
  case object Mapping extends PseudonymMode("MAPPING")

  val All: Seq[PseudonymMode] = Seq(Hash, Encrypt, Mapping)

  def parse(raw: String): PseudonymMode = {
    val name = Option(raw).map(_.trim.toUpperCase(Locale.ROOT)).getOrElse("")
    All.find(_.name == name).getOrElse(throw new IllegalArgumentException(
      s"Invalid pseudonym mode '$raw': must be one of ${All.map(_.name).mkString(", ")}"))
  }
}
