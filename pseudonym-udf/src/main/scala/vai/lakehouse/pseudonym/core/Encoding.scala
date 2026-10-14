package vai.lakehouse.pseudonym.core

import java.util.Base64

/** Mã hoá byte -> chuỗi dùng chung cho các chế độ giả danh. */
private[pseudonym] object Encoding {

  private val HexDigits = "0123456789abcdef".toCharArray

  /** Hex chữ thường của `length` byte đầu tiên. */
  def hex(bytes: Array[Byte], length: Int): String = {
    val out = new Array[Char](length * 2)
    var i = 0
    while (i < length) {
      out(2 * i) = HexDigits((bytes(i) >> 4) & 0xf)
      out(2 * i + 1) = HexDigits(bytes(i) & 0xf)
      i += 1
    }
    new String(out)
  }

  def hex(bytes: Array[Byte]): String = hex(bytes, bytes.length)

  def unhex(s: String): Array[Byte] = {
    require(s.length % 2 == 0 && s.forall(c => Character.digit(c, 16) >= 0), "not a valid hex string")
    s.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
  }

  def base64Url(bytes: Array[Byte]): String = Base64.getUrlEncoder.withoutPadding().encodeToString(bytes)

  def unbase64Url(s: String): Array[Byte] = Base64.getUrlDecoder.decode(s)
}
