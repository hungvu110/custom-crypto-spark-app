package vai.lakehouse.columncrypto.cdr.audit

/** Dựng chuỗi cho audit: escape JSON, mô tả biểu thức mà không lộ giá trị, giới hạn độ dài. */
object AuditText {

  /** Độ dài tối đa của `valueExpr`/`keyFieldExpr` trong event. */
  val MaxExprLength = 200

  /** Độ dài tối đa của các chuỗi tự do khác (user, statementId). */
  val MaxTextLength = 256

  // Literal chuỗi như Expression.sql in ra: '...' với \' và \\ đã được escape.
  private val StringLiteral = """'(?:[^'\\]|\\.)*'""".r

  /** `Expression.sql` đã che mọi literal chuỗi thành `'***'` và cắt còn [[MaxExprLength]] ký tự. */
  def describeExpr(sql: String): String = truncate(StringLiteral.replaceAllIn(sql, "'***'"), MaxExprLength)

  def limit(text: String): String = truncate(text, MaxTextLength)

  private def truncate(text: String, max: Int): String = if (text.length <= max) text else text.take(max) + "…"

  /** Chuỗi JSON (gồm cả dấu nháy kép) của `s`. Ký tự điều khiển và U+2028/U+2029 được escape để một event luôn là một dòng. */
  def quote(s: String): String = {
    val sb = new StringBuilder(s.length + 2).append('"')
    s.foreach {
      case '"'  => sb.append("\\\"")
      case '\\' => sb.append("\\\\")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case c if c < 0x20 || c.toInt == 0x2028 || c.toInt == 0x2029 => sb.append(f"\\u${c.toInt}%04x")
      case c => sb.append(c)
    }
    sb.append('"').toString
  }
}
