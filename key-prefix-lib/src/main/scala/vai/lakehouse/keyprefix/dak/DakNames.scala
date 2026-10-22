package vai.lakehouse.keyprefix

import java.util.Locale

/** Định danh bảng gửi lên DAK: `database.table`, luôn chữ thường. */
final case class DakTableRef(database: String, table: String) {
  def qualified: String = s"$database.$table"
}

object DakTableRef {

  // Tên Hive không chứa dấu chấm nên tách được đúng 2 phần; bộ ký tự an toàn để đặt thẳng vào path URL.
  private val Pattern = "^([A-Za-z0-9_]{1,128})\\.([A-Za-z0-9_]{1,128})$".r

  /**
   * Hive không phân biệt hoa thường, nên chuẩn hoá chữ thường: `Demo_DB.X` và `demo_db.x` phải ra cùng một key,
   * nếu không dữ liệu ghi bằng cách viết này sẽ không giải mã được bằng cách viết kia.
   */
  def parse(name: String): DakTableRef = Option(name).getOrElse("") match {
    case Pattern(database, table) => DakTableRef(database.toLowerCase(Locale.ROOT), table.toLowerCase(Locale.ROOT))
    case other => throw new IllegalArgumentException(
      s"With source=dak the table must be '<database>.<table>' (letters, digits and '_', up to 128 characters " +
        s"each), got '${other.take(300)}'")
  }
}

/** Tên cột gửi lên DAK: cùng bộ ký tự với tên bảng, luôn chữ thường (cùng lý do với [[DakTableRef]]). */
object DakColumnRef {

  private val Pattern = "^[A-Za-z0-9_]{1,128}$".r

  def parse(name: String): String = Option(name).getOrElse("") match {
    case n if Pattern.pattern.matcher(n).matches() => n.toLowerCase(Locale.ROOT)
    case other => throw new IllegalArgumentException(
      s"With source=dak a column name must be letters, digits and '_', up to 128 characters, got '${other.take(300)}'")
  }
}
