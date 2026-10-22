package vai.lakehouse.keyprefix

/**
 * Chia danh sách cột thành các lô hợp lệ cho một lời gọi `by-column`: tối đa [[MaxColumns]] cột và [[MaxLength]] ký tự cho
 * chuỗi `columns`, TÍNH CẢ dấu phẩy (đúng cách dak-mock / DAK đếm). Giữ nguyên thứ tự.
 */
object ColumnBatches {

  val MaxColumns = 100
  val MaxLength = 4096

  def split(columns: Seq[String]): Seq[Seq[String]] = {
    val batches = Seq.newBuilder[Seq[String]]
    var current = Vector.empty[String]
    var length = 0
    columns.foreach { c =>
      val added = if (current.isEmpty) c.length else length + 1 + c.length
      if (current.nonEmpty && (current.size == MaxColumns || added > MaxLength)) {
        batches += current
        current = Vector(c)
        length = c.length
      } else {
        current :+= c
        length = added
      }
    }
    if (current.nonEmpty) batches += current
    batches.result()
  }
}
