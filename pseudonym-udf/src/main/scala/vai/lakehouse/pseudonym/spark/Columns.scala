package vai.lakehouse.pseudonym.spark

import org.apache.spark.sql.{Column, DataFrame}

/** Thao tác cột dùng chung của tầng Spark — nơi DUY NHẤT quote tên cột và thay 1 cột tại chỗ. */
private[pseudonym] object Columns {

  /** Quote bằng backtick để tên cột có dấu chấm/ký tự lạ vẫn là 1 cột duy nhất. */
  def quote(name: String): String = s"`${name.replace("`", "``")}`"

  def ref(df: DataFrame, name: String): Column = df.col(quote(name))

  /**
   * Chọn mọi cột của `df` theo thứ tự cũ, thay `column` bằng `f(column)` đặt tên `output`.
   * Nếu `output` khác `column` thì cột gốc biến mất — output không bao giờ chứa đồng thời cột gốc và cột giả.
   */
  def replace(df: DataFrame, column: String, output: String)(f: Column => Column): DataFrame =
    df.select(df.columns.map(c => if (c == column) f(ref(df, c)).as(output) else ref(df, c)): _*)
}
