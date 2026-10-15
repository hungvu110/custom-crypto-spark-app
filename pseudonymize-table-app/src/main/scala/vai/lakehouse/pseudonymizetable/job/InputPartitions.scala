package vai.lakehouse.pseudonymizetable.job

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import vai.lakehouse.pseudonymizetable.config.{FileInput, InputRange}

/** Một thư mục `<dateKey>=<yyyyMMdd>[/<hourKey>=<HH>]` thuộc khoảng cần xử lý; `hour` giữ nguyên chuỗi trên đĩa ("05"). */
final case class InputPartition(date: String, hour: Option[String], path: String) {
  def label: String = hour.fold(date)(h => s"$date/$h")
}

/**
 * Chọn các thư mục ngày/giờ trong khoảng [[InputRange]] bằng cách LIỆT KÊ thư mục thật dưới `INPUT_PATH` (không đoán tên):
 * chịu được `hour=5` lẫn `hour=05`, bỏ qua ngày/giờ không tồn tại, và chỉ thư mục đúng khuôn `key=value` mới được xét.
 */
object InputPartitions {

  private val DatePattern = "^[0-9]{8}$".r
  private val HourPattern = "^[0-9]{1,2}$".r

  def resolve(spark: SparkSession, input: FileInput, range: InputRange): Seq[InputPartition] = {
    val base = new Path(input.path)
    val fs = base.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(base)) throw new IllegalArgumentException(s"INPUT_PATH không tồn tại: ${input.path}")

    def children(dir: Path, key: String): Seq[(String, Path)] =
      fs.listStatus(dir).toSeq.filter(_.isDirectory).flatMap { s =>
        val name = s.getPath.getName
        if (name.startsWith(key + "=")) Some(name.substring(key.length + 1) -> s.getPath) else None
      }

    val dates = children(base, range.dateKey).collect { case (DatePattern(), p) => p.getName.split("=", 2)(1) -> p }
      .filter { case (d, _) => range.acceptsDate(d) }.sortBy(_._1)

    dates.flatMap { case (date, datePath) =>
      val hours = children(datePath, range.hourKey).collect { case (v @ HourPattern(), p) => (v, v.toInt, p) }
      if (hours.isEmpty) {
        // Bố cục chỉ có thư mục ngày (không có thư mục giờ): chỉ nhận khi không ràng buộc giờ.
        if (range.accepts(date, None)) Seq(InputPartition(date, None, datePath.toString)) else Nil
      } else {
        hours.filter { case (_, h, _) => range.accepts(date, Some(h)) }.sortBy(_._2)
          .map { case (raw, _, p) => InputPartition(date, Some(raw), p.toString) }
      }
    }
  }
}
