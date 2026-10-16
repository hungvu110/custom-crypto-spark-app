package vai.lakehouse.pseudonymizetable.job

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import vai.lakehouse.pseudonymizetable.config.{FileInput, InputRange}

/** A directory `<dateKey>=<yyyyMMdd>[/<hourKey>=<HH|HHMM>]` inside the range; `hour` keeps the on-disk string ("05", "0020"). */
final case class InputPartition(date: String, hour: Option[String], path: String) {
  def label: String = hour.fold(date)(h => s"$date/$h")
}

/**
 * Chọn các thư mục ngày/giờ trong khoảng [[InputRange]] bằng cách LIỆT KÊ thư mục thật dưới `INPUT_PATH` (không đoán tên):
 * chịu được `hour=5` lẫn `hour=05`, bỏ qua ngày/giờ không tồn tại, và chỉ thư mục đúng khuôn `key=value` mới được xét.
 */
object InputPartitions {

  private val DatePattern = "^[0-9]{8}$".r
  private val TimePattern = "^[0-9]{1,4}$".r

  /** 1-2 digits = hour (`hour=05`); 3-4 digits = HHMM (`batch=0020`). `None` if hour > 23 or minute > 59. */
  private def toTime(value: String): Option[Int] = {
    val t = if (value.length <= 2) value.toInt * 100 else value.toInt
    if (t / 100 <= 23 && t % 100 <= 59) Some(t) else None
  }

  def resolve(spark: SparkSession, input: FileInput, range: InputRange): Seq[InputPartition] = {
    val base = new Path(input.path)
    val fs = base.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(base)) throw new IllegalArgumentException(s"INPUT_PATH does not exist: ${input.path}")

    def children(dir: Path, key: String): Seq[(String, Path)] =
      fs.listStatus(dir).toSeq.filter(_.isDirectory).flatMap { s =>
        val name = s.getPath.getName
        if (name.startsWith(key + "=")) Some(name.substring(key.length + 1) -> s.getPath) else None
      }

    val dates = children(base, range.dateKey).collect { case (DatePattern(), p) => p.getName.split("=", 2)(1) -> p }
      .filter { case (d, _) => range.acceptsDate(d) }.sortBy(_._1)

    dates.flatMap { case (date, datePath) =>
      val hours = children(datePath, range.hourKey).collect { case (v @ TimePattern(), p) => toTime(v).map(t => (v, t, p)) }.flatten
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
