package vai.lakehouse.silveringest.job

import java.util.regex.Pattern

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import vai.lakehouse.silveringest.config.Hours

/**
 * Gắn cột giờ cho dữ liệu đọc từ nhiều file. Giờ được suy ra từ ĐƯỜNG DẪN file ([[Hours.pathRegex]]) ngay ở driver, nên file được chia
 * thành các nhóm cùng giờ, mỗi nhóm đọc (hoặc giải mã) riêng rồi gắn giờ của nhóm; cách này dùng được cho cả file đã giải mã trong bộ nhớ,
 * khi từng dòng không còn biết nó thuộc file nào.
 */
object HourGroups {

  /** Giờ của file không khớp regex: nhìn thấy ngay trong bảng, không để rỗng. */
  val Unknown = "NA"

  def hourOf(pattern: Pattern, path: String): String = {
    val matcher = pattern.matcher(path)
    if (matcher.find()) Option(matcher.group(1)).getOrElse(Unknown) else Unknown
  }

  /** Nhóm file theo giờ, theo thứ tự giờ tăng dần. */
  def group(hours: Hours, files: Seq[SourceFile]): Seq[(String, Seq[SourceFile])] = {
    val pattern = Pattern.compile(hours.pathRegex)
    files.groupBy(f => hourOf(pattern, f.path)).toSeq.sortBy(_._1)
  }

  /** `load` đọc một nhóm file thành DataFrame; kết quả là hợp của mọi nhóm, mỗi dòng có cột giờ của nhóm của nó. */
  def read(hours: Hours, files: Seq[SourceFile])(load: Seq[SourceFile] => DataFrame): DataFrame = {
    val parts = group(hours, files).map { case (hour, groupFiles) =>
      val df = load(groupFiles)
      df.columns.find(_.equalsIgnoreCase(hours.column)).foreach { c =>
        throw new IllegalArgumentException(s"HOUR_COLUMN '${hours.column}' already exists in the data (column '$c'): choose another HOUR_COLUMN")
      }
      df.withColumn(hours.column, lit(hour))
    }
    parts.reduce(_ unionByName _)
  }
}
