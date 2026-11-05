package vai.lakehouse.silveringest.job

import java.util.regex.Pattern

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, lpad, trim, when}
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

  /** Giờ trong cột nguồn: `0`..`23` (một hoặc hai chữ số). */
  private val SourceHour = "^([01]?\\d|2[0-3])$"

  /**
   * Gắn cột giờ lấy từ cột `source` của chính dữ liệu: `5` và `05` đều thành `05`; giá trị ngoài `0`..`23`, chữ hay rỗng thành [[Unknown]].
   * Cột nguồn được giữ nguyên. Cột giờ đích đã có trong dữ liệu (mà không phải cột nguồn) là lỗi, như cột giờ lấy từ đường dẫn.
   */
  def fromColumn(hours: Hours, source: String, df: DataFrame): DataFrame = {
    val sourceName = df.columns.find(_.equalsIgnoreCase(source)).getOrElse {
      throw new IllegalArgumentException(s"HOUR_FROM_COLUMN '$source' is not a column of the data (columns: ${df.columns.mkString(", ")})")
    }
    df.columns.find(c => c.equalsIgnoreCase(hours.column) && !c.equalsIgnoreCase(sourceName)).foreach { c =>
      throw new IllegalArgumentException(s"HOUR_COLUMN '${hours.column}' already exists in the data (column '$c'): choose another HOUR_COLUMN")
    }
    val value = trim(col(s"`${sourceName.replace("`", "``")}`").cast("string"))
    df.withColumn(hours.column, when(value.rlike(SourceHour), lpad(value, 2, "0")).otherwise(lit(Unknown)))
  }

  /**
   * `load` đọc một nhóm file thành DataFrame; kết quả là hợp của mọi nhóm, mỗi dòng có cột giờ của nhóm của nó. Giờ lấy từ cột dữ liệu
   * ([[Hours.sourceColumn]]) thì đọc một lần rồi suy ra giờ từng dòng ([[fromColumn]]).
   */
  def read(hours: Hours, files: Seq[SourceFile])(load: Seq[SourceFile] => DataFrame): DataFrame = hours.sourceColumn match {
    case Some(source) => fromColumn(hours, source, load(files))
    case None => readByPath(hours, files)(load)
  }

  private def readByPath(hours: Hours, files: Seq[SourceFile])(load: Seq[SourceFile] => DataFrame): DataFrame = {
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
