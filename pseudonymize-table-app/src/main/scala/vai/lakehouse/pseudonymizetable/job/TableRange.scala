package vai.lakehouse.pseudonymizetable.job

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import vai.lakehouse.pseudonymizetable.config.InputRange

/**
 * Keeps the rows of a TABLE source inside an [[InputRange]], using its partition columns `<dateKey>` (string yyyyMMdd)
 * and `<hourKey>` (string HH). Same meaning as the directory range of a file source: both ends inclusive, the hour of a
 * row counts from its start (`hour=05` is 05:00), `INPUT_HOURS` applies to every day.
 *
 * Filters are plain comparisons on the partition columns so Spark prunes partitions and reads only what is needed.
 */
object TableRange {

  /** Fails early, before any write, when the table lacks a partition column the range needs. */
  def requireColumns(columns: Seq[String], range: InputRange, source: String): Unit = {
    val missing = Seq(range.dateKey, range.hourKey).filterNot(columns.contains)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(s"INPUT_FROM/INPUT_TO/INPUT_HOURS need the columns ${range.dateKey} (yyyyMMdd) and " +
        s"${range.hourKey} (HH) in $source, missing: ${missing.mkString(", ")}")
    }
  }

  def condition(range: InputRange): Column = {
    val date = col(quote(range.dateKey))
    val hour = col(quote(range.hourKey))
    val dateFrom = if (range.from == 0L) None else Some(f"${range.from / 10000}%08d")
    val dateTo = if (range.to == Long.MaxValue) None else Some(f"${range.to / 10000}%08d")
    // Start time of the row's hour, same scale as range.from/to (yyyyMMddHHmm with minute 00).
    val startKey = date.cast("long") * 10000L + hour.cast("long") * 100L
    val parts = Seq(
      dateFrom.map(d => date >= lit(d)), dateTo.map(d => date <= lit(d)),
      if (range.from == 0L) None else Some(startKey >= lit(range.from)),
      if (range.to == Long.MaxValue) None else Some(startKey <= lit(range.to)),
      range.hours.map { case (a, b) => hour.cast("int").between(a, b) }).flatten
    parts.reduceOption(_ && _).getOrElse(lit(true))
  }

  def apply(df: DataFrame, range: InputRange, source: String): DataFrame = {
    requireColumns(df.columns, range, source)
    df.where(condition(range))
  }

  private def quote(identifier: String): String = s"`${identifier.replace("`", "``")}`"
}
