package vai.lakehouse.pseudonymizetable.job

import scala.collection.JavaConverters._

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, input_file_name, udf, when}
import org.apache.spark.util.CollectionAccumulator

/**
 * Biết file nào ĐÃ cho ra dòng khi đọc với `spark.sql.files.ignoreCorruptFiles=true` (Spark bỏ file lỗi trong im lặng,
 * chỉ có WARN ở log executor). Hiệu của "file có trong thư mục" và "file đã cho ra ít nhất một dòng" là các file bị bỏ.
 *
 * Cách làm: bọc 1 cột đầu ra trong `CASE WHEN track(input_file_name()) IS NOT NULL THEN cột END`. Biểu thức này nằm trong
 * chính dữ liệu được ghi nên không bị tối ưu cắt bỏ và chạy trên mọi đường ghi (saveAsTable/insertInto). Mỗi task giữ
 * một tập tên file riêng (closure được serialize theo task) để chỉ cộng vào accumulator một lần cho mỗi file.
 */
object FileTracker {

  def newAccumulator(spark: SparkSession): CollectionAccumulator[String] =
    spark.sparkContext.collectionAccumulator[String]("input-files-with-rows")

  /** Trả về `df` với cột `column` bọc biểu thức theo dõi; giá trị cột không đổi. */
  def attach(df: DataFrame, column: String, acc: CollectionAccumulator[String]): DataFrame = {
    val seen = new java.util.HashSet[String]()
    val track = udf((file: String) => { if (file != null && seen.add(file)) acc.add(file); file }).asNondeterministic()
    val original: Column = col(s"`${column.replace("`", "``")}`")
    df.withColumn(column, when(track(input_file_name()).isNotNull, original))
  }

  /** So sánh theo phần path đã giải mã (URI `file:///x` và `file:/x` là cùng một file). */
  def normalize(file: String): String =
    scala.util.Try(new java.net.URI(file).getPath).toOption.filter(_ != null).getOrElse(file)

  def seenPaths(acc: CollectionAccumulator[String]): Set[String] = acc.value.asScala.map(normalize).toSet
}
