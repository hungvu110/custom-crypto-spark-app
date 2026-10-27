package vai.lakehouse.silveringest.job

import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession}
import vai.lakehouse.silveringest.config.{InputFormat, InputSpec}

/** Đọc file nguồn thành DataFrame theo `INPUT_FORMAT`, `INPUT_SCHEMA`, `INPUT_OPTION_*`. Chỉ đọc, không sửa file nguồn. */
object SourceReader {

  /** Đọc thẳng các file đã là plaintext (VTT, hoặc file VTNET đã giải mã ra thư mục tạm). */
  def read(spark: SparkSession, input: InputSpec, paths: Seq[String]): DataFrame =
    reader(spark, input).format(input.format.sparkFormat).load(paths: _*)

  /** Đọc từ các dòng text đã giải mã trong bộ nhớ (VTNET, định dạng text): plaintext không chạm đĩa. */
  def fromLines(spark: SparkSession, input: InputSpec, lines: Dataset[String]): DataFrame = input.format match {
    case InputFormat.Csv | InputFormat.Tsv => reader(spark, input).csv(lines)
    case InputFormat.Json => reader(spark, input).json(lines)
    case InputFormat.Text => lines.toDF("value")
    case other => throw new IllegalArgumentException(s"INPUT_FORMAT=${other.name} cannot be read from decrypted lines")
  }

  private def reader(spark: SparkSession, input: InputSpec): DataFrameReader = {
    val base = spark.read.options(input.readOptions)
    input.schema.fold(base)(base.schema)
  }
}
