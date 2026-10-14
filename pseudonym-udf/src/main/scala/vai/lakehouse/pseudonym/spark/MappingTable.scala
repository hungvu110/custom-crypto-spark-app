package vai.lakehouse.pseudonym.spark

import java.security.SecureRandom

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.delta.DeltaConcurrentModificationException
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, current_timestamp, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import vai.lakehouse.pseudonym.ConfigurationException
import vai.lakehouse.pseudonym.core.Encoding

/**
 * Chế độ MAPPING — "bảng ánh xạ" của tài liệu: mỗi giá trị gốc nhận 1 token ngẫu nhiên `t_<32 hex>`, cặp
 * `(raw_value, token)` lưu trong bảng Delta ở `path` (vùng HDFS riêng, phân quyền như dữ liệu gốc vì chứa giá trị
 * gốc). Token không suy ra được từ giá trị gốc dù lộ khoá — vì không có khoá nào.
 *
 * Luồng: giá trị chưa có token -> sinh token -> MERGE (chỉ INSERT) vào bảng -> ĐỌC LẠI bảng rồi mới join. Không join
 * thẳng với DataFrame chứa token vừa sinh: `new_token()` không tất định, Spark có thể tính lại khi retry task, khi đó
 * 1 giá trị nhận 2 token. Bảng đã ghi là nguồn sự thật duy nhất.
 *
 * SparkSession phải có Delta: `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` và
 * `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`.
 */
final class MappingTable(session: SparkSession, val path: String) {
  import MappingTable._

  if (path == null || path.trim.isEmpty) throw new ConfigurationException("Mapping table path must not be empty")

  /** Bảng ánh xạ hiện tại (đọc snapshot mới nhất mỗi lần dùng). */
  def table: DataFrame = session.read.format("delta").load(path)

  /** Thay cột `column` bằng token (cột tên `output`, cùng vị trí); `null` -> `null`. */
  def pseudonymize(df: DataFrame, column: String, output: String): DataFrame = {
    val raw = Columns.ref(df, column).cast(StringType)
    ensureTokens(df.select(raw.as(RawValue)).where(col(RawValue).isNotNull).distinct())
    val lookup = table.select(col(RawValue).as(JoinKey), col(Token).as(Joined))
    replaceViaJoin(df, column, output, raw, lookup)
  }

  /** Tái định danh: thay cột token `column` bằng giá trị gốc (cột `output`); token không có trong bảng -> `null`. */
  def reveal(df: DataFrame, column: String, output: String): DataFrame = {
    val lookup = table.select(col(Token).as(JoinKey), col(RawValue).as(Joined))
    replaceViaJoin(df, column, output, Columns.ref(df, column).cast(StringType), lookup)
  }

  /** Gán token cho các giá trị (cột `raw_value`, không null, không trùng) chưa có trong bảng. */
  def ensureTokens(values: DataFrame): Unit = {
    createIfMissing()
    retryOnConflict(MaxAttempts) {
      val missing = values.join(table.select(RawValue), Seq(RawValue), "left_anti")
        .withColumn(Token, newTokenUdf())
        .withColumn(CreatedAt, current_timestamp())
      DeltaTable.forPath(session, path).as("m")
        .merge(missing.as("n"), s"m.$RawValue = n.$RawValue")
        .whenNotMatched().insertAll()
        .execute()
    }
  }

  private def createIfMissing(): Unit =
    if (!DeltaTable.isDeltaTable(session, path)) retryOnConflict(MaxAttempts) {
      session.createDataFrame(session.sparkContext.emptyRDD[Row], Schema)
        .write.format("delta").mode(SaveMode.Ignore).save(path)
    }

  private def replaceViaJoin(df: DataFrame, column: String, output: String, key: Column, lookup: DataFrame): DataFrame = {
    val joined = df.withColumn(JoinKey, key).join(lookup, Seq(JoinKey), "left")
    joined.select(df.columns.map(c => if (c == column) col(Joined).as(output) else Columns.ref(joined, c)): _*)
  }
}

object MappingTable {
  val RawValue    = "raw_value"
  val Token       = "token"
  val CreatedAt   = "created_at"
  val TokenPrefix = "t_"
  val TokenBytes  = 16

  val Schema: StructType = StructType(Seq(
    StructField(RawValue, StringType, nullable = false),
    StructField(Token, StringType, nullable = false),
    StructField(CreatedAt, TimestampType, nullable = true)))

  private val JoinKey     = "__pseudonym_join_key"
  private val Joined      = "__pseudonym_joined"
  private val MaxAttempts = 5

  private val random = new SecureRandom()

  def newToken(): String = {
    val bytes = new Array[Byte](TokenBytes)
    random.nextBytes(bytes)
    TokenPrefix + Encoding.hex(bytes)
  }

  val newTokenUdf: UserDefinedFunction = udf(() => newToken()).asNondeterministic().withName("new_token")

  /** 2 job cùng chèn 1 giá trị -> Delta báo conflict cho 1 job; job đó chạy lại và thấy token đã có. */
  private def retryOnConflict(attempts: Int)(body: => Unit): Unit =
    try body
    catch {
      case _: DeltaConcurrentModificationException if attempts > 1 => retryOnConflict(attempts - 1)(body)
    }
}
