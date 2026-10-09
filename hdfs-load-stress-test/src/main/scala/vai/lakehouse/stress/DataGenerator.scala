package vai.lakehouse.stress

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Sinh dữ liệu sự kiện tổng hợp ngay trên executor (không đọc nguồn nào), nên tải đo được là tải GHI vào HDFS.
 *
 * Cột `payload` là chuỗi hex nối từ SHA-256, gần như không nén được: dung lượng trên HDFS bám sát PAYLOAD_BYTES,
 * kết quả không bị thổi phồng nhờ nén. Dữ liệu tất định theo (id, seed): ghi lại một batch cho cùng nội dung.
 */
object DataGenerator {

  val HexCharsPerHash = 64

  private val SeedPattern = "^[A-Za-z0-9_-]{1,64}$"

  def generate(
      spark: SparkSession,
      firstId: Long,
      rows: Long,
      partitions: Int,
      payloadBytes: Int,
      seed: String): DataFrame = {
    // seed đi thẳng vào biểu thức SQL: chỉ nhận ký tự an toàn (RUN_ID đã được validate cùng quy tắc).
    require(seed.matches(SeedPattern), s"seed không hợp lệ: '$seed'")
    val hashes = math.max(1, payloadBytes / HexCharsPerHash)
    spark.range(firstId, firstId + rows, 1, partitions).selectExpr(
      "id",
      "current_timestamp() AS ingest_ts",
      "concat('user_', cast(id % 1000000 AS string)) AS user_id",
      "element_at(array('view', 'click', 'purchase', 'share', 'comment'), cast(id % 5 AS int) + 1) AS event_type",
      "cast(id % 100000 AS double) / 100 AS amount",
      s"concat_ws('', transform(sequence(1, $hashes), " +
        s"i -> sha2(concat(cast(id AS string), ':', cast(i AS string), ':', '$seed'), 256))) AS payload")
  }
}
