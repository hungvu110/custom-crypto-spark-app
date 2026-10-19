package vai.lakehouse.columncrypto.cdr.audit

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

/**
 * Đếm số dòng mà một lần dùng hàm đã xử lý, bằng accumulator của Spark: cộng trên executor (đi theo closure của UDF), đọc trên driver
 * khi execution kết thúc. Accumulator thường của Spark bỏ phần cộng của task lỗi, nên chỉ task thành công được tính; lỗi được thể hiện
 * bằng `status=FAILED` của END.
 */
final class AuditMeter private (rows: LongAccumulator, nullRows: LongAccumulator) extends CountSource with Serializable {

  def record(value: String, fieldValue: String): Unit = {
    rows.add(1)
    if (value == null || fieldValue == null) nullRows.add(1)
  }

  override def snapshot: UsageCounts = UsageCounts(rows.value, nullRows.value)
}

object AuditMeter {

  /** Accumulator không đặt tên nên không hiện trên Spark UI. */
  def register(sc: SparkContext): AuditMeter = {
    val rows = new LongAccumulator
    val nullRows = new LongAccumulator
    sc.register(rows)
    sc.register(nullRows)
    new AuditMeter(rows, nullRows)
  }
}
