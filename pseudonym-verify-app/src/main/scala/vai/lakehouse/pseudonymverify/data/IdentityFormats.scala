package vai.lakehouse.pseudonymverify.data

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat, lit, lpad, pmod}

/**
 * Định danh gốc của chủ thể `s ∈ [0, N)`, sinh bằng HOÁN VỊ SONG ÁNH (affine mod 10^k, hệ số nguyên tố cùng nhau với 10):
 * mỗi chủ thể có định danh duy nhất mà không cần shuffle/distinct, và ground truth chính là `s`. Mỗi loại định danh có
 * 1 định dạng duy nhất (lib xử lý giá trị nguyên trạng).
 */
object IdentityFormats {

  private val IsdnModulus = 100000000L       // 10^8 — 8 chữ số sau "849"
  private val ImeiModulus = 10000000000000L  // 10^13 — 13 chữ số sau "35"

  /** `84` + `9` + 8 chữ số, vd `84903217465`. 48271 nguyên tố, không chia hết cho 2 và 5 -> song ánh trên [0, 10^8). */
  def isdn(s: Column): Column =
    concat(lit("849"), lpad(pmod(s * 48271L + 12345L, lit(IsdnModulus)).cast("string"), 8, "0"))

  /** 15 chữ số `35` + 13 chữ số, vd `352099001761481`. 7919 nguyên tố -> song ánh trên [0, 10^13). */
  def imei(s: Column): Column =
    concat(lit("35"), lpad(pmod(s * 7919L + 104729L, lit(ImeiModulus)).cast("string"), 13, "0"))

  /** `KH` + 10 chữ số, vd `KH0000123456`. */
  def customerId(s: Column): Column = concat(lit("KH"), lpad(s.cast("string"), 10, "0"))

  /** Theo tên cột định danh của dữ liệu tổng hợp. */
  def forColumn(name: String): Column => Column = name match {
    case SyntheticData.Isdn       => isdn
    case SyntheticData.Imei       => imei
    case SyntheticData.CustomerId => customerId
    case other => throw new IllegalArgumentException(s"Not an identifier column: $other")
  }
}
