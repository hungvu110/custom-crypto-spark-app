package vai.lakehouse.pseudonymverify.data

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Bảng sự kiện thuê bao tổng hợp, có đáp án (chủ thể thật biết trước), tất định theo `seed`.
 *
 * Cột: `row_id` (khoá dòng, không giả danh), `customer_id`, `isdn` (09xxxxxxxx), `imei` (15 số), `plan`, `data_mb`.
 * Mỗi chủ thể xuất hiện nhiều dòng (để kiểm liên kết dọc); `isdn`/`imei` có `nullRate` giá trị NULL.
 */
object SyntheticSubscribers {

  val IdentityColumns: Seq[String] = Seq("customer_id", "isdn", "imei")

  private val Plans = Array("basic", "silver", "gold", "platinum")

  def generate(spark: SparkSession, rows: Int, subscribers: Int, nullRate: Double = 0.05, seed: Long = 42L): DataFrame = {
    require(rows > 0 && subscribers > 0, "rows and subscribers must be positive")
    val random = new java.util.Random(seed)
    // Mỗi chủ thể có 1 bộ định danh cố định: cùng người -> cùng isdn/imei ở mọi dòng.
    val people = Array.tabulate(subscribers) { i =>
      (f"C$i%08d", f"09$i%08d", f"86${i.toLong * 7919L % 1000000000000L}%013d")
    }
    val data = Seq.tabulate(rows) { r =>
      val (customer, isdn, imei) = people(random.nextInt(subscribers))
      val nullable = (v: String) => if (random.nextDouble() < nullRate) null else v
      (r.toLong, customer, nullable(isdn), nullable(imei), Plans(random.nextInt(Plans.length)), random.nextInt(5000))
    }
    import spark.implicits._
    data.toDF("row_id", "customer_id", "isdn", "imei", "plan", "data_mb")
  }
}
