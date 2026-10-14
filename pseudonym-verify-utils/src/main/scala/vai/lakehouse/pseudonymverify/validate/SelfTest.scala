package vai.lakehouse.pseudonymverify.validate

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, sha2}
import vai.lakehouse.keyprefix.PrefixSource
import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.core.{AesSivPseudonymizer, HmacPseudonymizer, KeyProvider, PrefixSourceKeyProvider, PseudonymKey}
import vai.lakehouse.pseudonym.policy.{ColumnRule, TablePolicy}
import vai.lakehouse.pseudonym.spark.Pseudonymizer
import vai.lakehouse.pseudonymverify.data.SyntheticSubscribers

/**
 * Tự sinh dữ liệu có đáp án -> giả danh bằng `pseudonym-udf` với khoá ngẫu nhiên (chỉ trong bộ nhớ) -> kiểm.
 * Dùng để nghiệm thu lib độc lập với bất kỳ bảng thật nào. Không hỗ trợ MAPPING (cần bảng ánh xạ Delta).
 *
 * Ngoài các phép kiểm của [[PseudonymValidator]] (nguồn ↔ đích), vì có khoá dòng `row_id` nên kiểm thêm:
 *  - ánh xạ là hàm và đơn ánh theo từng dòng (cùng người -> cùng định danh giả, khác người -> khác);
 *  - kẻ tấn công băm SHA-256 không khoá toàn bộ không gian định danh thì khớp 0;
 *  - chạy lại cho kết quả y hệt (tất định), còn khoá khác thì ra định danh khác hoàn toàn.
 */
object SelfTest {

  def run(spark: SparkSession, mode: PseudonymMode, rows: Int = 2000, subscribers: Int = 200): ValidationReport = {
    require(mode != PseudonymMode.Mapping, "selftest does not support MAPPING (needs a Delta mapping table)")
    val columns = SyntheticSubscribers.IdentityColumns
    val source = SyntheticSubscribers.generate(spark, rows, subscribers).cache()
    val policy = TablePolicy("selftest", columns.map(ColumnRule(_, mode)))

    val keys = ephemeralKeys()
    val target = new Pseudonymizer(keys).pseudonymize(source, policy).cache()
    val again = new Pseudonymizer(keys).pseudonymize(source, policy)
    val otherKey = new Pseudonymizer(ephemeralKeys()).pseudonymize(source, policy)

    val base = PseudonymValidator.validate(source, target, columns, mode).results
    val extra = columns.flatMap(c => rowAligned(source, target, c)) ++
      columns.map(c => dictionaryAttack(source, target, c)) ++
      columns.flatMap(c => keyDependence(target, again, otherKey, c))
    ValidationReport(base ++ extra)
  }

  /** Cùng giá trị gốc -> đúng 1 định danh giả, và khác giá trị gốc -> khác định danh giả (ghép theo `row_id`). */
  private def rowAligned(source: DataFrame, target: DataFrame, c: String): Seq[CheckResult] = {
    val pairs = source.select(col("row_id"), col(c).as("o")).join(target.select(col("row_id"), col(c).as("p")), "row_id")
      .where(col("o").isNotNull).select("o", "p").distinct().cache()
    val total = pairs.count()
    val originals = pairs.select("o").distinct().count()
    val pseudos = pairs.select("p").distinct().count()
    Seq(
      CheckResult(s"$c: mỗi giá trị gốc ↔ đúng 1 định danh giả", total == originals, s"cặp=$total gốc=$originals"),
      CheckResult(s"$c: không 2 giá trị gốc cùng 1 định danh giả", total == pseudos, s"cặp=$total giả=$pseudos"))
  }

  /** Băm SHA-256 không khoá mọi giá trị gốc rồi tìm trong đích: phải khớp 0 (nếu có thì thực chất là băm không khoá). */
  private def dictionaryAttack(source: DataFrame, target: DataFrame, c: String): CheckResult = {
    val hashes = source.where(col(c).isNotNull).select(sha2(col(c), 256).as("v")).distinct()
    val hits = target.where(col(c).isNotNull).select(col(c).as("v")).distinct().join(hashes, Seq("v"), "left_semi").count()
    CheckResult(s"$c: tấn công từ điển SHA-256 không khoá", hits == 0, s"khớp=$hits")
  }

  private def keyDependence(target: DataFrame, again: DataFrame, otherKey: DataFrame, c: String): Seq[CheckResult] = {
    val t = target.select(col("row_id"), col(c).as("v"))
    val a = again.select(col("row_id"), col(c).as("a"))
    val w = otherKey.select(col("row_id"), col(c).as("w"))
    val changed = t.join(a, "row_id").where(!(col("v") <=> col("a"))).count()
    val collisions = t.join(w, "row_id").where(col("v").isNotNull && col("v") === col("w")).count()
    Seq(
      CheckResult(s"$c: chạy lại cùng khoá cho kết quả y hệt", changed == 0, s"dòng khác=$changed"),
      CheckResult(s"$c: khoá khác cho định danh khác", collisions == 0, s"dòng trùng=$collisions"))
  }

  private def ephemeralKeys(): KeyProvider = {
    val values = Map(
      PrefixSourceKeyProvider.hmacName(1)   -> PseudonymKey.randomEncoded(HmacPseudonymizer.KeyBytes),
      PrefixSourceKeyProvider.aesSivName(1) -> PseudonymKey.randomEncoded(AesSivPseudonymizer.KeyBytes))
    new PrefixSourceKeyProvider(new PrefixSource {
      override def read(datasetName: String): String =
        values.getOrElse(datasetName, throw new IllegalStateException(s"No ephemeral key '$datasetName'"))
    })
  }
}
