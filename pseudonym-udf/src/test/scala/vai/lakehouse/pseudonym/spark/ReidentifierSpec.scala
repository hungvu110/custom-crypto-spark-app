package vai.lakehouse.pseudonym.spark

import vai.lakehouse.pseudonym.ConfigurationException
import vai.lakehouse.pseudonym.TestKeys
import vai.lakehouse.pseudonym.SparkTestSession
import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.policy.{ColumnRule, TablePolicy}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Luồng ưu đãi Viettel++: danh sách pseudonym_id -> đúng customer_id gốc, với cả 3 chế độ. */
class ReidentifierSpec extends AnyFlatSpec with Matchers with SparkTestSession {

  private lazy val mapPath       = tempPath("reid_map")
  private lazy val pseudonymizer = new Pseudonymizer(TestKeys.provider, Some(mapPath))
  private lazy val reidentifier  = new Reidentifier(TestKeys.provider, Some(mapPath))

  private val customers = (1 to 50).map(i => f"KH$i%010d")

  private def loyalty = {
    val s = spark
    import s.implicits._
    customers.zipWithIndex.map { case (c, i) => (i.toLong, c, i * 10) }.toDF("txn_id", "customer_id", "points")
  }

  private def knownCustomers = {
    val s = spark
    import s.implicits._
    customers.toDF("customer_id")
  }

  private def policyFor(mode: PseudonymMode) =
    TablePolicy("loyalty.loyalty_txn", Seq(ColumnRule("customer_id", mode, Some("pseudonym_id"))))

  PseudonymMode.All.foreach { mode =>
    s"Reidentifier ($mode)" should "đổi danh sách pseudonym_id về đúng customer_id gốc" in {
      val offers = pseudonymizer.pseudonymize(loyalty, policyFor(mode))
        .select("txn_id", "pseudonym_id").where("txn_id % 5 = 0")

      val known = if (mode == PseudonymMode.Hash) Some(knownCustomers) else None
      val back = reidentifier.reidentify(offers, "pseudonym_id", mode, "customer_id", known)

      back.columns.toSeq shouldBe Seq("txn_id", "customer_id")
      back.collect().map(r => r.getLong(0) -> r.getString(1)).toMap shouldBe
        customers.zipWithIndex.collect { case (c, i) if i % 5 == 0 => i.toLong -> c }.toMap
    }
  }

  "Reidentifier (HASH)" should "báo lỗi khi không có knownValues (HASH là một chiều)" in {
    val ex = intercept[ConfigurationException](
      reidentifier.reidentify(loyalty, "customer_id", PseudonymMode.Hash, "customer_id"))
    ex.getMessage should include("one-way")
  }

  it should "trả null cho pseudonym không có trong danh sách giá trị đã biết" in {
    val s = spark
    import s.implicits._
    val targets = Seq("k1_00000000000000000000000000000000").toDF("pseudonym_id")

    reidentifier.reidentify(targets, "pseudonym_id", PseudonymMode.Hash, "customer_id", Some(knownCustomers))
      .collect().head.isNullAt(0) shouldBe true
  }
}
