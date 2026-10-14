package vai.lakehouse.pseudonym.spark

import vai.lakehouse.pseudonym.{ConfigurationException, InvalidPolicyException}
import vai.lakehouse.pseudonym.TestKeys
import vai.lakehouse.pseudonym.SparkTestSession
import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.core.{AesSivPseudonymizer, HmacPseudonymizer, KeyProvider, PseudonymKey}
import vai.lakehouse.pseudonym.policy.{ColumnRule, TablePolicy}

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PseudonymizerSpec extends AnyFlatSpec with Matchers with SparkTestSession {

  private lazy val pseudonymizer = new Pseudonymizer(TestKeys.provider, Some(tempPath("pseudonym_map")))

  private def loyalty: DataFrame = {
    val s = spark
    import s.implicits._
    Seq(
      (1L, "KH0000000001", "earn", 100),
      (2L, "KH0000000002", "earn", 50),
      (3L, "KH0000000001", "redeem", 30),
      (4L, null, "earn", 10)
    ).toDF("txn_id", "customer_id", "txn_type", "points")
  }

  private def policy(mode: PseudonymMode) =
    TablePolicy("loyalty.loyalty_txn", Seq(ColumnRule("customer_id", mode, Some("pseudonym_id"))))

  "Pseudonymizer (HASH)" should "thay cột tại chỗ, đổi tên thành pseudonym_id, xoá cột gốc, giữ cột khác" in {
    val out = pseudonymizer.pseudonymize(loyalty, policy(PseudonymMode.Hash))

    out.columns.toSeq shouldBe Seq("txn_id", "pseudonym_id", "txn_type", "points")
    val rows = out.orderBy("txn_id").collect()
    rows(0).getString(1) shouldBe new HmacPseudonymizer(TestKeys.hmac).pseudonymize("KH0000000001")
    rows(0).getString(1) shouldBe rows(2).getString(1) // cùng khách hàng -> cùng pseudonym_id (liên kết được)
    rows(1).getString(1) should not be rows(0).getString(1)
    rows(3).isNullAt(1) shouldBe true
    rows.map(_.getInt(3)).toSeq shouldBe Seq(100, 50, 30, 10)
  }

  it should "giữ nguyên tên cột khi không có output, và cast cột số sang chuỗi" in {
    val s = spark
    import s.implicits._
    val df = Seq((1L, 84903217465L)).toDF("id", "isdn")

    val out = pseudonymizer.pseudonymize(df, TablePolicy("t", Seq(ColumnRule("isdn", PseudonymMode.Hash))))

    out.columns.toSeq shouldBe Seq("id", "isdn")
    out.collect().head.getString(1) shouldBe "k1_3fe27d9b4727fb8a0f900bc4126c6164"
  }

  it should "KHÔNG để khoá xuất hiện trong plan (EXPLAIN)" in {
    val out = pseudonymizer.pseudonymize(loyalty, policy(PseudonymMode.Hash))
    val plans = out.queryExecution.toString
    val keyHex = TestKeys.hexOfRange(32)

    plans should not include keyHex
    plans should include("pseudo_hash")
  }

  "Pseudonymizer (ENCRYPT)" should "sinh e1_ tất định và giải mã lại được" in {
    val out = pseudonymizer.pseudonymize(loyalty, policy(PseudonymMode.Encrypt)).orderBy("txn_id").collect()
    val aes = new AesSivPseudonymizer(TestKeys.aesSiv)

    out(0).getString(1) should startWith("e1_")
    out(0).getString(1) shouldBe out(2).getString(1)
    aes.decrypt(out(1).getString(1)) shouldBe "KH0000000002"
    out(3).isNullAt(1) shouldBe true
  }

  "Pseudonymizer (MAPPING)" should "thay bằng token t_ và cùng giá trị -> cùng token" in {
    val out = pseudonymizer.pseudonymize(loyalty, policy(PseudonymMode.Mapping)).orderBy("txn_id").collect()

    out.length shouldBe 4
    out(0).getString(1) should fullyMatch regex "t_[0-9a-f]{32}"
    out(0).getString(1) shouldBe out(2).getString(1)
    out(1).getString(1) should not be out(0).getString(1)
    out(3).isNullAt(1) shouldBe true
  }

  it should "báo lỗi rõ ràng khi chưa cấu hình đường dẫn bảng ánh xạ" in {
    val ex = intercept[ConfigurationException](
      new Pseudonymizer(TestKeys.provider).pseudonymize(loyalty, policy(PseudonymMode.Mapping)))
    ex.getMessage should include(PseudonymConf.MappingPathKey)
  }

  "Pseudonymizer" should "báo lỗi khi cột trong policy không có trong DataFrame" in {
    val ex = intercept[InvalidPolicyException](pseudonymizer.pseudonymize(loyalty,
      TablePolicy("t", Seq(ColumnRule("isdn", PseudonymMode.Hash)))))
    ex.getMessage should include("not found")
  }

  it should "báo lỗi khi output trùng với 1 cột đang có" in {
    val ex = intercept[InvalidPolicyException](pseudonymizer.pseudonymize(loyalty,
      TablePolicy("t", Seq(ColumnRule("customer_id", PseudonymMode.Hash, Some("points"))))))
    ex.getMessage should include("already exist")
  }

  it should "chỉ đọc khoá của chế độ được dùng" in {
    var aesRead = false
    val keys = new KeyProvider {
      override def hmacKey: PseudonymKey = TestKeys.hmac
      override def aesSivKey: PseudonymKey = { aesRead = true; TestKeys.aesSiv }
    }
    new Pseudonymizer(keys).pseudonymize(loyalty, policy(PseudonymMode.Hash)).collect()
    aesRead shouldBe false
  }
}
