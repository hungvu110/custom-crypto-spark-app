package vai.lakehouse.pseudonymverify

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonymverify.app.PseudonymValidateApp
import vai.lakehouse.pseudonymverify.validate.{PseudonymValidator, SelfTest}

class PseudonymValidatorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder().appName("validator-spec").master("local[1]")
    .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate()

  override protected def afterAll(): Unit = spark.stop()

  private val token1 = "k1_" + "a" * 32
  private val token2 = "k1_" + "b" * 32

  private def frame(rows: (Int, String, String)*): DataFrame = {
    import spark.implicits._
    rows.toDF("id", "isdn", "plan")
  }

  private def source = frame((1, "0901", "gold"), (2, "0902", "silver"), (3, "0901", "gold"), (4, null, "gold"))
  private def good = frame((1, token1, "gold"), (2, token2, "silver"), (3, token1, "gold"), (4, null, "gold"))

  private def failed(df: DataFrame, mode: PseudonymMode = PseudonymMode.Hash): Seq[String] =
    PseudonymValidator.validate(source, df, Seq("isdn"), mode).results.filterNot(_.passed).map(_.name)

  "validate" should "pass for a correctly pseudonymized table" in {
    PseudonymValidator.validate(source, good, Seq("isdn"), PseudonymMode.Hash).passed shouldBe true
  }

  it should "fail when original values were left in the target" in {
    failed(frame((1, token1, "gold"), (2, "0902", "silver"), (3, token1, "gold"), (4, null, "gold"))) should
      contain("isdn: không sót giá trị gốc")
  }

  it should "fail when two different values collapse to one token" in {
    failed(frame((1, token1, "gold"), (2, token1, "silver"), (3, token1, "gold"), (4, null, "gold"))) should
      contain("isdn: ánh xạ 1-1 (số giá trị khác nhau giữ nguyên)")
  }

  it should "fail when nulls are replaced by tokens" in {
    failed(frame((1, token1, "gold"), (2, token2, "silver"), (3, token1, "gold"), (4, token2, "gold"))) should
      contain("isdn: null giữ nguyên")
  }

  it should "fail when the token does not match the mode format" in {
    failed(good, PseudonymMode.Mapping) should contain("isdn: đúng định dạng MAPPING")
  }

  it should "fail when an untouched column was modified" in {
    failed(frame((1, token1, "gold"), (2, token2, "SILVER"), (3, token1, "gold"), (4, null, "gold"))) should
      contain("cột không giả danh giữ nguyên")
  }

  it should "fail when row counts differ" in {
    failed(frame((1, token1, "gold"), (2, token2, "silver"))) should contain("số dòng nguồn == đích")
  }

  it should "report a missing column without throwing" in {
    val report = PseudonymValidator.validate(source, good, Seq("imei"), PseudonymMode.Hash)
    report.passed shouldBe false
    report.results.head.detail should include("imei")
  }

  "PseudonymValidateApp" should "return 0 for PASS, 1 for FAIL and 2 for bad arguments" in {
    source.createOrReplaceTempView("src_tbl")
    good.createOrReplaceTempView("good_tbl")

    PseudonymValidateApp.execute(spark, Array("src_tbl", "good_tbl", "isdn")) shouldBe 0
    PseudonymValidateApp.execute(spark, Array("src_tbl", "src_tbl", "isdn")) shouldBe 1
    PseudonymValidateApp.execute(spark, Array("src_tbl")) shouldBe 2
    PseudonymValidateApp.execute(spark, Array("src_tbl", "good_tbl", "isdn", "ROT13")) shouldBe 2
    PseudonymValidateApp.execute(spark, Array("src_tbl", "no_such_tbl", "isdn")) shouldBe 2
  }

  "SelfTest" should "pass for HASH and ENCRYPT on generated data" in {
    Seq(PseudonymMode.Hash, PseudonymMode.Encrypt).foreach { mode =>
      val report = SelfTest.run(spark, mode, rows = 400, subscribers = 40)
      withClue(report.render) { report.passed shouldBe true }
    }
  }

  it should "be reachable from the CLI and reject MAPPING" in {
    PseudonymValidateApp.execute(spark, Array("selftest", "HASH", "300")) shouldBe 0
    PseudonymValidateApp.execute(spark, Array("selftest", "MAPPING")) shouldBe 2
    PseudonymValidateApp.execute(spark, Array("selftest", "HASH", "abc")) shouldBe 2
  }
}
