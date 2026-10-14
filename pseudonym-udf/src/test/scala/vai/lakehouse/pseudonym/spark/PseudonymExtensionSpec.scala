package vai.lakehouse.pseudonym.spark

import vai.lakehouse.pseudonym.ConfigurationException
import vai.lakehouse.pseudonym.TestKeys
import vai.lakehouse.pseudonym.SparkTestSession
import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.core.AesSivPseudonymizer
import vai.lakehouse.pseudonym.policy.{ColumnRule, TablePolicy}

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PseudonymExtensionSpec extends AnyFlatSpec with Matchers with SparkTestSession {

  override protected def extraExtensions: Seq[SparkSessionExtensions => Unit] =
    Seq(new PseudonymExtension(() => TestKeys.provider), new PseudonymRevealExtension(() => TestKeys.provider))

  private def sql(q: String) = spark.sql(q)

  "pseudo_hash" should "khớp DataFrame API (cùng lõi) và golden vector" in {
    val s = spark
    import s.implicits._
    val df = Seq((1, "84903217465"), (2, null)).toDF("id", "isdn")
    df.createOrReplaceTempView("app_event")

    val viaSql = sql("SELECT id, pseudo_hash(isdn) AS isdn FROM app_event ORDER BY id").collect().map(_.toSeq)
    val viaApi = new Pseudonymizer(TestKeys.provider)
      .pseudonymize(df, TablePolicy("app_event", Seq(ColumnRule("isdn", PseudonymMode.Hash))))
      .orderBy("id").collect().map(_.toSeq)

    viaSql shouldBe viaApi
    viaSql.head(1) shouldBe "k1_3fe27d9b4727fb8a0f900bc4126c6164"
    Option(viaSql(1)(1)) shouldBe None
  }

  "pseudo_encrypt / pseudo_reveal" should "roundtrip qua SQL và khớp DataFrame API" in {
    val enc = sql("SELECT pseudo_encrypt('KH0000123456') AS p").collect().head.getString(0)

    enc shouldBe new AesSivPseudonymizer(TestKeys.aesSiv).encrypt("KH0000123456")
    sql(s"SELECT pseudo_reveal('$enc')").collect().head.getString(0) shouldBe "KH0000123456"
  }

  "SQL function" should "báo lỗi rõ ràng khi sai số tham số" in {
    intercept[ConfigurationException](sql("SELECT pseudo_hash('a', 'b')")).getMessage should include("expects 1 argument")
  }

  it should "có mô tả trong DESCRIBE FUNCTION và EXPLAIN không chứa khoá" in {
    sql("DESCRIBE FUNCTION pseudo_hash").collect().map(_.getString(0)).mkString("\n") should include("HMAC-SHA256")
    val plan = sql("EXPLAIN EXTENDED SELECT pseudo_hash('84903217465')").collect().map(_.getString(0)).mkString("\n")
    plan should include("pseudo_hash")
    plan should not include TestKeys.hexOfRange(32)
  }

  "PseudonymExtension() qua spark.sql.extensions" should "đọc khoá từ spark.pseudonym.* (file) và KHÔNG có pseudo_reveal" in {
    val dir = Files.createTempDirectory("pseudonym-keys")
    Files.write(dir.resolve("pseudonym.hmac.v1"), (TestKeys.HmacEncoded + "\n").getBytes(StandardCharsets.UTF_8))
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val confSpark = SparkSession.builder()
      .appName("PseudonymExtensionConf").master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", classOf[PseudonymExtension].getName)
      .config("spark.pseudonym.source", "file")
      .config("spark.pseudonym.file.dir", dir.toString)
      .getOrCreate()
    try {
      confSpark.sql("SELECT pseudo_hash('84903217465')").collect().head.getString(0) shouldBe
        "k1_3fe27d9b4727fb8a0f900bc4126c6164"
      val ex = intercept[Exception](confSpark.sql("SELECT pseudo_reveal('e1_x')"))
      ex.getMessage should include("pseudo_reveal")
    } finally confSpark.stop()
  }
}
