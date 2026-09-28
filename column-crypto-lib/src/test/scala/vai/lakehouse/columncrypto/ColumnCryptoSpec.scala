package vai.lakehouse.columncrypto

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.ExtendedMode
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ColumnCryptoSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("ColumnCryptoSpec").master("local[2]").getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private val schema = StructType(Seq(
    StructField("id", LongType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("created_at", StringType, nullable = true)
  ))

  private val rows = Seq(
    Row(1L, "Alice Nguyen", "Ho Chi Minh City", "2025-01-01 08:00:00"),
    Row(2L, "Bob Tran", "Hanoi", "2025-01-02 09:30:00"),
    Row(3L, null, null, "2025-01-03 10:15:00") // giá trị null phải sống sót qua roundtrip
  )

  private def sampleDf() = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  private val cfg = ColumnCryptoConfig("sub_rel", "created_at", Seq("name", "city"))

  "encryptColumns rồi decryptColumns" should "trả về đúng giá trị gốc (roundtrip)" in {
    val df = sampleDf()
    val encrypted = ColumnCrypto.encryptColumns(df, cfg)
    val decrypted = ColumnCrypto.decryptColumns(encrypted, cfg)

    val expected = df.collect().map(r => (r.getAs[Long]("id"), r.getAs[String]("name"), r.getAs[String]("city"))).toSet
    val actual = decrypted.collect().map(r => (r.getAs[Long]("id"), r.getAs[String]("name"), r.getAs[String]("city"))).toSet
    actual shouldBe expected
  }

  it should "sinh ciphertext khác nhau giữa các dòng dù cùng giá trị plaintext (key khác nhau theo keyField)" in {
    val dup = Seq(
      Row(1L, "SameName", "SameCity", "2025-01-01 00:00:00"),
      Row(2L, "SameName", "SameCity", "2025-01-02 00:00:00")
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(dup), schema)
    val encrypted = ColumnCrypto.encryptColumns(df, cfg).collect()

    encrypted(0).getAs[String]("name") should not equal encrypted(1).getAs[String]("name")
  }

  it should "giữ nguyên giá trị null qua cả 2 chiều" in {
    val df = sampleDf()
    val roundTrip = ColumnCrypto.decryptColumns(ColumnCrypto.encryptColumns(df, cfg), cfg)
      .collect().find(_.getAs[Long]("id") == 3L).get

    roundTrip.getAs[String]("name") shouldBe null
    roundTrip.getAs[String]("city") shouldBe null
  }

  "decryptColumns với sai keyPrefix" should "ném lỗi thay vì âm thầm trả sai dữ liệu (AEAD phát hiện sai key)" in {
    val df = sampleDf()
    val encrypted = ColumnCrypto.encryptColumns(df, cfg)
    val wrongCfg = cfg.copy(keyPrefix = "khong-dung-prefix")

    val ex = intercept[Exception] {
      ColumnCrypto.decryptColumns(encrypted, wrongCfg).collect()
    }
    // aes_decrypt của Spark ném lỗi liên quan tới xác thực GCM tag khi sai key.
    ex.getMessage.toLowerCase should (include("tag") or include("decrypt") or include("aes"))
  }

  "encryptColumns/decryptColumns" should "ném lỗi rõ ràng nếu keyField nằm trong encryptedColumns" in {
    val invalidCfg = ColumnCryptoConfig("p", "name", Seq("name"))
    val ex = intercept[IllegalArgumentException](ColumnCrypto.encryptColumns(sampleDf(), invalidCfg))
    ex.getMessage should include("keyField")
  }

  it should "ném lỗi rõ ràng nếu encryptedColumns có cột không tồn tại trong schema" in {
    val invalidCfg = ColumnCryptoConfig("p", "created_at", Seq("khong_ton_tai"))
    val ex = intercept[IllegalArgumentException](ColumnCrypto.encryptColumns(sampleDf(), invalidCfg))
    ex.getMessage should include("khong_ton_tai")
  }

  private val RedactionKey = "spark.sql.redaction.string.regex"

  "redactKeyPrefixInPlans" should "che keyPrefix khỏi explain/queryExecution.toString (dùng cho Spark UI và event log)" in {
    val secretCfg = ColumnCryptoConfig("LEAKY_PREFIX_ABC123", "created_at", Seq("name", "city"))
    try {
      val before = ColumnCrypto.encryptColumns(sampleDf(), secretCfg).queryExecution
      before.explainString(ExtendedMode) should include("LEAKY_PREFIX_ABC123")

      ColumnCrypto.redactKeyPrefixInPlans(spark, secretCfg)

      val after = ColumnCrypto.encryptColumns(sampleDf(), secretCfg).queryExecution
      after.explainString(ExtendedMode) should not include "LEAKY_PREFIX_ABC123"
      after.toString should not include "LEAKY_PREFIX_ABC123"
    } finally spark.conf.unset(RedactionKey)
  }

  it should "giữ nguyên regex che dữ liệu đã cấu hình sẵn và không làm hỏng dữ liệu thật" in {
    val secretCfg = ColumnCryptoConfig("a.b(c)+d", "created_at", Seq("name", "city"))
    try {
      spark.conf.set(RedactionKey, "EXISTING_PATTERN")

      ColumnCrypto.redactKeyPrefixInPlans(spark, secretCfg)

      spark.conf.get(RedactionKey) should include("EXISTING_PATTERN")
      val decrypted = ColumnCrypto.decryptColumns(ColumnCrypto.encryptColumns(sampleDf(), secretCfg), secretCfg)
      decrypted.collect().map(_.getAs[String]("name")).toSet shouldBe Set("Alice Nguyen", "Bob Tran", null)
    } finally spark.conf.unset(RedactionKey)
  }
}
