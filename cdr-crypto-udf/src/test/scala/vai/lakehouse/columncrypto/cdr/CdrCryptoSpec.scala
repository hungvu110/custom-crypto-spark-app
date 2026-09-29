package vai.lakehouse.columncrypto.cdr

import java.nio.file.Files

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.PrefixSource

/**
 * Test DataFrame API của `CdrCrypto`, đối chiếu với SQL function `cdr_encrypt`/`cdr_decrypt` và với
 * `CdrCipherCore` gọi trực tiếp — 3 đường phải cho đúng cùng kết quả.
 */
class CdrCryptoSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val Prefix = "231x#@Vie3" // trùng prefix trong test vector đối tác
  private val Dataset = "sub_rel_product"

  private val stubSource = new PrefixSource {
    override def read(datasetName: String): String = {
      if (datasetName == "missing") throw new IllegalStateException("no prefix for missing")
      Prefix
    }
  }

  private val cfg = CdrCrypto.loadConfig(Dataset, "start_datetime", Seq("isdn"), stubSource)

  private val warehouse = Files.createTempDirectory("cdr-crypto-df-wh")
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder()
      .appName("CdrCryptoSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.toString)
      .withExtensions(new CdrCryptoExtension(() => stubSource))
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private val schema = StructType(Seq(
    StructField("isdn",           StringType, nullable = true),
    StructField("start_datetime", StringType, nullable = false)))

  private def sampleDf: DataFrame = spark.createDataFrame(
    spark.sparkContext.parallelize(Seq(
      Row("978356099", "20260928123456"),
      Row("912345678", "20260101000000"),
      Row(null,         "20260101000001"))),
    schema)

  "CdrCrypto.encryptColumns/decryptColumns" should "roundtrip đúng dữ liệu gốc, giữ NULL và cột keyField" in {
    val encrypted = CdrCrypto.encryptColumns(sampleDf, cfg)
    val decrypted = CdrCrypto.decryptColumns(encrypted, cfg)

    val enc = encrypted.orderBy("start_datetime").collect()
    enc.map(_.getAs[String]("start_datetime")).toSeq shouldBe
      Seq("20260101000000", "20260101000001", "20260928123456")
    enc.map(_.getAs[String]("isdn")).toSeq.filter(_ != null) should not contain oneOf("978356099", "912345678")
    enc(1).getAs[String]("isdn") shouldBe null

    decrypted.orderBy("start_datetime").collect().map(_.getAs[String]("isdn")).toSeq shouldBe
      Seq("912345678", null, "978356099")
  }

  it should "khớp byte-for-byte với CdrCipherCore.encrypt gọi trực tiếp" in {
    val viaDf = CdrCrypto.encryptColumns(sampleDf, cfg)
      .where("start_datetime = '20260928123456'").collect().head.getAs[String]("isdn")

    viaDf shouldBe CdrCipherCore.encrypt("978356099", Prefix + "20260928123456")
  }

  it should "khớp byte-for-byte với SQL cdr_encrypt (2 đường cho cùng kết quả)" in {
    sampleDf.createOrReplaceTempView("cdr_df_sample")
    val viaSql = spark.sql(s"SELECT cdr_encrypt('$Dataset', isdn, start_datetime) AS c FROM cdr_df_sample " +
      "WHERE isdn = '978356099'").collect().head.getString(0)
    val viaDf = CdrCrypto.encryptColumns(sampleDf, cfg)
      .where("start_datetime = '20260928123456'").collect().head.getAs[String]("isdn")

    viaDf shouldBe viaSql
  }

  it should "giải mã đúng ciphertext mẫu thật của đối tác" in {
    val partnerSample = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("MmQtOaOWlG1rRkt49Gn6Bw==", "20260928123456"))), schema)

    CdrCrypto.decryptColumns(partnerSample, cfg).collect().head.getAs[String]("isdn") shouldBe "978356099"
  }

  it should "mã hoá được nhiều cột cùng lúc" in {
    val multi = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("a", "b", "20260928123456"))),
      StructType(Seq(
        StructField("c1", StringType), StructField("c2", StringType),
        StructField("start_datetime", StringType))))
    val multiCfg = cfg.copy(encryptedColumns = Seq("c1", "c2"))

    val back = CdrCrypto.decryptColumns(CdrCrypto.encryptColumns(multi, multiCfg), multiCfg).collect().head

    (back.getAs[String]("c1"), back.getAs[String]("c2")) shouldBe (("a", "b"))
  }

  it should "ném lỗi rõ ràng nếu schema không hợp lệ" in {
    Seq(
      cfg.copy(encryptedColumns = Seq("khong_co")),
      cfg.copy(keyField = "khong_co"),
      cfg.copy(encryptedColumns = Seq("isdn", "start_datetime")),
      cfg.copy(encryptedColumns = Seq.empty)
    ).foreach { bad =>
      intercept[IllegalArgumentException](CdrCrypto.encryptColumns(sampleDf, bad))
    }
  }

  "CdrCrypto.loadConfig" should "tra keyPrefix từ nguồn theo tên dataset và đẩy lỗi nguồn lên" in {
    CdrCrypto.loadConfig(Dataset, "start_datetime", Seq("isdn"), stubSource).keyPrefix shouldBe Prefix
    intercept[IllegalStateException](CdrCrypto.loadConfig("missing", "start_datetime", Seq("isdn"), stubSource))
  }

  "CdrCryptoConfig.toString" should "không để lộ keyPrefix" in {
    cfg.toString should not include Prefix
    cfg.toString should include("keyField=start_datetime")
  }
}
