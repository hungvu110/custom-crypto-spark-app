package vai.lakehouse.columncrypto.cdr

import java.nio.file.Files
import java.util.Locale

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.{ColumnKeySource, KeyIntent}

/**
 * Test DataFrame API của `CdrCrypto`, đối chiếu với SQL function `cdr_encrypt`/`cdr_decrypt` và với
 * `CdrCipherCore` gọi trực tiếp — 3 đường phải cho đúng cùng kết quả. Mỗi cột một key: cột `c2` dùng `OtherPrefix`.
 */
class CdrCryptoSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val Prefix = "231x#@Vie3" // trùng prefix trong test vector đối tác
  private val OtherPrefix = "other-col-key"
  private val Dataset = "sub_rel_product"

  @volatile private var lastCall: Option[(String, Seq[String], KeyIntent)] = None

  private val stubSource = new ColumnKeySource {
    override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
      lastCall = Some((datasetName, columns, intent))
      if (datasetName == "missing") throw new IllegalStateException("no key for missing")
      columns.map { c =>
        val lower = c.toLowerCase(Locale.ROOT)
        lower -> (if (lower == "c2") OtherPrefix else Prefix)
      }.toMap
    }
  }

  private val cfg = CdrCrypto.loadConfig(Dataset, "start_datetime", Seq("isdn"), KeyIntent.Encrypt, stubSource)

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

  private def multiDf: DataFrame = spark.createDataFrame(
    spark.sparkContext.parallelize(Seq(Row("a", "b", "20260928123456"))),
    StructType(Seq(StructField("c1", StringType), StructField("c2", StringType), StructField("start_datetime", StringType))))

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
    val viaSql = spark.sql(s"SELECT cdr_encrypt('$Dataset', 'isdn', isdn, start_datetime) AS c FROM cdr_df_sample " +
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

  it should "mã hoá nhiều cột cùng lúc, mỗi cột bằng key của chính nó" in {
    val multiCfg = CdrCrypto.loadConfig(Dataset, "start_datetime", Seq("c1", "c2"), KeyIntent.Encrypt, stubSource)

    val encrypted = CdrCrypto.encryptColumns(multiDf, multiCfg).collect().head
    val back = CdrCrypto.decryptColumns(CdrCrypto.encryptColumns(multiDf, multiCfg), multiCfg).collect().head

    encrypted.getAs[String]("c1") shouldBe CdrCipherCore.encrypt("a", Prefix + "20260928123456")
    encrypted.getAs[String]("c2") shouldBe CdrCipherCore.encrypt("b", OtherPrefix + "20260928123456")
    (back.getAs[String]("c1"), back.getAs[String]("c2")) shouldBe (("a", "b"))
  }

  it should "ném lỗi rõ ràng nếu schema không hợp lệ hoặc thiếu key của một cột" in {
    Seq(
      cfg.copy(encryptedColumns = Seq("khong_co")),
      cfg.copy(keyField = "khong_co"),
      cfg.copy(encryptedColumns = Seq("isdn", "start_datetime")),
      cfg.copy(encryptedColumns = Seq.empty),
      cfg.copy(keyPrefixes = Map.empty)
    ).foreach { bad =>
      intercept[IllegalArgumentException](CdrCrypto.encryptColumns(sampleDf, bad))
    }
  }

  "CdrCrypto.loadConfig" should "xin key của mọi cột trong một lời gọi, đúng intent, và đẩy lỗi nguồn lên" in {
    val loaded = CdrCrypto.loadConfig(Dataset, "start_datetime", Seq("c1", "C2"), KeyIntent.Decrypt, stubSource)

    loaded.keyPrefixes shouldBe Map("c1" -> Prefix, "c2" -> OtherPrefix)
    lastCall shouldBe Some((Dataset, Seq("c1", "C2"), KeyIntent.Decrypt))
    intercept[IllegalStateException](
      CdrCrypto.loadConfig("missing", "start_datetime", Seq("isdn"), KeyIntent.Decrypt, stubSource))
  }

  "CdrCryptoConfig.toString" should "không để lộ key của cột nào" in {
    val multiCfg = CdrCrypto.loadConfig(Dataset, "start_datetime", Seq("c1", "c2"), KeyIntent.Encrypt, stubSource)

    multiCfg.toString should not include Prefix
    multiCfg.toString should not include OtherPrefix
    multiCfg.toString should include("keyField=start_datetime")
    multiCfg.toString should include("c2")
  }

  // ---------------------------------------------------------------- lỗi giải mã: đủ để chẩn đoán, không lộ dữ liệu

  private def decryptFailure(value: String, field: String, prefix: String = "wrong-prefix"): String = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(value, field))), schema)
    val wrong = CdrCryptoConfig(Map("isdn" -> prefix), "start_datetime", Seq("isdn"), Dataset)
    val error = intercept[Exception](CdrCrypto.decryptColumns(df, wrong).collect())
    Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).map(_.getMessage).filter(_ != null).mkString(" | ")
  }

  "Lỗi giải mã" should "nêu bảng, cột, keyField, hình dạng của keyField và bản mã, và loại lỗi (khoá sai)" in {
    val ciphertext = CdrCipherCore.encrypt("978356099", Prefix + "20260928123456")

    val message = decryptFailure(ciphertext, "20260928123456")

    message should include(s"dataset $Dataset, column isdn, keyField start_datetime")
    message should include("keyField value: 14 chars d14")
    message should include("ciphertext: 24 chars base64, 16 bytes (a multiple of 16)")
    message should include("BadPaddingException")
    message should include("wrong key: the prefix, or the keyField value as a string, differs from the one used to encrypt")
    // Nothing of the data or of the key.
    message should not include "978356099"
    message should not include "20260928123456"
    message should not include ciphertext
    message should not include "wrong-prefix"
  }

  it should "cho thấy keyField khác định dạng (vd timestamp ép sang chuỗi) qua hình dạng, không qua giá trị" in {
    val ciphertext = CdrCipherCore.encrypt("978356099", Prefix + "20260928123456")

    val message = decryptFailure(ciphertext, "2026-09-28 12:34:56", Prefix)

    message should include("keyField value: 19 chars d4-d2-d2_1d2:d2:d2")
    message should not include "2026-09-28"
  }

  it should "nói rõ khi giá trị không phải bản mã (bản rõ, độ dài không phải bội số 16)" in {
    val message = decryptFailure("84912345678", "20260928123456", Prefix)

    message should include("ciphertext: 11 chars, not base64 of a multiple of 16 bytes")
    message should include("not a provider ciphertext")
    message should not include "84912345678"
  }

  "Hình dạng (CdrCrypto.shapeOf)" should "gộp các ký tự cùng loại, giữ dấu câu, và cắt khi quá dài" in {
    CdrCrypto.shapeOf("") shouldBe "0 chars"
    CdrCrypto.shapeOf("VN84a-1") shouldBe "7 chars a2d2a1-d1"
    CdrCrypto.shapeOf("1.0E9") shouldBe "5 chars d1.d1a1d1"
    CdrCrypto.shapeOf(" 123 ") shouldBe "5 chars _1d3_1"
    CdrCrypto.shapeOf("a-" * 40) should endWith("...")
  }
}

