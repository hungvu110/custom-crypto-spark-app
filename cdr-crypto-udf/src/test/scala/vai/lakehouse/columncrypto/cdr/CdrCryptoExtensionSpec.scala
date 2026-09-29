package vai.lakehouse.columncrypto.cdr

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.PrefixSource

/**
 * Test qua SQL thật (`local[2]`), dùng thẳng jar đối tác (đã cài `install:install-file` — xem
 * `docs/PARTNER_CDR_CRYPTO_UDF_PLAN.md` mục 1.1). Đối chiếu 2 chiều với [[CdrCipherCore]] gọi
 * trực tiếp (không qua SQL) để xác nhận UDF không làm lệch kết quả so với lõi crypto.
 */
class CdrCryptoExtensionSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val Prefix = "231x#@Vie3" // trùng prefix trong test vector đối tác, để cross-check dễ đọc
  private val lookups = new AtomicInteger()

  private val stubSource = new PrefixSource {
    override def read(datasetName: String): String = {
      lookups.incrementAndGet()
      if (datasetName == "missing") throw new IllegalStateException("no prefix for missing")
      Prefix
    }
  }

  private val warehouse = Files.createTempDirectory("cdr-crypto-wh")
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder()
      .appName("CdrCryptoExtensionSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.toString)
      .withExtensions(new CdrCryptoExtension(() => stubSource))
      .getOrCreate()
    val schema = StructType(Seq(
      StructField("isdn",           StringType, nullable = true),
      StructField("start_datetime", StringType, nullable = false)))
    val rows = Seq(
      Row("978356099", "20260928123456"),
      Row("912345678", "20260101000000"),
      Row(null,         "20260101000001"))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema).createOrReplaceTempView("cdr_sample")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def sql(q: String) = spark.sql(q)

  "cdr_encrypt / cdr_decrypt" should "roundtrip qua SQL và giữ NULL" in {
    val rows = sql("SELECT cdr_decrypt('sub_rel_product', " +
      "cdr_encrypt('sub_rel_product', isdn, start_datetime), start_datetime) AS isdn " +
      "FROM cdr_sample ORDER BY start_datetime").collect()

    // Sắp theo start_datetime tăng dần: 20260101000000 (912345678) < 20260101000001 (null) <
    // 20260928123456 (978356099).
    rows.map(_.getAs[String]("isdn")).toSeq shouldBe Seq("912345678", null, "978356099")
  }

  it should "khớp byte-for-byte với CdrCipherCore.encrypt gọi trực tiếp (không qua SQL)" in {
    val viaSql = sql("SELECT cdr_encrypt('sub_rel_product', isdn, start_datetime) AS c " +
      "FROM cdr_sample WHERE isdn = '978356099'").collect().head.getString(0)

    val viaDirect = CdrCipherCore.encrypt("978356099", Prefix + "20260928123456")

    viaSql shouldBe viaDirect
  }

  it should "giải mã được ciphertext mẫu thật của đối tác (test vector đã xác nhận)" in {
    sql("SELECT '978356099' AS isdn, '20260928123456' AS start_datetime")
      .createOrReplaceTempView("known_vector")

    val decrypted = sql("SELECT cdr_decrypt('sub_rel_product', 'MmQtOaOWlG1rRkt49Gn6Bw==', start_datetime) AS isdn " +
      "FROM known_vector").collect().head.getString(0)

    decrypted shouldBe "978356099"
  }

  it should "ghi/đọc qua bảng thật: INSERT ... SELECT cdr_encrypt rồi SELECT cdr_decrypt" in {
    sql("CREATE TABLE cdr_tbl (isdn STRING, start_datetime STRING) USING parquet")
    sql("INSERT INTO cdr_tbl SELECT cdr_encrypt('sub_rel_product', isdn, start_datetime), start_datetime FROM cdr_sample")

    val raw = sql("SELECT isdn FROM cdr_tbl WHERE start_datetime = '20260928123456'").collect().head.getString(0)
    raw should not include "978356099"

    sql("SELECT cdr_decrypt('sub_rel_product', isdn, start_datetime) AS isdn FROM cdr_tbl ORDER BY start_datetime")
      .collect().map(_.getAs[String]("isdn")).toSeq shouldBe Seq("912345678", null, "978356099")
  }

  it should "dùng được trong view để che việc giải mã" in {
    sql("CREATE OR REPLACE TEMP VIEW cdr_enc AS " +
      "SELECT cdr_encrypt('sub_rel_product', isdn, start_datetime) AS isdn, start_datetime FROM cdr_sample")
    sql("CREATE OR REPLACE TEMP VIEW cdr_plain AS " +
      "SELECT cdr_decrypt('sub_rel_product', isdn, start_datetime) AS isdn FROM cdr_enc")

    sql("SELECT isdn FROM cdr_plain WHERE isdn = '912345678'").collect().head.getString(0) shouldBe "912345678"
  }

  it should "ném lỗi khi giải mã bằng prefix của bảng khác (sai key)" in {
    sql("SELECT cdr_encrypt('sub_rel_product', isdn, start_datetime) AS isdn, start_datetime " +
      "FROM cdr_sample WHERE isdn IS NOT NULL").createOrReplaceTempView("enc_a")

    // stubSource trả cùng 1 Prefix cho mọi dataset, nên đổi field (start_datetime khác) là đủ để
    // tạo sai keyInput -> mô phỏng "prefix của bảng khác" mà không cần 2 giá trị prefix riêng.
    val ex = intercept[Exception](
      sql("SELECT cdr_decrypt('sub_rel_product', isdn, '00000000000000') FROM enc_a").collect())

    ex.getMessage.toLowerCase should include("cdr partner decrypt failed")
  }

  it should "báo lỗi rõ ràng nếu tham số bảng không phải chuỗi hằng" in {
    val ex = intercept[IllegalArgumentException](
      sql("SELECT cdr_encrypt(start_datetime, isdn, start_datetime) FROM cdr_sample"))

    ex.getMessage should include("cdr_encrypt")
    ex.getMessage should include("constant string")
  }

  it should "báo lỗi rõ ràng nếu sai số lượng tham số" in {
    val ex = intercept[IllegalArgumentException](
      sql("SELECT cdr_decrypt('sub_rel_product', isdn) FROM cdr_sample"))

    ex.getMessage should include("3 arguments")
  }

  it should "đẩy lỗi của nguồn keyPrefix lên câu query" in {
    val ex = intercept[IllegalStateException](
      sql("SELECT cdr_encrypt('missing', isdn, start_datetime) FROM cdr_sample"))

    ex.getMessage should include("no prefix for missing")
  }

  it should "có mô tả trong DESCRIBE FUNCTION" in {
    val text = sql("DESCRIBE FUNCTION cdr_decrypt").collect().map(_.getString(0)).mkString("\n")

    text should include("cdr_decrypt(table, value, fieldValue)")
  }

  "CdrCryptoExtension() qua spark.sql.extensions" should "dựng nguồn từ spark.cdrcrypto.* (file), namespace RIÊNG với column_encrypt" in {
    val dir = Files.createTempDirectory("cdr-ext-prefix")
    Files.write(dir.resolve("sub_rel_product"), (Prefix + "\n").getBytes(StandardCharsets.UTF_8))
    val confSpark = SparkSession.builder()
      .appName("CdrCryptoExtensionConf").master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", classOf[CdrCryptoExtension].getName)
      .config("spark.cdrcrypto.source", "file")
      .config("spark.cdrcrypto.file.dir", dir.toString)
      .getOrCreate().newSession()
    try {
      val out = confSpark.sql(
        "SELECT cdr_decrypt('sub_rel_product', " +
          "cdr_encrypt('sub_rel_product', '978356099', '20260928123456'), '20260928123456')")
        .collect().head.getString(0)
      out shouldBe "978356099"
    } finally confSpark.stop()
  }
}
