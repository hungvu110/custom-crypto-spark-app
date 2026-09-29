package vai.lakehouse.columncrypto.sql

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.execution.ExtendedMode
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.columncrypto.{ColumnCrypto, ColumnCryptoConfig}
import vai.lakehouse.keyprefix.PrefixSource

class ColumnCryptoExtensionSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val Prefix = "TEST_PREFIX_XYZ"
  private val lookups = new AtomicInteger()

  private val stubSource = new PrefixSource {
    override def read(datasetName: String): String = {
      lookups.incrementAndGet()
      if (datasetName == "missing") throw new IllegalStateException("no prefix for missing")
      s"$Prefix-$datasetName"
    }
  }

  private val warehouse = Files.createTempDirectory("column-crypto-wh")
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder()
      .appName("ColumnCryptoExtensionSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.toString)
      .withExtensions(new ColumnCryptoExtension(() => stubSource))
      .getOrCreate()
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("city", StringType, nullable = true)))
    val rows = Seq(Row(1L, "Alice Nguyen", "Hanoi"), Row(2L, "Bob Tran", "Da Nang"), Row(3L, null, "Hue"))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema).createOrReplaceTempView("people")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def sql(q: String) = spark.sql(q)

  "column_encrypt / column_decrypt" should "roundtrip qua SQL và giữ NULL" in {
    val rows = sql("SELECT id, column_decrypt('customers', column_encrypt('customers', name, id), id) AS name " +
      "FROM people ORDER BY id").collect()

    rows.map(_.getAs[String]("name")).toSeq shouldBe Seq("Alice Nguyen", "Bob Tran", null)
  }

  it should "sinh ciphertext Base64 khác plaintext và khác nhau giữa các dòng cùng giá trị" in {
    val enc = sql("SELECT column_encrypt('customers', 'same', id) AS c FROM people ORDER BY id")
      .collect().map(_.getString(0))

    enc.foreach(_ should not include "same")
    enc.toSet.size shouldBe enc.length
  }

  it should "giải mã được dữ liệu do DataFrame API mã hoá (cùng bảng, cùng keyValue)" in {
    val cfg = ColumnCryptoConfig(s"$Prefix-customers", "id", Seq("name"))
    ColumnCrypto.encryptColumns(spark.table("people"), cfg).createOrReplaceTempView("people_enc_df")

    val names = sql("SELECT column_decrypt('customers', name, id) AS name FROM people_enc_df ORDER BY id")
      .collect().map(_.getAs[String]("name")).toSeq

    names shouldBe Seq("Alice Nguyen", "Bob Tran", null)
  }

  it should "sinh dữ liệu mà DataFrame API giải mã được" in {
    sql("SELECT id, column_encrypt('customers', name, id) AS name, city FROM people")
      .createOrReplaceTempView("people_enc_sql")
    val cfg = ColumnCryptoConfig(s"$Prefix-customers", "id", Seq("name"))

    val names = ColumnCrypto.decryptColumns(spark.table("people_enc_sql"), cfg).orderBy("id")
      .collect().map(_.getAs[String]("name")).toSeq

    names shouldBe Seq("Alice Nguyen", "Bob Tran", null)
  }

  it should "ném lỗi khi giải mã bằng prefix của bảng khác" in {
    sql("SELECT id, column_encrypt('customers', name, id) AS name FROM people").createOrReplaceTempView("enc_a")

    val ex = intercept[Exception](
      sql("SELECT column_decrypt('orders', name, id) FROM enc_a WHERE id = 1").collect())

    ex.getMessage.toLowerCase should (include("tag") or include("aes"))
  }

  it should "ghi/đọc qua bảng thật: INSERT ... SELECT column_encrypt rồi SELECT column_decrypt" in {
    sql("CREATE TABLE customers_tbl (id BIGINT, name STRING, city STRING) USING parquet")
    sql("INSERT INTO customers_tbl SELECT id, column_encrypt('customers', name, id), city FROM people")

    val raw = sql("SELECT name FROM customers_tbl WHERE id = 1").collect().head.getString(0)
    raw should not include "Alice"
    sql("SELECT column_decrypt('customers', name, id) AS name FROM customers_tbl ORDER BY id")
      .collect().map(_.getAs[String]("name")).toSeq shouldBe Seq("Alice Nguyen", "Bob Tran", null)
  }

  it should "dùng được trong view để che việc giải mã" in {
    sql("CREATE OR REPLACE TEMP VIEW enc_tbl AS SELECT id, column_encrypt('customers', name, id) AS name FROM people")
    sql("CREATE OR REPLACE TEMP VIEW plain_v AS SELECT id, column_decrypt('customers', name, id) AS name FROM enc_tbl")

    sql("SELECT name FROM plain_v WHERE id = 2").collect().head.getString(0) shouldBe "Bob Tran"
  }

  it should "che keyPrefix khỏi explain và không làm regex che dữ liệu phình ra khi gọi lặp lại" in {
    val query = "SELECT column_encrypt('customers', name, id) FROM people"
    sql(query).collect()
    val regexAfterFirst = spark.conf.get("spark.sql.redaction.string.regex")
    (1 to 3).foreach(_ => sql(query).collect())

    sql(query).queryExecution.explainString(ExtendedMode) should not include Prefix
    spark.conf.get("spark.sql.redaction.string.regex") shouldBe regexAfterFirst
  }

  it should "báo lỗi rõ ràng nếu tham số bảng không phải chuỗi hằng" in {
    val ex = intercept[IllegalArgumentException](sql("SELECT column_encrypt(city, name, id) FROM people"))

    ex.getMessage should include("column_encrypt")
    ex.getMessage should include("constant string")
  }

  it should "báo lỗi rõ ràng nếu sai số lượng tham số" in {
    val ex = intercept[IllegalArgumentException](sql("SELECT column_decrypt('customers', name) FROM people"))

    ex.getMessage should include("3 arguments")
  }

  it should "đẩy lỗi của nguồn keyPrefix lên câu query" in {
    val ex = intercept[IllegalStateException](sql("SELECT column_encrypt('missing', name, id) FROM people"))

    ex.getMessage should include("no prefix for missing")
    ex.getMessage should not include Prefix
  }

  it should "có mô tả trong DESCRIBE FUNCTION" in {
    val text = sql("DESCRIBE FUNCTION column_encrypt").collect().map(_.getString(0)).mkString("\n")

    text should include("column_encrypt(table, value, keyValue)")
  }

  "ColumnCryptoExtension() qua spark.sql.extensions" should "dựng nguồn từ spark.columncrypto.* (file)" in {
    val dir = Files.createTempDirectory("ext-prefix")
    Files.write(dir.resolve("customers"), "conf_prefix\n".getBytes(StandardCharsets.UTF_8))
    val confSpark = SparkSession.builder()
      .appName("ColumnCryptoExtensionConf").master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", classOf[ColumnCryptoExtension].getName)
      .config("spark.columncrypto.source", "file")
      .config("spark.columncrypto.file.dir", dir.toString)
      .getOrCreate().newSession()
    try {
      val out = confSpark.sql("SELECT column_decrypt('customers', column_encrypt('customers', 'x', 1), 1)")
        .collect().head.getString(0)
      out shouldBe "x"
    } finally confSpark.stop()
  }
}
