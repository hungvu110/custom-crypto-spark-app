package vai.lakehouse.columncrypto.cdr

import java.nio.file.Files
import java.util.Locale
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.{ColumnKeyIssue, ColumnKeySource, ColumnKeysUnavailableException, KeyIntent}

/**
 * Test qua SQL thật (`local[2]`), dùng thẳng jar đối tác (đã cài `install:install-file` — xem
 * `docs/cdr-crypto/PARTNER_CDR_CRYPTO_UDF_PLAN.md` mục 1.1). Đối chiếu 2 chiều với [[CdrCipherCore]] gọi
 * trực tiếp (không qua SQL) để xác nhận UDF không làm lệch kết quả so với lõi crypto.
 *
 * Nguồn key giả: cột `isdn` (và mọi cột khác) dùng `Prefix`, riêng cột `imsi` dùng `OtherPrefix` — để kiểm mỗi cột một key.
 */
class CdrCryptoExtensionSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val Prefix = "231x#@Vie3" // trùng prefix trong test vector đối tác, để cross-check dễ đọc
  private val OtherPrefix = "other-col-key"
  private val calls = new ConcurrentLinkedQueue[(String, Seq[String], KeyIntent)]()

  private val stubSource = new ColumnKeySource {
    override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
      calls.add((datasetName, columns, intent))
      if (datasetName == "missing") throw new IllegalStateException("no key for missing")
      if (columns.contains("pending_col")) {
        throw new ColumnKeysUnavailableException(datasetName,
          Seq(ColumnKeyIssue("pending_col", "PENDING", Some("YC-2026-000009"), None)), Seq("req-1"))
      }
      columns.map { c =>
        val lower = c.toLowerCase(Locale.ROOT)
        lower -> (if (lower == "imsi") OtherPrefix else Prefix)
      }.toMap
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
    val rows = sql("SELECT cdr_decrypt('sub_rel_product', 'isdn', " +
      "cdr_encrypt('sub_rel_product', 'isdn', isdn, start_datetime), start_datetime) AS isdn " +
      "FROM cdr_sample ORDER BY start_datetime").collect()

    // Sắp theo start_datetime tăng dần: 20260101000000 (912345678) < 20260101000001 (null) <
    // 20260928123456 (978356099).
    rows.map(_.getAs[String]("isdn")).toSeq shouldBe Seq("912345678", null, "978356099")
  }

  it should "khớp byte-for-byte với CdrCipherCore.encrypt gọi trực tiếp bằng key của cột (không qua SQL)" in {
    val viaSql = sql("SELECT cdr_encrypt('sub_rel_product', 'isdn', isdn, start_datetime) AS c " +
      "FROM cdr_sample WHERE isdn = '978356099'").collect().head.getString(0)

    viaSql shouldBe CdrCipherCore.encrypt("978356099", Prefix + "20260928123456")
  }

  it should "giải mã được ciphertext mẫu thật của đối tác (test vector đã xác nhận)" in {
    sql("SELECT '978356099' AS isdn, '20260928123456' AS start_datetime")
      .createOrReplaceTempView("known_vector")

    val decrypted = sql("SELECT cdr_decrypt('sub_rel_product', 'isdn', 'MmQtOaOWlG1rRkt49Gn6Bw==', start_datetime) " +
      "AS isdn FROM known_vector").collect().head.getString(0)

    decrypted shouldBe "978356099"
  }

  it should "dùng key của cột ghi ở tham số 2: hai cột của cùng bảng có hai key khác nhau" in {
    val byIsdn = sql("SELECT cdr_encrypt('sub_rel_product', 'isdn', '978356099', '20260928123456')").collect().head.getString(0)
    val byImsi = sql("SELECT cdr_encrypt('sub_rel_product', 'imsi', '978356099', '20260928123456')").collect().head.getString(0)

    byImsi shouldBe CdrCipherCore.encrypt("978356099", OtherPrefix + "20260928123456")
    byImsi should not be byIsdn
    // Mã hoá bằng key cột isdn rồi giải mã bằng key cột imsi thì không ra giá trị gốc.
    val wrongKey = scala.util.Try(
      sql(s"SELECT cdr_decrypt('sub_rel_product', 'imsi', '$byIsdn', '20260928123456')").collect().head.getString(0))
    wrongKey.toOption should not be Some("978356099")
  }

  it should "nhận hằng số làm giá trị để tìm theo giá trị đã mã hoá" in {
    sql("CREATE OR REPLACE TEMP VIEW cdr_enc_search AS " +
      "SELECT cdr_encrypt('sub_rel_product', 'isdn', isdn, start_datetime) AS isdn, start_datetime FROM cdr_sample")

    val found = sql("SELECT start_datetime FROM cdr_enc_search " +
      "WHERE isdn = cdr_encrypt('sub_rel_product', 'isdn', '978356099', start_datetime)").collect().map(_.getString(0))

    found.toSeq shouldBe Seq("20260928123456")
  }

  it should "chọn key theo tên ở tham số 2 kể cả khi cột đã bị đổi tên (alias, subquery)" in {
    val out = sql("SELECT cdr_decrypt('sub_rel_product', 'isdn', x, k) AS isdn FROM (" +
      "SELECT cdr_encrypt('sub_rel_product', 'isdn', isdn, start_datetime) AS x, start_datetime AS k FROM cdr_sample) " +
      "WHERE k = '20260928123456'").collect().head.getString(0)

    out shouldBe "978356099"
  }

  it should "không phân biệt hoa thường ở tên cột" in {
    val upper = sql("SELECT cdr_encrypt('sub_rel_product', 'ISDN', '978356099', '20260928123456')").collect().head.getString(0)

    upper shouldBe CdrCipherCore.encrypt("978356099", Prefix + "20260928123456")
  }

  it should "xin key với intent Encrypt khi mã hoá và Decrypt khi giải mã" in {
    // Key được xin ở bước analyze (lúc gọi sql()), nên không cần chạy câu lệnh.
    calls.clear()
    sql("SELECT cdr_encrypt('sub_rel_product', 'isdn', isdn, start_datetime) FROM cdr_sample")
    val encryptCalls = calls.asScala.toList
    calls.clear()
    sql("SELECT cdr_decrypt('sub_rel_product', 'isdn', isdn, start_datetime) FROM cdr_sample")
    val decryptCalls = calls.asScala.toList

    encryptCalls should not be empty
    all(encryptCalls) shouldBe (("sub_rel_product", Seq("isdn"), KeyIntent.Encrypt))
    decryptCalls should not be empty
    all(decryptCalls) shouldBe (("sub_rel_product", Seq("isdn"), KeyIntent.Decrypt))
  }

  it should "ghi/đọc qua bảng thật: INSERT ... SELECT cdr_encrypt rồi SELECT cdr_decrypt" in {
    sql("CREATE TABLE cdr_tbl (isdn STRING, start_datetime STRING) USING parquet")
    sql("INSERT INTO cdr_tbl SELECT cdr_encrypt('sub_rel_product', 'isdn', isdn, start_datetime), start_datetime " +
      "FROM cdr_sample")

    val raw = sql("SELECT isdn FROM cdr_tbl WHERE start_datetime = '20260928123456'").collect().head.getString(0)
    raw should not include "978356099"

    sql("SELECT cdr_decrypt('sub_rel_product', 'isdn', isdn, start_datetime) AS isdn FROM cdr_tbl ORDER BY start_datetime")
      .collect().map(_.getAs[String]("isdn")).toSeq shouldBe Seq("912345678", null, "978356099")
  }

  it should "dùng được trong view để che việc giải mã" in {
    sql("CREATE OR REPLACE TEMP VIEW cdr_enc AS " +
      "SELECT cdr_encrypt('sub_rel_product', 'isdn', isdn, start_datetime) AS isdn, start_datetime FROM cdr_sample")
    sql("CREATE OR REPLACE TEMP VIEW cdr_plain AS " +
      "SELECT cdr_decrypt('sub_rel_product', 'isdn', isdn, start_datetime) AS isdn FROM cdr_enc")

    sql("SELECT isdn FROM cdr_plain WHERE isdn = '912345678'").collect().head.getString(0) shouldBe "912345678"
  }

  it should "ném lỗi khi giải mã sai keyField (sai keyInput)" in {
    sql("SELECT cdr_encrypt('sub_rel_product', 'isdn', isdn, start_datetime) AS isdn, start_datetime " +
      "FROM cdr_sample WHERE isdn IS NOT NULL").createOrReplaceTempView("enc_a")

    val ex = intercept[Exception](
      sql("SELECT cdr_decrypt('sub_rel_product', 'isdn', isdn, '00000000000000') FROM enc_a").collect())

    ex.getMessage.toLowerCase should include("cdr partner decrypt failed")
  }

  it should "báo lỗi rõ ràng nếu tham số bảng hoặc tham số cột không phải chuỗi hằng" in {
    val badTable = intercept[IllegalArgumentException](
      sql("SELECT cdr_encrypt(start_datetime, 'isdn', isdn, start_datetime) FROM cdr_sample"))
    badTable.getMessage should include("cdr_encrypt")
    badTable.getMessage should include("first argument")

    val badColumn = intercept[IllegalArgumentException](
      sql("SELECT cdr_encrypt('sub_rel_product', isdn, isdn, start_datetime) FROM cdr_sample"))
    badColumn.getMessage should include("second argument")
    badColumn.getMessage should include("column name")
  }

  it should "báo lỗi rõ ràng nếu sai số lượng tham số, kể cả cú pháp 3 tham số cũ" in {
    val ex = intercept[IllegalArgumentException](
      sql("SELECT cdr_decrypt('sub_rel_product', isdn, start_datetime) FROM cdr_sample"))

    ex.getMessage should include("4 arguments")
    ex.getMessage should include("cdr_decrypt('demo_db.sub_rel_product', 'isdn', isdn, start_datetime)")
  }

  it should "đẩy lỗi của nguồn key lên câu query" in {
    val ex = intercept[IllegalStateException](
      sql("SELECT cdr_encrypt('missing', 'isdn', isdn, start_datetime) FROM cdr_sample"))

    ex.getMessage should include("no key for missing")
  }

  it should "báo cột chưa có key kèm mã YC, không lộ key" in {
    val ex = intercept[ColumnKeysUnavailableException](
      sql("SELECT cdr_encrypt('sub_rel_product', 'pending_col', isdn, start_datetime) FROM cdr_sample"))

    ex.getMessage should include("PENDING")
    ex.getMessage should include("YC-2026-000009")
    ex.getMessage should not include Prefix
  }

  it should "có mô tả cú pháp 4 tham số trong DESCRIBE FUNCTION" in {
    val text = sql("DESCRIBE FUNCTION cdr_decrypt").collect().map(_.getString(0)).mkString("\n")

    text should include("cdr_decrypt(table, column, value, fieldValue)")
  }

  "CdrCryptoExtension() qua spark.sql.extensions" should "báo lỗi rõ khi spark.cdrcrypto.source không phải dak" in {
    // Phải là SparkContext MỚI thì spark.sql.extensions và spark.cdrcrypto.* mới có tác dụng; getOrCreate() trên session đang
    // chạy sẽ trả lại session cũ (nguồn key giả). Test cuối của suite nên dừng session chính được.
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val confSpark = SparkSession.builder()
      .appName("CdrCryptoExtensionConf").master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", classOf[CdrCryptoExtension].getName)
      .config("spark.cdrcrypto.source", "file")
      .getOrCreate()
    try {
      val ex = intercept[IllegalArgumentException](
        confSpark.sql("SELECT cdr_encrypt('demo_db.t', 'isdn', '978356099', '20260928123456')").collect())
      ex.getMessage should include("only available from DAK")
      ex.getMessage should include("spark.cdrcrypto.source")
    } finally confSpark.stop()
  }
}
