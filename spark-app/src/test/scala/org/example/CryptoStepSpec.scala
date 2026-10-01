package org.example

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Kiểm tra bước mã hoá theo cấu hình. `column-crypto-lib` + `key-prefix-lib` chỉ có ở scope test:
 * mô phỏng đúng cách deploy thật (extension nạp qua spark.sql.extensions, app chỉ gọi hàm theo tên).
 */
class CryptoStepSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val Table = "sample_table"
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val prefixDir = Files.createTempDirectory("crypto-step-prefix")
    prefixDir.toFile.deleteOnExit()
    val prefixFile = Files.write(prefixDir.resolve(Table), "test-prefix".getBytes(StandardCharsets.UTF_8))
    prefixFile.toFile.deleteOnExit()

    spark = SparkSession.builder()
      .appName("CryptoStepSpec")
      .master("local[2]")
      .config("spark.sql.extensions", "vai.lakehouse.columncrypto.sql.ColumnCryptoExtension")
      .config("spark.columncrypto.source", "file")
      .config("spark.columncrypto.file.dir", prefixDir.toString)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private val settings =
    CryptoSettings("column_encrypt", "column_decrypt", Seq("name", "city"), "created_at")

  "parse" should "trả None khi cả 2 hàm đều rỗng (không mã hoá)" in {
    CryptoStep.parse("", "  ", "name", "created_at") shouldBe None
  }

  it should "tách danh sách cột theo dấu phẩy và bỏ khoảng trắng" in {
    CryptoStep.parse("column_encrypt", "column_decrypt", " name , city ,", "created_at") shouldBe Some(settings)
  }

  it should "từ chối cấu hình dở dang hoặc sai" in {
    Seq(
      ("column_encrypt", "", "name", "created_at"),
      ("", "column_decrypt", "name", "created_at"),
      ("column_encrypt; DROP", "column_decrypt", "name", "created_at"),
      ("column_encrypt", "column_decrypt", " , ", "created_at"),
      ("column_encrypt", "column_decrypt", "name", ""),
      ("column_encrypt", "column_decrypt", "name,created_at", "created_at")
    ).foreach { case (enc, dec, cols, key) =>
      intercept[IllegalArgumentException](CryptoStep.parse(enc, dec, cols, key))
    }
  }

  "encrypt/decrypt" should "mã hoá đúng cột cấu hình và giải mã lại đúng dữ liệu gốc" in {
    val plain     = BusinessLogic.sampleDataFrame(spark)
    val encrypted = CryptoStep.encrypt(plain, Table, settings)

    val encRows = encrypted.orderBy("id").collect()
    val plainRows = plain.orderBy("id").collect()
    encRows.zip(plainRows).foreach { case (e, p) =>
      e.getAs[String]("name") should not be p.getAs[String]("name")
      e.getAs[String]("city") should not be p.getAs[String]("city")
      e.getAs[String]("created_at") shouldBe p.getAs[String]("created_at")
    }

    val decrypted = CryptoStep.decrypt(encrypted, Table, settings).orderBy("id").collect()
    decrypted.map(_.toSeq) shouldBe plainRows.map(_.toSeq)
  }

  it should "ném lỗi rõ ràng nếu cột hoặc keyField không có trong schema" in {
    val plain = BusinessLogic.sampleDataFrame(spark)

    val missingCol = intercept[IllegalArgumentException](
      CryptoStep.encrypt(plain, Table, settings.copy(encryptedColumns = Seq("khong_co"))))
    missingCol.getMessage should include("khong_co")

    val missingKey = intercept[IllegalArgumentException](
      CryptoStep.encrypt(plain, Table, settings.copy(keyField = "khong_co")))
    missingKey.getMessage should include("keyField")
  }

  "encrypt" should "báo lỗi phân tích khi hàm chưa được extension nào đăng ký" in {
    val plain = BusinessLogic.sampleDataFrame(spark)

    intercept[AnalysisException](
      CryptoStep.encrypt(plain, Table, settings.copy(encryptFunction = "ham_chua_nap")))
  }
}
