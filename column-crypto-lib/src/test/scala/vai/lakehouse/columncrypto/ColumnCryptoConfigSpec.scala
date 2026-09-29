package vai.lakehouse.columncrypto

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.{FilePrefixSource, PrefixSource}

class ColumnCryptoConfigSpec extends AnyFlatSpec with Matchers {

  private val settingsYaml =
    """
      |datasets:
      |  sample_table:
      |    keyField: "created_at"
      |    encryptedColumns:
      |      - "name"
      |      - "city"
      |  other_table:
      |    keyField: "province_code"
      |    encryptedColumns:
      |      - "isdn"
      |""".stripMargin

  private val DefaultSettings  = "conf/column-crypto.yaml"
  private val DefaultPrefixDir = "conf/key-prefix"

  "parseSettings" should "parse đúng nhiều dataset trong 1 file" in {
    val all = ColumnCryptoConfig.parseSettings(settingsYaml)

    all.keySet shouldBe Set("sample_table", "other_table")
    all("sample_table") shouldBe CryptoSettings("created_at", Seq("name", "city"))
    all("other_table") shouldBe CryptoSettings("province_code", Seq("isdn"))
  }

  it should "trả về map rỗng nếu không có 'datasets'" in {
    ColumnCryptoConfig.parseSettings("foo: bar") shouldBe empty
  }

  it should "ném lỗi rõ ràng nếu thiếu keyField" in {
    val yaml =
      """
        |datasets:
        |  t1:
        |    encryptedColumns: ["c1"]
        |""".stripMargin

    val ex = intercept[IllegalArgumentException](ColumnCryptoConfig.parseSettings(yaml))
    ex.getMessage should include("keyField")
  }

  it should "ném lỗi rõ ràng nếu thiếu encryptedColumns" in {
    val yaml =
      """
        |datasets:
        |  t1:
        |    keyField: "f1"
        |""".stripMargin

    val ex = intercept[IllegalArgumentException](ColumnCryptoConfig.parseSettings(yaml))
    ex.getMessage should include("encryptedColumns")
  }

  "merge" should "ghép settings và prefix thành ColumnCryptoConfig đầy đủ" in {
    val settings = ColumnCryptoConfig.parseSettings(settingsYaml)

    ColumnCryptoConfig.merge("sample_table", settings, "sub_rel") shouldBe
      ColumnCryptoConfig("sub_rel", "created_at", Seq("name", "city"))
  }

  it should "ném lỗi rõ ràng nếu dataset thiếu ở file settings, và không đọc prefix" in {
    val ex = intercept[IllegalArgumentException](
      ColumnCryptoConfig.merge("sample_table", Map.empty, throw new RuntimeException("prefix must not be read")))
    ex.getMessage should include("sample_table")
  }

  "ColumnCryptoConfig.toString" should "không để lộ keyPrefix" in {
    val text = ColumnCryptoConfig("super-secret-prefix", "created_at", Seq("name")).toString
    text should not include "super-secret-prefix"
    text should include("created_at")
  }

  "load" should "đọc đúng config từ classpath resource mặc định" in {
    val cfg = ColumnCryptoConfig.load(DefaultSettings, new FilePrefixSource(DefaultPrefixDir), "sample_table")

    cfg.keyPrefix should not be empty
    cfg.keyField shouldBe "created_at"
    cfg.encryptedColumns should contain allOf ("name", "city")
  }

  it should "lấy keyPrefix từ PrefixSource được truyền vào" in {
    val source = new PrefixSource { override def read(datasetName: String): String = s"prefix-of-$datasetName" }

    ColumnCryptoConfig.load(DefaultSettings, source, "sample_table") shouldBe
      ColumnCryptoConfig("prefix-of-sample_table", "created_at", Seq("name", "city"))
  }

  it should "ném lỗi rõ ràng nếu dataset không tồn tại trong file settings, và không gọi PrefixSource" in {
    val source = new PrefixSource { override def read(datasetName: String): String = throw new RuntimeException("must not be called") }

    val ex = intercept[IllegalArgumentException](
      ColumnCryptoConfig.load(DefaultSettings, source, "khong_ton_tai"))
    ex.getMessage should include("khong_ton_tai")
    ex.getMessage should include("settings")
  }

  it should "ném lỗi rõ ràng nếu file settings không tồn tại ở cả đĩa lẫn classpath" in {
    val ex = intercept[IllegalArgumentException](
      ColumnCryptoConfig.load("khong/ton/tai.yaml", new FilePrefixSource(DefaultPrefixDir), "any"))
    ex.getMessage should include("not found")
  }
}
