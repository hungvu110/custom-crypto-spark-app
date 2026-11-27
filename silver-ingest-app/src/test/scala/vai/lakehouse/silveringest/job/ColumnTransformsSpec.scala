package vai.lakehouse.silveringest.job

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.silveringest.config.{ColumnTransform, TransformKind}

class ColumnTransformsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().appName("ColumnTransformsSpec").master("local[1]")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def bands(kind: TransformKind, values: Seq[String]): Seq[String] = {
    val session = spark
    import session.implicits._
    val df = values.zipWithIndex.map { case (v, i) => (i, v) }.toDF("id", "value")
    ColumnTransforms(df, Seq(ColumnTransform("value", kind))).orderBy("id").collect().map(_.getString(1)).toSeq
  }

  // en-dash (U+2013), as in the request of the data science team
  private val dash = "–"

  "AGE_BAND" should "gom tuổi vào đúng khung, biên trên và dưới của mỗi khung" in {
    val input = Seq("0", "17", "18", "24", "25", "34", "35", "44", "45", "54", "55", "64", "65", "120")
    bands(TransformKind.AgeBand, input) shouldBe Seq("Under 18", "Under 18", s"18${dash}24", s"18${dash}24", s"25${dash}34",
      s"25${dash}34", s"35${dash}44", s"35${dash}44", s"45${dash}54", s"45${dash}54", s"55${dash}64", s"55${dash}64", "65+", "65+")
  }

  it should "đặt Unknown cho null, rỗng, chữ, số âm, số thập phân và tuổi quá 120" in {
    bands(TransformKind.AgeBand, Seq(null, "", "  ", "abc", "-1", "30.5", "121", "1000", "99999999999")) shouldBe
      Seq.fill(9)("Unknown")
  }

  it should "bỏ khoảng trắng hai đầu trước khi so khung" in {
    bands(TransformKind.AgeBand, Seq(" 30 ", "\t17")) shouldBe Seq(s"25${dash}34", "Under 18")
  }

  "HOUR_BAND" should "gom giờ vào đúng khung, biên trên và dưới của mỗi khung" in {
    val input = Seq("0", "5", "6", "8", "9", "11", "12", "13", "14", "16", "17", "19", "20", "23")
    bands(TransformKind.HourBand, input) shouldBe Seq(s"0${dash}5h", s"0${dash}5h", s"6${dash}8h", s"6${dash}8h", s"9${dash}11h",
      s"9${dash}11h", s"12${dash}13h", s"12${dash}13h", s"14${dash}16h", s"14${dash}16h", s"17${dash}19h", s"17${dash}19h",
      s"20${dash}23h", s"20${dash}23h")
  }

  it should "nhận giờ một hoặc hai chữ số (5 và 05 như nhau)" in {
    bands(TransformKind.HourBand, Seq("5", "05", "09", "9", "00")) shouldBe
      Seq(s"0${dash}5h", s"0${dash}5h", s"9${dash}11h", s"9${dash}11h", s"0${dash}5h")
  }

  it should "đặt Unknown cho null, rỗng, chữ, giờ ngoài 0 đến 23 và số âm" in {
    bands(TransformKind.HourBand, Seq(null, "", "x", "24", "99", "-1", "5.0", "005")) shouldBe Seq.fill(8)("Unknown")
  }

  "ISDN_NATIONAL" should "đưa 84, +84 hoặc 0 cộng 9 chữ số về 9 chữ số như isdn của VTT" in {
    bands(TransformKind.IsdnNational, Seq("84961234567", "+84961234567", "0961234567", " 84321234567 ")) shouldBe
      Seq("961234567", "961234567", "961234567", "321234567")
  }

  it should "giữ nguyên giá trị đã đúng định dạng VTT và mọi giá trị khác, NULL vẫn là NULL" in {
    // 9 chữ số (VTT), 84 + 8 hoặc 10 chữ số, chữ, rỗng: không đoán, giữ nguyên.
    bands(TransformKind.IsdnNational, Seq("961234567", "8496123456", "849612345678", "abc", "", null)) shouldBe
      Seq("961234567", "8496123456", "849612345678", "abc", "", null)
  }

  "ColumnTransforms.apply" should "chỉ đổi các cột đã khai, giữ nguyên tên, thứ tự và các cột còn lại" in {
    val session = spark
    import session.implicits._
    val df = Seq(("a", "30", "05", "x1")).toDF("id", "customer_age", "hour_id", "other")

    val out = ColumnTransforms(df, Seq(ColumnTransform("customer_age", TransformKind.AgeBand),
      ColumnTransform("hour_id", TransformKind.HourBand)))

    out.columns.toSeq shouldBe Seq("id", "customer_age", "hour_id", "other")
    out.collect().head.toSeq shouldBe Seq("a", s"25${dash}34", s"0${dash}5h", "x1")
  }

  it should "đổi kiểu số về chuỗi khi cột nguồn không phải chuỗi" in {
    val session = spark
    import session.implicits._
    val df = Seq((30, 7)).toDF("customer_age", "hour_id")

    val out = ColumnTransforms(df, Seq(ColumnTransform("customer_age", TransformKind.AgeBand),
      ColumnTransform("hour_id", TransformKind.HourBand)))

    out.collect().head.toSeq shouldBe Seq(s"25${dash}34", s"6${dash}8h")
  }

  it should "tìm cột không phân biệt hoa thường và dùng đúng tên cột của dữ liệu" in {
    val session = spark
    import session.implicits._
    val out = ColumnTransforms(Seq("30").toDF("Customer_Age"), Seq(ColumnTransform("customer_age", TransformKind.AgeBand)))

    out.columns.toSeq shouldBe Seq("Customer_Age")
    out.collect().head.getString(0) shouldBe s"25${dash}34"
  }

  it should "báo lỗi nêu tên cột khi cột cần gom nhóm không có trong dữ liệu" in {
    val session = spark
    import session.implicits._
    val error = intercept[IllegalArgumentException] {
      ColumnTransforms(Seq("a").toDF("id"), Seq(ColumnTransform("customer_age", TransformKind.AgeBand)))
    }
    error.getMessage should include("customer_age")
    error.getMessage should include("id")
  }

  it should "trả nguyên DataFrame khi không có transform nào" in {
    val session = spark
    import session.implicits._
    val df = Seq("a").toDF("id")

    ColumnTransforms(df, Nil).collect().head.getString(0) shouldBe "a"
  }
}
