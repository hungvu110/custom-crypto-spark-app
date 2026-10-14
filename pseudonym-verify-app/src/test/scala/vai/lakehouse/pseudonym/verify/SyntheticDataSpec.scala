package vai.lakehouse.pseudonym.verify

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.pseudonym.verify.SyntheticData._

class SyntheticDataSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = spark = LocalSpark.session("SyntheticDataSpec", KeyMaterial.ephemeral())

  override def afterAll(): Unit = LocalSpark.stop(spark)

  private def generate(extra: (String, String)*) = SyntheticData.generate(spark, LocalSpark.smallConfig(extra: _*))

  "IdentityFormats" should "là song ánh: mỗi chủ thể 1 isdn/imei/customer_id riêng, đúng định dạng" in {
    val sub = generate("NULL_RATE" -> "0", "N_SUBJECTS" -> "5000").find(_.name == Subscriber).get.raw

    Seq(Isdn, Imei, CustomerId).foreach(c => sub.select(c).distinct().count() shouldBe 5000)
    sub.where(!col(Isdn).rlike("^849[0-9]{8}$")).count() shouldBe 0
    sub.where(!col(Imei).rlike("^35[0-9]{13}$")).count() shouldBe 0
    sub.where(!col(CustomerId).rlike("^KH[0-9]{10}$")).count() shouldBe 0
  }

  "SyntheticData" should "sinh đủ 4 bảng với số dòng theo cấu hình" in {
    generate().map(t => t.name -> t.raw.count()).toMap shouldBe
      Map(Subscriber -> 300, AppEvent -> 1200, BtsConnection -> 900, LoyaltyTxn -> 600)
  }

  it should "tất định theo SEED, khác SEED thì khác dữ liệu" in {
    val a = generate().find(_.name == AppEvent).get.raw
    val b = generate().find(_.name == AppEvent).get.raw
    val c = generate("SEED" -> "other").find(_.name == AppEvent).get.raw

    a.exceptAll(b).count() shouldBe 0
    a.exceptAll(c).count() should be > 0L
  }

  it should "tách đáp án khỏi dữ liệu thô: raw không có cột ẩn, truth có subject + cờ null" in {
    val t = generate().find(_.name == LoyaltyTxn).get

    t.raw.columns.toSeq shouldBe Seq("txn_id", CustomerId, "txn_type", "points", "txn_ts")
    t.truth.columns.toSeq shouldBe Seq("txn_id", SubjectId, nullFlag(CustomerId))
  }

  it should "cài null đúng chỗ cờ báo, tỉ lệ xấp xỉ NULL_RATE" in {
    val all = generate("NULL_RATE" -> "0.2", "N_SUBJECTS" -> "2000").find(_.name == AppEvent).get.all
    val total = all.count().toDouble

    all.where(col(nullFlag(Isdn)) =!= col(Isdn).isNull).count() shouldBe 0
    (all.where(col(Isdn).isNull).count() / total) shouldBe 0.2 +- 0.03
  }

  it should "định danh trong bảng sự kiện khớp đúng chủ thể của nó (đáp án đáng tin)" in {
    val t = generate("NULL_RATE" -> "0").find(_.name == AppEvent).get
    t.all.where(col(Isdn) =!= IdentityFormats.isdn(col(SubjectId))).count() shouldBe 0
  }
}
