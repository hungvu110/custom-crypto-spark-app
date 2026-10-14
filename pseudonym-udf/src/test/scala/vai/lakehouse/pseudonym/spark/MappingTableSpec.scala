package vai.lakehouse.pseudonym.spark

import vai.lakehouse.pseudonym.SparkTestSession
import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class MappingTableSpec extends AnyFlatSpec with Matchers with SparkTestSession {

  private def values(vs: String*) = {
    val s = spark
    import s.implicits._
    vs.toDF("isdn")
  }

  private def tokens(table: MappingTable): Map[String, String] =
    table.table.collect().map(r => r.getAs[String](MappingTable.RawValue) -> r.getAs[String](MappingTable.Token)).toMap

  "MappingTable" should "gán token ngẫu nhiên t_<32 hex> cho mỗi giá trị, null giữ null" in {
    val table = new MappingTable(spark, tempPath("map_basic"))
    val out = table.pseudonymize(values("84900000001", "84900000002", "84900000001", null), "isdn", "isdn").collect()

    out.length shouldBe 4
    val nonNull = out.flatMap(r => Option(r.getString(0)))
    nonNull.length shouldBe 3
    nonNull.foreach(_ should fullyMatch regex "t_[0-9a-f]{32}")
    nonNull.distinct.length shouldBe 2
    tokens(table).keySet shouldBe Set("84900000001", "84900000002")
  }

  it should "giữ nguyên token cũ qua nhiều lần chạy, chỉ thêm token cho giá trị mới" in {
    val table = new MappingTable(spark, tempPath("map_rerun"))
    table.pseudonymize(values("a1", "a2"), "isdn", "isdn").collect()
    val first = tokens(table)

    table.pseudonymize(values("a2", "a3"), "isdn", "isdn").collect()
    val second = tokens(table)

    second("a1") shouldBe first("a1")
    second("a2") shouldBe first("a2")
    second.keySet shouldBe Set("a1", "a2", "a3")
  }

  it should "mỗi giá trị đúng 1 token kể cả khi nhiều job ghi song song" in {
    val table = new MappingTable(spark, tempPath("map_concurrent"))
    table.ensureTokens(values().toDF(MappingTable.RawValue)) // tạo bảng trước, 4 luồng chỉ còn đua MERGE
    val all = (0 until 200).map(i => f"v$i%03d")
    val pool = Executors.newFixedThreadPool(4)
    try {
      val tasks = (0 until 4).map(_ => new Callable[Unit] {
        def call(): Unit = table.ensureTokens(values(all: _*).toDF(MappingTable.RawValue))
      })
      pool.invokeAll(tasks.asJava).asScala.foreach(_.get(300, TimeUnit.SECONDS))
    } finally pool.shutdownNow()

    val rows = table.table.collect()
    rows.length shouldBe all.size
    rows.map(_.getAs[String](MappingTable.RawValue)).distinct.length shouldBe all.size
  }

  it should "đổi tên cột output và giữ vị trí cột" in {
    val s = spark
    import s.implicits._
    val df = Seq((1, "KH1", 10), (2, "KH2", 20)).toDF("id", "customer_id", "points")

    val out = new MappingTable(spark, tempPath("map_rename")).pseudonymize(df, "customer_id", "pseudonym_id")

    out.columns.toSeq shouldBe Seq("id", "pseudonym_id", "points")
  }

  it should "reveal trả lại giá trị gốc, token lạ -> null" in {
    val table = new MappingTable(spark, tempPath("map_reveal"))
    val pseudo = table.pseudonymize(values("84900000009"), "isdn", "isdn")
      .union(values("t_00000000000000000000000000000000"))

    table.reveal(pseudo, "isdn", "isdn").collect().map(r => Option(r.getString(0))).toSet shouldBe
      Set(Some("84900000009"), None)
  }

  "MappingTable.newToken" should "không lặp lại" in {
    Seq.fill(10000)(MappingTable.newToken()).distinct.size shouldBe 10000
  }
}
