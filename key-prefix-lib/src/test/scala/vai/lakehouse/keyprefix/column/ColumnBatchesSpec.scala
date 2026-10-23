package vai.lakehouse.keyprefix

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ColumnBatchesSpec extends AnyFlatSpec with Matchers {

  "DakColumnRef.parse" should "chuẩn hoá tên cột về chữ thường" in {
    DakColumnRef.parse("MSISDN") shouldBe "msisdn"
    DakColumnRef.parse("start_DateTime_2") shouldBe "start_datetime_2"
  }

  it should "từ chối tên cột rỗng, có ký tự lạ hoặc dài quá 128 ký tự" in {
    Seq(null, "", " ", "start-time", "a.b", "số_điện_thoại", "x" * 129).foreach { bad =>
      intercept[IllegalArgumentException](DakColumnRef.parse(bad))
    }
  }

  "ColumnBatches.split" should "để nguyên một lô khi chưa vượt giới hạn" in {
    ColumnBatches.split(Seq("a", "b", "c")) shouldBe Seq(Seq("a", "b", "c"))
  }

  it should "chia theo số cột, tối đa 100 cột mỗi lô" in {
    val columns = (0 until 120).map(i => s"c$i")

    val batches = ColumnBatches.split(columns)

    batches.map(_.size) shouldBe Seq(100, 20)
    batches.flatten shouldBe columns
  }

  it should "chia theo tổng độ dài, tính cả dấu phẩy" in {
    // 40 tên 120 ký tự: 34 tên = 34*120 + 33 dấu phẩy = 4113 > 4096, nên mỗi lô tối đa 33 tên (33*120 + 32 = 3992).
    val columns = (0 until 40).map(i => f"$i%03d" + "x" * 117)

    val batches = ColumnBatches.split(columns)

    batches.map(_.size) shouldBe Seq(33, 7)
    batches.foreach(b => b.mkString(",").length should be <= ColumnBatches.MaxLength)
    batches.flatten shouldBe columns
  }

  it should "sang lô mới khi thêm một cột làm vượt 4096 ký tự" in {
    // 32 tên 127 ký tự + 31 dấu phẩy = 4095; thêm một tên 1 ký tự + dấu phẩy = 4097 -> sang lô mới.
    val columns = (0 until 32).map(i => f"$i%02d" + "y" * 125) :+ "z"

    ColumnBatches.split(columns).map(_.size) shouldBe Seq(32, 1)
  }

  it should "trả danh sách rỗng khi không có cột" in {
    ColumnBatches.split(Nil) shouldBe Nil
  }
}
