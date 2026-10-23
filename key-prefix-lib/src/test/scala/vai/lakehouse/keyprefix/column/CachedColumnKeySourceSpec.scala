package vai.lakehouse.keyprefix

import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CachedColumnKeySourceSpec extends AnyFlatSpec with Matchers {

  /** Nguồn giả: key = "<bảng>/<cột>", ghi lại các cột được hỏi ở mỗi lần gọi; `failing` thì ném lỗi. */
  private final class FakeSource extends ColumnKeySource {
    val calls = mutable.Buffer.empty[(String, Seq[String], KeyIntent)]
    var failing = false

    override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
      calls += ((datasetName, columns, intent))
      if (failing) throw new IllegalStateException("DAK down")
      columns.map(c => c.toLowerCase(Locale.ROOT) -> s"${datasetName.toLowerCase(Locale.ROOT)}/${c.toLowerCase(Locale.ROOT)}")
        .toMap
    }
  }

  private val Ttl = TimeUnit.SECONDS.toNanos(300)

  private def fixture() = {
    val fake = new FakeSource
    var now = 0L
    val cached = new CachedColumnKeySource(fake, Ttl, () => now)
    (fake, cached, (nanos: Long) => now += nanos)
  }

  "readColumns" should "chỉ gọi nguồn thật cho các cột chưa có trong cache" in {
    val (fake, cached, _) = fixture()

    cached.readColumns("demo_db.t", Seq("a", "b"), KeyIntent.Encrypt) shouldBe Map("a" -> "demo_db.t/a", "b" -> "demo_db.t/b")
    cached.readColumns("demo_db.t", Seq("b", "c"), KeyIntent.Decrypt) shouldBe Map("b" -> "demo_db.t/b", "c" -> "demo_db.t/c")

    fake.calls.map(_._2) shouldBe Seq(Seq("a", "b"), Seq("c"))
    fake.calls.last._3 shouldBe KeyIntent.Decrypt
  }

  it should "không gọi nguồn thật khi mọi cột đã có trong cache" in {
    val (fake, cached, _) = fixture()
    cached.readColumns("demo_db.t", Seq("a"), KeyIntent.Decrypt)

    cached.readColumns("demo_db.t", Seq("a"), KeyIntent.Decrypt)

    fake.calls should have size 1
  }

  it should "không phân biệt hoa thường khi tra cache" in {
    val (fake, cached, _) = fixture()
    cached.readColumns("demo_db.t", Seq("msisdn"), KeyIntent.Decrypt)

    cached.readColumns("Demo_DB.T", Seq("MSISDN"), KeyIntent.Decrypt) shouldBe Map("msisdn" -> "demo_db.t/msisdn")

    fake.calls should have size 1
  }

  it should "gọi lại nguồn thật sau khi hết TTL" in {
    val (fake, cached, advance) = fixture()
    cached.readColumns("demo_db.t", Seq("a"), KeyIntent.Decrypt)

    advance(Ttl + 1)
    cached.readColumns("demo_db.t", Seq("a"), KeyIntent.Decrypt)

    fake.calls should have size 2
  }

  it should "tách cache theo bảng" in {
    val (fake, cached, _) = fixture()
    cached.readColumns("demo_db.t1", Seq("a"), KeyIntent.Decrypt)

    cached.readColumns("demo_db.t2", Seq("a"), KeyIntent.Decrypt) shouldBe Map("a" -> "demo_db.t2/a")

    fake.calls should have size 2
  }

  it should "không cache khi nguồn thật lỗi" in {
    val (fake, cached, _) = fixture()
    fake.failing = true
    intercept[IllegalStateException](cached.readColumns("demo_db.t", Seq("a"), KeyIntent.Decrypt))

    fake.failing = false
    cached.readColumns("demo_db.t", Seq("a"), KeyIntent.Decrypt) shouldBe Map("a" -> "demo_db.t/a")

    fake.calls should have size 2
  }
}
