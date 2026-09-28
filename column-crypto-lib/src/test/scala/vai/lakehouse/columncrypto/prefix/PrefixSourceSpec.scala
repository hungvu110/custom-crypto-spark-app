package vai.lakehouse.columncrypto.prefix

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrefixSourceSpec extends AnyFlatSpec with Matchers {

  private def source(f: String => String): PrefixSource = new PrefixSource {
    override def read(datasetName: String): String = f(datasetName)
  }

  private def failing(msg: String): PrefixSource = source(_ => throw new IllegalStateException(msg))

  "ChainedPrefixSource" should "dùng nguồn đầu tiên thành công và không gọi các nguồn sau" in {
    val calls = new AtomicInteger()
    val chained = new ChainedPrefixSource(Seq(
      "file"  -> source(d => s"file-$d"),
      "vault" -> source { d => calls.incrementAndGet(); s"vault-$d" }))

    chained.read("t1") shouldBe "file-t1"
    calls.get shouldBe 0
  }

  it should "fallback sang nguồn kế tiếp khi nguồn đầu lỗi" in {
    val chained = new ChainedPrefixSource(Seq(
      "file"  -> failing("file not found"),
      "vault" -> source(d => s"vault-$d")))

    chained.read("t1") shouldBe "vault-t1"
  }

  it should "gộp lý do của từng nguồn khi tất cả đều lỗi" in {
    val chained = new ChainedPrefixSource(Seq(
      "file"  -> failing("file not found"),
      "vault" -> failing("HTTP 403")))

    val ex = intercept[IllegalStateException](chained.read("t1"))

    ex.getMessage should include("t1")
    ex.getMessage should include("file: file not found")
    ex.getMessage should include("vault: HTTP 403")
  }

  it should "từ chối tên bảng không hợp lệ mà không gọi nguồn nào" in {
    val calls = new AtomicInteger()
    val chained = new ChainedPrefixSource(Seq("file" -> source { d => calls.incrementAndGet(); d }))

    intercept[IllegalArgumentException](chained.read("../x"))
    calls.get shouldBe 0
  }

  "CachedPrefixSource" should "chỉ gọi nguồn 1 lần trong thời gian TTL" in {
    val calls = new AtomicInteger()
    var now = 0L
    val cached = new CachedPrefixSource(source { d => calls.incrementAndGet(); s"p-$d" }, ttlNanos = 100, () => now)

    cached.read("t1") shouldBe "p-t1"
    now = 99
    cached.read("t1") shouldBe "p-t1"

    calls.get shouldBe 1
  }

  it should "đọc lại nguồn khi hết TTL" in {
    val calls = new AtomicInteger()
    var now = 0L
    val cached = new CachedPrefixSource(source { _ => s"v${calls.incrementAndGet()}" }, ttlNanos = 100, () => now)

    cached.read("t1") shouldBe "v1"
    now = 100
    cached.read("t1") shouldBe "v2"
  }

  it should "cache riêng theo từng bảng" in {
    val cached = new CachedPrefixSource(source(d => s"p-$d"), ttlNanos = 100, () => 0L)

    cached.read("a") shouldBe "p-a"
    cached.read("b") shouldBe "p-b"
  }

  it should "không cache kết quả lỗi" in {
    val calls = new AtomicInteger()
    val cached = new CachedPrefixSource(
      source { _ => if (calls.incrementAndGet() == 1) throw new IllegalStateException("boom") else "ok" },
      ttlNanos = 100, () => 0L)

    intercept[IllegalStateException](cached.read("t1"))
    cached.read("t1") shouldBe "ok"
  }
}
