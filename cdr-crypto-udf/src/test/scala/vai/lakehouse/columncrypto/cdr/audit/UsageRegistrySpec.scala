package vai.lakehouse.columncrypto.cdr.audit

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UsageRegistrySpec extends AnyFlatSpec with Matchers {

  private val Ttl = 1000L

  private def usage(id: Int, createdAt: Long, counts: Option[CountSource] = None) =
    new Usage(UsageInfo(f"u$id%012x", "cdr_decrypt", "d.t", "isdn", "start_datetime"), None, None, counts, createdAt)

  "UsageRegistry" should "trả usage đã đăng ký theo usageId" in {
    val registry = new UsageRegistry(Ttl, 100, () => 0L)
    val u = usage(1, 0)

    registry.register(u) shouldBe empty

    registry.get(u.usageId) shouldBe Some(u)
    registry.get("u0000000000ff") shouldBe None
  }

  it should "chỉ dọn usage đã rảnh quá TTL" in {
    var now = 0L
    val registry = new UsageRegistry(Ttl, 100, () => now)
    val old = usage(1, now)
    registry.register(old)
    now = 500
    val fresh = usage(2, now)
    registry.register(fresh)

    now = 1200

    registry.sweep() shouldBe Seq(old)
    registry.get(fresh.usageId) shouldBe Some(fresh)
  }

  it should "không dọn usage đang chạy, và tính TTL lại từ lúc execution kết thúc" in {
    var now = 0L
    val registry = new UsageRegistry(Ttl, 100, () => now)
    val u = usage(1, now)
    registry.register(u)
    u.begin(now)

    now = 5000
    registry.sweep() shouldBe empty
    u.end(now)
    now = 5500
    registry.sweep() shouldBe empty
    now = 6100

    registry.sweep() shouldBe Seq(u)
  }

  it should "đẩy usage cũ nhất ra khi vượt maxUsages" in {
    val registry = new UsageRegistry(Ttl, 2, () => 0L)
    val Seq(u1, u2, u3) = (1 to 3).map(usage(_, 0))

    registry.register(u1) shouldBe empty
    registry.register(u2) shouldBe empty

    registry.register(u3) shouldBe Seq(u1)
    registry.get(u1.usageId) shouldBe None
  }

  it should "không đẩy usage đang chạy hoặc usage vừa đăng ký" in {
    val registry = new UsageRegistry(Ttl, 1, () => 0L)
    val Seq(u1, u2) = (1 to 2).map(usage(_, 0))
    registry.register(u1)
    u1.begin(0)

    registry.register(u2) shouldBe empty
    registry.size shouldBe 2
  }

  "Usage" should "trả số đếm phát sinh kể từ lần báo trước" in {
    val counts = new FakeCounts
    val u = usage(1, 0, Some(counts))
    counts.rows = 3

    u.begin(0)
    u.end(1) shouldBe Some(UsageCounts(3, 0))

    counts.rows = 5
    counts.nullRows = 1
    u.takeDelta() shouldBe Some(UsageCounts(2, 1))
    u.takeDelta() shouldBe Some(UsageCounts(0, 0))
  }

  it should "không có số đếm khi không gắn nguồn đếm" in {
    val u = usage(1, 0)
    u.begin(0)

    u.end(1) shouldBe None
    u.wasExecuted shouldBe true
  }

  "StatementUserTracker" should "nhớ user theo statementId, quên khi remove hoặc quá TTL" in {
    var now = 0L
    val tracker = new StatementUserTracker(Ttl, 100, () => now)
    tracker.put("s1", "alice")
    tracker.put("s2", "bob")

    tracker.get("s1") shouldBe Some("alice")
    tracker.remove("s1")
    tracker.get("s1") shouldBe None

    now = 1500
    tracker.sweep()
    tracker.get("s2") shouldBe None
  }

  it should "giới hạn số entry, bỏ entry cũ nhất" in {
    val tracker = new StatementUserTracker(Ttl, 2, () => 0L)
    Seq("a", "b", "c").foreach(s => tracker.put(s, s"user-$s"))

    tracker.get("a") shouldBe None
    tracker.get("c") shouldBe Some("user-c")
  }
}
