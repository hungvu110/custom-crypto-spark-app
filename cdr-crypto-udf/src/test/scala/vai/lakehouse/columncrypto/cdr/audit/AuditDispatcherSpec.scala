package vai.lakehouse.columncrypto.cdr.audit

import java.time.Instant
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

class AuditDispatcherSpec extends AnyFlatSpec with Matchers with Eventually {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(5, Seconds), Span(20, Millis))

  private def ev(id: String) =
    AuditEvent(EventType.Start, Instant.EPOCH, "app", EngineIdentity("t", "w", "tm", "e"), Nil, eventId = id)

  "AuditDispatcher" should "giao event cho sink theo thứ tự" in {
    val sink = new InMemoryAuditSink
    val dispatcher = new AuditDispatcher(sink, maxQueueSize = 100, coalesceMs = 0)
    try {
      Seq("a", "b", "c").foreach(id => dispatcher.submit(ev(id)))

      eventually(sink.events.map(_.eventId) shouldBe Seq("a", "b", "c"))
    } finally dispatcher.close(1000)
  }

  it should "bỏ event cũ nhất khi hàng đợi đầy và ghi số bị bỏ vào event kế tiếp" in {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val sink = new InMemoryAuditSink {
      override def write(events: Seq[AuditEvent]): Unit = {
        if (events.exists(_.eventId == "a")) { entered.countDown(); release.await(5, TimeUnit.SECONDS) }
        super.write(events)
      }
    }
    val dispatcher = new AuditDispatcher(sink, maxQueueSize = 2, coalesceMs = 0)
    try {
      dispatcher.submit(ev("a"))
      entered.await(5, TimeUnit.SECONDS) shouldBe true
      Seq("b", "c", "d").foreach(id => dispatcher.submit(ev(id)))
      release.countDown()

      eventually(sink.events.map(_.eventId) shouldBe Seq("a", "c", "d"))
      sink.events.find(_.eventId == "c").map(_.dropped) shouldBe Some(1)
    } finally dispatcher.close(1000)
  }

  it should "tiếp tục giao event sau khi sink lỗi, không ném lên caller" in {
    val calls = new AtomicInteger()
    val sink = new InMemoryAuditSink {
      override def write(events: Seq[AuditEvent]): Unit =
        if (calls.incrementAndGet() == 1) throw new IllegalStateException("down") else super.write(events)
    }
    val dispatcher = new AuditDispatcher(sink, maxQueueSize = 100, coalesceMs = 0)
    try {
      dispatcher.submit(ev("a"))
      eventually(dispatcher.failedBatches shouldBe 1)
      dispatcher.submit(ev("b"))

      eventually(sink.events.map(_.eventId) shouldBe Seq("b"))
    } finally dispatcher.close(1000)
  }

  it should "gom các event đến trong khoảng coalesceMs thành một batch" in {
    val sink = new InMemoryAuditSink
    val dispatcher = new AuditDispatcher(sink, maxQueueSize = 100, coalesceMs = 300)
    try {
      Seq("a", "b", "c").foreach(id => dispatcher.submit(ev(id)))

      eventually(sink.batchSizes shouldBe Seq(3))
    } finally dispatcher.close(1000)
  }

  it should "nhận event từ nhiều thread cùng lúc" in {
    val sink = new InMemoryAuditSink
    val dispatcher = new AuditDispatcher(sink, maxQueueSize = 10000, coalesceMs = 0)
    try {
      val threads = (1 to 4).map(t => new Thread(() => (1 to 250).foreach(i => dispatcher.submit(ev(s"$t-$i")))))
      threads.foreach(_.start())
      threads.foreach(_.join())

      eventually(sink.events.map(_.eventId).distinct should have size 1000)
    } finally dispatcher.close(1000)
  }

  it should "ghi hết hàng đợi khi close rồi đóng sink" in {
    val sink = new InMemoryAuditSink
    val dispatcher = new AuditDispatcher(sink, maxQueueSize = 10000, coalesceMs = 50)
    (1 to 500).foreach(i => dispatcher.submit(ev(s"e$i")))

    dispatcher.close(5000)

    sink.events should have size 500
    sink.closed shouldBe true
  }

  it should "bỏ qua event gửi tới sau khi close" in {
    val sink = new InMemoryAuditSink
    val dispatcher = new AuditDispatcher(sink, maxQueueSize = 100, coalesceMs = 0)
    dispatcher.close(1000)

    dispatcher.submit(ev("late"))
    Thread.sleep(100)

    sink.events shouldBe empty
  }

  "CompositeAuditSink" should "vẫn ghi vào sink còn lại khi một sink lỗi, rồi báo lỗi" in {
    val bad = new InMemoryAuditSink {
      override def write(events: Seq[AuditEvent]): Unit = throw new IllegalStateException("bad")
    }
    val good = new InMemoryAuditSink

    val ex = intercept[IllegalStateException](new CompositeAuditSink(Seq(bad, good)).write(Seq(ev("a"))))

    ex.getMessage shouldBe "bad"
    good.events.map(_.eventId) shouldBe Seq("a")
  }
}
