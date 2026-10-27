package vai.lakehouse.keyprefix

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WaitingColumnKeySourceSpec extends AnyFlatSpec with Matchers {

  /** Đồng hồ giả: `sleep` chỉ cộng thời gian, không ngủ thật. */
  private class FakeTime {
    var now = 0L
    val sleeps = ArrayBuffer.empty[Long]
    def sleep(millis: Long): Unit = { sleeps += millis / 1000; now += TimeUnit.MILLISECONDS.toNanos(millis) }
  }

  /** Nguồn giả: trả lần lượt các kết quả đã khai (exception hoặc key), lặp lại kết quả cuối. */
  private class Scripted(results: Either[ColumnKeysUnavailableException, Map[String, String]]*) extends ColumnKeySource {
    var calls = 0
    override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
      val r = results(math.min(calls, results.size - 1))
      calls += 1
      r.fold(e => throw e, identity)
    }
  }

  private def unavailable(states: (String, String)*) = new ColumnKeysUnavailableException("silver.t",
    states.map { case (c, s) => ColumnKeyIssue(c, s, if (s == "PENDING" || s == "REJECTED") Some(s"YC-2026-00000${c.length}") else None,
      None) }, Seq("req-1"))

  private val stillPending = Left(unavailable("msisdn" -> "PENDING", "imei" -> "PENDING"))
  private val ready = Right(Map("msisdn" -> "k1", "imei" -> "k2"))

  private def source(delegate: ColumnKeySource, time: FakeTime, wait: PendingWait = PendingWait(3600, 30, 300, 2.0)) =
    new WaitingColumnKeySource(delegate, wait, time.sleep, () => time.now)

  "WaitingColumnKeySource" should "gọi lại theo exponential backoff tới khi admin DAK duyệt (mọi cột READY)" in {
    val time = new FakeTime
    val delegate = new Scripted(stillPending, stillPending, stillPending, stillPending, stillPending, ready)

    source(delegate, time).readColumns("silver.t", Seq("msisdn", "imei"), KeyIntent.Encrypt) shouldBe Map("msisdn" -> "k1",
      "imei" -> "k2")

    delegate.calls shouldBe 6
    time.sleeps shouldBe Seq(30, 60, 120, 240, 300) // nhân đôi, chặn ở 300
  }

  it should "không chờ khi lần đầu đã READY" in {
    val time = new FakeTime
    source(new Scripted(ready), time).readColumns("silver.t", Seq("msisdn"), KeyIntent.Encrypt) should have size 2
    time.sleeps shouldBe empty
  }

  it should "dừng khi hết timeout, lần chờ cuối không vượt quá thời gian còn lại, ném lỗi của lần gọi cuối" in {
    val time = new FakeTime
    val delegate = new Scripted(stillPending)

    val ex = intercept[ColumnKeysUnavailableException](
      source(delegate, time, PendingWait(100, 30, 300, 2.0)).readColumns("silver.t", Seq("msisdn", "imei"), KeyIntent.Encrypt))

    ex.issues.map(_.state).distinct shouldBe Seq("PENDING")
    time.sleeps shouldBe Seq(30, 60, 10)
    delegate.calls shouldBe 4
  }

  it should "không chờ khi có cột REJECTED, DISABLED hay MISSING (không bao giờ tự thành READY)" in {
    Seq("REJECTED", "DISABLED", "MISSING").foreach { state =>
      val time = new FakeTime
      val delegate = new Scripted(Left(unavailable("msisdn" -> "PENDING", "imei" -> state)))

      intercept[ColumnKeysUnavailableException](source(delegate, time).readColumns("silver.t", Seq("msisdn", "imei"),
        KeyIntent.Encrypt))

      time.sleeps shouldBe empty
      delegate.calls shouldBe 1
    }
  }

  it should "không bắt lỗi khác (mạng, hợp đồng)" in {
    val failing = new ColumnKeySource {
      override def readColumns(d: String, c: Seq[String], i: KeyIntent): Map[String, String] = throw new IllegalStateException("x")
    }
    intercept[IllegalStateException](source(failing, new FakeTime).readColumns("silver.t", Seq("a"), KeyIntent.Encrypt))
  }

  "PendingWait.load" should "mặc định tắt, đọc cấu hình và từ chối giá trị sai theo tên vật lý" in {
    def cfg(values: (String, String)*): ConfigSource = new ConfigSource {
      private val m = values.toMap
      override def get(key: String): Option[String] = m.get(key)
      override def describe(key: String): String = s"TEST_$key"
    }
    import PrefixSourceFactory.Keys

    PendingWait.load(cfg()).enabled shouldBe false
    PendingWait.load(cfg(Keys.DakPendingTimeoutSeconds -> "7200", Keys.DakPendingInitialSeconds -> "10",
      Keys.DakPendingMaxIntervalSeconds -> "120", Keys.DakPendingMultiplier -> "1.5")) shouldBe PendingWait(7200, 10, 120, 1.5)
    Seq(Keys.DakPendingTimeoutSeconds -> "-1", Keys.DakPendingInitialSeconds -> "0", Keys.DakPendingMultiplier -> "0.5",
      Keys.DakPendingTimeoutSeconds -> "abc").foreach { kv =>
      intercept[IllegalArgumentException](PendingWait.load(cfg(kv))).getMessage should include(s"TEST_${kv._1}")
    }
    intercept[IllegalArgumentException](PendingWait.load(cfg(Keys.DakPendingInitialSeconds -> "60",
      Keys.DakPendingMaxIntervalSeconds -> "30"))).getMessage should include("TEST_dak.pendingWait.maxIntervalSeconds")
  }
}
