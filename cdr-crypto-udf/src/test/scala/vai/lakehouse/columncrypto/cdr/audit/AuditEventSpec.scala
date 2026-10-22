package vai.lakehouse.columncrypto.cdr.audit

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AuditEventSpec extends AnyFlatSpec with Matchers {

  private val identity = EngineIdentity("tenantx", "wsx", "teamx", "en-test")
  private val ts = Instant.parse("2026-10-19T08:15:30.123Z")

  private def info(i: Int) =
    UsageInfo(f"u$i%012x", "cdr_decrypt", "demo_db.sub_rel_product", "isdn", "isdn", "start_datetime")

  "AuditEvent.toJson" should "ghi đủ trường của START theo thứ tự cố định và bỏ trường không có" in {
    val e = AuditEvent(EventType.Start, ts, "app-1", identity, Seq(UsageRecord(info(1).usageId, Some(info(1)), None)),
      executionId = Some(7), user = Some("alice"), userSource = Some("thrift"), statementId = Some("stmt-1"), eventId = "e-1")

    e.toJson shouldBe
      """{"auditVersion":2,"eventId":"e-1","event":"START","ts":"2026-10-19T08:15:30.123Z","appId":"app-1",""" +
        """"executionId":7,"user":"alice","userSource":"thrift","statementId":"stmt-1","tenant":"tenantx",""" +
        """"workspace":"wsx","team":"teamx","engineId":"en-test","usages":[{"usageId":"u000000000001",""" +
        """"function":"cdr_decrypt","dataset":"demo_db.sub_rel_product","column":"isdn","valueExpr":"isdn",""" +
        """"keyFieldExpr":"start_datetime"}]}"""
  }

  it should "ghi trạng thái, thời lượng và số đếm của END" in {
    val e = AuditEvent(EventType.End, ts, "app-1", identity,
      Seq(UsageRecord(info(1).usageId, None, Some(UsageCounts(3, 1)))),
      executionId = Some(7), status = Some(Status.Succeeded), durationMs = Some(1500), eventId = "e-2")

    e.toJson shouldBe
      """{"auditVersion":2,"eventId":"e-2","event":"END","ts":"2026-10-19T08:15:30.123Z","appId":"app-1",""" +
        """"executionId":7,"tenant":"tenantx","workspace":"wsx","team":"teamx","engineId":"en-test",""" +
        """"status":"SUCCEEDED","durationMs":1500,"usages":[{"usageId":"u000000000001","rows":3,"nullRows":1}]}"""
  }

  it should "escape ký tự đặc biệt trong chuỗi" in {
    // Scala 2.12 xử lý unicode escape ngay khi đọc mã nguồn (kể cả trong chuỗi """ và comment), nên dựng ký tự điều khiển
    // và dấu gạch chéo ngược bằng mã số.
    val backslash = "\\"
    val e = AuditEvent(EventType.Start, ts, "app", identity, Nil,
      user = Some("a\"b" + backslash + "c\nd" + 1.toChar + "e" + 0x2028.toChar), eventId = "e")

    e.toJson should include("\"user\":\"a\\\"b\\\\c\\nd" + backslash + "u0001e" + backslash + "u2028\"")
  }

  it should "ghi dropped khi khác 0" in {
    val e = AuditEvent(EventType.End, ts, "app", identity, Nil, eventId = "e").copy(dropped = 3)

    e.toJson should include(""""dropped":3,"usages":[]""")
  }

  "AuditEvent.split" should "tách usages thành nhiều event có part/parts và eventId riêng" in {
    val e = AuditEvent(EventType.Start, ts, "app", identity, (1 to 12).map(i => UsageRecord(info(i).usageId, Some(info(i)), None)),
      executionId = Some(7))

    val parts = AuditEvent.split(e, 5)

    parts.map(_.usages.size) shouldBe Seq(5, 5, 2)
    parts.map(_.part) shouldBe Seq(Some(1 -> 3), Some(2 -> 3), Some(3 -> 3))
    parts.map(_.eventId).distinct should have size 3
    parts.flatMap(_.usages) shouldBe e.usages
    parts.head.toJson should include(""""part":1,"parts":3""")
  }

  it should "giữ nguyên event khi không vượt giới hạn" in {
    val e = AuditEvent(EventType.Start, ts, "app", identity, Seq(UsageRecord(info(1).usageId, Some(info(1)), None)))

    AuditEvent.split(e, 5) shouldBe Seq(e)
  }

  "Một event" should "nhỏ hơn 8 KB với maxUsagesPerEvent mặc định và trường dài tối đa" in {
    val longest = UsageInfo("u000000000001", "cdr_decrypt", "d" * 128 + "." + "t" * 128, "c" * 128,
      AuditText.describeExpr("v" * 500), AuditText.describeExpr("k" * 500))
    val e = AuditEvent(EventType.Expired, ts, "spark-application-" + "a" * 40, identity,
      Seq.fill(AuditConfig.DefaultMaxUsagesPerEvent)(UsageRecord(longest.usageId, Some(longest), Some(UsageCounts(Long.MaxValue, Long.MaxValue)))),
      executionId = Some(Long.MaxValue), user = Some(AuditText.limit("u" * 1000)), userSource = Some("thrift"),
      statementId = Some(AuditText.limit("s" * 1000)))

    e.toJson.getBytes(UTF_8).length should be < 8192
  }

  "AuditText.describeExpr" should "che literal chuỗi, kể cả có dấu nháy được escape" in {
    AuditText.describeExpr("concat(isdn, 'se\\'cret', 'x')") shouldBe "concat(isdn, '***', '***')"
  }

  it should "cắt biểu thức dài" in {
    val described = AuditText.describeExpr("x" * 300)

    described.length shouldBe AuditText.MaxExprLength + 1
    described should endWith("…")
  }
}
