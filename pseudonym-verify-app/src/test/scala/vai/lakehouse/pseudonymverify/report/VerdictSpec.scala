package vai.lakehouse.pseudonymverify.report

import vai.lakehouse.pseudonymverify.check.{CheckResult, Level, Status}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VerdictSpec extends AnyFlatSpec with Matchers {

  private def result(id: String, level: Level, status: Status) =
    CheckResult(id, level, status, "", "", 0, "", Nil, Map.empty, None)

  "Verdict.of" should "PASS khi mọi check MUST/CONTROL đạt (INFO/SKIPPED không tính)" in {
    Verdict.of(Seq(result("U1", Level.Must, Status.Pass), result("U11", Level.Info, Status.Reported),
      result("U10", Level.Must, Status.Skipped), result("P6", Level.Control, Status.Pass))) shouldBe Verdict.Pass
  }

  it should "FAIL khi có check MUST không đạt" in {
    Verdict.of(Seq(result("U1", Level.Must, Status.Fail), result("P6", Level.Control, Status.Pass))) shouldBe Verdict.Fail
  }

  it should "ERROR khi đối chứng P6 không đạt hoặc có check lỗi — không kết luận được, kể cả khi có MUST fail" in {
    Verdict.of(Seq(result("U1", Level.Must, Status.Fail), result("P6", Level.Control, Status.Fail))) shouldBe Verdict.Error
    Verdict.of(Seq(result("U9", Level.Must, Status.Error))) shouldBe Verdict.Error
  }

  "Verdict.worst" should "lấy verdict xấu nhất, và exit code 0/1/2" in {
    Verdict.worst(Seq(Verdict.Pass, Verdict.Fail)) shouldBe Verdict.Fail
    Verdict.worst(Seq(Verdict.Pass, Verdict.Error, Verdict.Fail)) shouldBe Verdict.Error
    Seq(Verdict.Pass, Verdict.Fail, Verdict.Error).map(_.exitCode) shouldBe Seq(0, 1, 2)
  }
}
