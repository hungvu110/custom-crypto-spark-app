package vai.lakehouse.pseudonym.policy

import vai.lakehouse.pseudonym.InvalidPolicyException
import vai.lakehouse.pseudonym.PseudonymMode

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PseudonymPolicySpec extends AnyFlatSpec with Matchers {

  private val yaml =
    """tables:
      |  myviettel.app_event:
      |    isdn: { mode: HASH }
      |  loyalty.loyalty_txn:
      |    customer_id: { mode: hash, output: pseudonym_id }
      |  crm.subscriber:
      |    isdn: { mode: HASH }
      |    imei: { mode: HASH }
      |    customer_id: { mode: HASH, output: pseudonym_id }
      |""".stripMargin

  "PseudonymPolicy.parse" should "đọc bảng, cột, chế độ (không phân biệt hoa thường) và output" in {
    val policies = PseudonymPolicy.parse(yaml)

    policies.keySet shouldBe Set("myviettel.app_event", "loyalty.loyalty_txn", "crm.subscriber")
    policies("loyalty.loyalty_txn").columns shouldBe Seq(ColumnRule("customer_id", PseudonymMode.Hash, Some("pseudonym_id")))
    policies("crm.subscriber").columns.map(_.column) shouldBe Seq("isdn", "imei", "customer_id")
    policies("crm.subscriber").ruleFor("customer_id").map(_.outputName) shouldBe Some("pseudonym_id")
    policies("myviettel.app_event").ruleFor("isdn").map(_.outputName) shouldBe Some("isdn")
  }

  it should "đổi chế độ cho mọi cột bằng withMode" in {
    PseudonymPolicy.parse(yaml)("crm.subscriber").withMode(PseudonymMode.Mapping).columns.map(_.mode).distinct shouldBe
      Seq(PseudonymMode.Mapping)
  }

  it should "báo lỗi khi thiếu mode hoặc mode không hợp lệ" in {
    intercept[InvalidPolicyException](PseudonymPolicy.parse("tables:\n  t:\n    c: { output: x }")).getMessage should
      include("missing 'mode'")
    intercept[InvalidPolicyException](PseudonymPolicy.parse("tables:\n  t:\n    c: { mode: SHA2 }")).getMessage should
      include("Invalid pseudonym mode")
  }

  it should "báo lỗi khi output trùng nhau hoặc tên cột không hợp lệ" in {
    intercept[InvalidPolicyException](PseudonymPolicy.parse(
      "tables:\n  t:\n    a: { mode: HASH, output: x }\n    b: { mode: HASH, output: x }")).getMessage should include("duplicated")
    intercept[InvalidPolicyException](PseudonymPolicy.parse(
      "tables:\n  t:\n    a: { mode: HASH, output: 'x y' }")).getMessage should include("invalid column name")
  }

  it should "báo lỗi khi cùng 1 cột định danh dùng chế độ khác nhau ở 2 bảng (sẽ không join được)" in {
    val ex = intercept[InvalidPolicyException](PseudonymPolicy.parse(
      "tables:\n  a:\n    isdn: { mode: HASH }\n  b:\n    isdn: { mode: ENCRYPT }"))
    ex.getMessage should include("different modes")
  }

  it should "báo lỗi khi thiếu 'tables'" in {
    intercept[InvalidPolicyException](PseudonymPolicy.parse("other: 1")).getMessage should include("'tables'")
  }
}
