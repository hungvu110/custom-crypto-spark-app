package vai.lakehouse.pseudonymizetable.job

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.core.{HmacPseudonymizer, PseudonymKey}

/**
 * VERIFY kiểm "không còn giá trị chưa giả danh" bằng DẠNG của định danh giả: regex phải khớp đúng đầu ra của thư viện (đổi định dạng ở
 * pseudonym-udf mà quên regex thì test này đỏ), và không khớp giá trị gốc thường gặp (số thuê bao, IMSI, bản mã base64 của bước 1).
 */
class PseudonymFormatSpec extends AnyFlatSpec with Matchers {

  private val hash = new HmacPseudonymizer(PseudonymKey.parse("test", 3, PseudonymKey.randomEncoded(HmacPseudonymizer.KeyBytes), HmacPseudonymizer.KeyBytes))

  "PseudonymFormat.Hash" should "khớp mọi đầu ra của HmacPseudonymizer" in {
    Seq("84900000001", "452041234567890", "", "Nguyễn Văn A", "aGVsbG8=").foreach { v =>
      hash.pseudonymize(v) should fullyMatch regex PseudonymFormat.Hash
    }
  }

  it should "không khớp giá trị gốc hay bản mã của bước silver" in {
    Seq("84900000001", "452041234567890", "6313cdfe2b612b82a333535b6ef74de1", "q3Lr0x9cJvC1m0Z8F2yS7w==", "k3_ABC", "t_" + "0" * 32)
      .foreach(v => v should not (fullyMatch regex PseudonymFormat.Hash))
  }

  "PseudonymFormat.Mapping" should "khớp token t_ + 32 hex của bảng ánh xạ" in {
    "t_" + "0123456789abcdef" * 2 should fullyMatch regex PseudonymFormat.Mapping
    "k1_" + "0" * 32 should not (fullyMatch regex PseudonymFormat.Mapping)
  }

  "PseudonymFormat.of" should "không có dạng cố định cho ENCRYPT (VERIFY quay về so với nguồn)" in {
    PseudonymFormat.of(PseudonymMode.Hash) shouldBe Some(PseudonymFormat.Hash)
    PseudonymFormat.of(PseudonymMode.Mapping) shouldBe Some(PseudonymFormat.Mapping)
    PseudonymFormat.of(PseudonymMode.Encrypt) shouldBe None
  }
}
