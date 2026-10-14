package vai.lakehouse.pseudonym.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.{DakTableRef, PrefixFiles}

/**
 * Tên khoá phải hợp lệ với MỌI nguồn của key-prefix-lib. Riêng DAK chỉ chấp nhận `<database>.<table>` (đúng 2 phần,
 * chỉ chữ/số/`_`) nên tên 3 phần kiểu `pseudonym.hmac.v1` làm nguồn `dak` không dùng được.
 */
class KeyNamesSpec extends AnyFlatSpec with Matchers {

  private val names = Seq(1, 2, 10).flatMap(v => Seq(PrefixSourceKeyProvider.hmacName(v), PrefixSourceKeyProvider.aesSivName(v)))

  "PrefixSourceKeyProvider key names" should "hợp lệ với nguồn dak (<database>.<table>)" in {
    names.foreach { n =>
      val ref = DakTableRef.parse(n)
      ref.database shouldBe "pseudonym"
      ref.qualified shouldBe n
    }
  }

  it should "hợp lệ với nguồn file/vault (tên dataset)" in {
    names.foreach(n => PrefixFiles.requireValidDatasetName(n))
  }

  it should "có dạng cố định, gồm version" in {
    PrefixSourceKeyProvider.hmacName(1) shouldBe "pseudonym.hmac_v1"
    PrefixSourceKeyProvider.aesSivName(3) shouldBe "pseudonym.aes_siv_v3"
  }
}
