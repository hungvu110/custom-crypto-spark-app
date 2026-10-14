package vai.lakehouse.pseudonym.core

import vai.lakehouse.pseudonym.InvalidKeyException
import vai.lakehouse.pseudonym.TestKeys
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class HmacPseudonymizerSpec extends AnyFlatSpec with Matchers {

  private val pseudonymizer = new HmacPseudonymizer(TestKeys.hmac)

  "HMAC-SHA256" should "khớp test vector RFC 4231 (chứng minh đúng HMAC, không phải băm tự chế)" in {
    // Test Case 1: key = 20 byte 0x0b, data = "Hi There"
    Encoding.hex(HmacPseudonymizer.hmacSha256(Array.fill(20)(0x0b.toByte), "Hi There".getBytes(UTF_8))) shouldBe
      "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    // Test Case 2: key = "Jefe"
    Encoding.hex(HmacPseudonymizer.hmacSha256("Jefe".getBytes(UTF_8), "what do ya want for nothing?".getBytes(UTF_8))) shouldBe
      "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
  }

  "HmacPseudonymizer" should "ra đúng golden vector (tính độc lập bằng openssl, khoá 0x00..0x1f) — chặn đổi format vô tình" in {
    // printf '%s' <value> | openssl dgst -sha256 -mac HMAC -macopt hexkey:000102...1f  -> lấy 32 hex đầu
    pseudonymizer.pseudonymize("84903217465") shouldBe "k1_3fe27d9b4727fb8a0f900bc4126c6164"
    pseudonymizer.pseudonymize("KH0000123456") shouldBe "k1_e2bfb802ec75bb5483681ef08b15f1d6"
    pseudonymizer.pseudonymize("352099001761481") shouldBe "k1_e922fc3540183d991b8032fb13750ad6"
    pseudonymizer.pseudonymize("") shouldBe "k1_d38b42096d80f45f826b44a9d5607de7"
  }

  it should "tất định, null vào -> null ra, giá trị khác nhau -> định danh giả khác nhau" in {
    pseudonymizer.pseudonymize("84903217465") shouldBe pseudonymizer.pseudonymize("84903217465")
    pseudonymizer.pseudonymize(null) shouldBe null
    pseudonymizer.pseudonymize("84903217465") should not be pseudonymizer.pseudonymize("84903217466")
  }

  it should "băm nguyên trạng — không tự đổi định dạng (đầu vào phải đồng nhất ở bước trước)" in {
    pseudonymizer.pseudonymize("0903217465") should not be pseudonymizer.pseudonymize("84903217465")
  }

  it should "mang version của khoá trong tiền tố" in {
    val v3 = PseudonymKey.parse("pseudonym.hmac_v3", 3, TestKeys.HmacEncoded, HmacPseudonymizer.KeyBytes)
    new HmacPseudonymizer(v3).pseudonymize("84903217465") shouldBe "k3_3fe27d9b4727fb8a0f900bc4126c6164"
  }

  it should "khác khoá -> khác định danh giả" in {
    val other = new HmacPseudonymizer(TestKeys.randomProvider().hmacKey)
    other.pseudonymize("84903217465") should not be pseudonymizer.pseudonymize("84903217465")
  }

  it should "an toàn khi gọi song song từ nhiều thread (Mac không thread-safe)" in {
    val values = (0 until 20000).map(i => s"849${"%08d".format(i)}")
    val expected = values.map(pseudonymizer.pseudonymize)
    val pool = Executors.newFixedThreadPool(16)
    try {
      val tasks = (0 until 16).map(_ => new Callable[Seq[String]] { def call(): Seq[String] = values.map(pseudonymizer.pseudonymize) })
      pool.invokeAll(tasks.asJava).asScala.foreach(_.get(60, TimeUnit.SECONDS) shouldBe expected)
    } finally pool.shutdownNow()
  }

  it should "dùng được sau khi serialize (closure UDF gửi xuống executor)" in {
    val bytes = new java.io.ByteArrayOutputStream()
    val out = new java.io.ObjectOutputStream(bytes)
    out.writeObject(pseudonymizer)
    out.close()
    val copy = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
      .readObject().asInstanceOf[HmacPseudonymizer]

    copy.pseudonymize("84903217465") shouldBe "k1_3fe27d9b4727fb8a0f900bc4126c6164"
  }

  it should "từ chối khoá sai độ dài" in {
    intercept[InvalidKeyException](new HmacPseudonymizer(TestKeys.aesSiv))
  }
}
