package vai.lakehouse.pseudonym.core

import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.DeidKey
import vai.lakehouse.pseudonym.{ConfigurationException, InvalidKeyException}
import vai.lakehouse.pseudonym.spark.{DeidTarget, PseudonymConf}

class DakDeidKeyProviderSpec extends AnyFlatSpec with Matchers {

  private val bytes = Array.tabulate[Byte](HmacPseudonymizer.KeyBytes)(i => (i + 1).toByte)
  private val encoded = Base64.getEncoder.encodeToString(bytes)

  "hmacKey" should "be the keyPrefix of DAK, identical to the 'base64:' value of the old Secret, with the keyVersion of DAK" in {
    val provider = new DakDeidKeyProvider(() => DeidKey("REQ-2026-000011", encoded, 3))
    val fromSecret = PseudonymKey.parse("pseudonym.hmac_v1", 1, "base64:" + encoded, HmacPseudonymizer.KeyBytes)

    provider.hmacKey.exportBytes() shouldBe fromSecret.exportBytes()
    provider.hmacKey.version shouldBe 3
    provider.deidKey.requestCode shouldBe "REQ-2026-000011"
  }

  it should "ask DAK once per provider, not on every use" in {
    val calls = new AtomicInteger()
    val provider = new DakDeidKeyProvider(() => { calls.incrementAndGet(); DeidKey("REQ-2026-000011", encoded, 1) })

    provider.hmacKey
    provider.hmacKey
    provider.deidKey

    calls.get shouldBe 1
  }

  it should "not call DAK until a key is needed" in {
    val calls = new AtomicInteger()
    new DakDeidKeyProvider(() => { calls.incrementAndGet(); DeidKey("REQ-2026-000011", encoded, 1) })
    calls.get shouldBe 0
  }

  it should "let the error of DAK (403, 409...) stop the job" in {
    val provider = new DakDeidKeyProvider(() => throw new IllegalStateException("DAK denied (HTTP 403)"))
    the[IllegalStateException] thrownBy provider.hmacKey should have message "DAK denied (HTTP 403)"
  }

  it should "reject a key of the wrong size without printing it" in {
    val short = Base64.getEncoder.encodeToString(Array.fill[Byte](16)(5))
    val e = the[InvalidKeyException] thrownBy new DakDeidKeyProvider(() => DeidKey("REQ-2026-000011", short, 1)).hmacKey
    e.getMessage should not include short
  }

  "aesSivKey" should "report that DAK gives a HASH key only" in {
    val e = the[ConfigurationException] thrownBy new DakDeidKeyProvider(() => DeidKey("REQ-2026-000011", encoded, 1)).aesSivKey
    e.getMessage should include("HASH only")
  }

  "PseudonymConf.keyProvider(conf, deid)" should "use DAK when the source is dak" in {
    val conf = new SparkConf(false)
      .set("spark.pseudonym.source", "dak")
      .set("spark.pseudonym.dak.addr", "https://dak.example")
      .set("spark.pseudonym.dak.tokenUrl", "https://sso.example/token")
      .set("spark.pseudonym.dak.clientId", "team")
      .set("spark.pseudonym.dak.clientSecret", "s3cret")

    PseudonymConf.keyProvider(conf, DeidTarget("demo_db", "users", Some("REQ-2026-000011"))) shouldBe a[DakDeidKeyProvider]
  }

  it should "reject a source table that cannot be sent to DAK" in {
    val conf = new SparkConf(false)
      .set("spark.pseudonym.source", "dak")
      .set("spark.pseudonym.dak.addr", "https://dak.example")
      .set("spark.pseudonym.dak.tokenUrl", "https://sso.example/token")
      .set("spark.pseudonym.dak.clientId", "team")
      .set("spark.pseudonym.dak.clientSecret", "s3cret")

    an[ConfigurationException] should be thrownBy PseudonymConf.keyProvider(conf, DeidTarget("demo db", "users", None))
  }

  it should "keep reading the Secret directory when the source is file" in {
    val conf = new SparkConf(false).set("spark.pseudonym.source", "file").set("spark.pseudonym.file.dir", "/nonexistent")

    PseudonymConf.keyProvider(conf, DeidTarget("demo_db", "users", None)) shouldBe a[PrefixSourceKeyProvider]
  }
}
