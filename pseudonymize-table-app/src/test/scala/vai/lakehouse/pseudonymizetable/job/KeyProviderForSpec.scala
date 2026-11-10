package vai.lakehouse.pseudonymizetable.job

import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.pseudonym.core.{DakDeidKeyProvider, PrefixSourceKeyProvider}
import vai.lakehouse.pseudonymizetable.config.JobConfig

class KeyProviderForSpec extends AnyFlatSpec with Matchers {

  private val table = Map("DB_NAME" -> "crm", "TABLE_NAME" -> "subscriber", "PSEUDO_COLUMNS" -> "isdn",
    "REQUEST_CODE" -> "REQ-2026-000011")

  private val dak = new SparkConf(false)
    .set("spark.pseudonym.source", "dak")
    .set("spark.pseudonym.dak.addr", "https://dak.example")
    .set("spark.pseudonym.dak.tokenUrl", "https://sso.example/token")
    .set("spark.pseudonym.dak.clientId", "team")
    .set("spark.pseudonym.dak.clientSecret", "s3cret")

  private def cfg(env: Map[String, String]) = JobConfig.fromEnv(env).toOption.get

  "keyProviderFor" should "take the key from DAK (keys/deid) for a table source when the source is dak" in {
    PseudonymizeTableJob.keyProviderFor(cfg(table), dak) shouldBe a[DakDeidKeyProvider]
  }

  it should "keep reading the Secret directory when the source is file" in {
    val file = new SparkConf(false).set("spark.pseudonym.source", "file").set("spark.pseudonym.file.dir", "/etc/pseudonym-keys")

    PseudonymizeTableJob.keyProviderFor(cfg(table), file) shouldBe a[PrefixSourceKeyProvider]
  }

  it should "not call DAK before the key is needed" in {
    // the endpoint does not exist: building the provider must not touch the network
    PseudonymizeTableJob.keyProviderFor(cfg(table - "REQUEST_CODE"), dak) shouldBe a[DakDeidKeyProvider]
  }
}
