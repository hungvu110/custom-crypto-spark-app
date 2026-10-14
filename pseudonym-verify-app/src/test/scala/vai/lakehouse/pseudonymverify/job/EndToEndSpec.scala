package vai.lakehouse.pseudonymverify.job

import vai.lakehouse.pseudonymverify.LocalSpark
import vai.lakehouse.pseudonym.core.{AesSivPseudonymizer, HmacPseudonymizer, PrefixSourceKeyProvider, PseudonymKey}
import vai.lakehouse.pseudonymverify.config.VerifyConfig
import vai.lakehouse.pseudonymverify.data.SyntheticData
import vai.lakehouse.pseudonymverify.key.KeyMaterial
import vai.lakehouse.pseudonymverify.report.Verdict

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

/**
 * Chạy app thật (không qua spark-submit) trên dữ liệu nhỏ: đây là tiêu chí nghiệm thu mục 13 của plan, thu nhỏ.
 *   - full, cả 3 chế độ -> PASS
 *   - selftest, cả 3 chế độ -> 10/10 mutation bị bắt
 *   - guard trên parquet thật với khoá từ file -> PASS và có output
 */
class EndToEndSpec extends AnyFlatSpec with Matchers {

  private def withSession[T](name: String, cfg: VerifyConfig, keys: KeyMaterial)(body: VerifySession => T): T = {
    val spark = LocalSpark.session(name, keys)
    try body(new VerifySession(spark, cfg, keys)) finally LocalSpark.stop(spark)
  }

  private def report(cfg: VerifyConfig, name: String): java.util.Map[String, Any] =
    new ObjectMapper().readValue(new String(Files.readAllBytes(Paths.get(new URI(s"${cfg.runPath}/$name"))), UTF_8),
      classOf[java.util.Map[String, Any]])

  "RUN_MODE=full" should "PASS cho cả HASH, ENCRYPT và MAPPING, ghi report.json" in {
    val cfg = LocalSpark.smallConfig("PSEUDO_MODES" -> "HASH,ENCRYPT,MAPPING")
    val verdict = withSession("e2e-full", cfg, KeyMaterial.ephemeral())(s => VerifyJob.forMode(cfg.runMode).run(s))

    val modes = report(cfg, "report.json").get("modes").asInstanceOf[java.util.Map[String, java.util.Map[String, Any]]]
    val failures = modes.asScala.toSeq.flatMap { case (mode, m) =>
      m.get("checks").asInstanceOf[java.util.List[java.util.Map[String, Any]]].asScala
        .filter(c => Set[Any]("FAIL", "ERROR").contains(c.get("status")))
        .map(c => s"$mode/${c.get("id")}: ${c.get("metric")}=${c.get("observed")} ${Option(c.get("error")).getOrElse("")}")
    }
    failures shouldBe empty
    verdict shouldBe Verdict.Pass
    modes.keySet().asScala shouldBe Set("HASH", "ENCRYPT", "MAPPING")
  }

  "RUN_MODE=selftest" should "bắt được cả 10 mutation (bộ check đáng tin)" in {
    val cfg = LocalSpark.smallConfig("RUN_MODE" -> "selftest", "PSEUDO_MODES" -> "HASH,ENCRYPT,MAPPING")
    val verdict = withSession("e2e-selftest", cfg, KeyMaterial.ephemeral())(s => VerifyJob.forMode(cfg.runMode).run(s))

    val mutations = report(cfg, "selftest-report.json").get("mutations").asInstanceOf[java.util.List[java.util.Map[String, Any]]].asScala
    val missed = mutations.filterNot(_.get("caught") == true)
      .map(m => s"${m.get("id")}: verdict=${m.get("verdict")} failed=${m.get("failedChecks")} expected=${m.get("expected")}")
    missed shouldBe empty
    mutations.size shouldBe 10
    verdict shouldBe Verdict.Pass
  }

  it should "mặc định (MAPPING tắt) chỉ chạy 8 mutation của HASH + ENCRYPT" in {
    Mutations.all.count(m => VerifyConfig.DefaultModes.contains(m.mode)) shouldBe 8
  }

  "RUN_MODE=guard" should "PASS trên parquet thật với khoá từ file và chỉ ghi output khi PASS" in {
    val keyDir = Files.createTempDirectory("pseudonym-keys")
    Files.write(keyDir.resolve(PrefixSourceKeyProvider.hmacName(1)),
      PseudonymKey.randomEncoded(HmacPseudonymizer.KeyBytes).getBytes(UTF_8))
    Files.write(keyDir.resolve(PrefixSourceKeyProvider.aesSivName(1)),
      PseudonymKey.randomEncoded(AesSivPseudonymizer.KeyBytes).getBytes(UTF_8))
    val keys = KeyMaterial.fromConf(new SparkConf(false).set("spark.pseudonym.file.dir", keyDir.toString), "file")

    val prepared = LocalSpark.smallConfig()
    val input = s"${prepared.runPath}/real_input/subscriber"
    withSession("e2e-guard-input", prepared, keys) { s =>
      SyntheticData.generate(s.spark, prepared).find(_.name == SyntheticData.Subscriber).get.raw.write.parquet(input)
    }

    val cfg = LocalSpark.smallConfig("RUN_MODE" -> "guard", "KEY_SOURCE" -> "file", "INPUT_TABLE" -> input,
      "GUARD_TABLE" -> "subscriber", "IDENTIFIER_SAMPLE_FRACTION" -> "0.5")
    val verdict = withSession("e2e-guard", cfg, keys)(s => VerifyJob.forMode(cfg.runMode).run(s))

    val r = report(cfg, "guard-report.json")
    val failed = r.get("checks").asInstanceOf[java.util.List[java.util.Map[String, Any]]].asScala
      .filterNot(_.get("status") == "PASS").map(c => s"${c.get("id")}=${c.get("status")} ${c.get("samples")}")
    failed shouldBe empty
    verdict shouldBe Verdict.Pass
    Files.exists(Paths.get(new URI(s"${cfg.outputDir}/subscriber"))) shouldBe true
  }
}
