package vai.lakehouse.pseudonymverify.key

import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.core.{AesSivPseudonymizer, HmacPseudonymizer, KeyProvider, PrefixSourceKeyProvider, PseudonymKey}
import vai.lakehouse.pseudonym.spark.PseudonymConf

import java.util.Base64

import org.apache.spark.SparkConf
import vai.lakehouse.keyprefix.{ChainedConfigSource, ConfigSource, EnvConfigSource, PrefixSource, PrefixSourceFactory, SparkConfSource}

/**
 * Khoá dùng cho lần chạy + các dạng chuỗi của khoá để check P7 quét rò rỉ.
 *   - `ephemeral`: sinh ngẫu nhiên trong driver, bọc vào `PrefixSource` trong bộ nhớ, không ghi đi đâu.
 *   - `file` / `vault` / `dak`: đi đúng đường production (`key-prefix-lib`, namespace `spark.pseudonym.*` rồi biến môi
 *     trường), với key logic `source` ép theo KEY_SOURCE.
 */
final class KeyMaterial(val provider: KeyProvider, val description: String) {

  /** Base64, base64url và hex của khoá mà chế độ `mode` dùng (MAPPING không có khoá). */
  def secretStrings(mode: PseudonymMode): Seq[String] = mode match {
    case PseudonymMode.Hash    => forms(provider.hmacKey.exportBytes())
    case PseudonymMode.Encrypt => forms(provider.aesSivKey.exportBytes())
    case PseudonymMode.Mapping => Nil
  }

  private def forms(bytes: Array[Byte]): Seq[String] = Seq(
    Base64.getEncoder.encodeToString(bytes),
    Base64.getUrlEncoder.withoutPadding().encodeToString(bytes),
    bytes.map(b => f"${b & 0xff}%02x").mkString)
}

object KeyMaterial {

  def ephemeral(): KeyMaterial = {
    val keys = Map(
      PrefixSourceKeyProvider.hmacName(1)   -> PseudonymKey.randomEncoded(HmacPseudonymizer.KeyBytes),
      PrefixSourceKeyProvider.aesSivName(1) -> PseudonymKey.randomEncoded(AesSivPseudonymizer.KeyBytes))
    new KeyMaterial(new PrefixSourceKeyProvider(new InMemoryPrefixSource(keys)), "ephemeral (SecureRandom, chỉ trong bộ nhớ)")
  }

  /** Khoá ngẫu nhiên mới mỗi lần gọi — "sai khoá" cho check P5 và mutation M6. */
  def random(): KeyProvider = ephemeral().provider

  def fromConf(conf: SparkConf, keySource: String): KeyMaterial = {
    val cfg = new ChainedConfigSource(Seq(
      new ForcedSource(keySource),
      new SparkConfSource(conf, PseudonymConf.ConfPrefix),
      new EnvConfigSource()))
    new KeyMaterial(PseudonymConf.keyProvider(conf, PrefixSourceFactory.create(cfg)), s"$keySource (key-prefix-lib)")
  }

  /** Ép key logic `source` theo KEY_SOURCE của app. */
  private final class ForcedSource(source: String) extends ConfigSource {
    override def get(key: String): Option[String] =
      if (key == PrefixSourceFactory.Keys.Source) Some(source) else None
    override def describe(key: String): String = if (key == PrefixSourceFactory.Keys.Source) "KEY_SOURCE" else key
  }

  private final class InMemoryPrefixSource(values: Map[String, String]) extends PrefixSource {
    override def read(datasetName: String): String =
      values.getOrElse(datasetName, throw new IllegalStateException(s"No ephemeral key '$datasetName'"))
  }
}
