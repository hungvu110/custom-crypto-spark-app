package vai.lakehouse.pseudonym.spark

import org.apache.spark.SparkConf
import vai.lakehouse.keyprefix.{ChainedConfigSource, EnvConfigSource, PrefixSource, PrefixSourceFactory, SparkConfSource}
import vai.lakehouse.pseudonym.ConfigurationException
import vai.lakehouse.pseudonym.core.{KeyProvider, PrefixSourceKeyProvider}

import scala.util.Try

/**
 * Cấu hình của lib, namespace riêng `spark.pseudonym.*` (không đụng `spark.columncrypto.*`/`spark.cdrcrypto.*`):
 * {{{
 * spark.pseudonym.source / file.dir / vault.xxx / dak.xxx   nguồn khoá — cùng key logic với key-prefix-lib;
 *                                                         thiếu thì fallback bộ biến môi trường CRYPTO_ / VAULT_ / DAK_
 * spark.pseudonym.hmac.version       version khoá HMAC đang dùng (mặc định 1)
 * spark.pseudonym.aes-siv.version    version khoá AES-256-SIV đang dùng (mặc định 1)
 * spark.pseudonym.mapping.path       đường dẫn bảng ánh xạ Delta (bắt buộc khi dùng MAPPING)
 * }}}
 *
 * Mọi hàm nhận `SparkConf` tường minh — lib KHÔNG đọc `SparkEnv` toàn cục (trừ extension do Spark tự khởi tạo, xem
 * `PseudonymExtension`).
 */
object PseudonymConf {

  val ConfPrefix       = "spark.pseudonym."
  val HmacVersionKey   = "spark.pseudonym.hmac.version"
  val AesSivVersionKey = "spark.pseudonym.aes-siv.version"
  val MappingPathKey   = "spark.pseudonym.mapping.path"

  def prefixSource(conf: SparkConf): PrefixSource =
    PrefixSourceFactory.create(new ChainedConfigSource(Seq(
      new SparkConfSource(conf, ConfPrefix),
      new EnvConfigSource())))

  def keyProvider(conf: SparkConf, source: PrefixSource): KeyProvider =
    new PrefixSourceKeyProvider(source, version(conf, HmacVersionKey), version(conf, AesSivVersionKey))

  /** Khoá dựng hoàn toàn từ `conf` (+ biến môi trường), không cần `SparkEnv`. */
  def keyProvider(conf: SparkConf): KeyProvider = keyProvider(conf, prefixSource(conf))

  def mappingPath(conf: SparkConf): Option[String] = conf.getOption(MappingPathKey).map(_.trim).filter(_.nonEmpty)

  private def version(conf: SparkConf, key: String): Int =
    conf.getOption(key).map { raw =>
      Try(raw.trim.toInt).toOption.filter(_ >= 1).getOrElse(
        throw new ConfigurationException(s"Invalid $key: must be an integer >= 1"))
    }.getOrElse(1)
}
