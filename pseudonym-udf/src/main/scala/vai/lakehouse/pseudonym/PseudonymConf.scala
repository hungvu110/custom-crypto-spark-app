package vai.lakehouse.pseudonym

import org.apache.spark.{SparkConf, SparkEnv}
import vai.lakehouse.keyprefix.{ChainedConfigSource, EnvConfigSource, PrefixSource, PrefixSourceFactory, SparkConfSource}

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

  def mappingPath(conf: SparkConf): Option[String] = conf.getOption(MappingPathKey).map(_.trim).filter(_.nonEmpty)

  /**
   * Dùng chung cho cả tiến trình (extension bị khởi tạo lại theo từng SparkSession của Thrift Server, nhưng chỉ
   * nên đọc khoá 1 lần). Dựng từ SparkConf của driver.
   */
  lazy val sharedKeyProvider: KeyProvider = {
    val conf = SparkEnv.get.conf
    keyProvider(conf, prefixSource(conf))
  }

  private def version(conf: SparkConf, key: String): Int =
    conf.getOption(key).map { raw =>
      Try(raw.trim.toInt).toOption.filter(_ >= 1).getOrElse(
        throw new IllegalArgumentException(s"Invalid $key: must be an integer >= 1"))
    }.getOrElse(1)
}
