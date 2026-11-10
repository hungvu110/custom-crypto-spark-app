package vai.lakehouse.pseudonym.spark

import org.apache.spark.SparkConf
import vai.lakehouse.keyprefix.{ChainedConfigSource, ConfigSource, DakTableRef, DeidKey, DeidKeySourceFactory, EnvConfigSource, PrefixSource, PrefixSourceFactory, SparkConfSource}
import vai.lakehouse.pseudonym.ConfigurationException
import vai.lakehouse.pseudonym.core.{DakDeidKeyProvider, KeyProvider, PrefixSourceKeyProvider}

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
/** Bảng và phiếu khai thác mà job khử định danh đang chạy; dùng khi nguồn khoá là `dak` (`GET /api/v1/keys/deid`). */
final case class DeidTarget(database: String, table: String, requestCode: Option[String])

object PseudonymConf {

  val ConfPrefix       = "spark.pseudonym."
  val HmacVersionKey   = "spark.pseudonym.hmac.version"
  val AesSivVersionKey = "spark.pseudonym.aes-siv.version"
  val MappingPathKey   = "spark.pseudonym.mapping.path"

  private def configSource(conf: SparkConf): ConfigSource =
    new ChainedConfigSource(Seq(new SparkConfSource(conf, ConfPrefix), new EnvConfigSource()))

  def prefixSource(conf: SparkConf): PrefixSource = PrefixSourceFactory.create(configSource(conf))

  /**
   * Khoá của job khử định danh một bảng. Nguồn `dak`: một lần GET `/api/v1/keys/deid` cho `deid` (khoá HASH của phiếu), không đọc
   * Secret. `onFetched` được gọi sau mỗi lần GET thành công (để job ghi `requestCode`/`keyVersion` vào log, không có giá trị khoá).
   * Nguồn khác (`file`, `vault`): như `keyProvider(conf)` (`deid` bị bỏ qua).
   */
  def keyProvider(conf: SparkConf, deid: DeidTarget, onFetched: DeidKey => Unit = _ => ()): KeyProvider = {
    val cfg = configSource(conf)
    if (DeidKeySourceFactory.isDak(cfg)) {
      val source = DeidKeySourceFactory.create(cfg)
      val table = Try(DakTableRef.parse(s"${deid.database}.${deid.table}")).getOrElse(
        throw new ConfigurationException("With source=dak the source table must be '<database>.<table>' (letters, digits, '_')"))
      new DakDeidKeyProvider(() => {
        val key = source.fetch(table, deid.requestCode)
        onFetched(key)
        key
      })
    } else keyProvider(conf)
  }

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
