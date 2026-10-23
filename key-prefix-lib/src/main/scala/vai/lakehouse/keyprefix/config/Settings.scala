package vai.lakehouse.keyprefix

import java.net.URI
import java.util.Locale

import scala.util.Try
import scala.util.matching.Regex

/**
 * Đọc và kiểm từng giá trị cấu hình, dùng chung cho các factory. Mọi lỗi là `IllegalArgumentException` nêu TÊN VẬT LÝ của key
 * ([[ConfigSource.describe]]) và KHÔNG in lại giá trị (có thể là secret, hoặc URL chứa user:pass).
 */
private[keyprefix] object Settings {

  import PrefixSourceFactory.Keys

  def required(cfg: ConfigSource, key: String): String =
    cfg.get(key).getOrElse(throw new IllegalArgumentException(s"Missing required setting: ${cfg.describe(key)}"))

  /** Giá trị tuỳ chọn phải khớp `pattern`; sai thì báo lỗi kèm pattern. */
  def optionalMatching(cfg: ConfigSource, key: String, pattern: Regex): Option[String] =
    cfg.get(key).map { value =>
      require(pattern.pattern.matcher(value).matches(), s"Invalid ${cfg.describe(key)}: must match ${pattern.regex}")
      value
    }

  /**
   * URL tuyệt đối có host, bắt buộc https:// trừ khi `allowHttp`.
   *
   * @param allowHttpKey key bật http://, chỉ để thông báo lỗi chỉ đúng chỗ cần sửa
   */
  def httpsUrl(cfg: ConfigSource, key: String, allowHttp: Boolean, allowHttpKey: String): String = {
    val raw = required(cfg, key)
    val uri = Try(URI.create(raw)).toOption.filter(u => Option(u.getHost).exists(_.nonEmpty))
    val scheme = uri.flatMap(u => Option(u.getScheme)).map(_.toLowerCase(Locale.ROOT))
    val accepted = scheme.contains("https") || (allowHttp && scheme.contains("http"))
    require(accepted,
      s"Invalid ${cfg.describe(key)}: must be an absolute https:// URL " +
        s"(http:// is accepted only with ${cfg.describe(allowHttpKey)}=true, for local testing)")
    raw
  }

  /** `cacheTtlSeconds`: số giây ≥ 0, mặc định [[PrefixSourceFactory.DefaultCacheTtlSeconds]]; 0 = tắt cache. */
  def cacheTtlSeconds(cfg: ConfigSource): Long =
    cfg.get(Keys.CacheTtlSeconds).map { raw =>
      Try(raw.toLong).toOption.filter(_ >= 0).getOrElse(throw new IllegalArgumentException(
        s"Invalid ${cfg.describe(Keys.CacheTtlSeconds)}: must be a non-negative number of seconds"))
    }.getOrElse(PrefixSourceFactory.DefaultCacheTtlSeconds)
}
