package vai.lakehouse.keyprefix

import java.util.Locale
import java.util.concurrent.TimeUnit

/**
 * Dựng [[ColumnKeySource]] (key theo cột) từ cùng các key logic của [[PrefixSourceFactory]]:
 * {{{
 * source            BẮT BUỘC dak — key theo cột chỉ có ở DAK, không có ở file hay Vault cũ
 * dak.addr, dak.tokenUrl, dak.clientId, dak.clientSecret, dak.allowInsecureHttp   như nguồn dak của key cả bảng
 * dak.tag           client nội bộ của by-column: internal; client team: để trống
 * dak.caller        (tuỳ chọn) gửi làm X-Dak-Caller
 * cacheTtlSeconds   mặc định 300; 0 = tắt cache
 * }}}
 * Không có fallback sang nguồn khác: chưa có dữ liệu nào mã hoá bằng key cột ngoài DAK.
 */
object ColumnKeySourceFactory {

  import PrefixSourceFactory.Keys

  /** Ném `IllegalArgumentException` (không lộ giá trị nhạy cảm) nếu cấu hình sai hoặc thiếu. */
  def create(cfg: ConfigSource): ColumnKeySource = {
    val source = cfg.get(Keys.Source).map(_.toLowerCase(Locale.ROOT))
    require(source.contains(PrefixSourceFactory.DakSource),
      s"Column keys are only available from DAK: set ${cfg.describe(Keys.Source)}=${PrefixSourceFactory.DakSource} " +
        s"(got: ${source.getOrElse("<not set>")})")
    val dak = DakSettings.config(cfg)
    val base: ColumnKeySource = new DakColumnKeySource(dak, DakSettings.tokens(cfg, dak))
    val ttlSeconds = Settings.cacheTtlSeconds(cfg)
    if (ttlSeconds > 0) new CachedColumnKeySource(base, TimeUnit.SECONDS.toNanos(ttlSeconds)) else base
  }
}
