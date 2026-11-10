package vai.lakehouse.keyprefix

/**
 * Dựng [[DakDeidKeySource]] từ cùng các key `dak.*` với nguồn `dak` của [[PrefixSourceFactory]] (`DAK_ADDR`, `DAK_TOKEN_URL`,
 * `DAK_CLIENT_ID`, `DAK_CLIENT_SECRET`, `DAK_ALLOW_INSECURE_HTTP`, `DAK_CALLER`). Client là client nội bộ; không gửi `tag`.
 */
object DeidKeySourceFactory {

  /** Ném `IllegalArgumentException` (không lộ giá trị nhạy cảm) nếu cấu hình sai hoặc thiếu. */
  def create(cfg: ConfigSource): DakDeidKeySource = {
    val dak = DakSettings.config(cfg)
    new DakDeidKeySource(dak, DakSettings.tokens(cfg, dak))
  }

  /** `true` khi cấu hình chọn nguồn `dak` (và chỉ `dak`). */
  def isDak(cfg: ConfigSource): Boolean =
    cfg.get(PrefixSourceFactory.Keys.Source).map(_.trim.toLowerCase).contains(PrefixSourceFactory.DakSource)
}
