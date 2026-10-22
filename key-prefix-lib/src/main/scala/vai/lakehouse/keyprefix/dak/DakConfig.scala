package vai.lakehouse.keyprefix

/**
 * Cấu hình nguồn `dak`, dùng chung cho key cả bảng ([[DakPrefixSource]]) và key theo cột ([[DakColumnKeySource]]).
 * `toString` che secret để không lộ ra log khi in nguyên case class.
 *
 * @param tag    gửi làm tham số `tag` của `by-column` (client nội bộ: `internal`); `None` = không gửi (client team)
 * @param caller gửi làm header `X-Dak-Caller` (tên engine/job, chỉ để DAK ghi audit); `None` = không gửi
 */
final case class DakConfig(addr: String, tokenUrl: String, clientId: String, clientSecret: String,
                           tag: Option[String] = None, caller: Option[String] = None) {
  override def toString: String =
    s"DakConfig(addr=$addr, tokenUrl=$tokenUrl, clientId=$clientId, clientSecret=***, " +
      s"tag=${tag.getOrElse("-")}, caller=${caller.getOrElse("-")})"
}

/** Dựng [[DakConfig]] và token provider từ các key `dak.*` ([[PrefixSourceFactory.Keys]]). */
private[keyprefix] object DakSettings {

  import PrefixSourceFactory.Keys

  private val TagPattern    = "^[a-z0-9_-]{1,64}$".r
  private val CallerPattern = "^[A-Za-z0-9._-]{1,128}$".r

  def config(cfg: ConfigSource): DakConfig = {
    val allowHttp = cfg.get(Keys.DakAllowInsecureHttp).exists(_.equalsIgnoreCase("true"))
    DakConfig(
      addr         = Settings.httpsUrl(cfg, Keys.DakAddr, allowHttp, Keys.DakAllowInsecureHttp),
      tokenUrl     = Settings.httpsUrl(cfg, Keys.DakTokenUrl, allowHttp, Keys.DakAllowInsecureHttp),
      clientId     = Settings.required(cfg, Keys.DakClientId),
      clientSecret = Settings.required(cfg, Keys.DakClientSecret),
      tag          = Settings.optionalMatching(cfg, Keys.DakTag, TagPattern),
      caller       = Settings.optionalMatching(cfg, Keys.DakCaller, CallerPattern))
  }

  /** Token provider dùng chung trong tiến trình cho cùng `(tokenUrl, clientId, clientSecret)`. */
  def tokens(cfg: ConfigSource, dak: DakConfig): KeycloakTokenProvider = {
    val hint = Seq(Keys.DakTokenUrl, Keys.DakClientId, Keys.DakClientSecret).map(cfg.describe).mkString(", ")
    KeycloakTokenProvider.shared(dak.tokenUrl, dak.clientId, dak.clientSecret, hint)
  }
}
