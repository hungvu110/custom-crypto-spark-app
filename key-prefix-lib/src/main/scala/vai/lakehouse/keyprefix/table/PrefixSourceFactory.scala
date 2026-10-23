package vai.lakehouse.keyprefix

import java.util.concurrent.TimeUnit

/**
 * Dựng `PrefixSource` (key cấp BẢNG) từ cấu hình. Key theo CỘT: [[ColumnKeySourceFactory]]. Các key logic:
 * {{{
 * source            file | vault | file,vault | dak   (mặc định file; nhiều nguồn = thử lần lượt, fallback;
 *                   dak KHÔNG được ghép với nguồn khác)
 * file.dir          thư mục chứa file <tên bảng> (K8s Secret mount)
 * dak.addr          (bắt buộc khi dùng dak) base URL của DAK, https://
 * dak.tokenUrl      (bắt buộc khi dùng dak) token endpoint Keycloak của realm của tenant, https://
 * dak.clientId      (bắt buộc khi dùng dak) client Keycloak của team
 * dak.clientSecret  (bắt buộc khi dùng dak)
 * dak.allowInsecureHttp  mặc định false; true cho phép http:// (chỉ dùng khi test local)
 * dak.tag           (key theo cột) gửi làm tham số tag của by-column; client nội bộ đặt internal, client team để trống
 * dak.caller        gửi làm header X-Dak-Caller (tên engine/job, DAK chỉ dùng để ghi audit); [A-Za-z0-9._-]{1,128}
 * vault.addr        (bắt buộc khi dùng vault)
 * vault.authMethod  kubernetes | token   (mặc định kubernetes)
 * vault.token       (bắt buộc khi authMethod=token) token Vault tĩnh; KHÔNG bị login/revoke
 * vault.role        (bắt buộc khi authMethod=kubernetes)  role Kubernetes auth
 * vault.kvBasePath  (bắt buộc)     đường dẫn gốc trong KV v2
 * vault.authMount   mặc định kubernetes
 * vault.kvMount     mặc định kv
 * vault.keyField    mặc định keyPrefix
 * vault.jwtPath     mặc định /var/run/secrets/kubernetes.io/serviceaccount/token
 * cacheTtlSeconds   mặc định 300; 0 = tắt cache
 * }}}
 */
object PrefixSourceFactory {

  object Keys {
    val Source          = "source"
    val FileDir         = "file.dir"
    val VaultAddr       = "vault.addr"
    val VaultAuthMethod = "vault.authMethod"
    val VaultToken      = "vault.token"
    val VaultRole       = "vault.role"
    val VaultAuthMount  = "vault.authMount"
    val VaultKvMount    = "vault.kvMount"
    val VaultKvBasePath = "vault.kvBasePath"
    val VaultKeyField   = "vault.keyField"
    val VaultJwtPath    = "vault.jwtPath"
    val CacheTtlSeconds = "cacheTtlSeconds"
    val DakAddr              = "dak.addr"
    val DakTokenUrl          = "dak.tokenUrl"
    val DakClientId          = "dak.clientId"
    val DakClientSecret      = "dak.clientSecret"
    val DakAllowInsecureHttp = "dak.allowInsecureHttp"
    val DakTag               = "dak.tag"
    val DakCaller            = "dak.caller"
  }

  val FileSource  = "file"
  val VaultSource = "vault"
  val DakSource   = "dak"
  private val SupportedSources = Seq(FileSource, VaultSource, DakSource)

  val KubernetesAuth = "kubernetes"
  val TokenAuth      = "token"
  private val SupportedAuthMethods = Seq(KubernetesAuth, TokenAuth)

  val DefaultFileDir         = "conf/key-prefix"
  val DefaultJwtPath         = "/var/run/secrets/kubernetes.io/serviceaccount/token"
  val DefaultCacheTtlSeconds = 300L

  /** Ném `IllegalArgumentException` (không lộ giá trị nhạy cảm) nếu cấu hình sai hoặc thiếu. */
  def create(cfg: ConfigSource): PrefixSource = {
    val names = sourceNames(cfg)
    val built = names.map(n => n -> build(n, cfg))
    val base: PrefixSource = if (built.size == 1) built.head._2 else new ChainedPrefixSource(built)
    val ttlSeconds = Settings.cacheTtlSeconds(cfg)
    if (ttlSeconds > 0) new CachedPrefixSource(base, TimeUnit.SECONDS.toNanos(ttlSeconds)) else base
  }

  private def sourceNames(cfg: ConfigSource): Seq[String] = {
    val names = cfg.get(Keys.Source).getOrElse(FileSource).toLowerCase
      .split(",").map(_.trim).filter(_.nonEmpty).toSeq.distinct
    val invalid = names.filterNot(SupportedSources.contains)
    require(names.nonEmpty && invalid.isEmpty,
      s"Invalid ${cfg.describe(Keys.Source)}: must be a comma-separated list of ${SupportedSources.mkString(", ")}" +
        (if (invalid.nonEmpty) s" (got: ${invalid.mkString(", ")})" else ""))
    // ChainedPrefixSource chuyển nguồn khi gặp BẤT KỲ lỗi nào, kể cả 403 của DAK -> nguồn sau sẽ trả key
    // mà DAK vừa từ chối. Fallback về Vault chỉ được làm thủ công (đổi source rồi restart).
    require(!(names.contains(DakSource) && names.size > 1),
      s"Invalid ${cfg.describe(Keys.Source)}: '$DakSource' cannot be combined with other sources " +
        s"(got: ${names.mkString(",")}). A fallback source is used on any DAK error, including 403, " +
        s"which would bypass DAK permissions. Use '$DakSource' alone; if DAK is down, switch " +
        s"${cfg.describe(Keys.Source)} manually and restart.")
    names
  }

  private def build(name: String, cfg: ConfigSource): PrefixSource = name match {
    case VaultSource => new VaultPrefixSource(vaultConfig(cfg))
    case DakSource   =>
      val dak = DakSettings.config(cfg)
      new DakPrefixSource(dak, DakSettings.tokens(cfg, dak))
    case _           => new FilePrefixSource(cfg.get(Keys.FileDir).getOrElse(DefaultFileDir))
  }

  private def vaultConfig(cfg: ConfigSource): VaultConfig = {
    val method = cfg.get(Keys.VaultAuthMethod).getOrElse(KubernetesAuth).toLowerCase
    require(SupportedAuthMethods.contains(method),
      s"Invalid ${cfg.describe(Keys.VaultAuthMethod)}: must be one of ${SupportedAuthMethods.mkString(", ")}")
    val token = if (method == TokenAuth) Some(Settings.required(cfg, Keys.VaultToken)) else None
    VaultConfig(
      addr       = Settings.required(cfg, Keys.VaultAddr),
      // role chỉ cần cho Kubernetes auth; chế độ token không login nên không dùng tới.
      role       = if (token.isDefined) cfg.get(Keys.VaultRole).getOrElse("") else Settings.required(cfg, Keys.VaultRole),
      authMount  = cfg.get(Keys.VaultAuthMount).getOrElse("kubernetes"),
      kvMount    = cfg.get(Keys.VaultKvMount).getOrElse("kv"),
      kvBasePath = Settings.required(cfg, Keys.VaultKvBasePath),
      keyField   = cfg.get(Keys.VaultKeyField).getOrElse("keyPrefix"),
      jwtPath    = cfg.get(Keys.VaultJwtPath).getOrElse(DefaultJwtPath),
      token      = token)
  }
}
