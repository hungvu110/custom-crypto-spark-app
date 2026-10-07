package vai.lakehouse.keyprefix

import java.net.URI
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf

import scala.util.Try

/**
 * Nơi đọc cấu hình theo TÊN LOGIC (`source`, `vault.addr`, ...). Mỗi môi trường ánh xạ tên logic sang
 * tên vật lý của nó: biến môi trường của SparkApplication, hay `spark.<lib>.*` của sql-engine.
 * Nhờ vậy 1 factory phục vụ cả hai cách deploy.
 */
trait ConfigSource {
  /** Giá trị đã trim; `None` nếu chưa đặt hoặc rỗng. */
  def get(key: String): Option[String]

  /** Tên vật lý của key, dùng cho thông báo lỗi (vd `VAULT_ADDR` hay `spark.columncrypto.vault.addr`). */
  def describe(key: String): String
}

/**
 * sql-engine / SparkApplication cấu hình qua Spark conf: `<prefix><key>`. Mỗi lib crypto truyền
 * namespace conf riêng của nó (vd `"spark.columncrypto."`, `"spark.cdrcrypto."`) để 2 loại prefix
 * cùng tồn tại trên 1 engine mà không đụng cấu hình Vault của nhau.
 */
class SparkConfSource(conf: SparkConf, prefix: String) extends ConfigSource {
  override def describe(key: String): String = s"$prefix$key"
  override def get(key: String): Option[String] =
    conf.getOption(describe(key)).map(_.trim).filter(_.nonEmpty)
}

/**
 * Thử lần lượt từng `ConfigSource` theo thứ tự, dùng giá trị đầu tiên có mặt. Cho phép một key vừa
 * cấu hình được qua Spark conf (sql-engine) VỪA fallback về biến môi trường chung
 * (SparkApplication) mà không cần trùng lặp logic đọc — ví dụ ưu tiên `spark.cdrcrypto.*` nếu có,
 * nếu không thì dùng chung bộ `VAULT_*`/`CRYPTO_*` của `EnvConfigSource`.
 */
class ChainedConfigSource(sources: Seq[ConfigSource]) extends ConfigSource {
  override def get(key: String): Option[String] = sources.view.flatMap(_.get(key)).headOption
  override def describe(key: String): String = sources.map(_.describe(key)).mkString(" hoặc ")
}

/**
 * Dựng `PrefixSource` từ cấu hình. Các key logic:
 * {{{
 * source            file | vault | file,vault | dak   (mặc định file; nhiều nguồn = thử lần lượt, fallback;
 *                   dak KHÔNG được ghép với nguồn khác)
 * file.dir          thư mục chứa file <tên bảng> (K8s Secret mount)
 * dak.addr          (bắt buộc khi dùng dak) base URL của DAK, https://
 * dak.tokenUrl      (bắt buộc khi dùng dak) token endpoint Keycloak của realm của tenant, https://
 * dak.clientId      (bắt buộc khi dùng dak) client Keycloak của team
 * dak.clientSecret  (bắt buộc khi dùng dak)
 * dak.allowInsecureHttp  mặc định false; true cho phép http:// (chỉ dùng khi test local)
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
    val ttlSeconds = cacheTtlSeconds(cfg)
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
    case DakSource   => dakSource(cfg)
    case _           => new FilePrefixSource(cfg.get(Keys.FileDir).getOrElse(DefaultFileDir))
  }

  private def dakSource(cfg: ConfigSource): PrefixSource = {
    val allowHttp = cfg.get(Keys.DakAllowInsecureHttp).exists(_.equalsIgnoreCase("true"))
    val dak = DakConfig(
      addr         = httpsUrl(cfg, Keys.DakAddr, allowHttp),
      tokenUrl     = httpsUrl(cfg, Keys.DakTokenUrl, allowHttp),
      clientId     = required(cfg, Keys.DakClientId),
      clientSecret = required(cfg, Keys.DakClientSecret))
    val hint = Seq(Keys.DakTokenUrl, Keys.DakClientId, Keys.DakClientSecret).map(cfg.describe).mkString(", ")
    new DakPrefixSource(dak, KeycloakTokenProvider.shared(dak.tokenUrl, dak.clientId, dak.clientSecret, hint))
  }

  /** URL tuyệt đối có host; bắt buộc https:// trừ khi bật allowInsecureHttp. Không in lại giá trị (URL có thể chứa user:pass). */
  private def httpsUrl(cfg: ConfigSource, key: String, allowHttp: Boolean): String = {
    val raw = required(cfg, key)
    val uri = Try(URI.create(raw)).toOption.filter(u => Option(u.getHost).exists(_.nonEmpty))
    val scheme = uri.flatMap(u => Option(u.getScheme)).map(_.toLowerCase(Locale.ROOT))
    val accepted = scheme.contains("https") || (allowHttp && scheme.contains("http"))
    require(accepted,
      s"Invalid ${cfg.describe(key)}: must be an absolute https:// URL " +
        s"(http:// is accepted only with ${cfg.describe(Keys.DakAllowInsecureHttp)}=true, for local testing)")
    raw
  }

  private def vaultConfig(cfg: ConfigSource): VaultConfig = {
    val method = cfg.get(Keys.VaultAuthMethod).getOrElse(KubernetesAuth).toLowerCase
    require(SupportedAuthMethods.contains(method),
      s"Invalid ${cfg.describe(Keys.VaultAuthMethod)}: must be one of ${SupportedAuthMethods.mkString(", ")}")
    val token = if (method == TokenAuth) Some(required(cfg, Keys.VaultToken)) else None
    VaultConfig(
      addr       = required(cfg, Keys.VaultAddr),
      // role chỉ cần cho Kubernetes auth; chế độ token không login nên không dùng tới.
      role       = if (token.isDefined) cfg.get(Keys.VaultRole).getOrElse("") else required(cfg, Keys.VaultRole),
      authMount  = cfg.get(Keys.VaultAuthMount).getOrElse("kubernetes"),
      kvMount    = cfg.get(Keys.VaultKvMount).getOrElse("kv"),
      kvBasePath = required(cfg, Keys.VaultKvBasePath),
      keyField   = cfg.get(Keys.VaultKeyField).getOrElse("keyPrefix"),
      jwtPath    = cfg.get(Keys.VaultJwtPath).getOrElse(DefaultJwtPath),
      token      = token)
  }

  private def required(cfg: ConfigSource, key: String): String =
    cfg.get(key).getOrElse(throw new IllegalArgumentException(s"Missing required setting: ${cfg.describe(key)}"))

  private def cacheTtlSeconds(cfg: ConfigSource): Long =
    cfg.get(Keys.CacheTtlSeconds).map { raw =>
      Try(raw.toLong).toOption.filter(_ >= 0).getOrElse(throw new IllegalArgumentException(
        s"Invalid ${cfg.describe(Keys.CacheTtlSeconds)}: must be a non-negative number of seconds"))
    }.getOrElse(DefaultCacheTtlSeconds)
}
