package vai.lakehouse.keyprefix

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
 * source            file | vault | file,vault   (mặc định file; nhiều nguồn = thử lần lượt, fallback)
 * file.dir          thư mục chứa file <tên bảng> (K8s Secret mount)
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
  }

  val FileSource  = "file"
  val VaultSource = "vault"
  private val SupportedSources = Seq(FileSource, VaultSource)

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
    names
  }

  private def build(name: String, cfg: ConfigSource): PrefixSource = name match {
    case VaultSource => new VaultPrefixSource(vaultConfig(cfg))
    case _           => new FilePrefixSource(cfg.get(Keys.FileDir).getOrElse(DefaultFileDir))
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
