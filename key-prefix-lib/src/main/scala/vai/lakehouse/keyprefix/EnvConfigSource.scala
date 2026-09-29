package vai.lakehouse.keyprefix

/**
 * Đọc cấu hình lấy keyPrefix từ biến môi trường của tiến trình (SparkApplication driver/executor).
 * Mọi lib crypto (`column-crypto-lib`, `cdr-crypto-udf`) dùng chung bộ biến `VAULT_*`/`CRYPTO_*` này:
 * extension do Spark tự khởi tạo qua `spark.sql.extensions` (constructor không tham số) nên không
 * ai truyền cấu hình vào được, nhưng biến môi trường là cấp OS nên đọc trực tiếp được.
 *
 * @param env mặc định là biến môi trường của tiến trình; tiêm map khác để test.
 */
class EnvConfigSource(env: Map[String, String] = sys.env) extends ConfigSource {
  import PrefixSourceFactory.Keys

  private val names = Map(
    Keys.Source          -> "CRYPTO_PREFIX_SOURCE",
    Keys.FileDir         -> "CRYPTO_KEY_PREFIX_DIR",
    Keys.VaultAddr       -> "VAULT_ADDR",
    Keys.VaultAuthMethod -> "VAULT_AUTH_METHOD",
    Keys.VaultToken      -> "VAULT_TOKEN",
    Keys.VaultRole       -> "VAULT_ROLE",
    Keys.VaultAuthMount  -> "VAULT_AUTH_MOUNT",
    Keys.VaultKvMount    -> "VAULT_KV_MOUNT",
    Keys.VaultKvBasePath -> "VAULT_KV_PATH",
    Keys.VaultKeyField   -> "VAULT_KEY_FIELD",
    Keys.VaultJwtPath    -> "VAULT_JWT_PATH",
    Keys.CacheTtlSeconds -> "CRYPTO_CACHE_TTL_SECONDS"
  )

  override def describe(key: String): String = names.getOrElse(key, key)

  override def get(key: String): Option[String] =
    env.get(describe(key)).map(_.trim).filter(_.nonEmpty)
}
