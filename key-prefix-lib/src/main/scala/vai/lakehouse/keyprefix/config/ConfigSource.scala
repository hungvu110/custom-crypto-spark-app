package vai.lakehouse.keyprefix

import org.apache.spark.SparkConf

/**
 * Nơi đọc cấu hình theo TÊN LOGIC (`source`, `vault.addr`, ...). Mỗi môi trường ánh xạ tên logic sang
 * tên vật lý của nó: biến môi trường của SparkApplication ([[EnvConfigSource]]), hay `spark.<lib>.*` của sql-engine
 * ([[SparkConfSource]]). Nhờ vậy 1 factory phục vụ cả hai cách deploy. Tên logic: [[PrefixSourceFactory.Keys]].
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
