package vai.lakehouse.pseudonymverify.app

import vai.lakehouse.pseudonym.spark.PseudonymExtension
import vai.lakehouse.pseudonymverify.key.KeyMaterial

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Dựng `SparkSession` cho app: Delta (cho MAPPING), patch block token Isilon, và `PseudonymExtension` (cho check U9). */
object SparkSessions {

  val BlockTokenPluginClass = "vai.lakehouse.hdfs.BlockTokenFixPlugin"
  val DeltaExtension        = "io.delta.sql.DeltaSparkSessionExtension"
  val DeltaCatalog          = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

  /** Thêm `extra` vào danh sách đã khai trong manifest (không ghi đè), bỏ trùng. */
  def merged(existing: Option[String], extra: String): String =
    (existing.toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty) :+ extra).distinct.mkString(",")

  /**
   * Session của job trên cluster: đọc `spark.plugins`/`spark.sql.extensions` từ manifest rồi nối thêm patch + Delta.
   * `PseudonymExtension` dùng đúng khoá của lần chạy để `pseudo_hash`/`pseudo_encrypt` khớp DataFrame API.
   */
  def create(appName: String, keys: KeyMaterial, conf: SparkConf = new SparkConf()): SparkSession =
    SparkSession.builder().appName(appName)
      .config("spark.plugins", merged(conf.getOption("spark.plugins"), BlockTokenPluginClass))
      .config("spark.sql.extensions", merged(conf.getOption("spark.sql.extensions"), DeltaExtension))
      .config("spark.sql.catalog.spark_catalog", DeltaCatalog)
      .withExtensions(new PseudonymExtension(() => keys.provider))
      .getOrCreate()
}
