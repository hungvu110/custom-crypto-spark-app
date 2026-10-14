package vai.lakehouse.pseudonymizetable.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Dựng `SparkSession` cho app: Delta (đọc/ghi bảng Delta, chế độ MAPPING) + patch block token Isilon (+ Hive tuỳ chọn). */
object SparkSessions {

  val BlockTokenPluginClass = "vai.lakehouse.hdfs.BlockTokenFixPlugin"
  val DeltaExtension        = "io.delta.sql.DeltaSparkSessionExtension"
  val DeltaCatalog          = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

  /** Thêm `extra` vào danh sách đã khai trong manifest (không ghi đè), bỏ trùng. */
  def merged(existing: Option[String], extra: String): String =
    (existing.toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty) :+ extra).distinct.mkString(",")

  /**
   * `fs.defaultFS` lấy từ core-site.xml của `hadoopConfigMap`, KHÔNG hard-code ở đây (đổi cụm HDFS không phải build lại
   * image). `master` do spark-submit/Operator cấp.
   */
  def create(appName: String, enableHiveSupport: Boolean, conf: SparkConf = new SparkConf()): SparkSession = {
    val builder = SparkSession.builder().appName(appName)
      .config("spark.plugins", merged(conf.getOption("spark.plugins"), BlockTokenPluginClass))
      .config("spark.sql.extensions", merged(conf.getOption("spark.sql.extensions"), DeltaExtension))
      .config("spark.sql.catalog.spark_catalog", DeltaCatalog)
    (if (enableHiveSupport) builder.enableHiveSupport() else builder).getOrCreate()
  }
}
