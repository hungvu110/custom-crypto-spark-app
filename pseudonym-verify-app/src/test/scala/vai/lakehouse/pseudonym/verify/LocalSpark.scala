package vai.lakehouse.pseudonym.verify

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import vai.lakehouse.pseudonym.sql.PseudonymExtension

/** SparkSession `local[2]` giống [[PseudoVerifyApp.createSession]] (Delta + PseudonymExtension với khoá của lần chạy). */
object LocalSpark {

  def session(name: String, keys: KeyMaterial): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkSession.builder().appName(name).master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("pv-wh").toString)
      .config("spark.sql.extensions", PseudoVerifyApp.DeltaExtension)
      .config("spark.sql.catalog.spark_catalog", PseudoVerifyApp.DeltaCatalog)
      .withExtensions(new PseudonymExtension(() => keys.provider))
      .getOrCreate()
  }

  def stop(spark: SparkSession): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  /** Cấu hình nhỏ cho test (vài trăm chủ thể), output ở thư mục tạm. */
  def smallConfig(extra: (String, String)*): VerifyConfig = {
    val out = Files.createTempDirectory("pseudonym-verify").toUri.toString
    VerifyConfig.fromEnv(Map(
      "RUN_ID" -> "test", "OUTPUT_PATH" -> out, "N_SUBJECTS" -> "300", "APP_EVENTS_PER_SUBJECT" -> "4",
      "BTS_CONN_PER_SUBJECT" -> "3", "LOYALTY_TXN_PER_SUBJECT" -> "2", "PARTITIONS" -> "3", "NULL_RATE" -> "0.05",
      "OFFER_SAMPLE" -> "50") ++ extra).fold(e => throw new IllegalArgumentException(e.mkString("; ")), identity)
  }
}
