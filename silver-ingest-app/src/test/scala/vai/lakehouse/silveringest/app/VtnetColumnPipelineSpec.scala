package vai.lakehouse.silveringest.app

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.columncrypto.cdr.CdrCrypto
import vai.lakehouse.keyprefix.{ColumnKeySource, KeyIntent, PathKey, PathKeyResult, PathKeySource}
import vai.lakehouse.silveringest.config.Mode
import vai.lakehouse.silveringest.crypto.VtnetCipher
import vai.lakehouse.silveringest.security.InputToken

/**
 * Luồng EBS-4G của VTNET: CSV có dòng tiêu đề, VTNET mã hoá `imsi` bằng `PassTranformer` (key = key `by-path`, không có keyField theo dòng), job
 * giải mã rồi mã hoá lại bằng key global với cột hằng số `key_scope=vtt`, NHƯ f022 của VTT: cùng một imsi ra cùng một bản mã ở hai bảng nên nối
 * được. Key trong test là chuỗi tự đặt; imsi là giá trị tổng hợp.
 */
class VtnetColumnPipelineSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val workDir = Files.createTempDirectory("vtnet-column-pipeline")
  private var spark: SparkSession = _

  private val vtnetKey = "TEST-VTNET-INPUT-KEY"
  private val globalPrefix = "TEST-GLOBAL-PREFIX-"
  private val header = "imsi,lac,sac,eci,event_time,event_id,event_result"
  private val schema = "imsi STRING, lac STRING, sac STRING, eci BIGINT, event_time BIGINT, event_id STRING, event_result STRING"

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().appName("VtnetColumnPipelineSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", workDir.resolve("warehouse").toString)
      .config("spark.sql.extensions", SparkSessions.DeltaExtension)
      .config("spark.sql.catalog.spark_catalog", SparkSessions.DeltaCatalog)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private class PathKeys(key: Option[String]) extends PathKeySource {
    override def readPath(path: String): PathKeyResult =
      key.fold[PathKeyResult](PathKeyResult.NotAvailable)(k => PathKeyResult.Available(PathKey(path, k, Some(1L))))
  }

  private object GlobalKeys extends ColumnKeySource {
    override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] =
      columns.map(_.toLowerCase -> globalPrefix).toMap
  }

  /** Bản mã global mà f022 của VTT ghi cho cùng imsi bản rõ (key global, keyField hằng số `vtt`). */
  private def f022Ciphertext(imsi: String): String = CdrCrypto.transform(globalPrefix, encryptMode = true)(imsi, "vtt")

  private def csv(name: String, rows: Seq[String]): Path = {
    val d = Files.createDirectories(workDir.resolve(name))
    Files.write(d.resolve("ebs_3g_4g-231050.csv"), (header +: rows).mkString("", "\n", "\n").getBytes(UTF_8))
    d
  }

  private def row(imsi: String, eci: Long, time: Long): String = s"$imsi,,,$eci,$time,1_service_request,success"

  private def env(input: Path, table: String, extra: (String, String)*): Map[String, String] =
    Map("INPUT_PATH" -> input.toString, "INPUT_FORMAT" -> "csv", "INPUT_OPTION_header" -> "true", "INPUT_SCHEMA" -> schema,
      "OUTPUT_DB" -> "silver", "OUTPUT_TABLE" -> table, "ENABLE_HIVE_SUPPORT" -> "false", "BATCH_COLUMN" -> "dt", "BATCH_VALUE" -> "20261120",
      "DECRYPT_MODE" -> "column", "KEY_PATH" -> "ebs_3g_4g", "PROVIDER_CIPHER" -> "vtnet", "PROVIDER_DECRYPT_COLUMNS" -> "imsi",
      "CDR_ENCRYPT_COLUMNS" -> "imsi", "CDR_KEY_FIELD" -> "key_scope", "CDR_KEY_FIELD_VALUE" -> "vtt", "GLOBAL_KEY_ZONE" -> "silver_vtt") ++ extra

  private def run(env: Map[String, String], key: Option[String] = Some(vtnetKey)): Int =
    SilverApp.execute(Mode.DecryptEncrypt, env, Some(spark), Some(KeySources(new PathKeys(key), GlobalKeys)))

  "PROVIDER_CIPHER=vtnet" should "giải mã imsi của VTNET rồi mã hoá lại ra đúng bản mã của f022 cho cùng imsi" in {
    val imsis = Seq("452041000000001", "452041000000002")
    val in = csv("ok", Seq(row(VtnetCipher.encrypt(vtnetKey, imsis(0)), 28291331L, 1795193804394L),
      row(VtnetCipher.encrypt(vtnetKey, imsis(1)), 227972880L, 1795193810082L)))

    run(env(in, "ebs_ok")) shouldBe SilverApp.ExitOk

    val written = spark.table("silver.ebs_ok").orderBy("event_time").collect()
    written.map(_.getAs[String]("imsi")).toSeq shouldBe imsis.map(f022Ciphertext)
    written.map(_.getAs[Long]("eci")).toSeq shouldBe Seq(28291331L, 227972880L) // các cột bản rõ giữ nguyên
    written.map(_.getAs[String]("key_scope")).distinct.toSeq shouldBe Seq("vtt")
    written.map(_.getAs[String]("lac")).distinct.toSeq shouldBe Seq(null)
  }

  it should "đọc mọi file CSV trong các thư mục batch=... của ngày, bỏ manifest.json và _SUCCESS cạnh chúng" in {
    val day = Files.createDirectories(workDir.resolve("mapping/dt=20261105"))
    for ((batch, imsi) <- Seq("01" -> "452041000000011", "02" -> "452041000000012")) {
      val dir = Files.createDirectories(day.resolve(s"batch=$batch"))
      Files.write(dir.resolve(s"part-$batch.csv"), (header +: Seq(row(VtnetCipher.encrypt(vtnetKey, imsi), 1L, 1L))).mkString("", "\n", "\n").getBytes(UTF_8))
      Files.write(dir.resolve("manifest.json"), "{\"files\": 1}".getBytes(UTF_8))
      Files.write(dir.resolve("_SUCCESS"), Array.emptyByteArray)
    }

    run(env(day, "ebs_batches", "INPUT_OPTION_recursiveFileLookup" -> "true", "INPUT_OPTION_pathGlobFilter" -> "*.csv")) shouldBe SilverApp.ExitOk

    spark.table("silver.ebs_batches").select("imsi").collect().map(_.getString(0)).sorted.toSeq shouldBe
      Seq("452041000000011", "452041000000012").map(f022Ciphertext).sorted
  }

  it should "dừng khi key by-path không phải key VTNET đã dùng (không ghi gì)" in {
    val in = csv("wrong_key", (1 to 40).map(i => row(VtnetCipher.encrypt(vtnetKey, f"452041$i%09d"), i.toLong, 1795193804394L + i)))

    run(env(in, "ebs_wrong_key"), key = Some("ANOTHER-KEY")) shouldBe SilverApp.ExitRuntimeError
    spark.catalog.tableExists("silver", "ebs_wrong_key") shouldBe false
  }

  it should "dừng khi imsi trong file là bản rõ (không mã hoá chồng lên giá trị không phải bản mã VTNET)" in {
    val in = csv("plain", Seq(row("452041000000001", 1L, 1795193804394L)))

    run(env(in, "ebs_plain")) shouldBe SilverApp.ExitRuntimeError
    spark.catalog.tableExists("silver", "ebs_plain") shouldBe false
  }

  it should "dừng khi DAK chưa có key by-path" in {
    run(env(csv("no_key", Seq(row(VtnetCipher.encrypt(vtnetKey, "452041000000001"), 1L, 1L))), "ebs_no_key"), key = None) shouldBe
      SilverApp.ExitRuntimeError
  }

  it should "từ chối PROVIDER_KEY_FIELD (key VTNET không có phần theo dòng)" in {
    run(env(csv("key_field", Nil), "ebs_key_field", "PROVIDER_KEY_FIELD" -> "event_time")) shouldBe SilverApp.ExitConfigError
  }

  "INPUT_TOKEN_FILE" should "dừng trước khi dựng Spark khi file token không có (exit 2)" in {
    val e = env(csv("token", Nil), "ebs_token") ++ Map("INPUT_PATH" -> "hdfs://localhost:8020/landing/ebs_3g_4g/dt=20261120",
      InputToken.FileEnv -> workDir.resolve("missing.token").toString)
    run(e) shouldBe SilverApp.ExitConfigError
  }
}
