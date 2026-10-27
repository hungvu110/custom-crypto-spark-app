package vai.lakehouse.silveringest.app

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.columncrypto.cdr.CdrCrypto
import vai.lakehouse.keyprefix.{ColumnKeyIssue, ColumnKeySource, ColumnKeysUnavailableException, KeyIntent, PathKey,
  PathKeyResult, PathKeySource, PendingWait, WaitingColumnKeySource}
import vai.lakehouse.silveringest.config.Source
import vai.lakehouse.silveringest.crypto.AesGcmFileCipher

class SilverAppSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val workDir = Files.createTempDirectory("silver-ingest")
  private var spark: SparkSession = _

  private val pathKeyPrefix = Base64.getEncoder.encodeToString(Array.tabulate[Byte](32)(i => (i * 7).toByte))
  private val aesKey = AesGcmFileCipher.key(pathKeyPrefix)
  private val columnKeys = Map("msisdn" -> "231x#@Vie3", "imei" -> "9a!kP0q#2b")

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().appName("SilverAppSpec").master("local[2]")
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

  // ---------------------------------------------------------------- helpers

  private val csvHeader = "msisdn,imei,start_time,amount"
  private val csvRows = Seq("84900000001,352099001761481,20261027120000,10", "84900000002,352099001761499,20261027120500,20",
    "84900000003,,20261027121000,30")

  private def dir(name: String): Path = Files.createDirectories(workDir.resolve(name))

  private def write(path: Path, bytes: Array[Byte]): Path = {
    Files.createDirectories(path.getParent)
    Files.write(path, bytes)
  }

  private def csv(rows: Seq[String]): Array[Byte] = (csvHeader +: rows).mkString("", "\n", "\n").getBytes(UTF_8)

  private def encrypted(bytes: Array[Byte]): Array[Byte] = AesGcmFileCipher.encrypt(bytes, aesKey)

  private class StubPathKeys(result: String => PathKeyResult) extends PathKeySource {
    val calls = new AtomicInteger()
    override def readPath(path: String): PathKeyResult = { calls.incrementAndGet(); result(path) }
  }

  private class StubColumnKeys(keys: Map[String, String] = columnKeys) extends ColumnKeySource {
    val calls = new AtomicInteger()
    val intents = new java.util.concurrent.CopyOnWriteArrayList[KeyIntent]()
    override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
      calls.incrementAndGet()
      intents.add(intent)
      val missing = columns.map(_.toLowerCase).filterNot(keys.contains)
      if (missing.nonEmpty) {
        throw new ColumnKeysUnavailableException(datasetName,
          missing.map(c => ColumnKeyIssue(c, "PENDING", Some("YC-2026-000009"), None)), Seq("req-1"))
      }
      columns.map(c => c.toLowerCase -> keys(c.toLowerCase)).toMap
    }
  }

  private def available(path: String) = PathKeyResult.Available(PathKey(path, pathKeyPrefix, Some(1L)))

  private def vtt(input: Path, table: String, extra: (String, String)*): Int =
    SilverApp.execute(Source.Vtt, Map("INPUT_PATH" -> input.toString, "INPUT_FORMAT" -> "csv",
      "INPUT_OPTION_header" -> "true", "OUTPUT_DB" -> "silver", "OUTPUT_TABLE" -> table, "ENABLE_HIVE_SUPPORT" -> "false") ++ extra,
      Some(spark))

  private def vtnetEnv(input: Path, table: String, extra: (String, String)*): Map[String, String] =
    Map("INPUT_PATH" -> input.toString, "INPUT_FORMAT" -> "csv", "INPUT_OPTION_header" -> "true",
      "INPUT_SCHEMA" -> "msisdn STRING, imei STRING, start_time STRING, amount INT",
      "OUTPUT_DB" -> "silver", "OUTPUT_TABLE" -> table, "ENABLE_HIVE_SUPPORT" -> "false", "KEY_PATH_ROOT" -> workDir.toString,
      "CDR_ENCRYPT_COLUMNS" -> "msisdn,imei", "CDR_KEY_FIELD" -> "start_time") ++ extra

  private def vtnet(env: Map[String, String], pathKeys: PathKeySource = new StubPathKeys(available),
                    keys: ColumnKeySource = new StubColumnKeys()): Int =
    SilverApp.execute(Source.Vtnet, env, Some(spark), Some(KeySources(pathKeys, keys)))

  private def decryptedRows(table: String): Seq[(String, String, String, Int)] = {
    val cfg = CdrCrypto.loadConfig(s"silver.$table", "start_time", Seq("msisdn", "imei"), KeyIntent.Decrypt,
      new StubColumnKeys())
    CdrCrypto.decryptColumns(spark.table(s"silver.$table"), cfg)
      .select("msisdn", "imei", "start_time", "amount").orderBy("msisdn").collect()
      .map(r => (r.getString(0), r.getString(1), r.getString(2), r.getInt(3))).toSeq
  }

  private val expected = Seq(("84900000001", "352099001761481", "20261027120000", 10),
    ("84900000002", "352099001761499", "20261027120500", 20), ("84900000003", null, "20261027121000", 30))

  // ---------------------------------------------------------------- VTT

  "VTT" should "đọc mọi file trong thư mục (kể cả thư mục con, bỏ file ẩn) và ghi bảng, không đổi giá trị" in {
    val in = dir("vtt/batch-1")
    write(in.resolve("part-1.csv"), csv(csvRows.take(2)))
    write(in.resolve("sub/part-2.csv"), csv(csvRows.drop(2)))
    write(in.resolve("_SUCCESS"), Array.emptyByteArray)

    vtt(in, "vtt_cdr", "INPUT_OPTION_recursiveFileLookup" -> "true") shouldBe SilverApp.ExitOk

    spark.table("silver.vtt_cdr").count() shouldBe 3L
    spark.table("silver.vtt_cdr").where("msisdn = '84900000001'").count() shouldBe 1L
  }

  it should "overwrite mặc định (chạy lại cùng lô không ghi trùng), append ghi nối khi đặt rõ" in {
    val in = dir("vtt/batch-2")
    write(in.resolve("part.csv"), csv(csvRows))

    vtt(in, "vtt_modes") shouldBe SilverApp.ExitOk
    vtt(in, "vtt_modes") shouldBe SilverApp.ExitOk
    spark.table("silver.vtt_modes").count() shouldBe 3L
    vtt(in, "vtt_modes", "OUTPUT_MODE" -> "append") shouldBe SilverApp.ExitOk
    spark.table("silver.vtt_modes").count() shouldBe 6L
  }

  it should "trả exit 2 khi sai cấu hình và exit 1 khi INPUT_PATH không tồn tại hoặc rỗng" in {
    SilverApp.execute(Source.Vtt, Map("INPUT_PATH" -> "relative"), Some(spark)) shouldBe SilverApp.ExitConfigError
    vtt(workDir.resolve("vtt/missing"), "vtt_missing") shouldBe SilverApp.ExitRuntimeError
    vtt(dir("vtt/empty"), "vtt_empty") shouldBe SilverApp.ExitRuntimeError
    spark.catalog.tableExists("silver", "vtt_missing") shouldBe false
  }

  private def batches(table: String): Map[String, Long] =
    spark.table(s"silver.$table").groupBy("batch_date").count().collect().map(r => r.getString(0) -> r.getLong(1)).toMap

  Seq("delta", "hive").foreach { format =>
    it should s"overwrite-partitions ($format): mỗi lô một partition batch_date, chạy lại một lô chỉ thay lô đó" in {
      val day1 = dir(s"vtt/batch-$format-1")
      val day2 = dir(s"vtt/batch-$format-2")
      write(day1.resolve("part.csv"), csv(csvRows))
      write(day2.resolve("part.csv"), csv(csvRows.take(1)))
      val table = s"vtt_batch_$format"

      vtt(day1, table, "BATCH_VALUE" -> "20261027", "OUTPUT_FORMAT" -> format) shouldBe SilverApp.ExitOk
      vtt(day2, table, "BATCH_VALUE" -> "20261028", "OUTPUT_FORMAT" -> format) shouldBe SilverApp.ExitOk
      batches(table) shouldBe Map("20261027" -> 3L, "20261028" -> 1L)

      // dữ liệu lô 27 đổi -> chạy lại lô 27: thay đúng lô 27 (không ghi trùng), lô 28 giữ nguyên
      write(day1.resolve("part.csv"), csv(csvRows.take(2)))
      vtt(day1, table, "BATCH_VALUE" -> "20261027", "OUTPUT_FORMAT" -> format) shouldBe SilverApp.ExitOk
      batches(table) shouldBe Map("20261027" -> 2L, "20261028" -> 1L)
    }
  }

  it should "từ chối ghi theo lô vào bảng đã có mà không partition theo cột lô, và dữ liệu đã có cột lô" in {
    val in = dir("vtt/batch-mismatch")
    write(in.resolve("part.csv"), csv(csvRows))

    vtt(in, "vtt_no_partition") shouldBe SilverApp.ExitOk
    vtt(in, "vtt_no_partition", "BATCH_VALUE" -> "20261027") shouldBe SilverApp.ExitRuntimeError
    spark.table("silver.vtt_no_partition").count() shouldBe 3L

    vtt(in, "vtt_batch_clash", "BATCH_VALUE" -> "20261027", "BATCH_COLUMN" -> "amount") shouldBe SilverApp.ExitRuntimeError
    spark.catalog.tableExists("silver", "vtt_batch_clash") shouldBe false
  }

  // ---------------------------------------------------------------- VTNET

  "VTNET" should "giải mã file csv trong bộ nhớ, mã hoá cột bằng cdr-crypto và ghi bảng" in {
    val in = dir("vtnet/batch-csv")
    write(in.resolve("a.csv.enc"), encrypted(csv(csvRows.take(2))))
    write(in.resolve("b.csv.enc"), encrypted(csv(csvRows.drop(2))))
    val pathKeys = new StubPathKeys(available)

    vtnet(vtnetEnv(in, "vtnet_cdr"), pathKeys) shouldBe SilverApp.ExitOk

    val stored = spark.table("silver.vtnet_cdr").orderBy("start_time").collect()
    stored.map(_.getAs[String]("msisdn")).foreach(v => v should not startWith "849")
    stored.map(_.getAs[String]("start_time")).toSeq shouldBe expected.map(_._3) // keyField giữ plaintext
    stored.map(_.getAs[Int]("amount")).toSeq shouldBe Seq(10, 20, 30)
    decryptedRows("vtnet_cdr") shouldBe expected
    pathKeys.calls.get() shouldBe 1
  }

  it should "đọc json lines đã giải mã" in {
    val in = dir("vtnet/batch-json")
    val json = expected.map { case (m, i, t, a) =>
      s"""{"msisdn":"$m",${Option(i).map(v => s""""imei":"$v",""").getOrElse("")}"start_time":"$t","amount":$a}"""
    }.mkString("\n").getBytes(UTF_8)
    write(in.resolve("data.json.enc"), encrypted(json))

    vtnet(vtnetEnv(in, "vtnet_json", "INPUT_FORMAT" -> "json") - "INPUT_OPTION_header") shouldBe SilverApp.ExitOk

    decryptedRows("vtnet_json") shouldBe expected
  }

  it should "giải mã parquet ra thư mục tạm rồi xoá thư mục tạm sau khi ghi" in {
    val session = spark
    import session.implicits._
    val plainDir = workDir.resolve("vtnet-plain-parquet")
    expected.toDF("msisdn", "imei", "start_time", "amount").coalesce(1).write.parquet(plainDir.toString)
    val in = dir("vtnet/batch-parquet")
    Files.list(plainDir).filter(_.getFileName.toString.endsWith(".parquet")).forEach { f =>
      write(in.resolve(f.getFileName.toString + ".enc"), encrypted(Files.readAllBytes(f)))
    }
    val staging = workDir.resolve("staging")

    vtnet(vtnetEnv(in, "vtnet_parquet", "INPUT_FORMAT" -> "parquet", "DECRYPT_STAGING_PATH" -> staging.toString) -
      "INPUT_SCHEMA" - "INPUT_OPTION_header") shouldBe SilverApp.ExitOk

    decryptedRows("vtnet_parquet") shouldBe expected
    (!Files.exists(staging) || Files.list(staging).count() == 0L) shouldBe true
  }

  private val noKey = new StubPathKeys(_ => PathKeyResult.NotAvailable)

  it should "hỏi by-path với đường dẫn tương đối theo KEY_PATH_ROOT, hoặc đúng KEY_PATH khi được đặt" in {
    val in = dir("vtnet/key-path/20261027")
    write(in.resolve("a.enc"), encrypted(csv(csvRows)))
    val asked = new java.util.concurrent.CopyOnWriteArrayList[String]()
    val recording = new StubPathKeys(p => { asked.add(p); available(p) })

    vtnet(vtnetEnv(in, "vtnet_key_path"), recording) shouldBe SilverApp.ExitOk
    vtnet(vtnetEnv(in, "vtnet_key_path", "KEY_PATH" -> "/VTNET_cdr//20261027/"), recording) shouldBe SilverApp.ExitOk

    asked.toArray.toSeq shouldBe Seq("vtnet/key-path/20261027", "VTNET_cdr/20261027")
  }

  it should "trả exit 5, không ghi gì, khi DAK chưa có key của một cột cần mã hoá" in {
    val in = dir("vtnet/pending")
    write(in.resolve("a.enc"), encrypted(csv(csvRows)))

    vtnet(vtnetEnv(in, "vtnet_pending"), keys = new StubColumnKeys(Map("msisdn" -> "k"))) shouldBe
      SilverApp.ExitColumnKeysPending

    spark.catalog.tableExists("silver", "vtnet_pending") shouldBe false
  }

  it should "trả exit 1, không ghi gì, khi file không giải mã được bằng key của DAK" in {
    val in = dir("vtnet/wrong-key")
    write(in.resolve("a.enc"), AesGcmFileCipher.encrypt(csv(csvRows), Array.fill[Byte](32)(1)))

    vtnet(vtnetEnv(in, "vtnet_wrong_key")) shouldBe SilverApp.ExitRuntimeError

    spark.catalog.tableExists("silver", "vtnet_wrong_key") shouldBe false
  }

  it should "trả exit 1 khi cột cần mã hoá không có trong file" in {
    val in = dir("vtnet/missing-col")
    write(in.resolve("a.enc"), encrypted(csv(csvRows)))

    vtnet(vtnetEnv(in, "vtnet_missing_col", "CDR_ENCRYPT_COLUMNS" -> "msisdn,cell_id") - "INPUT_SCHEMA",
      keys = new StubColumnKeys(columnKeys + ("cell_id" -> "k"))) shouldBe SilverApp.ExitRuntimeError
  }

  it should "DAK không có key cho đường dẫn: đọc file nguyên trạng, vẫn mã hoá cột bằng cdr-crypto và chạy thành công" in {
    val in = dir("vtnet/plain-csv")
    write(in.resolve("a.csv"), csv(csvRows))
    val pathKeys = new StubPathKeys(_ => PathKeyResult.NotAvailable)

    vtnet(vtnetEnv(in, "vtnet_plain"), pathKeys) shouldBe SilverApp.ExitOk

    pathKeys.calls.get() shouldBe 1
    spark.table("silver.vtnet_plain").collect().map(_.getAs[String]("msisdn")).foreach(v => v should not startWith "849")
    decryptedRows("vtnet_plain") shouldBe expected
  }

  it should "DAK không có key: đọc parquet nguyên trạng, không cần giải mã ra thư mục tạm" in {
    val session = spark
    import session.implicits._
    val in = workDir.resolve("vtnet/plain-parquet")
    expected.toDF("msisdn", "imei", "start_time", "amount").coalesce(1).write.parquet(in.toString)
    val staging = workDir.resolve("staging-plain")

    vtnet(vtnetEnv(in, "vtnet_plain_parquet", "INPUT_FORMAT" -> "parquet", "DECRYPT_STAGING_PATH" -> staging.toString) -
      "INPUT_SCHEMA" - "INPUT_OPTION_header", noKey) shouldBe SilverApp.ExitOk

    decryptedRows("vtnet_plain_parquet") shouldBe expected
    Files.exists(staging) shouldBe false
  }

  it should "theo lô (file không mã hoá): cột lô không mã hoá, chạy lại lô thay đúng lô đó" in {
    val in = dir("vtnet/plain-batch")
    write(in.resolve("a.csv"), csv(csvRows))
    val env = vtnetEnv(in, "vtnet_batch", "BATCH_VALUE" -> "20261027")

    vtnet(env, noKey) shouldBe SilverApp.ExitOk
    vtnet(env, noKey) shouldBe SilverApp.ExitOk

    batches("vtnet_batch") shouldBe Map("20261027" -> 3L)
    decryptedRows("vtnet_batch") shouldBe expected
  }

  it should "gọi by-column với onMissing=request mặc định (lần đầu DAK mở YC) và none khi CDR_ON_MISSING=none" in {
    val in = dir("vtnet/on-missing")
    write(in.resolve("a.csv"), csv(csvRows))
    val byDefault = new StubColumnKeys()
    val noRequest = new StubColumnKeys()

    vtnet(vtnetEnv(in, "vtnet_on_missing"), noKey, keys = byDefault) shouldBe SilverApp.ExitOk
    vtnet(vtnetEnv(in, "vtnet_on_missing", "CDR_ON_MISSING" -> "none"), noKey, keys = noRequest) shouldBe SilverApp.ExitOk

    byDefault.intents.toArray.toSeq shouldBe Seq(KeyIntent.Encrypt) // onMissing=request
    noRequest.intents.toArray.toSeq shouldBe Seq(KeyIntent.Decrypt) // onMissing=none
  }

  it should "chờ DAK duyệt yêu cầu key cột (PENDING) theo backoff rồi ghi bảng khi key READY" in {
    val in = dir("vtnet/wait-approval")
    write(in.resolve("a.csv"), csv(csvRows))
    var calls = 0
    val approvedOnThirdCall = new ColumnKeySource {
      override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
        calls += 1
        if (calls < 3) {
          throw new ColumnKeysUnavailableException(datasetName,
            columns.map(c => ColumnKeyIssue(c.toLowerCase, "PENDING", Some("YC-2026-000042"), None)), Seq("req-1"))
        }
        new StubColumnKeys().readColumns(datasetName, columns, intent)
      }
    }
    val slept = new java.util.concurrent.CopyOnWriteArrayList[Long]()
    val waiting = new WaitingColumnKeySource(approvedOnThirdCall, PendingWait(3600, 30, 300, 2.0),
      millis => slept.add(millis / 1000))

    vtnet(vtnetEnv(in, "vtnet_wait_approval"), noKey, waiting) shouldBe SilverApp.ExitOk

    calls shouldBe 3
    slept.toArray.toSeq shouldBe Seq(30L, 60L)
    decryptedRows("vtnet_wait_approval") shouldBe expected
  }

  it should "file không mã hoá: cột chưa có key thì exit 5, không ghi gì" in {
    val in = dir("vtnet/plain-pending")
    write(in.resolve("a.csv"), csv(csvRows))

    vtnet(vtnetEnv(in, "vtnet_plain_pending"), noKey, keys = new StubColumnKeys(Map.empty)) shouldBe
      SilverApp.ExitColumnKeysPending
    spark.catalog.tableExists("silver", "vtnet_plain_pending") shouldBe false
  }

  it should "trả exit 2 khi thiếu cấu hình DAK và không có nguồn key dựng sẵn" in {
    SilverApp.execute(Source.Vtnet, vtnetEnv(dir("vtnet/no-dak"), "vtnet_no_dak"), Some(spark)) shouldBe
      SilverApp.ExitConfigError
  }
}
