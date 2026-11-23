package vai.lakehouse.silveringest.app

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.columncrypto.cdr.CdrCrypto
import vai.lakehouse.keyprefix.{ColumnKeySource, KeyIntent, PathKey, PathKeyResult, PathKeySource}
import vai.lakehouse.silveringest.config.Mode

/**
 * Luồng VTT của silver-ingest: giải mã từng cột VTT đã mã hoá (prefix lấy qua `by-path`, keyField do VTT quy định), gom nhóm tuổi/giờ,
 * mã hoá lại bằng MỘT key global với keyField hằng số (docs/silver-ingest/TRANSFORM-PHIEU-KHAI-THAC-VTT.md mục 3).
 * Prefix trong test là chuỗi tự đặt, không phải prefix thật.
 */
class VttColumnPipelineSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val workDir = Files.createTempDirectory("vtt-column-pipeline")
  private var spark: SparkSession = _

  private val providerPrefix = "TEST-VTT-PREFIX-"
  private val globalPrefix = "TEST-GLOBAL-PREFIX-"
  private val constantKeyField = "vtt"

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().appName("VttColumnPipelineSpec").master("local[2]")
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

  private class StubPathKeys(result: String => PathKeyResult) extends PathKeySource {
    val calls = new AtomicInteger()
    val asked = new CopyOnWriteArrayList[String]()
    override def readPath(path: String): PathKeyResult = { calls.incrementAndGet(); asked.add(path); result(path) }
  }

  /** Key global: MỌI cột cùng một key (như `GlobalColumnKeySource`). */
  private class GlobalKeys extends ColumnKeySource {
    val calls = new AtomicInteger()
    override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
      calls.incrementAndGet()
      columns.map(_.toLowerCase -> globalPrefix).toMap
    }
  }

  private def providerKey(path: String): PathKeyResult = PathKeyResult.Available(PathKey(path, providerPrefix, Some(1L)))

  private val noKey = new StubPathKeys(_ => PathKeyResult.NotAvailable)

  /** Bản mã VTT: `prefix ‖ keyField` đúng công thức của đối tác (cdr-crypto). */
  private def vttEncrypt(plain: String, keyFieldValue: String): String =
    CdrCrypto.transform(providerPrefix, encryptMode = true)(plain, keyFieldValue)

  /** Bản mã global mong đợi: key global và cột hằng số làm keyField. */
  private def globalEncrypt(plain: String): String =
    CdrCrypto.transform(globalPrefix, encryptMode = true)(plain, constantKeyField)

  private def dir(name: String): Path = Files.createDirectories(workDir.resolve(name))

  private def csv(dirName: String, header: String, rows: Seq[String]): Path = {
    val d = dir(dirName)
    Files.write(d.resolve("part-1.csv"), (header +: rows).mkString("", "\n", "\n").getBytes(UTF_8))
    d
  }

  private def baseEnv(input: Path, table: String, schema: String, extra: (String, String)*): Map[String, String] =
    Map("INPUT_PATH" -> input.toString, "INPUT_FORMAT" -> "csv", "INPUT_OPTION_header" -> "true", "INPUT_SCHEMA" -> schema,
      "OUTPUT_DB" -> "silver", "OUTPUT_TABLE" -> table, "ENABLE_HIVE_SUPPORT" -> "false", "BATCH_COLUMN" -> "dt",
      "BATCH_VALUE" -> "20261104", "GLOBAL_KEY_ZONE" -> "vtt", "CDR_KEY_FIELD" -> "key_scope",
      "CDR_KEY_FIELD_VALUE" -> constantKeyField) ++ extra

  private def run(env: Map[String, String], pathKeys: PathKeySource, keys: ColumnKeySource = new GlobalKeys): Int =
    SilverApp.execute(Mode.DecryptEncrypt, env, Some(spark), Some(KeySources(pathKeys, keys)))

  private def column(table: String, name: String, orderBy: String): Seq[String] =
    spark.table(s"silver.$table").orderBy(orderBy).select(name).collect().map(_.getString(0)).toSeq

  private val dash = "–"

  // ---------------------------------------------------------------- giải mã cột VTT rồi mã hoá global

  "Mode column" should "giải mã bằng prefix by-path + keyField của VTT rồi mã hoá bằng key global + cột hằng số" in {
    val in = csv("a", "isdn,sta_date,amount", Seq(
      s"${vttEncrypt("84900000001", "20261104")},20261104,10",
      s"${vttEncrypt("84900000002", "20261105")},20261105,20"))
    val pathKeys = new StubPathKeys(providerKey)

    val env = baseEnv(in, "f014_a", "isdn STRING, sta_date STRING, amount INT", "DECRYPT_MODE" -> "column",
      "KEY_PATH" -> "f014-mpre-tot-charge-d", "PROVIDER_DECRYPT_COLUMNS" -> "isdn", "PROVIDER_KEY_FIELD" -> "sta_date",
      "CDR_ENCRYPT_COLUMNS" -> "isdn")

    run(env, pathKeys) shouldBe SilverApp.ExitOk

    column("f014_a", "isdn", "sta_date") shouldBe Seq(globalEncrypt("84900000001"), globalEncrypt("84900000002"))
    column("f014_a", "sta_date", "sta_date") shouldBe Seq("20261104", "20261105") // keyField của VTT giữ nguyên
    column("f014_a", "key_scope", "sta_date") shouldBe Seq("vtt", "vtt") // cột hằng số do job thêm
    spark.table("silver.f014_a").orderBy("sta_date").select("amount").collect().map(_.getInt(0)).toSeq shouldBe Seq(10, 20)
    pathKeys.calls.get() shouldBe 1
    pathKeys.asked.toArray.toSeq shouldBe Seq("f014-mpre-tot-charge-d")
  }

  it should "cho cùng một số thuê bao ra cùng một bản mã ở hai bảng có keyField VTT khác nhau (điều kiện để nối được)" in {
    // f014: isdn, keyField sta_date. f002: calling_number, keyField sta_datetime (đổi theo từng dòng).
    val f014 = csv("b-f014", "isdn,sta_date", Seq(s"${vttEncrypt("84900000001", "20261104")},20261104"))
    val f002 = csv("b-f002", "calling_number,sta_datetime", Seq(
      s"${vttEncrypt("84900000001", "20261104103000")},20261104103000",
      s"${vttEncrypt("84900000001", "20261104110000")},20261104110000"))

    run(baseEnv(f014, "join_f014", "isdn STRING, sta_date STRING", "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f014",
      "PROVIDER_DECRYPT_COLUMNS" -> "isdn", "PROVIDER_KEY_FIELD" -> "sta_date", "CDR_ENCRYPT_COLUMNS" -> "isdn"),
      new StubPathKeys(providerKey)) shouldBe SilverApp.ExitOk
    run(baseEnv(f002, "join_f002", "calling_number STRING, sta_datetime STRING", "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f002",
      "PROVIDER_DECRYPT_COLUMNS" -> "calling_number", "PROVIDER_KEY_FIELD" -> "sta_datetime", "CDR_ENCRYPT_COLUMNS" -> "calling_number"),
      new StubPathKeys(providerKey)) shouldBe SilverApp.ExitOk

    val fromF014 = column("join_f014", "isdn", "sta_date").distinct
    val fromF002 = column("join_f002", "calling_number", "sta_datetime").distinct
    fromF014 shouldBe Seq(globalEncrypt("84900000001"))
    fromF002 shouldBe fromF014 // hai dòng có keyField khác nhau vẫn ra cùng một bản mã
    spark.sql("SELECT count(*) FROM silver.join_f014 a JOIN silver.join_f002 b ON a.isdn = b.calling_number")
      .head().getLong(0) shouldBe 2L
  }

  it should "mã hoá nhiều cột cùng lúc: cột đã giải mã và cột số rõ (customer_id) đều dùng một key global" in {
    val in = csv("c", "isdn,customer_id,sta_date", Seq(s"${vttEncrypt("84900000001", "20261104")},1234567,20261104"))

    run(baseEnv(in, "f014_c", "isdn STRING, customer_id STRING, sta_date STRING", "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f014",
      "PROVIDER_DECRYPT_COLUMNS" -> "isdn", "PROVIDER_KEY_FIELD" -> "sta_date", "CDR_ENCRYPT_COLUMNS" -> "isdn,customer_id"),
      new StubPathKeys(providerKey)) shouldBe SilverApp.ExitOk

    val row = spark.table("silver.f014_c").head()
    row.getAs[String]("isdn") shouldBe globalEncrypt("84900000001")
    row.getAs[String]("customer_id") shouldBe globalEncrypt("1234567")
  }

  it should "giải mã bằng keyField VTT là customer_id TRƯỚC khi mã hoá customer_id (f015, f016)" in {
    val in = csv("d", "isdn,customer_id", Seq(s"${vttEncrypt("84900000001", "1234567")},1234567"))

    run(baseEnv(in, "f016_d", "isdn STRING, customer_id STRING", "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f016",
      "PROVIDER_DECRYPT_COLUMNS" -> "isdn", "PROVIDER_KEY_FIELD" -> "customer_id", "CDR_ENCRYPT_COLUMNS" -> "isdn,customer_id"),
      new StubPathKeys(providerKey)) shouldBe SilverApp.ExitOk

    val row = spark.table("silver.f016_d").head()
    row.getAs[String]("isdn") shouldBe globalEncrypt("84900000001")
    row.getAs[String]("customer_id") shouldBe globalEncrypt("1234567")
  }

  it should "đọc parquet trực tiếp, không cần DECRYPT_STAGING_PATH" in {
    val session = spark
    import session.implicits._
    val in = workDir.resolve("e-parquet")
    Seq((vttEncrypt("84900000001", "20261104"), "20261104")).toDF("isdn", "sta_date").coalesce(1).write.parquet(in.toString)

    val env = Map("INPUT_PATH" -> in.toString, "INPUT_FORMAT" -> "parquet", "OUTPUT_DB" -> "silver", "OUTPUT_TABLE" -> "f014_e",
      "ENABLE_HIVE_SUPPORT" -> "false", "BATCH_COLUMN" -> "dt", "BATCH_VALUE" -> "20261104", "GLOBAL_KEY_ZONE" -> "vtt",
      "CDR_KEY_FIELD" -> "key_scope", "CDR_KEY_FIELD_VALUE" -> constantKeyField, "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f014",
      "PROVIDER_DECRYPT_COLUMNS" -> "isdn", "PROVIDER_KEY_FIELD" -> "sta_date", "CDR_ENCRYPT_COLUMNS" -> "isdn")

    run(env, new StubPathKeys(providerKey)) shouldBe SilverApp.ExitOk
    column("f014_e", "isdn", "sta_date") shouldBe Seq(globalEncrypt("84900000001"))
  }

  it should "trả exit 1 và không ghi gì khi by-path chưa có key (không được mã hoá chồng lên bản mã của VTT)" in {
    val in = csv("f", "isdn,sta_date", Seq(s"${vttEncrypt("84900000001", "20261104")},20261104"))
    val pathKeys = new StubPathKeys(_ => PathKeyResult.NotAvailable)

    run(baseEnv(in, "f014_f", "isdn STRING, sta_date STRING", "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f014",
      "PROVIDER_DECRYPT_COLUMNS" -> "isdn", "PROVIDER_KEY_FIELD" -> "sta_date", "CDR_ENCRYPT_COLUMNS" -> "isdn"), pathKeys) shouldBe
      SilverApp.ExitRuntimeError

    spark.catalog.tableExists("silver", "f014_f") shouldBe false
  }

  it should "trả exit 1 và không ghi gì khi cột giải mã hoặc keyField VTT không có trong dữ liệu" in {
    val in = csv("g", "isdn,sta_date", Seq(s"${vttEncrypt("84900000001", "20261104")},20261104"))

    run(baseEnv(in, "f014_g1", "isdn STRING, sta_date STRING", "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f014",
      "PROVIDER_DECRYPT_COLUMNS" -> "isdn", "PROVIDER_KEY_FIELD" -> "sta_datetime", "CDR_ENCRYPT_COLUMNS" -> "isdn"),
      new StubPathKeys(providerKey)) shouldBe SilverApp.ExitRuntimeError
    run(baseEnv(in, "f014_g2", "isdn STRING, sta_date STRING", "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f014",
      "PROVIDER_DECRYPT_COLUMNS" -> "msisdn", "PROVIDER_KEY_FIELD" -> "sta_date", "CDR_ENCRYPT_COLUMNS" -> "isdn"),
      new StubPathKeys(providerKey)) shouldBe SilverApp.ExitRuntimeError

    spark.catalog.tableExists("silver", "f014_g1") shouldBe false
    spark.catalog.tableExists("silver", "f014_g2") shouldBe false
  }

  it should "trả exit 1 và không ghi gì khi keyField của VTT là null ở dòng có cột cần giải mã (không để định danh thành NULL âm thầm)" in {
    // An empty CSV field is NULL: without a key field the provider ciphertext cannot be decrypted, and cdr-crypto would turn it into NULL.
    val in = csv("n1", "isdn,sta_date", Seq(s"${vttEncrypt("84900000001", "20261104")},20261104", s"${vttEncrypt("84900000002", "20261104")},"))

    run(baseEnv(in, "f014_n1", "isdn STRING, sta_date STRING", "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f014",
      "PROVIDER_DECRYPT_COLUMNS" -> "isdn", "PROVIDER_KEY_FIELD" -> "sta_date", "CDR_ENCRYPT_COLUMNS" -> "isdn"),
      new StubPathKeys(providerKey)) shouldBe SilverApp.ExitRuntimeError

    spark.catalog.tableExists("silver", "f014_n1") shouldBe false
  }

  it should "giữ NULL khi cả cột cần giải mã và keyField đều null (không có gì để giải mã)" in {
    val in = csv("n2", "isdn,sta_date,amount", Seq(s"${vttEncrypt("84900000001", "20261104")},20261104,1", ",,2"))

    run(baseEnv(in, "f014_n2", "isdn STRING, sta_date STRING, amount INT", "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f014",
      "PROVIDER_DECRYPT_COLUMNS" -> "isdn", "PROVIDER_KEY_FIELD" -> "sta_date", "CDR_ENCRYPT_COLUMNS" -> "isdn"),
      new StubPathKeys(providerKey)) shouldBe SilverApp.ExitOk

    val rows = spark.table("silver.f014_n2").orderBy("amount").collect()
    rows.map(_.getAs[String]("isdn")).toSeq shouldBe Seq(globalEncrypt("84900000001"), null)
  }

  it should "trả exit 1 và không ghi gì khi dữ liệu đã có cột hằng số trùng tên" in {
    val in = csv("h", "isdn,sta_date,key_scope", Seq(s"${vttEncrypt("84900000001", "20261104")},20261104,x"))

    run(baseEnv(in, "f014_h", "isdn STRING, sta_date STRING, key_scope STRING", "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f014",
      "PROVIDER_DECRYPT_COLUMNS" -> "isdn", "PROVIDER_KEY_FIELD" -> "sta_date", "CDR_ENCRYPT_COLUMNS" -> "isdn"),
      new StubPathKeys(providerKey)) shouldBe SilverApp.ExitRuntimeError

    spark.catalog.tableExists("silver", "f014_h") shouldBe false
  }

  // ---------------------------------------------------------------- cột VTT không mã hoá (f001.calling_number)

  "Mode none" should "chỉ mã hoá, không gọi by-path, và ra đúng bản mã như bảng đã giải mã (f001 so với f002, f003)" in {
    val in = csv("i", "calling_number,sta_datetime", Seq("84900000001,20261104103000", "84900000002,20261104110000"))
    val pathKeys = new StubPathKeys(providerKey)

    run(baseEnv(in, "f001_i", "calling_number STRING, sta_datetime STRING", "DECRYPT_MODE" -> "none",
      "CDR_ENCRYPT_COLUMNS" -> "calling_number"), pathKeys) shouldBe SilverApp.ExitOk

    pathKeys.calls.get() shouldBe 0
    column("f001_i", "calling_number", "sta_datetime") shouldBe Seq(globalEncrypt("84900000001"), globalEncrypt("84900000002"))
    column("f001_i", "sta_datetime", "sta_datetime") shouldBe Seq("20261104103000", "20261104110000")
  }

  it should "không mã hoá gì khi không có cột nào cần mã hoá, vẫn gom nhóm theo COLUMN_TRANSFORMS" in {
    val in = csv("j", "cust,customer_age", Seq("a,30", "b,70"))

    val env = Map("INPUT_PATH" -> in.toString, "INPUT_FORMAT" -> "csv", "INPUT_OPTION_header" -> "true",
      "INPUT_SCHEMA" -> "cust STRING, customer_age STRING", "OUTPUT_DB" -> "silver", "OUTPUT_TABLE" -> "f_customer_j",
      "ENABLE_HIVE_SUPPORT" -> "false", "DECRYPT_MODE" -> "none", "COLUMN_TRANSFORMS" -> "customer_age:AGE_BAND")
    val keys = new GlobalKeys

    run(env, new StubPathKeys(providerKey), keys) shouldBe SilverApp.ExitOk

    column("f_customer_j", "customer_age", "cust") shouldBe Seq(s"25${dash}34", "65+")
    keys.calls.get() shouldBe 0
  }

  // ---------------------------------------------------------------- gom nhóm

  "COLUMN_TRANSFORMS" should "ghi nhãn tiếng Anh, giữ cột giờ partition lấy từ hour_id gốc, và các cột khác nguyên giá trị" in {
    val in = csv("k", "isdn,hour_id,customer_age,amount", Seq(
      "84900000001,5,30,1", "84900000002,05,17,2", "84900000003,17,70,3", "84900000004,x,abc,4"))

    run(baseEnv(in, "f_k", "isdn STRING, hour_id STRING, customer_age STRING, amount INT", "DECRYPT_MODE" -> "none",
      "CDR_ENCRYPT_COLUMNS" -> "isdn", "HOUR_FROM_COLUMN" -> "hour_id", "OUTPUT_PARTITION_BY" -> "hour",
      "COLUMN_TRANSFORMS" -> "hour_id:HOUR_BAND,customer_age:AGE_BAND"), noKey) shouldBe SilverApp.ExitOk

    val rows = spark.table("silver.f_k").orderBy("amount").collect()
    rows.map(_.getAs[String]("hour_id")).toSeq shouldBe Seq(s"0${dash}5h", s"0${dash}5h", s"17${dash}19h", "Unknown")
    rows.map(_.getAs[String]("customer_age")).toSeq shouldBe Seq(s"25${dash}34", "Under 18", "65+", "Unknown")
    rows.map(_.getAs[String]("hour")).toSeq shouldBe Seq("05", "05", "17", "NA") // giờ chính xác lấy trước khi gom nhóm
    rows.map(_.getAs[Int]("amount")).toSeq shouldBe Seq(1, 2, 3, 4)
  }

  it should "chạy SAU khi giải mã: keyField VTT là chính cột được gom nhóm vẫn giải mã đúng" in {
    val in = csv("l", "isdn,hour_id", Seq(s"${vttEncrypt("84900000001", "5")},5", s"${vttEncrypt("84900000002", "17")},17"))

    run(baseEnv(in, "f_l", "isdn STRING, hour_id STRING", "DECRYPT_MODE" -> "column", "KEY_PATH" -> "f003",
      "PROVIDER_DECRYPT_COLUMNS" -> "isdn", "PROVIDER_KEY_FIELD" -> "hour_id", "CDR_ENCRYPT_COLUMNS" -> "isdn",
      "COLUMN_TRANSFORMS" -> "hour_id:HOUR_BAND"), new StubPathKeys(providerKey)) shouldBe SilverApp.ExitOk

    val rows = spark.table("silver.f_l").orderBy("hour_id").collect()
    rows.map(_.getAs[String]("isdn")).toSet shouldBe Set(globalEncrypt("84900000001"), globalEncrypt("84900000002"))
    rows.map(_.getAs[String]("hour_id")).toSet shouldBe Set(s"0${dash}5h", s"17${dash}19h")
  }

  it should "trả exit 1 và không ghi gì khi cột cần gom nhóm không có trong dữ liệu" in {
    val in = csv("m", "isdn,amount", Seq("84900000001,1"))

    run(baseEnv(in, "f_m", "isdn STRING, amount INT", "DECRYPT_MODE" -> "none", "CDR_ENCRYPT_COLUMNS" -> "isdn",
      "COLUMN_TRANSFORMS" -> "customer_age:AGE_BAND"), noKey) shouldBe SilverApp.ExitRuntimeError

    spark.catalog.tableExists("silver", "f_m") shouldBe false
  }
}
