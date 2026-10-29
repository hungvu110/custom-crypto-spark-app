package vai.lakehouse.silveringest.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IngestConfigSpec extends AnyFlatSpec with Matchers {

  private val common = Map(
    "INPUT_PATH" -> "hdfs://nn:8020/landing/vtt/2026-10-27",
    "INPUT_FORMAT" -> "csv",
    "OUTPUT_DB" -> "silver",
    "OUTPUT_TABLE" -> "vtt_cdr")

  private val vtnet = common ++ Map(
    "INPUT_PATH" -> "hdfs://nn:8020/landing/vtnet/2026-10-27",
    "OUTPUT_TABLE" -> "vtnet_cdr",
    "CDR_ENCRYPT_COLUMNS" -> "msisdn, imei",
    "CDR_KEY_FIELD" -> "start_time",
    "KEY_PATH_ROOT" -> "/landing/vtnet")

  private def errors(mode: Mode, env: Map[String, String]): String =
    IngestConfig.fromEnv(mode, env).left.toOption.getOrElse(fail("expected config errors")).mkString(" | ")

  "IngestConfig.fromEnv" should "dùng mặc định cho VTT: delta, overwrite, hive support, không có phần VTNET" in {
    val cfg = IngestConfig.fromEnv(Mode.Passthrough, common).toOption.get

    cfg.mode shouldBe Mode.Passthrough
    cfg.appName shouldBe "SilverIngest-passthrough"
    cfg.input.format shouldBe InputFormat.Csv
    cfg.input.schema shouldBe None
    cfg.target shouldBe "silver.vtt_cdr"
    cfg.outputFormat shouldBe OutputFormat.Delta
    cfg.outputMode shouldBe "overwrite"
    cfg.partitionBy shouldBe Nil
    cfg.enableHiveSupport shouldBe true
    cfg.decryptEncrypt shouldBe None
  }

  it should "nhận định dạng, schema, option và partition" in {
    val cfg = IngestConfig.fromEnv(Mode.Passthrough, common ++ Map("INPUT_FORMAT" -> "TSV", "INPUT_SCHEMA" -> "a STRING, b INT",
      "INPUT_OPTION_header" -> "true", "OUTPUT_MODE" -> "append", "OUTPUT_FORMAT" -> "hive",
      "OUTPUT_PARTITION_BY" -> "event_date", "ENABLE_HIVE_SUPPORT" -> "false",
      "WAREHOUSE_PATH" -> "hdfs://nn:8020/silver")).toOption.get

    cfg.input.format shouldBe InputFormat.Tsv
    cfg.input.schema shouldBe Some("a STRING, b INT")
    cfg.input.options shouldBe Map("header" -> "true")
    cfg.outputMode shouldBe "append"
    cfg.outputFormat shouldBe OutputFormat.Hive
    cfg.partitionBy shouldBe Seq("event_date")
    cfg.warehousePath shouldBe Some("hdfs://nn:8020/silver")
  }

  it should "gom mọi lỗi cấu hình một lượt" in {
    val all = errors(Mode.Passthrough, Map("INPUT_PATH" -> "relative/path", "INPUT_FORMAT" -> "xlsx", "OUTPUT_DB" -> "bad-db",
      "OUTPUT_MODE" -> "merge", "OUTPUT_FORMAT" -> "iceberg", "WAREHOUSE_PATH" -> "/no/scheme"))

    all should include("INPUT_PATH")
    all should include("INPUT_FORMAT='xlsx'")
    all should include("OUTPUT_DB")
    all should include("OUTPUT_TABLE: is required")
    all should include("OUTPUT_MODE='merge'")
    all should include("OUTPUT_FORMAT='iceberg'")
    all should include("WAREHOUSE_PATH")
  }

  it should "từ chối env của VTNET khi chạy VTT" in {
    errors(Mode.Passthrough, common ++ Map("CDR_ENCRYPT_COLUMNS" -> "a", "KEY_PATH" -> "/x")) should
      include("only valid for the decrypt-encrypt mode")
  }

  it should "dựng phần VTNET: cột cần mã hoá, keyField, đường dẫn key = phần của INPUT_PATH sau KEY_PATH_ROOT, cipher và onMissing mặc định" in {
    val v = IngestConfig.fromEnv(Mode.DecryptEncrypt, vtnet).toOption.get.decryptEncrypt.get

    v.encryptColumns shouldBe Seq("msisdn", "imei")
    v.keyField shouldBe Some("start_time")
    v.keyPath shouldBe "2026-10-27"
    v.onMissing shouldBe "request"
    v.cipher shouldBe "aes-256-gcm"
    v.stagingPath shouldBe None
  }

  it should "nhận KEY_PATH tương đối đặt thẳng (dạng chuẩn, giữ hoa thường), không cần KEY_PATH_ROOT" in {
    IngestConfig.fromEnv(Mode.DecryptEncrypt, vtnet - "KEY_PATH_ROOT" + ("KEY_PATH" -> "/VTNET_cdr//20261027/")).toOption.get
      .decryptEncrypt.get.keyPath shouldBe "VTNET_cdr/20261027"
  }

  it should "bắt buộc KEY_PATH_ROOT (hoặc KEY_PATH) khi file có mã hoá, và INPUT_PATH phải nằm dưới gốc" in {
    errors(Mode.DecryptEncrypt, vtnet - "KEY_PATH_ROOT") should include("KEY_PATH_ROOT: is required")
    errors(Mode.DecryptEncrypt, vtnet + ("KEY_PATH_ROOT" -> "/landing/vtt")) should include("must be strictly under KEY_PATH_ROOT")
    errors(Mode.DecryptEncrypt, vtnet + ("KEY_PATH_ROOT" -> "hdfs://nn:8020/landing/vtnet/2026-10-27")) should
      include("must be strictly under KEY_PATH_ROOT")
    errors(Mode.DecryptEncrypt, vtnet + ("KEY_PATH" -> "hdfs://nn:8020/landing/x")) should include("without a scheme")
    IngestConfig.fromEnv(Mode.DecryptEncrypt, vtnet + ("KEY_PATH_ROOT" -> "hdfs://nn:8020/landing/vtnet/")).toOption.get
      .decryptEncrypt.get.keyPath shouldBe "2026-10-27"
  }

  it should "nhận CDR_ON_MISSING=none và từ chối giá trị khác none/request" in {
    IngestConfig.fromEnv(Mode.DecryptEncrypt, vtnet + ("CDR_ON_MISSING" -> "NONE")).toOption.get.decryptEncrypt.get.onMissing shouldBe "none"
    errors(Mode.DecryptEncrypt, vtnet + ("CDR_ON_MISSING" -> "always")) should include("CDR_ON_MISSING='always'")
    errors(Mode.Passthrough, common + ("CDR_ON_MISSING" -> "none")) should include("only valid for the decrypt-encrypt mode")
  }

  it should "cho phép không có cột mã hoá (chỉ giải mã file), keyField chỉ bắt buộc khi có cột mã hoá" in {
    val withoutColumns = vtnet - "CDR_ENCRYPT_COLUMNS" - "CDR_KEY_FIELD"
    val settings = IngestConfig.fromEnv(Mode.DecryptEncrypt, withoutColumns).toOption.get.decryptEncrypt.get
    settings.encryptColumns shouldBe Nil
    settings.keyField shouldBe None
    IngestConfig.fromEnv(Mode.DecryptEncrypt, withoutColumns + ("CDR_ENCRYPT_COLUMNS" -> "")).toOption.get
      .decryptEncrypt.get.encryptColumns shouldBe Nil
    IngestConfig.fromEnv(Mode.DecryptEncrypt, vtnet - "CDR_ENCRYPT_COLUMNS").toOption.get
      .decryptEncrypt.get.keyField shouldBe Some("start_time")
    errors(Mode.DecryptEncrypt, vtnet - "CDR_KEY_FIELD") should include("CDR_KEY_FIELD: is required")
  }

  it should "kiểm quan hệ giữa cột mã hoá và keyField" in {
    errors(Mode.DecryptEncrypt, vtnet - "CDR_KEY_FIELD") should include("CDR_KEY_FIELD: is required")
    errors(Mode.DecryptEncrypt, vtnet + ("CDR_KEY_FIELD" -> "msisdn")) should include("must not be listed in CDR_ENCRYPT_COLUMNS")
    errors(Mode.DecryptEncrypt, vtnet + ("CDR_ENCRYPT_COLUMNS" -> "msisdn,MSISDN")) should include("duplicated")
    errors(Mode.DecryptEncrypt, vtnet + ("CDR_ENCRYPT_COLUMNS" -> "msisdn,bad-col")) should include("invalid column name")
    errors(Mode.DecryptEncrypt, vtnet + ("OUTPUT_PARTITION_BY" -> "imei")) should include("partition column 'imei'")
    errors(Mode.DecryptEncrypt, vtnet + ("FILE_CIPHER" -> "rot13")) should include("FILE_CIPHER='rot13'")
    errors(Mode.DecryptEncrypt, vtnet + ("KEY_PATH" -> "a/../b")) should include("KEY_PATH")
  }

  it should "bắt buộc thư mục tạm khi giải mã định dạng nhị phân, và thư mục tạm không được nằm trong INPUT_PATH" in {
    errors(Mode.DecryptEncrypt, vtnet + ("INPUT_FORMAT" -> "parquet")) should include("DECRYPT_STAGING_PATH: is required")
    IngestConfig.fromEnv(Mode.DecryptEncrypt, vtnet ++ Map("INPUT_FORMAT" -> "parquet",
      "DECRYPT_STAGING_PATH" -> "hdfs://nn:8020/tmp/silver-staging")).toOption.get.decryptEncrypt.get.stagingPath shouldBe
      Some("hdfs://nn:8020/tmp/silver-staging")
    errors(Mode.DecryptEncrypt, vtnet ++ Map("INPUT_FORMAT" -> "orc",
      "DECRYPT_STAGING_PATH" -> "hdfs://nn:8020/landing/vtnet/2026-10-27/tmp")) should include("must not be inside INPUT_PATH")
    errors(Mode.DecryptEncrypt, vtnet + ("DECRYPT_STAGING_PATH" -> "hdfs://nn:8020/tmp/x")) should
      include("only used with INPUT_FORMAT=parquet/orc/avro")
  }

  it should "không cho text multiLine khi giải mã trong bộ nhớ" in {
    errors(Mode.DecryptEncrypt, vtnet + ("INPUT_OPTION_multiLine" -> "true")) should include("multiLine")
  }

  it should "không còn FILE_CIPHER=none: file có mã hoá hay không do by-path quyết định lúc chạy" in {
    errors(Mode.DecryptEncrypt, vtnet + ("FILE_CIPHER" -> "none")) should include("FILE_CIPHER='none'")
  }

  it should "BATCH_VALUE: thêm cột lô (mặc định batch_date) làm partition đầu tiên, mặc định overwrite-partitions" in {
    val cfg = IngestConfig.fromEnv(Mode.Passthrough, common ++ Map("BATCH_VALUE" -> "20261027",
      "OUTPUT_PARTITION_BY" -> "hour")).toOption.get

    cfg.batch shouldBe Some(Batch("batch_date", "20261027"))
    cfg.partitionBy shouldBe Seq("batch_date", "hour")
    cfg.outputMode shouldBe "overwrite-partitions"
    IngestConfig.fromEnv(Mode.Passthrough, common ++ Map("BATCH_VALUE" -> "20261027", "BATCH_COLUMN" -> "ingest_day"))
      .toOption.get.partitionBy shouldBe Seq("ingest_day")
    IngestConfig.fromEnv(Mode.Passthrough, common ++ Map("BATCH_VALUE" -> "20261027", "OUTPUT_MODE" -> "append"))
      .toOption.get.outputMode shouldBe "append"
  }

  it should "từ chối cấu hình lô sai" in {
    errors(Mode.Passthrough, common + ("OUTPUT_MODE" -> "overwrite-partitions")) should include("requires BATCH_VALUE")
    errors(Mode.Passthrough, common + ("BATCH_COLUMN" -> "batch_date")) should include("BATCH_COLUMN: only used together with BATCH_VALUE")
    errors(Mode.Passthrough, common + ("BATCH_VALUE" -> "2026 10 27")) should include("BATCH_VALUE")
    errors(Mode.Passthrough, common + ("BATCH_VALUE" -> "a'b")) should include("BATCH_VALUE")
    errors(Mode.Passthrough, common ++ Map("BATCH_VALUE" -> "1", "BATCH_COLUMN" -> "bad-col")) should include("BATCH_COLUMN")
    errors(Mode.Passthrough, common ++ Map("BATCH_VALUE" -> "1", "OUTPUT_PARTITION_BY" -> "BATCH_DATE")) should include("duplicated")
    errors(Mode.DecryptEncrypt, vtnet ++ Map("BATCH_VALUE" -> "1", "BATCH_COLUMN" -> "msisdn")) should
      include("must not be listed in CDR_ENCRYPT_COLUMNS")
    errors(Mode.DecryptEncrypt, vtnet ++ Map("BATCH_VALUE" -> "1", "BATCH_COLUMN" -> "start_time")) should
      include("must not be CDR_KEY_FIELD")
  }

  it should "dựng phần giờ: cột hour mặc định, regex một nhóm bắt, các giờ cần thay" in {
    val env = common ++ Map("BATCH_VALUE" -> "20261013", "BATCH_COLUMN" -> "dt", "HOUR_PATH_REGEX" -> "/hour=(\\d{2})(?:/|$)")

    IngestConfig.fromEnv(Mode.Passthrough, env).toOption.get.hours shouldBe Some(Hours("hour", "/hour=(\\d{2})(?:/|$)", Nil))
    val some = IngestConfig.fromEnv(Mode.Passthrough, env ++ Map("HOUR_COLUMN" -> "h", "BATCH_HOURS" -> "10, 11,12")).toOption.get
    some.hours shouldBe Some(Hours("h", "/hour=(\\d{2})(?:/|$)", Seq("10", "11", "12")))
    // The hour column is a partition only when it is listed in OUTPUT_PARTITION_BY (after the batch column).
    IngestConfig.fromEnv(Mode.Passthrough, env ++ Map("OUTPUT_PARTITION_BY" -> "hour")).toOption.get.partitionBy shouldBe Seq("dt", "hour")
    IngestConfig.fromEnv(Mode.Passthrough, env).toOption.get.partitionBy shouldBe Seq("dt")
    IngestConfig.fromEnv(Mode.Passthrough, common).toOption.get.hours shouldBe None
    IngestConfig.fromEnv(Mode.Passthrough, env ++ Map("BATCH_HOURS" -> "")).toOption.get.hours.get.replace shouldBe Nil
  }

  it should "từ chối cấu hình giờ sai mà không lặp lại regex" in {
    val env = common ++ Map("BATCH_VALUE" -> "20261013", "HOUR_PATH_REGEX" -> "/hour=(\\d{2})/")

    errors(Mode.Passthrough, env ++ Map("HOUR_PATH_REGEX" -> "/hour=(\\d{2}")) should include("not a valid regular expression")
    errors(Mode.Passthrough, env ++ Map("HOUR_PATH_REGEX" -> "/hour=\\d{2}/")) should include("exactly one capture group")
    errors(Mode.Passthrough, env ++ Map("HOUR_PATH_REGEX" -> "/(a)(b)/")) should include("exactly one capture group")
    errors(Mode.Passthrough, env ++ Map("HOUR_COLUMN" -> "bad-name")) should include("invalid column name")
    errors(Mode.Passthrough, env ++ Map("BATCH_COLUMN" -> "hour")) should include("must differ from BATCH_COLUMN")
    errors(Mode.Passthrough, env ++ Map("BATCH_HOURS" -> "9")) should include("invalid hour")
    errors(Mode.Passthrough, env ++ Map("BATCH_HOURS" -> "24")) should include("invalid hour")
    errors(Mode.Passthrough, env ++ Map("BATCH_HOURS" -> "10,10")) should include("duplicated hour")
    errors(Mode.Passthrough, common ++ Map("HOUR_COLUMN" -> "hour")) should include("only used together with HOUR_PATH_REGEX")
    errors(Mode.Passthrough, common ++ Map("BATCH_HOURS" -> "10")) should include("only used together with HOUR_PATH_REGEX")
    errors(Mode.Passthrough, common ++ Map("HOUR_PATH_REGEX" -> "/hour=(\\d{2})/", "BATCH_HOURS" -> "10")) should
      include("only used together with BATCH_VALUE")
    errors(Mode.Passthrough, env ++ Map("HOUR_PATH_REGEX" -> "secret(")) should not include "secret"
  }

  "describe" should "không chứa giá trị nhạy cảm và nêu rõ nguồn" in {
    val text = IngestConfig.fromEnv(Mode.DecryptEncrypt, vtnet).toOption.get.describe.map { case (k, v) => s"$k=$v" }.mkString(";")

    text should include("MODE=decrypt-encrypt")
    text should include("CDR_ENCRYPT_COLUMNS=msisdn, imei")
  }
}
