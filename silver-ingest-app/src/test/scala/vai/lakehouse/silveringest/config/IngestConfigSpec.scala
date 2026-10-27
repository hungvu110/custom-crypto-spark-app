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

  private def errors(source: Source, env: Map[String, String]): String =
    IngestConfig.fromEnv(source, env).left.toOption.getOrElse(fail("expected config errors")).mkString(" | ")

  "IngestConfig.fromEnv" should "dùng mặc định cho VTT: delta, overwrite, hive support, không có phần VTNET" in {
    val cfg = IngestConfig.fromEnv(Source.Vtt, common).toOption.get

    cfg.source shouldBe Source.Vtt
    cfg.appName shouldBe "SilverIngest-vtt"
    cfg.input.format shouldBe InputFormat.Csv
    cfg.input.schema shouldBe None
    cfg.target shouldBe "silver.vtt_cdr"
    cfg.outputFormat shouldBe OutputFormat.Delta
    cfg.outputMode shouldBe "overwrite"
    cfg.partitionBy shouldBe Nil
    cfg.enableHiveSupport shouldBe true
    cfg.vtnet shouldBe None
  }

  it should "nhận định dạng, schema, option và partition" in {
    val cfg = IngestConfig.fromEnv(Source.Vtt, common ++ Map("INPUT_FORMAT" -> "TSV", "INPUT_SCHEMA" -> "a STRING, b INT",
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
    val all = errors(Source.Vtt, Map("INPUT_PATH" -> "relative/path", "INPUT_FORMAT" -> "xlsx", "OUTPUT_DB" -> "bad-db",
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
    errors(Source.Vtt, common ++ Map("CDR_ENCRYPT_COLUMNS" -> "a", "KEY_PATH" -> "/x")) should
      include("only valid for VTNET")
  }

  it should "dựng phần VTNET: cột cần mã hoá, keyField, đường dẫn key = phần của INPUT_PATH sau KEY_PATH_ROOT, cipher và onMissing mặc định" in {
    val v = IngestConfig.fromEnv(Source.Vtnet, vtnet).toOption.get.vtnet.get

    v.encryptColumns shouldBe Seq("msisdn", "imei")
    v.keyField shouldBe "start_time"
    v.keyPath shouldBe "2026-10-27"
    v.onMissing shouldBe "request"
    v.cipher shouldBe "aes-256-gcm"
    v.stagingPath shouldBe None
  }

  it should "nhận KEY_PATH tương đối đặt thẳng (dạng chuẩn, giữ hoa thường), không cần KEY_PATH_ROOT" in {
    IngestConfig.fromEnv(Source.Vtnet, vtnet - "KEY_PATH_ROOT" + ("KEY_PATH" -> "/VTNET_cdr//20261027/")).toOption.get
      .vtnet.get.keyPath shouldBe "VTNET_cdr/20261027"
  }

  it should "bắt buộc KEY_PATH_ROOT (hoặc KEY_PATH) khi file có mã hoá, và INPUT_PATH phải nằm dưới gốc" in {
    errors(Source.Vtnet, vtnet - "KEY_PATH_ROOT") should include("KEY_PATH_ROOT: is required")
    errors(Source.Vtnet, vtnet + ("KEY_PATH_ROOT" -> "/landing/vtt")) should include("must be strictly under KEY_PATH_ROOT")
    errors(Source.Vtnet, vtnet + ("KEY_PATH_ROOT" -> "hdfs://nn:8020/landing/vtnet/2026-10-27")) should
      include("must be strictly under KEY_PATH_ROOT")
    errors(Source.Vtnet, vtnet + ("KEY_PATH" -> "hdfs://nn:8020/landing/x")) should include("without a scheme")
    IngestConfig.fromEnv(Source.Vtnet, vtnet + ("KEY_PATH_ROOT" -> "hdfs://nn:8020/landing/vtnet/")).toOption.get
      .vtnet.get.keyPath shouldBe "2026-10-27"
  }

  it should "nhận CDR_ON_MISSING=none và từ chối giá trị khác none/request" in {
    IngestConfig.fromEnv(Source.Vtnet, vtnet + ("CDR_ON_MISSING" -> "NONE")).toOption.get.vtnet.get.onMissing shouldBe "none"
    errors(Source.Vtnet, vtnet + ("CDR_ON_MISSING" -> "always")) should include("CDR_ON_MISSING='always'")
    errors(Source.Vtt, common + ("CDR_ON_MISSING" -> "none")) should include("only valid for VTNET")
  }

  it should "bắt buộc cột cần mã hoá và keyField với VTNET, và kiểm quan hệ giữa chúng" in {
    errors(Source.Vtnet, vtnet - "CDR_ENCRYPT_COLUMNS") should include("CDR_ENCRYPT_COLUMNS: is required")
    errors(Source.Vtnet, vtnet - "CDR_KEY_FIELD") should include("CDR_KEY_FIELD: is required")
    errors(Source.Vtnet, vtnet + ("CDR_KEY_FIELD" -> "msisdn")) should include("must not be listed in CDR_ENCRYPT_COLUMNS")
    errors(Source.Vtnet, vtnet + ("CDR_ENCRYPT_COLUMNS" -> "msisdn,MSISDN")) should include("duplicated")
    errors(Source.Vtnet, vtnet + ("CDR_ENCRYPT_COLUMNS" -> "msisdn,bad-col")) should include("invalid column name")
    errors(Source.Vtnet, vtnet + ("OUTPUT_PARTITION_BY" -> "imei")) should include("partition column 'imei'")
    errors(Source.Vtnet, vtnet + ("FILE_CIPHER" -> "rot13")) should include("FILE_CIPHER='rot13'")
    errors(Source.Vtnet, vtnet + ("KEY_PATH" -> "a/../b")) should include("KEY_PATH")
  }

  it should "bắt buộc thư mục tạm khi giải mã định dạng nhị phân, và thư mục tạm không được nằm trong INPUT_PATH" in {
    errors(Source.Vtnet, vtnet + ("INPUT_FORMAT" -> "parquet")) should include("DECRYPT_STAGING_PATH: is required")
    IngestConfig.fromEnv(Source.Vtnet, vtnet ++ Map("INPUT_FORMAT" -> "parquet",
      "DECRYPT_STAGING_PATH" -> "hdfs://nn:8020/tmp/silver-staging")).toOption.get.vtnet.get.stagingPath shouldBe
      Some("hdfs://nn:8020/tmp/silver-staging")
    errors(Source.Vtnet, vtnet ++ Map("INPUT_FORMAT" -> "orc",
      "DECRYPT_STAGING_PATH" -> "hdfs://nn:8020/landing/vtnet/2026-10-27/tmp")) should include("must not be inside INPUT_PATH")
    errors(Source.Vtnet, vtnet + ("DECRYPT_STAGING_PATH" -> "hdfs://nn:8020/tmp/x")) should
      include("only used with INPUT_FORMAT=parquet/orc/avro")
  }

  it should "không cho text multiLine khi giải mã trong bộ nhớ" in {
    errors(Source.Vtnet, vtnet + ("INPUT_OPTION_multiLine" -> "true")) should include("multiLine")
  }

  it should "không còn FILE_CIPHER=none: file có mã hoá hay không do by-path quyết định lúc chạy" in {
    errors(Source.Vtnet, vtnet + ("FILE_CIPHER" -> "none")) should include("FILE_CIPHER='none'")
  }

  it should "BATCH_VALUE: thêm cột lô (mặc định batch_date) làm partition đầu tiên, mặc định overwrite-partitions" in {
    val cfg = IngestConfig.fromEnv(Source.Vtt, common ++ Map("BATCH_VALUE" -> "20261027",
      "OUTPUT_PARTITION_BY" -> "hour")).toOption.get

    cfg.batch shouldBe Some(Batch("batch_date", "20261027"))
    cfg.partitionBy shouldBe Seq("batch_date", "hour")
    cfg.outputMode shouldBe "overwrite-partitions"
    IngestConfig.fromEnv(Source.Vtt, common ++ Map("BATCH_VALUE" -> "20261027", "BATCH_COLUMN" -> "ingest_day"))
      .toOption.get.partitionBy shouldBe Seq("ingest_day")
    IngestConfig.fromEnv(Source.Vtt, common ++ Map("BATCH_VALUE" -> "20261027", "OUTPUT_MODE" -> "append"))
      .toOption.get.outputMode shouldBe "append"
  }

  it should "từ chối cấu hình lô sai" in {
    errors(Source.Vtt, common + ("OUTPUT_MODE" -> "overwrite-partitions")) should include("requires BATCH_VALUE")
    errors(Source.Vtt, common + ("BATCH_COLUMN" -> "batch_date")) should include("BATCH_COLUMN: only used together with BATCH_VALUE")
    errors(Source.Vtt, common + ("BATCH_VALUE" -> "2026 10 27")) should include("BATCH_VALUE")
    errors(Source.Vtt, common + ("BATCH_VALUE" -> "a'b")) should include("BATCH_VALUE")
    errors(Source.Vtt, common ++ Map("BATCH_VALUE" -> "1", "BATCH_COLUMN" -> "bad-col")) should include("BATCH_COLUMN")
    errors(Source.Vtt, common ++ Map("BATCH_VALUE" -> "1", "OUTPUT_PARTITION_BY" -> "BATCH_DATE")) should include("duplicated")
    errors(Source.Vtnet, vtnet ++ Map("BATCH_VALUE" -> "1", "BATCH_COLUMN" -> "msisdn")) should
      include("must not be listed in CDR_ENCRYPT_COLUMNS")
    errors(Source.Vtnet, vtnet ++ Map("BATCH_VALUE" -> "1", "BATCH_COLUMN" -> "start_time")) should
      include("must not be CDR_KEY_FIELD")
  }

  "describe" should "không chứa giá trị nhạy cảm và nêu rõ nguồn" in {
    val text = IngestConfig.fromEnv(Source.Vtnet, vtnet).toOption.get.describe.map { case (k, v) => s"$k=$v" }.mkString(";")

    text should include("SOURCE=vtnet")
    text should include("CDR_ENCRYPT_COLUMNS=msisdn, imei")
  }
}
