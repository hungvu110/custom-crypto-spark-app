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

  it should "nhận GLOBAL_KEY_ZONE (key global của vùng) cùng với cột mã hoá" in {
    val cfg = IngestConfig.fromEnv(Mode.DecryptEncrypt, vtnet + ("GLOBAL_KEY_ZONE" -> "khach_hang")).toOption.get

    cfg.decryptEncrypt.get.globalZone shouldBe Some("khach_hang")
    IngestConfig.fromEnv(Mode.DecryptEncrypt, vtnet).toOption.get.decryptEncrypt.get.globalZone shouldBe None
  }

  it should "từ chối GLOBAL_KEY_ZONE sai định dạng, thiếu cột mã hoá, hoặc dùng với passthrough" in {
    Seq("Khach-Hang", "a" * 65, "kh ach").foreach { zone =>
      errors(Mode.DecryptEncrypt, vtnet + ("GLOBAL_KEY_ZONE" -> zone)) should include("GLOBAL_KEY_ZONE: must match")
    }
    errors(Mode.DecryptEncrypt, vtnet - "CDR_ENCRYPT_COLUMNS" - "CDR_KEY_FIELD" + ("GLOBAL_KEY_ZONE" -> "khach_hang")) should
      include("GLOBAL_KEY_ZONE: needs CDR_ENCRYPT_COLUMNS")
    errors(Mode.Passthrough, common + ("GLOBAL_KEY_ZONE" -> "khach_hang")) should include("only valid for the decrypt-encrypt mode")
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

  it should "dựng giờ lấy từ một cột của dữ liệu (HOUR_FROM_COLUMN): cột hour mặc định, không có regex" in {
    val env = common ++ Map("BATCH_VALUE" -> "20261013", "BATCH_COLUMN" -> "dt", "HOUR_FROM_COLUMN" -> "hour_id")

    IngestConfig.fromEnv(Mode.Passthrough, env).toOption.get.hours shouldBe Some(Hours("hour", "", Nil, Some("hour_id")))
    IngestConfig.fromEnv(Mode.Passthrough, env ++ Map("HOUR_COLUMN" -> "h")).toOption.get.hours shouldBe
      Some(Hours("h", "", Nil, Some("hour_id")))
    // Same rule as the hour of a path: a partition only when listed in OUTPUT_PARTITION_BY (after the batch column).
    IngestConfig.fromEnv(Mode.Passthrough, env ++ Map("OUTPUT_PARTITION_BY" -> "hour")).toOption.get.partitionBy shouldBe Seq("dt", "hour")
    IngestConfig.fromEnv(Mode.Passthrough, env).toOption.get.partitionBy shouldBe Seq("dt")
    val described = IngestConfig.fromEnv(Mode.Passthrough, env).toOption.get.describe.toMap
    described("HOURS") should include("hour_id")
  }

  it should "từ chối cấu hình HOUR_FROM_COLUMN sai" in {
    val env = common ++ Map("BATCH_VALUE" -> "20261013", "BATCH_COLUMN" -> "dt", "HOUR_FROM_COLUMN" -> "hour_id")

    errors(Mode.Passthrough, env ++ Map("HOUR_PATH_REGEX" -> "/hour=(\\d{2})/")) should include("only one of")
    errors(Mode.Passthrough, env ++ Map("HOUR_FROM_COLUMN" -> "bad-name")) should include("invalid column name")
    errors(Mode.Passthrough, env ++ Map("HOUR_COLUMN" -> "bad-name")) should include("invalid column name")
    errors(Mode.Passthrough, env ++ Map("BATCH_COLUMN" -> "hour")) should include("must differ from BATCH_COLUMN")
    errors(Mode.Passthrough, env ++ Map("BATCH_HOURS" -> "10")) should include("not used with HOUR_FROM_COLUMN")
    errors(Mode.Passthrough, common ++ Map("HOUR_COLUMN" -> "hour")) should include("HOUR_PATH_REGEX or HOUR_FROM_COLUMN")
  }

  // ---------------------------------------------------------------- VTT: giải mã theo cột (cdr-crypto), mã hoá global, gom nhóm

  private val vttColumn = common ++ Map(
    "INPUT_PATH" -> "hdfs://nn:8020/landing/vtt/lakehouse/vtt-core/f014-mpre-tot-charge-d/date=20261104",
    "INPUT_FORMAT" -> "parquet",
    "OUTPUT_TABLE" -> "vtt_f014",
    "DECRYPT_MODE" -> "column",
    "PROVIDER_DECRYPT_COLUMNS" -> "isdn",
    "PROVIDER_KEY_FIELD" -> "sta_date",
    "KEY_PATH" -> "f014-mpre-tot-charge-d",
    "CDR_ENCRYPT_COLUMNS" -> "isdn, customer_id",
    "CDR_KEY_FIELD" -> "key_scope",
    "CDR_KEY_FIELD_VALUE" -> "vtt",
    "GLOBAL_KEY_ZONE" -> "vtt",
    "COLUMN_TRANSFORMS" -> "customer_age:AGE_BAND")

  private def settings(env: Map[String, String]): DecryptEncryptSettings =
    IngestConfig.fromEnv(Mode.DecryptEncrypt, env).toOption.get.decryptEncrypt.get

  "DECRYPT_MODE" should "mặc định là file: giữ nguyên hành vi giải mã cả file của VTNET" in {
    val v = settings(vtnet)

    v.decryptMode shouldBe DecryptMode.File
    v.providerDecrypt shouldBe None
    v.keyFieldValue shouldBe None
    v.transforms shouldBe Nil
  }

  it should "column: dùng key by-path làm prefix giải mã các cột VTT, không cần thư mục tạm cho parquet" in {
    val v = settings(vttColumn)

    v.decryptMode shouldBe DecryptMode.Column
    v.providerDecrypt shouldBe Some(ProviderDecrypt(Seq("isdn"), "sta_date"))
    v.keyPath shouldBe "f014-mpre-tot-charge-d"
    v.stagingPath shouldBe None
    v.encryptColumns shouldBe Seq("isdn", "customer_id")
    v.keyField shouldBe Some("key_scope")
    v.keyFieldValue shouldBe Some("vtt")
    v.globalZone shouldBe Some("vtt")
    v.transforms shouldBe Seq(ColumnTransform("customer_age", TransformKind.AgeBand))
  }

  it should "column: lấy đường dẫn key từ KEY_PATH_ROOT khi không đặt KEY_PATH" in {
    val env = vttColumn - "KEY_PATH" + ("KEY_PATH_ROOT" -> "hdfs://nn:8020/landing/vtt/lakehouse/vtt-core")

    settings(env).keyPath shouldBe "f014-mpre-tot-charge-d/date=20261104"
  }

  it should "none: không gọi by-path nên không cần và không nhận KEY_PATH, KEY_PATH_ROOT" in {
    val env = vttColumn - "DECRYPT_MODE" - "PROVIDER_DECRYPT_COLUMNS" - "PROVIDER_KEY_FIELD" - "KEY_PATH" + ("DECRYPT_MODE" -> "none")

    val v = settings(env)
    v.decryptMode shouldBe DecryptMode.NoDecrypt
    v.providerDecrypt shouldBe None
    v.keyPath shouldBe ""
    errors(Mode.DecryptEncrypt, env + ("KEY_PATH" -> "x")) should include("KEY_PATH: only used with DECRYPT_MODE=file/column")
    errors(Mode.DecryptEncrypt, env + ("KEY_PATH_ROOT" -> "/x")) should include("KEY_PATH_ROOT: only used with DECRYPT_MODE=file/column")
  }

  it should "từ chối giá trị khác file, column, none" in {
    errors(Mode.DecryptEncrypt, vttColumn + ("DECRYPT_MODE" -> "whole")) should include("DECRYPT_MODE='whole'")
    IngestConfig.fromEnv(Mode.DecryptEncrypt, vttColumn + ("DECRYPT_MODE" -> "COLUMN")).isRight shouldBe true
  }

  it should "column: bắt buộc cột giải mã và keyField của VTT, keyField không nằm trong cột giải mã" in {
    errors(Mode.DecryptEncrypt, vttColumn - "PROVIDER_DECRYPT_COLUMNS") should include("PROVIDER_DECRYPT_COLUMNS: is required")
    errors(Mode.DecryptEncrypt, vttColumn - "PROVIDER_KEY_FIELD") should include("PROVIDER_KEY_FIELD: is required")
    errors(Mode.DecryptEncrypt, vttColumn + ("PROVIDER_KEY_FIELD" -> "ISDN")) should include("PROVIDER_KEY_FIELD: 'ISDN' must not be listed")
    errors(Mode.DecryptEncrypt, vttColumn + ("PROVIDER_KEY_FIELD" -> "bad-name")) should include("PROVIDER_KEY_FIELD: invalid column name")
    errors(Mode.DecryptEncrypt, vttColumn + ("PROVIDER_DECRYPT_COLUMNS" -> "isdn,ISDN")) should include("PROVIDER_DECRYPT_COLUMNS: duplicated")
  }

  it should "column: bắt buộc KEY_PATH hoặc KEY_PATH_ROOT, không nhận DECRYPT_STAGING_PATH và FILE_CIPHER" in {
    errors(Mode.DecryptEncrypt, vttColumn - "KEY_PATH") should include("KEY_PATH_ROOT: is required")
    errors(Mode.DecryptEncrypt, vttColumn + ("DECRYPT_STAGING_PATH" -> "/tmp/s")) should
      include("DECRYPT_STAGING_PATH: only used with DECRYPT_MODE=file")
    errors(Mode.DecryptEncrypt, vttColumn + ("FILE_CIPHER" -> "aes-256-gcm")) should include("FILE_CIPHER: only used with DECRYPT_MODE=file")
  }

  it should "file và none: không nhận PROVIDER_DECRYPT_COLUMNS, PROVIDER_KEY_FIELD" in {
    errors(Mode.DecryptEncrypt, vtnet + ("PROVIDER_DECRYPT_COLUMNS" -> "msisdn")) should
      include("PROVIDER_DECRYPT_COLUMNS: only used with DECRYPT_MODE=column")
    errors(Mode.DecryptEncrypt, vtnet + ("PROVIDER_KEY_FIELD" -> "start_time")) should
      include("PROVIDER_KEY_FIELD: only used with DECRYPT_MODE=column")
    errors(Mode.DecryptEncrypt, (vttColumn - "KEY_PATH") + ("DECRYPT_MODE" -> "none")) should
      include("PROVIDER_DECRYPT_COLUMNS: only used with DECRYPT_MODE=column")
  }

  it should "file: parquet vẫn bắt buộc DECRYPT_STAGING_PATH (giải mã cả file ra thư mục tạm)" in {
    errors(Mode.DecryptEncrypt, vtnet + ("INPUT_FORMAT" -> "parquet")) should include("DECRYPT_STAGING_PATH: is required")
  }

  "CDR_KEY_FIELD_VALUE" should "đặt cột hằng số làm keyField của bước mã hoá, giá trị chỉ gồm ký tự an toàn" in {
    settings(vttColumn).keyFieldValue shouldBe Some("vtt")
    Seq("a b", "x" * 65, "a/b").foreach { value =>
      errors(Mode.DecryptEncrypt, vttColumn + ("CDR_KEY_FIELD_VALUE" -> value)) should include("CDR_KEY_FIELD_VALUE: must be")
    }
  }

  it should "cần CDR_KEY_FIELD và cột mã hoá, và không dùng với passthrough" in {
    errors(Mode.DecryptEncrypt, vttColumn - "CDR_KEY_FIELD") should include("CDR_KEY_FIELD_VALUE: needs CDR_KEY_FIELD")
    errors(Mode.DecryptEncrypt, vttColumn - "CDR_ENCRYPT_COLUMNS" - "GLOBAL_KEY_ZONE") should include("CDR_KEY_FIELD_VALUE: needs CDR_ENCRYPT_COLUMNS")
    errors(Mode.Passthrough, common + ("CDR_KEY_FIELD_VALUE" -> "vtt")) should include("only valid for the decrypt-encrypt mode")
  }

  "COLUMN_TRANSFORMS" should "nhận danh sách cột:KIND, không phân biệt hoa thường ở KIND" in {
    val v = settings(vttColumn + ("COLUMN_TRANSFORMS" -> "customer_age:AGE_BAND, hour_id:hour_band"))

    v.transforms shouldBe Seq(ColumnTransform("customer_age", TransformKind.AgeBand), ColumnTransform("hour_id", TransformKind.HourBand))
  }

  it should "từ chối KIND lạ, cú pháp sai, cột trùng, tên cột sai và cột đang được mã hoá" in {
    errors(Mode.DecryptEncrypt, vttColumn + ("COLUMN_TRANSFORMS" -> "customer_age:MASK")) should include("COLUMN_TRANSFORMS: unknown transform 'MASK'")
    errors(Mode.DecryptEncrypt, vttColumn + ("COLUMN_TRANSFORMS" -> "customer_age")) should include("COLUMN_TRANSFORMS: 'customer_age' must be column:KIND")
    errors(Mode.DecryptEncrypt, vttColumn + ("COLUMN_TRANSFORMS" -> "a:AGE_BAND,A:HOUR_BAND")) should include("COLUMN_TRANSFORMS: duplicated column")
    errors(Mode.DecryptEncrypt, vttColumn + ("COLUMN_TRANSFORMS" -> "bad-name:AGE_BAND")) should include("COLUMN_TRANSFORMS: invalid column name")
    errors(Mode.DecryptEncrypt, vttColumn + ("COLUMN_TRANSFORMS" -> "isdn:AGE_BAND")) should
      include("COLUMN_TRANSFORMS: column 'isdn' must not be listed in CDR_ENCRYPT_COLUMNS")
    errors(Mode.Passthrough, common + ("COLUMN_TRANSFORMS" -> "customer_age:AGE_BAND")) should include("only valid for the decrypt-encrypt mode")
  }

  "describe (VTT)" should "nêu chế độ giải mã, cột VTT, keyField, giá trị hằng số và transform" in {
    val described = IngestConfig.fromEnv(Mode.DecryptEncrypt, vttColumn).toOption.get.describe.toMap

    described("DECRYPT_MODE") shouldBe "column"
    described("PROVIDER_DECRYPT_COLUMNS") shouldBe "isdn"
    described("PROVIDER_KEY_FIELD") shouldBe "sta_date"
    described("CDR_KEY_FIELD_VALUE") shouldBe "vtt"
    described("COLUMN_TRANSFORMS") shouldBe "customer_age:AGE_BAND"
  }

  "describe" should "không chứa giá trị nhạy cảm và nêu rõ nguồn" in {
    val text = IngestConfig.fromEnv(Mode.DecryptEncrypt, vtnet).toOption.get.describe.map { case (k, v) => s"$k=$v" }.mkString(";")

    text should include("MODE=decrypt-encrypt")
    text should include("CDR_ENCRYPT_COLUMNS=msisdn, imei")
  }

  "OUTPUT_REBALANCE / OUTPUT_FILE_SHUFFLE_MB" should "mặc định auto (chỉ gom khi cần), khoảng 384 MB dữ liệu shuffle mỗi file" in {
    val files = IngestConfig.fromEnv(Mode.Passthrough, common).toOption.get.outputFiles

    files shouldBe OutputFiles(Rebalance.Auto, shuffleBytesPerFile = 384L * 1024 * 1024)
    OutputFiles.DefaultShuffleMb shouldBe 384
  }

  it should "nhận auto, true (luôn gom) và false (không gom), không phân biệt hoa thường" in {
    for ((value, mode) <- Seq("auto" -> Rebalance.Auto, "TRUE" -> Rebalance.Always, "false" -> Rebalance.Never)) {
      IngestConfig.fromEnv(Mode.Passthrough, common + ("OUTPUT_REBALANCE" -> value)).toOption.get.outputFiles.rebalance shouldBe mode
    }
    errors(Mode.Passthrough, common + ("OUTPUT_REBALANCE" -> "maybe")) should include("OUTPUT_REBALANCE='maybe': must be one of auto / true / false")
  }

  it should "chỉ gom ở chế độ auto khi bảng có cột partition khác cột lô (một lần chạy là một lô)" in {
    def gathers(extra: (String, String)*): Boolean =
      IngestConfig.fromEnv(Mode.Passthrough, common ++ Map("BATCH_COLUMN" -> "dt", "BATCH_VALUE" -> "20261104") ++ extra).toOption.get
        .rebalances

    gathers("OUTPUT_PARTITION_BY" -> "hour") shouldBe true // a task holds rows of every hour
    gathers() shouldBe false // only dt: a task already holds one partition, one file per task of about its input size
    gathers("OUTPUT_REBALANCE" -> "true") shouldBe true
    gathers("OUTPUT_PARTITION_BY" -> "hour", "OUTPUT_REBALANCE" -> "false") shouldBe false
    IngestConfig.fromEnv(Mode.Passthrough, common).toOption.get.rebalances shouldBe false // no partition, no batch
  }

  it should "nhận số MB dữ liệu shuffle mỗi file trong khoảng cho phép" in {
    for (mb <- Seq(16, 512, 4096)) {
      IngestConfig.fromEnv(Mode.Passthrough, common + ("OUTPUT_FILE_SHUFFLE_MB" -> mb.toString)).toOption.get.outputFiles
        .shuffleBytesPerFile shouldBe mb.toLong * 1024 * 1024
    }
  }

  it should "từ chối số MB không phải số nguyên trong khoảng 16 đến 4096" in {
    for (bad <- Seq("abc", "15", "4097", "-1", "1.5")) {
      errors(Mode.Passthrough, common + ("OUTPUT_FILE_SHUFFLE_MB" -> bad)) should include("OUTPUT_FILE_SHUFFLE_MB")
    }
  }

  it should "không nhận số MB khi OUTPUT_REBALANCE=false" in {
    errors(Mode.Passthrough, common ++ Map("OUTPUT_REBALANCE" -> "false", "OUTPUT_FILE_SHUFFLE_MB" -> "256")) should
      include("OUTPUT_FILE_SHUFFLE_MB: not used with OUTPUT_REBALANCE=false")
  }

  it should "nêu cách ghi file trong describe" in {
    def described(extra: (String, String)*): String =
      IngestConfig.fromEnv(Mode.Passthrough, common ++ Map("BATCH_COLUMN" -> "dt", "BATCH_VALUE" -> "20261104") ++ extra).toOption.get
        .describe.toMap.apply("OUTPUT_FILES")

    described("OUTPUT_PARTITION_BY" -> "hour") shouldBe "rebalance by dt, hour, about 384 MB of shuffle data per file (OUTPUT_REBALANCE=auto)"
    described() shouldBe "one file per read task: the run writes one partition dt (OUTPUT_REBALANCE=auto)"
    described("OUTPUT_REBALANCE" -> "false") shouldBe "one file per task and partition (OUTPUT_REBALANCE=false)"
  }
}
