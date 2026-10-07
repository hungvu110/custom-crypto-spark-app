package org.example

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession

import vai.lakehouse.hdfs.BlockTokenDiagnostics

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Sample Spark application đọc/ghi HDFS (Dell Isilon OneFS) với wire
 * encryption bật (xem SPARK_HDFS_WIRE_ENCRYPTION_TASK.md).
 *
 * Logic nghiệp vụ: tạo DB nếu chưa có -> tạo bảng nếu chưa có -> insert dữ
 * liệu mẫu -> query lại và in kết quả — cùng flow với
 * vlp-spark-sample/MainApp.scala, nhưng chạy trên cluster Isilon HKH nên cần
 * thêm Kerberos + patch block token + diagnostics wire encryption.
 *
 * Cấu hình qua biến môi trường (đọc ở cả driver lẫn executor — xem README):
 *   APP_NAME, DB_NAME, TABLE_NAME, TABLE_TYPE (delta|iceberg|hive),
 *   INSERT_MODE (append|overwrite), ENABLE_HIVE_SUPPORT (true|false).
 *
 * Mã hoá cột là TUỲ CHỌN và là plugin: jar này không chứa và không biên dịch với lib crypto nào. Bật bằng cách
 * nạp jar lib (Dockerfile + spark.jars), đăng ký extension (spark.sql.extensions) rồi khai tên hàm SQL:
 *   CRYPTO_ENCRYPT_FUNCTION/CRYPTO_DECRYPT_FUNCTION (vd column_encrypt/column_decrypt hoặc cdr_encrypt/cdr_decrypt),
 *   CRYPTO_ENCRYPTED_COLUMNS (phân cách bằng dấu phẩy) và CRYPTO_KEY_FIELD. Để trống cả 2 tên hàm = không mã hoá.
 * Tên key tra keyPrefix là `DB_NAME.TABLE_NAME` chữ thường (CryptoStep.keyName). Nguồn keyPrefix
 * (CRYPTO_PREFIX_SOURCE, DAK_*, VAULT_*, ...) do lib tự đọc từ biến môi trường, app không đụng tới.
 */
object SparkApp {

  private val NAMENODE = "hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020"

  private val VALID_TABLE_TYPES  = Set("delta", "iceberg", "hive")
  private val VALID_INSERT_MODES = Set("append", "overwrite")

  case class AppConfig(
    appName: String,
    dbName: String,
    tableName: String,
    tableType: String,
    insertMode: String,
    enableHiveSupport: Boolean,
    crypto: Option[CryptoSettings],
    hdfsSaslDebug: Boolean
  )

  // --------------------------------------------------------------------
  // Config
  // --------------------------------------------------------------------

  private def env(key: String, default: String): String = {
    val v = sys.env.getOrElse(key, "").trim
    if (v.isEmpty) default else v
  }

  private def loadConfig(): AppConfig = AppConfig(
    appName           = env("APP_NAME", "SampleSparkApplicationPrivacy"),
    dbName            = env("DB_NAME", "demo_db"),
    tableName         = env("TABLE_NAME", "sample_table"),
    tableType         = env("TABLE_TYPE", "delta").toLowerCase,
    insertMode        = env("INSERT_MODE", "append").toLowerCase,
    enableHiveSupport = env("ENABLE_HIVE_SUPPORT", "true").toLowerCase == "true",
    // Mặc định KHÔNG mã hoá (cả 2 tên hàm rỗng). Sai/thiếu cấu hình ném IllegalArgumentException.
    crypto            = CryptoStep.parse(
      env("CRYPTO_ENCRYPT_FUNCTION", ""), env("CRYPTO_DECRYPT_FUNCTION", ""),
      env("CRYPTO_ENCRYPTED_COLUMNS", ""), env("CRYPTO_KEY_FIELD", "")),
    // Mặc định TẮT — bật DEBUG cho SASL/Token in ra hex dump rất dài, chỉ nên
    // bật khi thật sự cần trace lỗi wire encryption (mục 14.2 của plan gốc).
    hdfsSaslDebug     = env("HDFS_SASL_DEBUG", "false").toLowerCase == "true"
  )

  // --------------------------------------------------------------------
  // Diagnostics (đặc thù Isilon — giữ nguyên từ bản gốc)
  // --------------------------------------------------------------------

  /**
   * Bật DEBUG cho 2 logger cần thiết khi verify (mục 14.2) — in ra hex dump
   * rất dài của SASL wrap/unwrap, chỉ gọi khi HDFS_SASL_DEBUG=true. Không
   * làm gì nếu backend không phải log4j2.
   */
  private def enableHdfsSaslDebug(enabled: Boolean): Unit = {
    if (!enabled) {
      Log.info("HDFS SASL DEBUG is OFF (set HDFS_SASL_DEBUG=true to enable when tracing wire-encryption issues)")
      return
    }
    try {
      val configurator = Class.forName("org.apache.logging.log4j.core.config.Configurator")
      val levelClass   = Class.forName("org.apache.logging.log4j.Level")
      val debug        = levelClass.getMethod("valueOf", classOf[String]).invoke(null, "DEBUG")
      val setLevel     = configurator.getMethod("setLevel", classOf[String], levelClass)

      Seq(
        "org.apache.hadoop.hdfs.protocol.datatransfer.sasl",
        "org.apache.hadoop.security.token.Token"
      ).foreach { name =>
        setLevel.invoke(null, name, debug)
        Log.kv("DEBUG enabled", name)
      }
    } catch {
      case t: Throwable => Log.warn(s"Could not enable log4j2 DEBUG: ${t.getClass.getSimpleName}")
    }
  }

  /**
   * Kiểm tra trạng thái wire encryption và trạng thái patch block token.
   * Đây là phần quan trọng nhất để trace khi job fail — chạy trước business
   * logic vì cả CREATE DATABASE/TABLE/INSERT đều có thể chạm HDFS qua SASL
   * handshake này.
   */
  private def reportHdfsSecurity(spark: SparkSession): Unit = {
    Log.section("HDFS SECURITY DIAGNOSTICS")

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    Log.kv("hadoop.security.authentication", hadoopConf.get("hadoop.security.authentication", "<unset>"))
    Log.kv("hadoop.rpc.protection",          hadoopConf.get("hadoop.rpc.protection", "<unset>"))
    Log.kv("dfs.data.transfer.protection",   hadoopConf.get("dfs.data.transfer.protection", "<unset>"))
    Log.kv("dfs.encrypt.data.transfer.cipher.suites",
      hadoopConf.get("dfs.encrypt.data.transfer.cipher.suites", "<unset>"))
    Log.kv("fs.defaultFS", hadoopConf.get("fs.defaultFS", "<unset>"))

    // Server defaults: đây mới là thứ QUYẾT ĐỊNH client có mã hoá hay không.
    try {
      val fs = FileSystem.get(new java.net.URI(NAMENODE), hadoopConf)
      val defaults = fs.getServerDefaults(new Path("/"))
      Log.kv("server encryptDataTransfer", defaults.getEncryptDataTransfer)
      if (defaults.getEncryptDataTransfer) {
        Log.info("=> Isilon IS enforcing encryption. Client must take the getEncryptedStreams() branch.")
      } else {
        Log.info("=> Isilon is NOT enforcing encryption. Client will not take the encrypted branch.")
      }
    } catch {
      case t: Throwable => Log.warn(s"Could not read server defaults: ${t.getMessage}")
    }

    // Patch status — if this line reports FAIL, every HDFS operation will crash.
    BlockTokenDiagnostics.registeredBlockTokenClass() match {
      case Some(cls) =>
        Log.kv("HDFS_BLOCK_TOKEN handler", cls)
        if (BlockTokenDiagnostics.isPatchActive()) {
          Log.ok("Patch is ACTIVE — LenientBlockTokenIdentifier won tokenKindMap")
        } else {
          Log.fail("Patch is NOT active — still using Hadoop's original class.")
          Log.fail("Check: is the jar in spark.jars? Is userClassPathFirst enabled?")
        }
      case None =>
        Log.warn("Could not read tokenKindMap (possibly a JDK module restriction) — skipping check")
    }
  }

  private def logHdfsPermissions(spark: SparkSession, label: String, hdfsPath: String): Unit = {
    Try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val path = new Path(hdfsPath)
      val fs = path.getFileSystem(hadoopConf)
      if (fs.exists(path)) {
        val status = fs.getFileStatus(path)
        Log.kv(s"[$label] path",        hdfsPath)
        Log.kv(s"[$label] permission",  status.getPermission)
        Log.kv(s"[$label] owner/group", s"${status.getOwner}/${status.getGroup}")
      } else {
        Log.warn(s"[$label] path does not exist: $hdfsPath")
      }
    }.recover { case e => Log.warn(s"[$label] could not stat '$hdfsPath': ${e.getMessage}") }
  }

  private def logRuntimeIdentity(spark: SparkSession): Unit = {
    val hadoopUser = Try(UserGroupInformation.getCurrentUser).toOption
    Log.kv("OS user",                sys.props.getOrElse("user.name", "<unknown>"))
    Log.kv("Spark user",             spark.sparkContext.sparkUser)
    Log.kv("Hadoop UGI user",        hadoopUser.map(_.getUserName).getOrElse("<unknown>"))
    Log.kv("fs.defaultFS",           spark.sparkContext.hadoopConfiguration.get("fs.defaultFS", "<unset>"))
    Log.kv("spark.sql.warehouse.dir", spark.conf.getOption("spark.sql.warehouse.dir").getOrElse("<unset>"))
  }

  // --------------------------------------------------------------------
  // Business logic: CREATE DATABASE -> CREATE TABLE -> INSERT -> QUERY
  // --------------------------------------------------------------------

  private def runCreateTableAndInsert(spark: SparkSession, cfg: AppConfig): Unit = {
    // Dựng DataFrame đã mã hoá TRƯỚC mọi thao tác ghi: Spark resolve hàm SQL của extension ngay ở đây
    // (extension tra keyPrefix và che nó khỏi plan/UI/event log), nên hàm chưa nạp / prefix hỏng thì fail sớm.
    val sampleDf    = BusinessLogic.sampleDataFrame(spark)
    val cryptoKey   = CryptoStep.keyName(cfg.dbName, cfg.tableName)
    val encryptedDf = cfg.crypto.fold(sampleDf)(c => CryptoStep.encrypt(sampleDf, cryptoKey, c))

    val warehouseDir    = spark.conf.getOption("spark.sql.warehouse.dir").getOrElse("")
    val fullTableQuoted = BusinessLogic.qualifiedTable(cfg.dbName, cfg.tableName)
    // V1 Hive insertInto expects db.table không có backtick (không phải V2 writeTo).
    val fullTableDot    = s"${cfg.dbName}.${cfg.tableName}"
    val dbHdfsPath       = s"$warehouseDir/${cfg.dbName}.db"
    val tableHdfsPath    = s"$dbHdfsPath/${cfg.tableName}"

    Log.section("CREATE DATABASE")
    Log.kv("db", cfg.dbName)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${BusinessLogic.quoteIdent(cfg.dbName)}")
    logHdfsPermissions(spark, "DB_DIR", dbHdfsPath)

    // EXEC-DEBUG: dump executor-side Hadoop credentials trước khi executor
    // chạm HDFS lần đầu (CREATE TABLE / INSERT). In ra executor stdout
    // (kubectl logs <exec-pod>). Không chạm HDFS nên luôn chạy được, kể cả
    // khi thao tác HDFS thật sau đó fail vì auth.
    spark.sparkContext.parallelize(1 to 2, 2).foreachPartition { _ =>
      val ugi = UserGroupInformation.getCurrentUser
      println(s"[EXEC-DEBUG] HADOOP_TOKEN_FILE_LOCATION=${System.getenv("HADOOP_TOKEN_FILE_LOCATION")}")
      println(s"[EXEC-DEBUG] UGI=${ugi.getUserName} auth=${ugi.getAuthenticationMethod}")
      val tokens = ugi.getCredentials.getAllTokens.asScala
      if (tokens.isEmpty) println("[EXEC-DEBUG] NO TOKENS in executor UGI credentials")
      tokens.foreach { t => println(s"[EXEC-DEBUG] TOKEN kind=${t.getKind} service=${t.getService}") }
    }

    Log.section("CREATE TABLE")
    val createTableSql = BusinessLogic.buildCreateTableSql(fullTableQuoted, cfg.tableType)
    Log.info(s"Executing DDL:\n$createTableSql")
    spark.sql(createTableSql)
    logHdfsPermissions(spark, "DB_DIR",    dbHdfsPath)
    logHdfsPermissions(spark, "TABLE_DIR", tableHdfsPath)

    Log.section("INSERT SAMPLE DATA")
    Log.info("Sample data schema:")
    sampleDf.printSchema()
    Log.kv("rows",              BusinessLogic.sampleRows.size)
    Log.kv("mode",              cfg.insertMode)
    Log.kv("target",            s"$fullTableDot [V1 insertInto]")
    cfg.crypto match {
      case Some(c) =>
        Log.kv("crypto",            c.description)
        Log.kv("crypto key name",   cryptoKey)
        Log.kv("encrypted columns", c.encryptedColumns.mkString(", "))
        Log.kv("key field",         c.keyField)
      case None => Log.info("Column crypto: DISABLED (CRYPTO_ENCRYPT_FUNCTION not set)")
    }

    val writeStart = System.nanoTime()
    encryptedDf.write
      .mode(cfg.insertMode)
      .insertInto(fullTableDot)
    spark.catalog.refreshTable(fullTableDot)
    Log.elapsed("insert duration", writeStart)
    logHdfsPermissions(spark, "DB_DIR",    dbHdfsPath)
    logHdfsPermissions(spark, "TABLE_DIR", tableHdfsPath)

    Log.section(if (cfg.crypto.isDefined) "QUERY RESULT — ENCRYPTED (before decrypt)" else "QUERY RESULT")
    Log.info(s"SELECT * FROM $fullTableQuoted ORDER BY id")
    val resultDf = spark.sql(s"SELECT * FROM $fullTableQuoted ORDER BY id")
    val rowCount = resultDf.count()
    Log.kv("row count", rowCount)
    cfg.crypto.foreach { c =>
      Log.info(s"Raw content read from the table (columns ${c.encryptedColumns.mkString(", ")} are still ciphertext):")
    }
    resultDf.show(truncate = false)

    cfg.crypto.foreach { c =>
      Log.section("QUERY RESULT — AFTER DECRYPT")
      CryptoStep.decrypt(resultDf, cryptoKey, c).show(truncate = false)
    }
  }

  // --------------------------------------------------------------------
  // Main
  // --------------------------------------------------------------------

  def main(args: Array[String]): Unit = {
    val jobStart = System.nanoTime()

    Log.banner("SAMPLE SPARK APPLICATION PRIVACY — CREATE DB/TABLE + INSERT SAMPLE DATA (Isilon wire encryption)")

    val cfg =
      try loadConfig()
      catch {
        case e: IllegalArgumentException =>
          Log.fail(e.getMessage)
          sys.exit(2)
      }
    if (!VALID_TABLE_TYPES.contains(cfg.tableType)) {
      Log.fail(s"Invalid TABLE_TYPE: '${cfg.tableType}'. Must be one of: ${VALID_TABLE_TYPES.mkString(", ")}")
      sys.exit(2)
    }
    if (!VALID_INSERT_MODES.contains(cfg.insertMode)) {
      Log.fail(s"Invalid INSERT_MODE: '${cfg.insertMode}'. Must be one of: ${VALID_INSERT_MODES.mkString(", ")}")
      sys.exit(2)
    }

    Log.section("1. CONFIG")
    Log.kv("APP_NAME",            cfg.appName)
    Log.kv("DB_NAME",             cfg.dbName)
    Log.kv("TABLE_NAME",          cfg.tableName)
    Log.kv("TABLE_TYPE",          cfg.tableType)
    Log.kv("INSERT_MODE",         cfg.insertMode)
    Log.kv("ENABLE_HIVE_SUPPORT", cfg.enableHiveSupport)
    cfg.crypto match {
      case Some(c) =>
        Log.kv("column crypto",           c.description)
        Log.kv("CRYPTO_ENCRYPTED_COLUMNS", c.encryptedColumns.mkString(", "))
        Log.kv("CRYPTO_KEY_FIELD",        c.keyField)
      case None => Log.kv("column crypto", "DISABLED")
    }
    Log.kv("HDFS_SASL_DEBUG",     cfg.hdfsSaslDebug)

    Log.section("2. SPARK SESSION")
    val builder = SparkSession.builder()
      .appName(cfg.appName)
      .config("spark.hadoop.fs.defaultFS", NAMENODE)
    val spark =
      if (cfg.enableHiveSupport) builder.enableHiveSupport().getOrCreate()
      else builder.getOrCreate()

    Log.kv("spark version",       spark.version)
    Log.kv("application id",      spark.sparkContext.applicationId)
    Log.kv("master",              spark.sparkContext.master)
    Log.kv("default parallelism", spark.sparkContext.defaultParallelism)
    logRuntimeIdentity(spark)

    enableHdfsSaslDebug(cfg.hdfsSaslDebug)

    var exitCode = 0
    try {
      reportHdfsSecurity(spark)

      Log.section("3. BUSINESS LOGIC")
      runCreateTableAndInsert(spark, cfg)

      Log.banner("JOB SUCCEEDED")
      Log.elapsed("total duration", jobStart)

    } catch {
      case t: Throwable =>
        exitCode = 1
        Log.banner("JOB FAILED")
        Log.causeChain(t)

        // Detect the block-token error precisely so tracing doesn't require guessing.
        val isBlockTokenIssue = Iterator
          .iterate[Throwable](t)(_.getCause)
          .takeWhile(_ != null)
          .exists { c =>
            c.isInstanceOf[NegativeArraySizeException] ||
            Option(c.getStackTrace).exists(_.exists(_.getClassName.contains("BlockTokenIdentifier")))
          }

        if (isBlockTokenIssue) {
          Log.fail("Error matches Isilon's block token parsing issue.")
          Log.fail("The LenientBlockTokenIdentifier patch may not have been loaded.")
          Log.fail("Check the 'HDFS SECURITY DIAGNOSTICS' section at the top of the log.")
        }
        Log.elapsed("time to failure", jobStart)

    } finally {
      spark.stop()
      Log.info("SparkSession stopped")
    }

    sys.exit(exitCode)
  }
}
