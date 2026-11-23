package vai.lakehouse.silveringest.job

import java.util.{Locale, UUID}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, raise_error, when}
import vai.lakehouse.columncrypto.cdr.{CdrCrypto, CdrCryptoConfig}
import vai.lakehouse.keyprefix.{ColumnKeySource, KeyIntent, PathKeyResult, PathKeySource}
import vai.lakehouse.silveringest.config.{DecryptEncryptSettings, DecryptMode, IngestConfig, ProviderDecrypt}
import vai.lakehouse.silveringest.crypto.FileCipher
import vai.lakehouse.silveringest.logging.Log

/** Kết quả của một lần chạy. */
sealed trait IngestResult

object IngestResult {

  /** Đã ghi bảng đích từ `files` file; `decrypted` = file hoặc cột của đối tác được giải mã bằng key `by-path`, `false` = đọc nguyên trạng. */
  final case class Written(files: Int, decrypted: Boolean = false) extends IngestResult
}

/** Đọc file plaintext của `INPUT_PATH`; có `HOUR_PATH_REGEX` thì chia theo giờ của file và gắn cột giờ ([[HourGroups]]). */
private[job] object PlainSource {

  def read(spark: SparkSession, cfg: IngestConfig, files: Seq[SourceFile]): DataFrame = cfg.hours match {
    case None => SourceReader.read(spark, cfg.input, Seq(cfg.input.path))
    case Some(h) => HourGroups.read(h, files)(group => SourceReader.read(spark, cfg.input, group.map(_.path)))
  }
}

/** Passthrough: files are read and written as they are (e.g. tables with nothing to protect). No DAK call, no decryption. */
object PassthroughJob {

  def run(spark: SparkSession, cfg: IngestConfig): IngestResult = {
    Log.section("3. READ SOURCE")
    val files = SourceFiles.list(spark, cfg.input.path)
    Log.kv("files", files.size)
    val df = PlainSource.read(spark, cfg, files)
    Log.kv("columns", df.columns.mkString(", "))

    Log.section("4. WRITE TARGET")
    Log.kv("target", s"${cfg.target} [${cfg.outputFormat.name}, ${cfg.outputMode}]")
    TableWriter.write(spark, cfg, df)
    Log.ok(s"written ${cfg.target}")
    IngestResult.Written(files.size)
  }
}

/**
 * Decrypt-encrypt: giải mã dữ liệu của đối tác (nếu cần), gom nhóm cột (nếu có), mã hoá cột (nếu có), ghi bảng. Cách giải mã chọn bằng
 * `DECRYPT_MODE` ([[DecryptMode]]):
 *
 *  - `file` (VTNET, docs/key-access/TRA-LOI-DAK-BY-PATH.md): hỏi DAK key của `KEY_PATH` (`by-path`). Có key: file bị mã hoá cả file, giải mã khi
 *    đọc (text trong bộ nhớ; nhị phân ra thư mục tạm riêng của lần chạy, luôn xoá ở `finally`). Chưa có key (DAK trả rỗng): đọc nguyên trạng.
 *  - `column` (VTT, docs/silver-ingest/TRANSFORM-PHIEU-KHAI-THAC-VTT.md mục 3): file đọc nguyên trạng (đối tác mã hoá theo từng cột). Key `by-path` là
 *    PREFIX giải mã các cột `PROVIDER_DECRYPT_COLUMNS` bằng cdr-crypto, `keyInput = prefix ‖ giá trị PROVIDER_KEY_FIELD`. Chưa có key là LỖI (mã hoá chồng
 *    lên bản mã của đối tác).
 *  - `none`: không gọi `by-path`, không giải mã (cột đối tác không mã hoá, hoặc bảng không có cột nhạy cảm).
 *
 * Sau giải mã: gom nhóm `COLUMN_TRANSFORMS` (cần giá trị gốc nên chạy SAU giải mã), thêm cột hằng số `CDR_KEY_FIELD` khi có `CDR_KEY_FIELD_VALUE`, rồi
 * mã hoá các cột `CDR_ENCRYPT_COLUMNS` bằng cdr-crypto (key theo cột, hoặc MỘT key global nếu có `GLOBAL_KEY_ZONE`). Key mã hoá được xin TRƯỚC khi đọc file:
 * cột chưa có key thì dừng (exception `ColumnKeysUnavailableException`) mà chưa đụng tới plaintext. Không có cột nào cần mã hoá thì bỏ qua việc này.
 */
object DecryptEncryptJob {

  def run(spark: SparkSession, cfg: IngestConfig, pathKeys: PathKeySource, columnKeys: ColumnKeySource): IngestResult = {
    val v = cfg.decryptEncrypt.getOrElse(throw new IllegalArgumentException("decrypt-encrypt settings are missing"))
    v.decryptMode match {
      case DecryptMode.File => runWholeFile(spark, cfg, v, pathKeys, columnKeys)
      case DecryptMode.Column => runProviderColumns(spark, cfg, v, pathKeys, columnKeys)
      case DecryptMode.NoDecrypt => runPlain(spark, cfg, v, columnKeys)
    }
  }

  // ---------------------------------------------------------------- DECRYPT_MODE=file (giải mã cả file)

  private def runWholeFile(spark: SparkSession, cfg: IngestConfig, v: DecryptEncryptSettings, pathKeys: PathKeySource,
                           columnKeys: ColumnKeySource): IngestResult = {
    Log.section("3. PATH KEY (DAK by-path)")
    Log.kv("path", v.keyPath)
    val fileKey = pathKeys.readPath(v.keyPath) match {
      case PathKeyResult.Available(pathKey) =>
        Log.ok(s"path key received (keyVersion=${pathKey.keyVersion.getOrElse("-")}): files are encrypted, decrypting with ${v.cipher}")
        val cipher = FileCipher.forName(v.cipher)
        Some(cipher -> cipher.key(pathKey.keyPrefix))
      case PathKeyResult.NotAvailable =>
        Log.info("DAK has no key for this path: files are not encrypted, reading them as they are")
        None
    }

    val finish = finishing(cfg, v, columnKeys)

    Log.section("5. READ, TRANSFORM, ENCRYPT COLUMNS, WRITE")
    val files = SourceFiles.list(spark, cfg.input.path)
    Log.kv("files", files.size)
    fileKey match {
      case None =>
        write(spark, cfg, finish(PlainSource.read(spark, cfg, files)))
        IngestResult.Written(files.size)
      case Some((cipher, key)) =>
        val staging = v.stagingPath.map(base => s"${base.stripSuffix("/")}/${spark.sparkContext.applicationId}-${UUID.randomUUID()}")
        try {
          val plain = cfg.hours.fold(decrypted(spark, cfg, files, cipher, key, staging)) { h =>
            HourGroups.read(h, files)(group => decrypted(spark, cfg, group, cipher, key, staging))
          }
          write(spark, cfg, finish(plain))
          IngestResult.Written(files.size, decrypted = true)
        } finally staging.foreach { dir =>
          if (EncryptedSource.delete(spark, dir)) Log.info(s"staging directory removed: $dir")
          else Log.fail(s"could NOT remove the staging directory with decrypted data, remove it by hand: $dir")
        }
    }
  }

  private def decrypted(spark: SparkSession, cfg: IngestConfig, files: Seq[SourceFile], cipher: FileCipher,
                        key: Array[Byte], staging: Option[String]): DataFrame = staging match {
    case None =>
      Log.info("decrypting in memory (text format)")
      SourceReader.fromLines(spark, cfg.input, EncryptedSource.decryptLines(spark, files, cipher, key))
    case Some(dir) =>
      Log.info(s"decrypting to the staging directory $dir")
      val written = EncryptedSource.decryptToStaging(spark, files, cipher, key, dir)
      SourceReader.read(spark, cfg.input, written)
  }

  // ---------------------------------------------------------------- DECRYPT_MODE=column (VTT mã hoá theo từng cột)

  private def runProviderColumns(spark: SparkSession, cfg: IngestConfig, v: DecryptEncryptSettings, pathKeys: PathKeySource,
                                 columnKeys: ColumnKeySource): IngestResult = {
    val provider = v.providerDecrypt.getOrElse(throw new IllegalArgumentException("PROVIDER_DECRYPT settings are missing"))
    val prefix = providerPrefix(v, provider, pathKeys)
    val finish = finishing(cfg, v, columnKeys)

    Log.section("5. READ, DECRYPT PROVIDER COLUMNS, TRANSFORM, ENCRYPT COLUMNS, WRITE")
    val files = SourceFiles.list(spark, cfg.input.path)
    Log.kv("files", files.size)
    val plain = PlainSource.read(spark, cfg, files)
    write(spark, cfg, finish(decryptProviderColumns(plain, cfg.target, provider, prefix)))
    IngestResult.Written(files.size, decrypted = true)
  }

  /** Key `by-path` = prefix của đối tác. Chưa có key là lỗi: cột còn là bản mã của đối tác, mã hoá tiếp sẽ chồng hai lớp mã. Prefix không được ghi log. */
  private def providerPrefix(v: DecryptEncryptSettings, provider: ProviderDecrypt, pathKeys: PathKeySource): String = {
    Log.section("3. PATH KEY (DAK by-path, prefix of the provider)")
    Log.kv("path", v.keyPath)
    pathKeys.readPath(v.keyPath) match {
      case PathKeyResult.Available(pathKey) =>
        Log.ok(s"path key received (keyVersion=${pathKey.keyVersion.getOrElse("-")}): decrypting ${provider.columns.mkString(", ")} " +
          s"with cdr-crypto (keyField=${provider.keyField})")
        pathKey.keyPrefix
      case PathKeyResult.NotAvailable =>
        throw new IllegalStateException(s"DAK has no by-path key for '${v.keyPath}': the provider-encrypted columns " +
          s"(${provider.columns.mkString(", ")}) cannot be decrypted, and encrypting them again would stack our encryption on the provider's. " +
          "Ask the DAK admin to create the key for this path (the exact string, case sensitive)")
    }
  }

  /**
   * `CdrCrypto.transform` trả NULL khi keyField là NULL: bản mã của đối tác sẽ thành NULL âm thầm và mất định danh. Dòng có cột cần giải mã mà keyField
   * null thì job DỪNG (kiểm từng dòng, không thêm lượt đọc); cả hai cùng null thì vẫn là NULL (không có gì để giải mã). Thông báo không chứa giá trị.
   */
  private def decryptProviderColumns(df: DataFrame, target: String, provider: ProviderDecrypt, prefix: String): DataFrame = {
    val missingKey = df.columns.find(_.equalsIgnoreCase(provider.keyField)).map(k => col(s"`${k.replace("`", "``")}`"))
    val guarded = missingKey.fold(df) { key =>
      provider.columns.foldLeft(df) { (acc, name) =>
        acc.columns.find(_.equalsIgnoreCase(name)).fold(acc) { actual =>
          val column = col(s"`${actual.replace("`", "``")}`")
          acc.withColumn(actual, when(key.isNull && column.isNotNull, raise_error(lit(
            s"PROVIDER_KEY_FIELD '${provider.keyField}' is NULL in a row where '$name' is not: it cannot be decrypted " +
              "(decrypting would silently turn it into NULL)"))).otherwise(column))
        }
      }
    }
    val keys = provider.columns.map(_.toLowerCase(Locale.ROOT) -> prefix).toMap
    CdrCrypto.decryptColumns(guarded, CdrCryptoConfig(keys, provider.keyField, provider.columns, target))
  }

  // ---------------------------------------------------------------- DECRYPT_MODE=none

  private def runPlain(spark: SparkSession, cfg: IngestConfig, v: DecryptEncryptSettings, columnKeys: ColumnKeySource): IngestResult = {
    Log.section("3. PATH KEY")
    Log.info("DECRYPT_MODE=none: DAK by-path is not called, the files are read as they are")
    val finish = finishing(cfg, v, columnKeys)

    Log.section("5. READ, TRANSFORM, ENCRYPT COLUMNS, WRITE")
    val files = SourceFiles.list(spark, cfg.input.path)
    Log.kv("files", files.size)
    write(spark, cfg, finish(PlainSource.read(spark, cfg, files)))
    IngestResult.Written(files.size)
  }

  // ---------------------------------------------------------------- gom nhóm, cột hằng số, mã hoá (chung cho mọi chế độ)

  /**
   * Phần sau giải mã: gom nhóm, thêm cột hằng số làm `keyField`, mã hoá cột. Xin key mã hoá NGAY (trước khi đọc file) để cột chưa có key làm job dừng
   * mà chưa đụng tới plaintext; không có cột nào cần mã hoá thì không gọi DAK `by-column` hay key global.
   */
  private def finishing(cfg: IngestConfig, v: DecryptEncryptSettings, columnKeys: ColumnKeySource): DataFrame => DataFrame = {
    val cdr = if (v.encryptColumns.isEmpty) {
      Log.section("4. COLUMN KEYS")
      Log.info("no column to encrypt: DAK by-column is not called, columns are written as they are")
      None
    } else {
      Log.section(v.globalZone.fold(s"4. COLUMN KEYS (DAK by-column, onMissing=${v.onMissing}, cdr-crypto)") { zone =>
        s"4. COLUMN KEYS (DAK global key, zone=$zone, one key for every column, cdr-crypto)"
      })
      val keyField = v.keyField.getOrElse(throw new IllegalArgumentException("CDR_KEY_FIELD is missing"))
      val intent = if (v.onMissing == "request") KeyIntent.Encrypt else KeyIntent.Decrypt
      val loaded = CdrCrypto.loadConfig(cfg.target, keyField, v.encryptColumns, intent, columnKeys)
      Log.ok(s"${if (v.globalZone.isDefined) "global key received" else "keys received"} for ${v.encryptColumns.mkString(", ")} (keyField=$keyField" +
        s"${v.keyFieldValue.fold("")(_ => ", constant column")})")
      Some(loaded)
    }
    if (v.transforms.nonEmpty) Log.kv("transforms", v.transforms.map(t => s"${t.column}:${t.kind.name}").mkString(", "))

    df => {
      val grouped = ColumnTransforms(df, v.transforms)
      val keyed = withConstantKeyField(grouped, v)
      cdr.fold(keyed)(config => CdrCrypto.encryptColumns(keyed, config))
    }
  }

  /**
   * `CDR_KEY_FIELD_VALUE`: `CDR_KEY_FIELD` là cột hằng số do job thêm, nên cùng một giá trị gốc luôn ra cùng một bản mã ở mọi dòng và mọi bảng. Cột
   * trùng tên đã có trong dữ liệu là lỗi (không âm thầm ghi đè một cột của nguồn).
   */
  private def withConstantKeyField(df: DataFrame, v: DecryptEncryptSettings): DataFrame = (v.keyField, v.keyFieldValue) match {
    case (Some(name), Some(value)) =>
      df.columns.find(_.equalsIgnoreCase(name)).foreach { existing =>
        throw new IllegalArgumentException(s"CDR_KEY_FIELD '$name' already exists in the data (column '$existing'): with CDR_KEY_FIELD_VALUE the job " +
          "adds this column itself, choose another name")
      }
      df.withColumn(name, lit(value))
    case _ => df
  }

  private def write(spark: SparkSession, cfg: IngestConfig, df: DataFrame): Unit = {
    Log.kv("columns", df.columns.mkString(", "))
    Log.kv("target", s"${cfg.target} [${cfg.outputFormat.name}, ${cfg.outputMode}]")
    TableWriter.write(spark, cfg, df)
    Log.ok(s"written ${cfg.target}")
  }
}
