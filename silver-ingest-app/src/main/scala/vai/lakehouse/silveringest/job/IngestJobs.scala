package vai.lakehouse.silveringest.job

import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import vai.lakehouse.columncrypto.cdr.CdrCrypto
import vai.lakehouse.keyprefix.{ColumnKeySource, KeyIntent, PathKeyResult, PathKeySource}
import vai.lakehouse.silveringest.config.IngestConfig
import vai.lakehouse.silveringest.crypto.FileCipher
import vai.lakehouse.silveringest.logging.Log

/** Kết quả của một lần chạy. */
sealed trait IngestResult

object IngestResult {

  /** Đã ghi bảng đích từ `files` file; `decrypted` = file được giải mã bằng key `by-path` (VTNET), `false` = đọc nguyên trạng. */
  final case class Written(files: Int, decrypted: Boolean = false) extends IngestResult
}

/** VTT: file đã được VTT mã hoá từng cột. Đọc rồi ghi bảng, không giải mã, không mã hoá thêm. */
object VttJob {

  def run(spark: SparkSession, cfg: IngestConfig): IngestResult = {
    Log.section("3. READ SOURCE")
    val files = SourceFiles.list(spark, cfg.input.path)
    Log.kv("files", files.size)
    val df = SourceReader.read(spark, cfg.input, Seq(cfg.input.path))
    Log.kv("columns", df.columns.mkString(", "))

    Log.section("4. WRITE TARGET")
    Log.kv("target", s"${cfg.target} [${cfg.outputFormat.name}, ${cfg.outputMode}]")
    TableWriter.write(spark, cfg, df)
    Log.ok(s"written ${cfg.target}")
    IngestResult.Written(files.size)
  }
}

/**
 * VTNET (luồng đã chốt, docs/key-access/TRA-LOI-DAK-BY-PATH.md):
 *
 *  1. Hỏi DAK key của `KEY_PATH` (`by-path`). Có key: file của đường dẫn bị mã hoá cả file, giải mã ở bước 3. Chưa có key (DAK trả rỗng):
 *     file không mã hoá, đọc nguyên trạng. Cả hai trường hợp đều chạy tiếp.
 *  1. Xin key các cột cần mã hoá (`by-column`, `onMissing` = `CDR_ON_MISSING`, mặc định `request`: lần đầu DAK mở YC, admin duyệt và
 *     sinh key) TRƯỚC khi đọc file: cột chưa có key thì dừng (exception `ColumnKeysUnavailableException`) mà chưa đụng tới plaintext.
 *  1. Đọc file: nguyên trạng, hoặc giải mã (text trong bộ nhớ; nhị phân ra thư mục tạm riêng của lần chạy, luôn xoá ở `finally`).
 *  1. Mã hoá cột bằng cdr-crypto, ghi bảng.
 */
object VtnetJob {

  def run(spark: SparkSession, cfg: IngestConfig, pathKeys: PathKeySource, columnKeys: ColumnKeySource): IngestResult = {
    val v = cfg.vtnet.getOrElse(throw new IllegalArgumentException("VTNET settings are missing"))

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

    Log.section(s"4. COLUMN KEYS (DAK by-column, onMissing=${v.onMissing}, cdr-crypto)")
    val intent = if (v.onMissing == "request") KeyIntent.Encrypt else KeyIntent.Decrypt
    val cdr = CdrCrypto.loadConfig(cfg.target, v.keyField, v.encryptColumns, intent, columnKeys)
    Log.ok(s"keys received for ${v.encryptColumns.mkString(", ")} (keyField=${v.keyField})")

    Log.section("5. READ, ENCRYPT COLUMNS, WRITE")
    val files = SourceFiles.list(spark, cfg.input.path)
    Log.kv("files", files.size)
    fileKey match {
      case None =>
        write(spark, cfg, CdrCrypto.encryptColumns(SourceReader.read(spark, cfg.input, Seq(cfg.input.path)), cdr))
        IngestResult.Written(files.size)
      case Some((cipher, key)) =>
        val staging = v.stagingPath.map(base => s"${base.stripSuffix("/")}/${spark.sparkContext.applicationId}-${UUID.randomUUID()}")
        try {
          write(spark, cfg, CdrCrypto.encryptColumns(decrypted(spark, cfg, files, cipher, key, staging), cdr))
          IngestResult.Written(files.size, decrypted = true)
        } finally staging.foreach { dir =>
          if (EncryptedSource.delete(spark, dir)) Log.info(s"staging directory removed: $dir")
          else Log.fail(s"could NOT remove the staging directory with decrypted data, remove it by hand: $dir")
        }
    }
  }

  private def write(spark: SparkSession, cfg: IngestConfig, df: DataFrame): Unit = {
    Log.kv("columns", df.columns.mkString(", "))
    Log.kv("target", s"${cfg.target} [${cfg.outputFormat.name}, ${cfg.outputMode}]")
    TableWriter.write(spark, cfg, df)
    Log.ok(s"written ${cfg.target}")
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
}
