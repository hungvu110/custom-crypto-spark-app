package vai.lakehouse.silveringest.job

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, SparkSession}
import vai.lakehouse.silveringest.crypto.{FileCipher, FileDecryptionException}

/**
 * Giải mã file VTNET trên executor, mỗi file nguyên khối (GCM kiểm tag trên cả file), nên một file phải nhỏ hơn [[MaxFileBytes]].
 * Key đã dựng ở driver và đi kèm closure như key cột của cdr-crypto; không bao giờ ghi ra log.
 */
object EncryptedSource {

  /** Giới hạn của mảng byte JVM, chừa đệm cho tag. */
  val MaxFileBytes: Long = Int.MaxValue - 1024L

  def checkSizes(files: Seq[SourceFile]): Unit =
    files.find(_.length > MaxFileBytes).foreach { f =>
      throw new IllegalArgumentException(s"${f.path}: ${f.length} bytes is too large to decrypt in one piece (max $MaxFileBytes)")
    }

  /**
   * Giải mã trong bộ nhớ và tách dòng (UTF-8, `\n` hoặc `\r\n`), cho định dạng text. Plaintext không chạm đĩa.
   */
  def decryptLines(spark: SparkSession, files: Seq[SourceFile], cipher: FileCipher, key: Array[Byte]): Dataset[String] = {
    import spark.implicits._
    checkSizes(files)
    spark.sparkContext.binaryFiles(files.map(_.path).mkString(","))
      .flatMap { case (path, stream) =>
        val text = new String(decryptOne(path, stream.toArray(), cipher, key), UTF_8)
        val lines = text.split("\r?\n", -1)
        // Dòng rỗng cuối cùng do ký tự xuống dòng ở cuối file.
        if (lines.nonEmpty && lines.last.isEmpty) lines.init.toSeq else lines.toSeq
      }
      .toDS()
  }

  /**
   * Giải mã ra `stagingDir` (một file ra cho mỗi file vào), cho định dạng nhị phân Spark chỉ đọc được từ file. Bên gọi phải xoá
   * `stagingDir` sau khi dùng ([[delete]]). Trả về đường dẫn các file đã ghi.
   */
  def decryptToStaging(spark: SparkSession, files: Seq[SourceFile], cipher: FileCipher, key: Array[Byte],
                       stagingDir: String): Seq[String] = {
    checkSizes(files)
    spark.sparkContext.binaryFiles(files.map(_.path).mkString(","))
      .map { case (path, stream) =>
        val plain = decryptOne(path, stream.toArray(), cipher, key)
        val source = new Path(path)
        // Tên ra = băm đường dẫn gốc + tên gốc: không trùng khi hai thư mục con có file cùng tên.
        val out = new Path(stagingDir, s"${digest(path)}-${source.getName}")
        val fs = out.getFileSystem(stream.getConfiguration)
        val os = fs.create(out, false)
        try os.write(plain) finally os.close()
        out.toString
      }
      .collect().toSeq.sorted
  }

  /** Xoá thư mục tạm (chứa plaintext); không ném lỗi để không che lỗi gốc của job, trả `false` nếu không xoá được. */
  def delete(spark: SparkSession, dir: String): Boolean = {
    val path = new Path(dir)
    try {
      val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      !fs.exists(path) || fs.delete(path, true)
    } catch {
      case _: Exception => false
    }
  }

  private def decryptOne(path: String, content: Array[Byte], cipher: FileCipher, key: Array[Byte]): Array[Byte] =
    try cipher.decrypt(content, key)
    catch {
      case e: FileDecryptionException => throw new FileDecryptionException(s"$path: ${e.getMessage}", e)
    }

  private def digest(path: String): String =
    MessageDigest.getInstance("SHA-256").digest(path.getBytes(UTF_8)).take(6).map("%02x".format(_)).mkString
}
