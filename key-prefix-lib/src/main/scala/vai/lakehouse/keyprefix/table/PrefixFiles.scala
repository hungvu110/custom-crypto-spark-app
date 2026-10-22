package vai.lakehouse.keyprefix

import java.io.{File, FileInputStream, InputStream}

/**
 * Tiện ích đọc keyPrefix từ file và kiểm tra tên dataset — dùng chung cho mọi nguồn prefix
 * (`FilePrefixSource`, `VaultPrefixSource`, `ChainedPrefixSource`) và cho mọi lib crypto dùng nguồn này.
 */
object PrefixFiles {

  // Trùng bộ ký tự hợp lệ của key trong K8s Secret, và không cho bắt đầu bằng "." nên
  // loại được "." / ".." — tên bảng đi vào đường dẫn file / URL Vault, không được thoát khỏi thư mục.
  private val DatasetName = "^[A-Za-z0-9_][A-Za-z0-9_.-]*$".r

  /** Tên bảng đi vào đường dẫn file / URL Vault nên phải được kiểm tra trước khi dùng. */
  def requireValidDatasetName(datasetName: String): Unit =
    require(DatasetName.pattern.matcher(datasetName).matches(),
      s"Invalid dataset name '$datasetName': only letters, digits, '_', '-' and '.' are allowed (must not start with '.')")

  /**
   * Đọc toàn bộ nội dung file UTF-8: đĩa trước, fallback classpath resource. `label` chỉ dùng cho
   * thông báo lỗi khi không thấy file (vd "Column crypto config file").
   */
  def readContent(path: String, label: String): String = {
    val file = new File(path)
    val stream: InputStream =
      if (file.exists()) new FileInputStream(file)
      else {
        val fromClasspath = getClass.getClassLoader.getResourceAsStream(path)
        if (fromClasspath == null) {
          throw new IllegalArgumentException(
            s"$label not found: '$path' (tried both disk path and classpath resource)")
        }
        fromClasspath
      }
    try scala.io.Source.fromInputStream(stream, "UTF-8").mkString
    finally stream.close()
  }

  /** Đọc keyPrefix của 1 bảng từ file `<prefixDir>/<datasetName>` (đĩa trước, fallback classpath). */
  def readPrefix(prefixDir: String, datasetName: String): String = {
    requireValidDatasetName(datasetName)
    val prefix = readContent(s"$prefixDir/$datasetName", "Key prefix file").replaceAll("[\\r\\n]+$", "")
    require(prefix.nonEmpty, s"keyPrefix for dataset '$datasetName' is empty")
    prefix
  }
}
