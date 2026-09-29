package vai.lakehouse.columncrypto

import org.yaml.snakeyaml.Yaml
import vai.lakehouse.keyprefix.{PrefixFiles, PrefixSource}

import scala.collection.JavaConverters._

/** Phần cấu hình KHÔNG nhạy cảm của 1 dataset (file settings, commit được vào Git). */
case class CryptoSettings(keyField: String, encryptedColumns: Seq[String])

/**
 * Cấu hình mã hoá cột cho 1 dataset/table, ghép từ 2 nguồn:
 *   - file settings (ConfigMap): keyField, encryptedColumns
 *   - keyPrefix: mỗi bảng 1 file trong thư mục mount từ K8s Secret `key-prefix`
 *     (tên file = tên bảng, nội dung = keyPrefix)
 *
 * `keyField` PHẢI là 1 cột không nằm trong `encryptedColumns` — giá trị của
 * nó (giữ nguyên plaintext) được dùng làm nguyên liệu sinh key cho từng dòng,
 * kết hợp với `keyPrefix` (cố định theo dataset).
 */
case class ColumnCryptoConfig(
  keyPrefix: String,
  keyField: String,
  encryptedColumns: Seq[String]
) {
  // Che keyPrefix để không lộ ra log khi in nguyên case class.
  override def toString: String =
    s"ColumnCryptoConfig(keyPrefix=***, keyField=$keyField, encryptedColumns=${encryptedColumns.mkString("[", ", ", "]")})"
}

object ColumnCryptoConfig {

  private def rootMap(yamlContent: String): java.util.Map[String, Any] =
    new Yaml().load[Any](yamlContent) match {
      case null => new java.util.HashMap[String, Any]()
      case m: java.util.Map[_, _] => m.asInstanceOf[java.util.Map[String, Any]]
      case _ => throw new IllegalArgumentException("Column crypto config must be a YAML map at the top level")
    }

  private def section(root: java.util.Map[String, Any], key: String): java.util.Map[String, Any] =
    Option(root.get(key))
      .map(_.asInstanceOf[java.util.Map[String, Any]])
      .getOrElse(new java.util.HashMap[String, Any]())

  /** Parse file settings, trả về map tên dataset -> keyField/encryptedColumns. Pure, dễ unit test. */
  def parseSettings(yamlContent: String): Map[String, CryptoSettings] =
    section(rootMap(yamlContent), "datasets").asScala.map { case (name, raw) =>
      val m = raw.asInstanceOf[java.util.Map[String, Any]]
      val encryptedColumns = m.get("encryptedColumns") match {
        case list: java.util.List[_] => list.asScala.map(_.toString).toSeq
        case null => throw new IllegalArgumentException(s"Dataset '$name': missing 'encryptedColumns'")
        case other => throw new IllegalArgumentException(s"Dataset '$name': 'encryptedColumns' must be a list, got: $other")
      }
      val keyField = Option(m.get("keyField")).map(_.toString)
        .getOrElse(throw new IllegalArgumentException(s"Dataset '$name': missing 'keyField'"))
      name -> CryptoSettings(keyField, encryptedColumns)
    }.toMap

  /**
   * Ghép settings + keyPrefix của 1 dataset. `keyPrefix` là by-name: chỉ được đọc sau khi
   * dataset đã có trong settings. Thông báo lỗi không bao giờ chứa giá trị prefix.
   */
  def merge(
    datasetName: String,
    settings: Map[String, CryptoSettings],
    keyPrefix: => String
  ): ColumnCryptoConfig = {
    val s = settings.getOrElse(datasetName, throw new IllegalArgumentException(
      s"No crypto settings found for dataset '$datasetName'. " +
        s"Declared datasets in settings: ${settings.keys.mkString(", ")}"))
    ColumnCryptoConfig(keyPrefix, s.keyField, s.encryptedColumns)
  }

  /** Đọc file settings, lấy keyPrefix của bảng từ `prefixSource`, trả config đầy đủ cho 1 dataset. */
  def load(settingsPath: String, prefixSource: PrefixSource, datasetName: String): ColumnCryptoConfig =
    merge(
      datasetName,
      parseSettings(PrefixFiles.readContent(settingsPath, "Column crypto config file")),
      prefixSource.read(datasetName))
}
