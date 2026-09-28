package vai.lakehouse.hdfs

import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

/**
 * Chỉ phục vụ chẩn đoán. Đọc (KHÔNG ghi) `Token.tokenKindMap` để biết class nào
 * đang thực sự được dùng khi decode HDFS_BLOCK_TOKEN.
 */
object BlockTokenDiagnostics {

  /** FQN của class đang thắng cho kind HDFS_BLOCK_TOKEN, hoặc None nếu không đọc được. */
  def registeredBlockTokenClass(): Option[String] = {
    try {
      // Ép Hadoop build tokenKindMap nếu chưa được build.
      // Token rỗng -> kind rỗng -> decodeIdentifier trả null, nhưng map đã được populate.
      try {
        new Token[TokenIdentifier]().decodeIdentifier()
      } catch {
        case _: Throwable => // bỏ qua
      }

      val field = classOf[Token[_]].getDeclaredField("tokenKindMap")
      field.setAccessible(true)

      // BlockTokenIdentifier.KIND_NAME là package-private trong Hadoop 3.3.4
      // (không public) -> không compile được từ ngoài package. Lấy kind qua
      // getKind() (public) trên một instance rỗng thay vì đọc field tĩnh.
      val kind = new BlockTokenIdentifier().getKind()
      val map  = field.get(null).asInstanceOf[java.util.Map[Text, Class[_]]]

      if (map == null) None
      else Option(map.get(kind)).map(_.getName)
    } catch {
      case _: Throwable => None
    }
  }

  /** true nếu patch đã thắng cuộc đua last-write-wins. */
  def isPatchActive(): Boolean =
    registeredBlockTokenClass().contains(classOf[LenientBlockTokenIdentifier].getName)
}
