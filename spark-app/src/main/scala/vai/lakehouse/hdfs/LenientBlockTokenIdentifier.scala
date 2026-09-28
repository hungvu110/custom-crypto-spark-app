package vai.lakehouse.hdfs

import java.io.DataInput

import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier

/**
 * Dell Isilon OneFS phát hành HDFS block token theo định dạng nội bộ của nó,
 * không tương thích với parser Writable/protobuf của Apache Hadoop.
 *
 * Từ Hadoop 3.2.1 (HDFS-13617 / HDFS-13699), client tự parse block token trong
 * SASL handshake để lấy `handshakeSecret` phục vụ selective-QOP. Với token của
 * Isilon, việc parse này ném NegativeArraySizeException và làm hỏng mọi thao tác
 * ghi/đọc block khi wire encryption đang bật.
 *
 * Class này khôi phục hành vi của client Hadoop <= 3.2.0: coi block token là
 * opaque, gửi SASL message không kèm handshake secret.
 *
 * QUAN TRỌNG: mã hoá đường truyền KHÔNG bị ảnh hưởng. SASL vẫn thương lượng
 * QOP auth-conf với cipher AES/CTR 256-bit. Byte token thật vẫn được gửi nguyên
 * vẹn xuống DataNode và vẫn được Isilon verify đầy đủ.
 *
 * Class được nạp qua ServiceLoader và ghi đè entry HDFS_BLOCK_TOKEN trong
 * `Token.tokenKindMap` theo cơ chế last-write-wins.
 */
class LenientBlockTokenIdentifier extends BlockTokenIdentifier {

  override def readFields(in: DataInput): Unit = {
    try {
      super.readFields(in)
    } catch {
      case _: Throwable =>
      // Có chủ đích: định dạng token không tương thích Apache.
      // Không log ở đây — method này nằm trên hot path của mọi block I/O,
      // log sẽ làm ngập stderr. Việc xác nhận class nào đang được dùng
      // đã có BlockTokenDiagnostics lo lúc khởi động.
    }
  }

  /**
   * Luôn trả null để `SaslDataTransferClient.doSaslHandshake` đi nhánh
   * "Handshake secret is null, sending without handshake secret".
   *
   * Không phụ thuộc vào việc `super.readFields` đã set field tới đâu trước
   * khi ném lỗi.
   */
  override def getHandshakeMsg(): Array[Byte] = null
}
