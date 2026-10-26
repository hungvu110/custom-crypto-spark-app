package vai.lakehouse.dakmock.deid;

/**
 * Giao key kiểu {@code noop}: KHÔNG đọc Vault, không tạo Secret. Chỉ trả tên Secret suy ra từ mã phiếu để các bước sau có đủ tham số,
 * và không có {@code keyVersion} vì không có key thật nào được đọc. Dùng để chạy thử toàn bộ luồng phiếu mà không cần hạ tầng.
 */
public class NoopKeyDelivery implements KeyDelivery {

  @Override
  public KeyHandle deliver(DeidRequest request) {
    return new KeyHandle(request.secretName(), null);
  }

  @Override
  public void release(DeidRequest request) {
    // Không có gì để thu hồi.
  }
}
