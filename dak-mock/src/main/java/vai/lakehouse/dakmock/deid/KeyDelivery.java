package vai.lakehouse.dakmock.deid;

/**
 * Giao key khử định danh theo bảng cho job của một phiếu (docs/dak-deid-flow/API_CONTRACT.md mục 6, plan mục 4.6b): đọc key bảng
 * từ Vault của DAK rồi đưa cho job BẰNG THAM CHIẾU (tên Secret), không bằng giá trị.
 *
 * <p>Hiện thực không được để key lọt vào giá trị trả về, log hay thông điệp exception. Lỗi ném {@link KeyDeliveryException} với
 * thông điệp cố định; mọi lỗi khác được người gọi quy về một thông điệp chung.
 */
public interface KeyDelivery {

  /**
   * Kết quả giao key: {@code secretName} là tên Secret chứa key của phiếu; {@code keyVersion} là version của key bảng đã đọc
   * (không phải giá trị key), {@code null} khi không đọc key thật (cách gửi job {@code noop}).
   */
  record KeyHandle(String secretName, Long keyVersion) {}

  /** Đọc key bảng của {@code request} và giao cho job. */
  KeyHandle deliver(DeidRequest request);

  /** Thu hồi key đã giao (xoá Secret). Gọi khi gửi job lỗi hoặc khi job kết thúc; gọi lại nhiều lần phải an toàn. */
  void release(DeidRequest request);
}
