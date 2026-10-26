package vai.lakehouse.dakmock.deid;

/**
 * Giao key thất bại. Thông điệp được ghi nguyên văn vào {@code submitError} của phiếu và hiện cho người dùng, nên PHẢI là một
 * chuỗi cố định (vd {@code table key not found}), tuyệt đối không chứa key, token hay nội dung do bên ngoài trả về.
 */
public class KeyDeliveryException extends RuntimeException {

  public KeyDeliveryException(String fixedMessage) {
    super(fixedMessage, null, false, false);
  }
}
