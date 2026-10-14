package vai.lakehouse.pseudonym

/**
 * Gốc của mọi lỗi do lib ném ra, để caller bắt đúng loại thay vì bắt `IllegalArgumentException` chung chung.
 * Kế thừa `IllegalArgumentException` có chủ đích: mọi lỗi đều là "đầu vào/cấu hình sai" (không retry được), và code gọi
 * cũ vẫn bắt được. Thông báo lỗi KHÔNG BAO GIỜ chứa giá trị khoá hay giá trị định danh gốc.
 */
sealed abstract class PseudonymException(message: String) extends IllegalArgumentException(message)

/** Khoá sai độ dài, sai định dạng (`base64:`/`hex:`), toàn byte 0, hoặc version không hợp lệ. */
final class InvalidKeyException(message: String) extends PseudonymException(message)

/** Policy YAML sai: thiếu `mode`, cột trùng tên, mode không hỗ trợ, cột trong policy không có trong DataFrame. */
final class InvalidPolicyException(message: String) extends PseudonymException(message)

/** Định danh giả không giải mã được: sai tiền tố/version, sai khoá, hoặc bản mã bị sửa. */
final class InvalidPseudonymException(message: String) extends PseudonymException(message)

/** Thiếu hoặc sai cấu hình runtime: đường dẫn bảng ánh xạ, version khoá, `knownValues` của tái định danh HASH... */
final class ConfigurationException(message: String) extends PseudonymException(message)
