package vai.lakehouse.dakmock.column;

import vai.lakehouse.dakmock.web.DakApiException;

/** Tham số {@code onMissing}: cột chưa có key thì mở YC ({@code request}, khi mã hoá) hay không ({@code none}, khi giải mã). */
public enum OnMissing {
  REQUEST,
  NONE;

  /** Không gửi = {@code none} (mặc định an toàn: quên tham số thì không âm thầm mở YC). */
  static OnMissing parse(String raw) {
    if (raw == null) {
      return NONE;
    }
    return switch (raw) {
      case "request" -> REQUEST;
      case "none" -> NONE;
      default -> throw DakApiException.invalidOnMissing();
    };
  }
}
