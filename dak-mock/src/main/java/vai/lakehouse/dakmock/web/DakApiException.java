package vai.lakehouse.dakmock.web;

import org.springframework.http.HttpStatus;

/**
 * Lỗi trả cho bên gọi theo bảng mã lỗi của docs/DAK_API_SPEC.md mục 5.4. {@code getMessage()} là thông điệp
 * công khai (không chứa token, secret, prefix); {@code internalReason} chỉ để ghi log.
 */
public final class DakApiException extends RuntimeException {

  private final HttpStatus status;
  private final String error;
  private final String internalReason;

  private DakApiException(HttpStatus status, String error, String publicMessage, String internalReason) {
    super(publicMessage, null, false, false);
    this.status = status;
    this.error = error;
    this.internalReason = internalReason;
  }

  public static DakApiException invalidTable() {
    return new DakApiException(HttpStatus.BAD_REQUEST, "invalid_table",
        "database and table must match [A-Za-z0-9_]{1,128}", "invalid database/table");
  }

  public static DakApiException invalidToken(String reason) {
    return new DakApiException(HttpStatus.UNAUTHORIZED, "invalid_token", "Invalid or missing access token", reason);
  }

  public static DakApiException accessDenied(String database, String table) {
    return new DakApiException(HttpStatus.FORBIDDEN, "access_denied",
        "Team has no access to key " + database + "." + table, "no key or no access");
  }

  public static DakApiException upstream(String reason) {
    return new DakApiException(HttpStatus.SERVICE_UNAVAILABLE, "upstream_unavailable",
        "A dependency of DAK is unavailable", reason);
  }

  public static DakApiException internal(String reason) {
    return new DakApiException(HttpStatus.INTERNAL_SERVER_ERROR, "internal_error", "Internal error", reason);
  }

  public HttpStatus status() {
    return status;
  }

  public String error() {
    return error;
  }

  public String internalReason() {
    return internalReason;
  }
}
