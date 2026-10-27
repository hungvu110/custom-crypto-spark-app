package vai.lakehouse.dakmock.web;

import java.util.List;

import org.springframework.http.HttpStatus;

/**
 * Lỗi trả cho bên gọi theo bảng mã lỗi của docs/key-access/DAK_API_SPEC.md mục 5.4. {@code getMessage()} là thông điệp
 * công khai (không chứa token, secret, prefix); {@code internalReason} chỉ để ghi log.
 */
public final class DakApiException extends RuntimeException {

  /**
   * Một lỗi cụ thể trong nội dung file phiếu khử định danh: {@code field} là đường dẫn trường (vd {@code columns[1].column}),
   * {@code code} là mã lỗi của trường, {@code message} là chuỗi cố định, không chứa giá trị người dùng nhập.
   */
  public record Problem(String field, String code, String message) {}

  private final HttpStatus status;
  private final String error;
  private final String internalReason;
  private final List<Problem> problems;

  private DakApiException(HttpStatus status, String error, String publicMessage, String internalReason) {
    this(status, error, publicMessage, internalReason, List.of());
  }

  private DakApiException(HttpStatus status, String error, String publicMessage, String internalReason,
                          List<Problem> problems) {
    super(publicMessage, null, false, false);
    this.status = status;
    this.error = error;
    this.internalReason = internalReason;
    this.problems = List.copyOf(problems);
  }

  /** File phiếu khử định danh hỏng ({@code problems} rỗng) hoặc có lỗi nội dung ({@code problems} liệt kê đủ). */
  public static DakApiException invalidFile(String publicMessage, List<Problem> problems) {
    return new DakApiException(HttpStatus.BAD_REQUEST, "invalid_file", publicMessage, publicMessage, problems);
  }

  public static DakApiException invalidTable() {
    return new DakApiException(HttpStatus.BAD_REQUEST, "invalid_table",
        "database and table must match [A-Za-z0-9_]{1,128}", "invalid database/table");
  }

  public static DakApiException invalidColumn(String publicMessage) {
    return new DakApiException(HttpStatus.BAD_REQUEST, "invalid_column", publicMessage, publicMessage);
  }

  public static DakApiException invalidTag(String publicMessage) {
    return new DakApiException(HttpStatus.BAD_REQUEST, "invalid_tag", publicMessage, publicMessage);
  }

  public static DakApiException invalidOnMissing() {
    return new DakApiException(HttpStatus.BAD_REQUEST, "invalid_on_missing",
        "onMissing must be 'request' or 'none'", "invalid onMissing");
  }

  public static DakApiException invalidPath(String publicMessage) {
    return new DakApiException(HttpStatus.BAD_REQUEST, "invalid_path", publicMessage, publicMessage);
  }

  /** Key của đường dẫn đang bị khoá: lỗi, KHÔNG phải "không có key" (file vẫn mã hoá). */
  public static DakApiException keyDisabled() {
    return new DakApiException(HttpStatus.FORBIDDEN, "key_disabled", "The key of this path is disabled", "path key disabled");
  }

  /** {@code by-path} chỉ dành cho client nội bộ (tài liệu mục 2: client không đúng loại → 403). */
  public static DakApiException pathAccessDenied() {
    return new DakApiException(HttpStatus.FORBIDDEN, "access_denied", "Path keys are for the internal client only",
        "team client asked for a path key");
  }

  /** Thân request JSON sai: {@code problems} liệt kê từng trường (rỗng nếu cả thân hỏng). */
  public static DakApiException invalidRequest(String publicMessage, List<Problem> problems) {
    return new DakApiException(HttpStatus.BAD_REQUEST, "invalid_request", publicMessage, publicMessage, problems);
  }

  /** Đã có phiếu chuẩn hoá silver đang hoạt động cho cùng nguồn, đường dẫn và ngày lô; {@code existingCode} là phiếu cũ của cùng team. */
  public static DakApiException duplicateBatch(String existingCode) {
    String message = "An active request for the same source, path and batchDate already exists: " + existingCode;
    return new DakApiException(HttpStatus.CONFLICT, "duplicate_request", message, message);
  }

  public static DakApiException invalidRequest(String publicMessage) {
    return new DakApiException(HttpStatus.BAD_REQUEST, "invalid_request", publicMessage, publicMessage);
  }

  public static DakApiException notFound(String publicMessage) {
    return new DakApiException(HttpStatus.NOT_FOUND, "not_found", publicMessage, publicMessage);
  }

  public static DakApiException invalidToken(String reason) {
    return new DakApiException(HttpStatus.UNAUTHORIZED, "invalid_token", "Invalid or missing access token", reason);
  }

  /** Chưa đăng nhập hoặc token portal giả sai: cùng một thông điệp cho mọi nguyên nhân, không lặp lại token. */
  public static DakApiException invalidCredentials() {
    return new DakApiException(HttpStatus.UNAUTHORIZED, "invalid_credentials", "Invalid or missing portal credentials",
        "invalid or missing portal credentials");
  }

  public static DakApiException accessDenied(String database, String table) {
    return new DakApiException(HttpStatus.FORBIDDEN, "access_denied",
        "Team has no access to key " + database + "." + table, "no key or no access");
  }

  /** Thao tác không hợp lệ ở trạng thái hiện tại của phiếu khử định danh; {@code currentState} là tên trạng thái. */
  public static DakApiException invalidState(String currentState) {
    String message = "Operation not allowed in state " + currentState;
    return new DakApiException(HttpStatus.CONFLICT, "invalid_state", message, message);
  }

  /** Đã có phiếu khử định danh đang hoạt động trùng bảng và tập cột; {@code existingCode} là phiếu cũ của cùng team. */
  public static DakApiException duplicateRequest(String existingCode) {
    String message = "An active request for the same table and columns already exists: " + existingCode;
    return new DakApiException(HttpStatus.CONFLICT, "duplicate_request", message, message);
  }

  public static DakApiException tooManyActiveRequests() {
    return new DakApiException(HttpStatus.TOO_MANY_REQUESTS, "too_many_active_requests",
        "Too many active requests for this team", "active request limit reached");
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

  /** Các lỗi nội dung kèm theo; rỗng với mọi lỗi khác. Không sửa được từ bên ngoài. */
  public List<Problem> problems() {
    return problems;
  }
}
