package vai.lakehouse.dakmock.web;

import java.util.regex.Pattern;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vai.lakehouse.dakmock.auth.TokenVerifier;
import vai.lakehouse.dakmock.column.ColumnKeyRequest;
import vai.lakehouse.dakmock.column.ColumnKeyResponse;
import vai.lakehouse.dakmock.column.ColumnKeyService;

/**
 * {@code GET /api/v1/keys/by-column} — key theo từng cột (docs/dak-mock-api/DAK_MOCK_BY_COLUMN_PLAN.md mục 1). Luôn {@code 200}
 * khi qua được kiểm tham số, token và quyền; trạng thái của từng cột nằm trong body.
 */
@RestController
@RequestMapping("/api/v1/keys")
public class ColumnKeyController {

  public static final String CALLER_HEADER = "X-Dak-Caller";
  private static final Pattern CALLER = Pattern.compile("[A-Za-z0-9._-]{1,128}");

  private final TokenVerifier verifier;
  private final ColumnKeyService keys;

  public ColumnKeyController(TokenVerifier verifier, ColumnKeyService keys) {
    this.verifier = verifier;
    this.keys = keys;
  }

  @GetMapping(value = "/by-column", produces = MediaType.APPLICATION_JSON_VALUE)
  public ColumnKeyResponse byColumn(
      @RequestParam(value = "database", required = false) String database,
      @RequestParam(value = "table", required = false) String table,
      @RequestParam(value = "columns", required = false) String columns,
      @RequestParam(value = "onMissing", required = false) String onMissing,
      @RequestParam(value = "tag", required = false) String tag,
      @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization,
      @RequestHeader(value = CALLER_HEADER, required = false) String dakCaller) {
    // Kiểm tham số TRƯỚC khi xác minh token: lỗi định dạng không cần gọi Keycloak.
    ColumnKeyRequest request = ColumnKeyRequest.parse(database, table, columns, onMissing, tag);
    TokenVerifier.Caller caller = verifier.verify(authorization);
    return keys.fetch(caller, request, validCaller(dakCaller));
  }

  // Header chỉ để audit, do bên gọi tự khai: sai định dạng thì bỏ qua, không từ chối lời gọi.
  private static String validCaller(String value) {
    return value != null && CALLER.matcher(value).matches() ? value : null;
  }
}
