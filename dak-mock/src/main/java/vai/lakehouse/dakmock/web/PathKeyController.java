package vai.lakehouse.dakmock.web;

import java.util.regex.Pattern;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vai.lakehouse.dakmock.auth.TokenVerifier;
import vai.lakehouse.dakmock.path.PathKeyRequest;
import vai.lakehouse.dakmock.path.PathKeyResponse;
import vai.lakehouse.dakmock.path.PathKeyService;

/**
 * {@code GET /api/v1/keys/by-path?path=...&tag=internal} — key giải mã toàn bộ nội dung một file (hoặc mọi file dưới một thư mục)
 * theo đường dẫn (docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md mục 2). {@code 200} có key, {@code 204} rỗng khi chưa có key (không mở
 * yêu cầu — docs/key-access/TRA-LOI-DAK-BY-PATH.md).
 */
@RestController
@RequestMapping("/api/v1/keys")
public class PathKeyController {

  private static final Pattern CALLER = Pattern.compile("[A-Za-z0-9._-]{1,128}");

  private final TokenVerifier verifier;
  private final PathKeyService keys;

  public PathKeyController(TokenVerifier verifier, PathKeyService keys) {
    this.verifier = verifier;
    this.keys = keys;
  }

  @GetMapping(value = "/by-path", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<PathKeyResponse.Found> byPath(
      @RequestParam(value = "path", required = false) String path,
      @RequestParam(value = "tag", required = false) String tag,
      @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization,
      @RequestHeader(value = ColumnKeyController.CALLER_HEADER, required = false) String dakCaller) {
    // Kiểm tham số TRƯỚC khi xác minh token: lỗi định dạng không cần gọi Keycloak.
    PathKeyRequest request = PathKeyRequest.parse(path, tag);
    TokenVerifier.Caller caller = verifier.verify(authorization);
    String validCaller = dakCaller != null && CALLER.matcher(dakCaller).matches() ? dakCaller : null;
    return keys.fetch(caller, request, validCaller).map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.noContent().build());
  }
}
