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
import vai.lakehouse.dakmock.global.GlobalKeyResponse;
import vai.lakehouse.dakmock.global.GlobalKeyService;

/**
 * {@code GET /api/v1/keys/global?zone=<vùng>} — key global của một vùng dữ liệu, để nhiều dataset silver mã hoá cột bằng CÙNG một key
 * (docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md mục 2). Không có tham số {@code tag}. {@code 200} có key (header
 * {@code Cache-Control: no-store}), {@code 400 invalid_zone}, {@code 401}, {@code 403 access_denied} / {@code key_disabled}.
 */
@RestController
@RequestMapping("/api/v1/keys")
public class GlobalKeyController {

  private static final Pattern CALLER = Pattern.compile("[A-Za-z0-9._-]{1,128}");

  private final TokenVerifier verifier;
  private final GlobalKeyService keys;

  public GlobalKeyController(TokenVerifier verifier, GlobalKeyService keys) {
    this.verifier = verifier;
    this.keys = keys;
  }

  @GetMapping(value = "/global", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<GlobalKeyResponse> global(
      @RequestParam(value = "zone", required = false) String zone,
      @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization,
      @RequestHeader(value = ColumnKeyController.CALLER_HEADER, required = false) String dakCaller) {
    // Kiểm tham số TRƯỚC khi xác minh token: lỗi định dạng không cần gọi Keycloak.
    String validZone = GlobalKeyService.requireZone(zone);
    TokenVerifier.Caller caller = verifier.verify(authorization);
    String validCaller = dakCaller != null && CALLER.matcher(dakCaller).matches() ? dakCaller : null;
    // Cache-Control: no-store do RequestIdFilter đặt cho mọi response.
    return ResponseEntity.ok(keys.fetch(caller, validZone, validCaller));
  }
}
