package vai.lakehouse.dakmock.web;

import java.util.Locale;
import java.util.regex.Pattern;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vai.lakehouse.dakmock.auth.TokenVerifier;
import vai.lakehouse.dakmock.deid.DeidKeyService;

/**
 * {@code GET /api/v1/keys/deid?database=&table=[&requestCode=]} — key khử định danh của phiếu khai thác, job tự gọi khi chạy
 * (docs/key-access/YEU-CAU-TU-DAK-BO-K8S-SECRET.md mục 2).
 */
@RestController
@RequestMapping("/api/v1/keys")
public class DeidKeyController {

  private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_]{1,128}");
  private static final Pattern REQUEST_CODE = Pattern.compile("(REQ|PYC)-\\d{4}-\\d{6}");

  private final TokenVerifier verifier;
  private final DeidKeyService keys;

  public DeidKeyController(TokenVerifier verifier, DeidKeyService keys) {
    this.verifier = verifier;
    this.keys = keys;
  }

  @GetMapping(value = "/deid", produces = MediaType.APPLICATION_JSON_VALUE)
  public DeidKeyService.DeidKeyResponse deidKey(
      @RequestParam(value = "database", required = false) String database,
      @RequestParam(value = "table", required = false) String table,
      @RequestParam(value = "requestCode", required = false) String requestCode,
      @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization) {
    // Kiểm tham số TRƯỚC khi xác minh token: lỗi định dạng không cần gọi Keycloak.
    String db = name(database);
    String tbl = name(table);
    if (requestCode != null && !REQUEST_CODE.matcher(requestCode).matches()) {
      throw DakApiException.invalidRequest("requestCode must look like REQ-<year>-<6 digits>");
    }
    TokenVerifier.Caller caller = verifier.verify(authorization);
    return keys.fetch(caller, db, tbl, requestCode);
  }

  // Hive không phân biệt hoa thường: chuẩn hoá để cùng một bảng luôn ra cùng một key.
  private static String name(String value) {
    if (value == null || !NAME.matcher(value).matches()) {
      throw DakApiException.invalidTable();
    }
    return value.toLowerCase(Locale.ROOT);
  }
}
