package vai.lakehouse.dakmock.web;

import java.util.Locale;
import java.util.regex.Pattern;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import vai.lakehouse.dakmock.auth.TokenVerifier;
import vai.lakehouse.dakmock.key.KeyService;

/** {@code GET /api/v1/keys/{database}/{table}} — API duy nhất của mock (docs/DAK_API_SPEC.md mục 5). */
@RestController
@RequestMapping("/api/v1/keys")
public class KeyController {

  private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_]{1,128}");

  private final TokenVerifier verifier;
  private final KeyService keys;

  public KeyController(TokenVerifier verifier, KeyService keys) {
    this.verifier = verifier;
    this.keys = keys;
  }

  @GetMapping(value = "/{database}/{table}", produces = MediaType.APPLICATION_JSON_VALUE)
  public KeyService.KeyResponse getKey(
      @PathVariable("database") String database,
      @PathVariable("table") String table,
      @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization) {
    // Kiểm tra tên TRƯỚC khi xác minh token (spec: 400 không cần gọi Keycloak).
    String db = normalize(database);
    String tbl = normalize(table);
    TokenVerifier.Caller caller = verifier.verify(authorization);
    return keys.fetch(caller, db, tbl);
  }

  // Hive không phân biệt hoa thường: chuẩn hoá để cùng một bảng luôn ra cùng một key.
  private static String normalize(String name) {
    if (name == null || !NAME.matcher(name).matches()) {
      throw DakApiException.invalidTable();
    }
    return name.toLowerCase(Locale.ROOT);
  }
}
