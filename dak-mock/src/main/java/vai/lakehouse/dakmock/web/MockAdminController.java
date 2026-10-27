package vai.lakehouse.dakmock.web;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import vai.lakehouse.dakmock.column.ColumnId;
import vai.lakehouse.dakmock.column.ColumnKeyRequest;
import vai.lakehouse.dakmock.column.ColumnKeyStore;
import vai.lakehouse.dakmock.column.YcRegistry;
import vai.lakehouse.dakmock.path.PathKeyRequest;
import vai.lakehouse.dakmock.path.PathKeyStore;
import vai.lakehouse.dakmock.key.VaultKeyReader.KeyMaterial;

/**
 * Endpoint quản trị riêng của mock, đóng vai "người có quyền bên DAK": xem YC, tạo key cho YC, từ chối YC, khoá/mở khoá cột, và tạo key
 * trực tiếp (không qua YC) cho một đường dẫn ({@code by-path}, do luồng mã hoá file tạo) hoặc một cột.
 * Chỉ bật khi {@code dak.admin.token} có giá trị và request mang đúng header {@code X-Mock-Admin-Token}; ngược lại trả
 * {@code 404} như endpoint không tồn tại. Không response nào chứa giá trị key.
 */
@RestController
@RequestMapping("/mock-admin")
public class MockAdminController {

  public static final String TOKEN_HEADER = "X-Mock-Admin-Token";
  private static final int MAX_KEY_VALUE_LENGTH = 1024;
  private static final int MAX_REASON_LENGTH = 500;
  private static final Logger log = LoggerFactory.getLogger(MockAdminController.class);

  /** Một YC như người quản trị thấy. */
  public record YcView(String requestCode, String database, String table, String column, YcRegistry.State state,
                       String clientId, Instant createdAt, String reasonText) {

    static YcView of(YcRegistry.Yc yc) {
      ColumnId c = yc.column();
      return new YcView(yc.requestCode(), c.database(), c.table(), c.column(), yc.state(), yc.clientId(), yc.createdAt(),
          yc.reasonText());
    }
  }

  public record FulfillRequest(String keyValue) {}

  public record RejectRequest(String reasonText) {}

  /** Kết quả tạo key: không có giá trị key. */
  public record FulfillResult(String requestCode, String database, String table, String column, long keyVersion) {}

  private final MockAdminAuth auth;
  private final YcRegistry registry;
  /** Tạo key cho một đường dẫn ({@code path} tương đối theo gốc, đúng chuỗi SA sẽ gửi {@code by-path}). */
  public record PathKeyCreate(String path, String keyValue) {}

  /** Tạo key cho một cột. */
  public record ColumnKeyCreate(String database, String table, String column, String keyValue) {}

  /** Key đang lưu sau khi tạo (đã có thì là key cũ, không ghi đè). Không có giá trị key. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record KeyCreated(String path, String database, String table, String column, long keyVersion) {}

  private final ColumnKeyStore store;
  private final PathKeyStore pathStore;

  public MockAdminController(MockAdminAuth auth, YcRegistry registry, ColumnKeyStore store, PathKeyStore pathStore) {
    this.auth = auth;
    this.registry = registry;
    this.store = store;
    this.pathStore = pathStore;
  }

  /**
   * Tạo key cho đường dẫn (sinh ngẫu nhiên 32 byte, hoặc lấy {@code keyValue}), vai trò luồng mã hoá file của DAK. Đường dẫn kiểm như
   * {@code by-path} và lưu NGUYÊN CHUỖI. Đã có key thì giữ key cũ.
   */
  @PostMapping("/path-keys")
  public KeyCreated createPathKey(@RequestHeader(value = TOKEN_HEADER, required = false) String token,
                                  @RequestBody(required = false) PathKeyCreate body) {
    authorize(token);
    if (body == null) {
      throw DakApiException.invalidRequest("body must be {\"path\": ..., \"keyValue\": ... (optional)}");
    }
    String path = PathKeyRequest.parse(body.path(), ColumnKeyRequest.INTERNAL_TAG).path();
    KeyMaterial key = pathStore.create(path, keyValue(body.keyValue()));
    log.info("MOCK_ADMIN action=create-path-key path={} keyVersion={}", path, key.version());
    return new KeyCreated(path, null, null, null, key.version());
  }

  /** Khoá key của đường dẫn đã có key: {@code by-path} trả {@code 403 key_disabled}. Body {@code {"path": ...}}. */
  @PostMapping("/path-keys/disable")
  public ResponseEntity<Void> disablePathKey(@RequestHeader(value = TOKEN_HEADER, required = false) String token,
                                             @RequestBody(required = false) PathKeyCreate body) {
    authorize(token);
    String path = pathOf(body);
    if (pathStore.read(path).isEmpty()) {
      throw DakApiException.notFound("Path " + path + " has no key");
    }
    pathStore.disable(path);
    log.info("MOCK_ADMIN action=disable-path-key path={}", path);
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/path-keys/enable")
  public ResponseEntity<Void> enablePathKey(@RequestHeader(value = TOKEN_HEADER, required = false) String token,
                                            @RequestBody(required = false) PathKeyCreate body) {
    authorize(token);
    String path = pathOf(body);
    pathStore.enable(path);
    log.info("MOCK_ADMIN action=enable-path-key path={}", path);
    return ResponseEntity.noContent().build();
  }

  private static String pathOf(PathKeyCreate body) {
    if (body == null) {
      throw DakApiException.invalidRequest("body must be {\"path\": ...}");
    }
    return PathKeyRequest.parse(body.path(), ColumnKeyRequest.INTERNAL_TAG).path();
  }

  /** Tạo key cho cột (sinh ngẫu nhiên 32 byte, hoặc lấy {@code keyValue}). Đã có key thì giữ key cũ. */
  @PostMapping("/column-keys")
  public KeyCreated createColumnKey(@RequestHeader(value = TOKEN_HEADER, required = false) String token,
                                    @RequestBody(required = false) ColumnKeyCreate body) {
    authorize(token);
    if (body == null) {
      throw DakApiException.invalidRequest("body must be {\"database\", \"table\", \"column\", \"keyValue\" (optional)}");
    }
    ColumnId id = columnId(body.database(), body.table(), body.column());
    KeyMaterial key = store.create(id, keyValue(body.keyValue()));
    log.info("MOCK_ADMIN action=create-column-key column={} keyVersion={}", id.qualified(), key.version());
    return new KeyCreated(null, id.database(), id.table(), id.column(), key.version());
  }

  @GetMapping("/yc")
  public List<YcView> list(@RequestHeader(value = TOKEN_HEADER, required = false) String token) {
    authorize(token);
    return registry.list().stream().map(YcView::of).toList();
  }

  /** Tạo key cho YC (sinh ngẫu nhiên, hoặc lấy {@code keyValue} được đưa vào) rồi đóng YC. Không ghi đè key đã có. */
  @PostMapping("/yc/{requestCode}/fulfill")
  public FulfillResult fulfill(@RequestHeader(value = TOKEN_HEADER, required = false) String token,
                               @PathVariable("requestCode") String requestCode,
                               @RequestBody(required = false) FulfillRequest body) {
    authorize(token);
    YcRegistry.Yc yc = registry.byCode(requestCode);
    String value = keyValue(body == null ? null : body.keyValue());
    KeyMaterial key = store.create(yc.column(), value);
    // Xoá YC SAU khi key đã nằm trong Vault: tạo key lỗi thì YC còn nguyên để làm lại.
    registry.remove(requestCode);
    log.info("MOCK_ADMIN action=fulfill requestCode={} column={} keyVersion={}", requestCode, yc.column().qualified(),
        key.version());
    ColumnId c = yc.column();
    return new FulfillResult(requestCode, c.database(), c.table(), c.column(), key.version());
  }

  @PostMapping("/yc/{requestCode}/reject")
  public YcView reject(@RequestHeader(value = TOKEN_HEADER, required = false) String token,
                       @PathVariable("requestCode") String requestCode,
                       @RequestBody(required = false) RejectRequest body) {
    authorize(token);
    String reason = body == null || body.reasonText() == null ? "" : body.reasonText().strip();
    if (reason.length() > MAX_REASON_LENGTH) {
      reason = reason.substring(0, MAX_REASON_LENGTH);
    }
    YcRegistry.Yc rejected = registry.reject(requestCode, reason);
    log.info("MOCK_ADMIN action=reject requestCode={} column={}", requestCode, rejected.column().qualified());
    return YcView.of(rejected);
  }

  /** Khoá một cột ĐÃ có key: lời gọi sau nhận {@code DISABLED}. Key trong Vault giữ nguyên. */
  @PostMapping("/columns/{database}/{table}/{column}/disable")
  public ResponseEntity<Void> disable(@RequestHeader(value = TOKEN_HEADER, required = false) String token,
                                      @PathVariable("database") String database, @PathVariable("table") String table,
                                      @PathVariable("column") String column) {
    authorize(token);
    ColumnId id = columnId(database, table, column);
    if (store.read(id).isEmpty()) {
      throw DakApiException.notFound("Column " + id.qualified() + " has no key");
    }
    store.disable(id);
    log.info("MOCK_ADMIN action=disable column={}", id.qualified());
    return ResponseEntity.noContent().build();
  }

  /** Mở khoá: cột trả lại đúng key cũ. */
  @PostMapping("/columns/{database}/{table}/{column}/enable")
  public ResponseEntity<Void> enable(@RequestHeader(value = TOKEN_HEADER, required = false) String token,
                                     @PathVariable("database") String database, @PathVariable("table") String table,
                                     @PathVariable("column") String column) {
    authorize(token);
    ColumnId id = columnId(database, table, column);
    store.enable(id);
    log.info("MOCK_ADMIN action=enable column={}", id.qualified());
    return ResponseEntity.noContent().build();
  }

  // Sai hoặc thiếu token, hay chưa bật quản trị: 404 như endpoint không tồn tại, không để lộ là có endpoint.
  private void authorize(String token) {
    auth.authorize(token);
  }

  private static ColumnId columnId(String database, String table, String column) {
    return new ColumnId(ColumnKeyRequest.tableName(database), ColumnKeyRequest.tableName(table),
        ColumnKeyRequest.columnName(column));
  }

  private static String keyValue(String given) {
    return given == null ? ColumnKeyStore.newKeyValue() : checkedKeyValue(given);
  }

  private static String checkedKeyValue(String value) {
    if (value.isBlank() || value.length() > MAX_KEY_VALUE_LENGTH) {
      throw DakApiException.invalidRequest("keyValue must be non-blank and at most " + MAX_KEY_VALUE_LENGTH + " characters");
    }
    return value;
  }
}
