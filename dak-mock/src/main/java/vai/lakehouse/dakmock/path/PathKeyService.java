package vai.lakehouse.dakmock.path;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import vai.lakehouse.dakmock.auth.TokenVerifier.Caller;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Logic của {@code by-path}. Chỉ client nội bộ: loại client khác nhận {@code 403}. Có key → {@code 200}; chưa có key → rỗng
 * ({@code 204}), KHÔNG mở yêu cầu; key đang bị khoá → {@code 403 key_disabled} (không im lặng {@code 204}, tránh đọc dữ liệu mã hoá như
 * nguyên trạng). Chưa có key → rỗng ({@code 204}), KHÔNG mở yêu cầu: với luồng chuẩn hoá, đường dẫn không có key nghĩa là file của nó không mã hoá (đã thống nhất với DAK,
 * docs/key-access/TRA-LOI-DAK-BY-PATH.md mục 5). Key theo đường dẫn do luồng mã hoá file tạo,
 * ở mock là {@code POST /mock-admin/path-keys}. Một đường dẫn một key, kể cả khi đường dẫn là thư mục.
 */
@Service
public class PathKeyService {

  private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");

  private final PathKeyStore store;

  public PathKeyService(PathKeyStore store) {
    this.store = store;
  }

  /**
   * @param dakCaller giá trị {@code X-Dak-Caller} đã kiểm định dạng, hoặc {@code null}; chỉ để ghi audit
   * @return key nếu có, rỗng nếu đường dẫn chưa có key
   */
  public Optional<PathKeyResponse.Found> fetch(Caller caller, PathKeyRequest req, String dakCaller) {
    if (!caller.isInternal()) {
      audit("ERROR", 403, caller, dakCaller, req);
      throw DakApiException.pathAccessDenied();
    }
    Optional<vai.lakehouse.dakmock.key.VaultKeyReader.KeyMaterial> key = store.read(req.path());
    if (key.isPresent() && store.isDisabled(req.path())) {
      audit("DISABLED", 403, caller, dakCaller, req);
      throw DakApiException.keyDisabled();
    }
    Optional<PathKeyResponse.Found> found = key
        .map(k -> new PathKeyResponse.Found(req.path(), k.keyPrefix(), k.version()));
    audit(found.isPresent() ? "READY" : "NONE", found.isPresent() ? 200 : 204, caller, dakCaller, req);
    return found;
  }

  // Không bao giờ ghi keyPrefix, token hay secret.
  private static void audit(String state, int status, Caller caller, String dakCaller, PathKeyRequest req) {
    AUDIT.info("KEY_FETCH_PATH state={} httpStatus={} clientId={} kind={} caller={} path={}",
        state, status, caller.clientId(), caller.kind(), dakCaller == null ? "-" : dakCaller, req.path());
  }
}
