package vai.lakehouse.dakmock.global;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import vai.lakehouse.dakmock.auth.TokenVerifier.Caller;
import vai.lakehouse.dakmock.key.VaultKeyReader.KeyMaterial;
import vai.lakehouse.dakmock.silver.SilverRequestParser;
import vai.lakehouse.dakmock.silver.SilverRequestRegistry;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Logic của {@code GET /api/v1/keys/global?zone=} (docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md mục 2). DAK phát key khi CÓ ÍT NHẤT MỘT phiếu
 * silver hiệu lực (đã duyệt hoặc đang chạy lịch) chọn đúng vùng đó VÀ token thuộc bên được phép: client nội bộ (cùng client của
 * {@code by-path}/{@code by-column}) hoặc client chính của team gửi phiếu. Không đạt thì {@code 403 access_denied} — CÙNG một câu trả lời với
 * vùng không tồn tại hoặc chưa có key, để không dò được vùng nào có. Key bị khoá: {@code 403 key_disabled}. Mock không giới hạn tốc độ
 * ({@code 429}). Mọi lần lấy key đều ghi audit (không ghi keyPrefix).
 */
@Service
public class GlobalKeyService {

  private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");

  private final GlobalKeyStore store;
  private final SilverRequestRegistry silver;

  public GlobalKeyService(GlobalKeyStore store, SilverRequestRegistry silver) {
    this.store = store;
    this.silver = silver;
  }

  /** Kiểm định dạng vùng, trước khi xác minh token (lỗi định dạng không cần gọi Keycloak). */
  public static String requireZone(String zone) {
    if (zone == null || !SilverRequestParser.ZONE.matcher(zone).matches()) {
      throw DakApiException.invalidZone();
    }
    return zone;
  }

  /** @param dakCaller giá trị {@code X-Dak-Caller} đã kiểm định dạng, hoặc {@code null}; chỉ để ghi audit */
  public GlobalKeyResponse fetch(Caller caller, String zone, String dakCaller) {
    if (!silver.hasEffectiveGlobalRequest(zone, caller.isInternal(), caller.team())) {
      audit("DENIED", 403, caller, dakCaller, zone);
      throw DakApiException.globalKeyAccessDenied();
    }
    KeyMaterial key = store.read(zone).orElse(null);
    if (key == null) {
      // Có phiếu nhưng admin chưa tạo key của vùng: vẫn là access_denied, giống vùng không tồn tại.
      audit("NO_KEY", 403, caller, dakCaller, zone);
      throw DakApiException.globalKeyAccessDenied();
    }
    if (store.isDisabled(zone)) {
      audit("DISABLED", 403, caller, dakCaller, zone);
      throw DakApiException.globalKeyDisabled();
    }
    audit("READY", 200, caller, dakCaller, zone);
    return new GlobalKeyResponse(zone, key.keyPrefix(), key.version());
  }

  // Không bao giờ ghi keyPrefix, token hay secret.
  private static void audit(String state, int status, Caller caller, String dakCaller, String zone) {
    AUDIT.info("KEY_FETCH_GLOBAL state={} httpStatus={} clientId={} kind={} team={} caller={} zone={}",
        state, status, caller.clientId(), caller.kind(), caller.team() == null ? "-" : caller.team(),
        dakCaller == null ? "-" : dakCaller, zone);
  }
}
