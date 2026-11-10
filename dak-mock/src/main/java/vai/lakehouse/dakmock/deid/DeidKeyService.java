package vai.lakehouse.dakmock.deid;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import vai.lakehouse.dakmock.auth.TokenVerifier.Caller;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.deid.DeidRequest.State;
import vai.lakehouse.dakmock.key.VaultKeyReader;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * {@code GET /api/v1/keys/deid}: job tự lấy key khử định danh của phiếu khai thác khi chạy, thay cho Secret
 * {@code deid-key-<mã>} (docs/key-access/YEU-CAU-TU-DAK-BO-K8S-SECRET.md mục 2).
 *
 * <p>Bên gọi là một trong hai loại client (DAK đã xác nhận luồng khử định danh vẫn dùng client NỘI BỘ):
 * <ul>
 *   <li>client của team: chỉ tính phiếu của team trong token (không có tham số {@code team});</li>
 *   <li>client nội bộ (không thuộc team nào): tính phiếu của mọi team, key đọc ở tenant/workspace của team chủ phiếu.</li>
 * </ul>
 * Chỉ tính phiếu đúng bảng, đã được duyệt và còn dùng được ({@link #USABLE}); không có thì {@code 403} (cùng mã cho mọi nguyên
 * nhân), nhiều phiếu mà không gửi {@code requestCode} thì {@code 409} (không tự chọn). Mock chưa mô hình hoá hạn phiếu/thu hồi
 * quyền: phiếu đã kết thúc là hết hạn.
 */
@Service
public class DeidKeyService {

  /** Phiếu đã duyệt mà job còn cần key: đã duyệt, gửi lỗi (chờ retry), đã gửi job, đang chạy. */
  static final Set<State> USABLE = Set.of(State.APPROVED, State.SUBMIT_FAILED, State.JOB_SUBMITTED, State.RUNNING);

  private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");

  /** Response 200 (mục 2.1 của tài liệu). */
  public record DeidKeyResponse(String requestCode, String database, String table, String keyPrefix, long keyVersion) {}

  private final DeidRequestRegistry registry;
  private final VaultKeyReader vault;
  private final Supplier<List<PortalUser>> portalUsers;

  @org.springframework.beans.factory.annotation.Autowired
  public DeidKeyService(DeidRequestRegistry registry, VaultKeyReader vault, DakProperties props) {
    this(registry, vault, () -> props.deid().portalUsers());
  }

  DeidKeyService(DeidRequestRegistry registry, VaultKeyReader vault, Supplier<List<PortalUser>> portalUsers) {
    this.registry = registry;
    this.vault = vault;
    this.portalUsers = portalUsers;
  }

  public DeidKeyResponse fetch(Caller caller, String database, String table, String requestCode) {
    boolean internal = caller.isInternal();
    if (!internal && (caller.team() == null || caller.team().isBlank())) {
      deny("NOT_A_TEAM", caller, database, table, requestCode);
      throw DakApiException.accessDenied(database, table);
    }
    List<DeidRequest> candidates = internal ? registry.listAll() : registry.listForTeam(caller.team());
    List<DeidRequest> matches = candidates.stream()
        .filter(r -> USABLE.contains(r.state()))
        .filter(r -> r.database().equalsIgnoreCase(database) && r.table().equalsIgnoreCase(table))
        .filter(r -> requestCode == null || r.requestCode().equals(requestCode))
        .toList();
    if (matches.isEmpty()) {
      deny("NO_APPROVED_REQUEST", caller, database, table, requestCode);
      throw DakApiException.accessDenied(database, table);
    }
    if (matches.size() > 1) {
      audit("ERROR", 409, "ambiguous_request", "-", caller, database, table, matches.get(0).requestCode(), null);
      throw DakApiException.ambiguousRequest(matches.stream().map(DeidRequest::requestCode).sorted().toList());
    }
    DeidRequest request = matches.get(0);
    try {
      String[] scope = scopeOf(caller, request);
      VaultKeyReader.KeyMaterial key = vault.read(scope[0], scope[1], database, table);
      audit("ALLOWED", 200, "-", "-", caller, database, table, request.requestCode(), key.version());
      return new DeidKeyResponse(request.requestCode(), database, table, key.keyPrefix(), key.version());
    } catch (VaultKeyReader.KeyNotFoundException e) {
      deny("KEY_NOT_FOUND", caller, database, table, request.requestCode());
      throw DakApiException.accessDenied(database, table);
    }
  }

  /** {tenant, workspace} nơi đọc key: của team trong token, hoặc (client nội bộ) của team chủ phiếu. */
  private String[] scopeOf(Caller caller, DeidRequest request) {
    if (!caller.isInternal()) {
      return new String[] {caller.tenant(), caller.workspace()};
    }
    PortalUser owner = portalUsers.get().stream().filter(u -> request.team().equals(u.team())).findFirst().orElse(null);
    String tenant = owner != null && owner.tenant() != null ? owner.tenant() : caller.tenant();
    String workspace = owner != null && owner.workspace() != null ? owner.workspace() : "";
    return new String[] {tenant, workspace};
  }

  private static void deny(String reason, Caller caller, String database, String table, String requestCode) {
    audit("DENIED", 403, "access_denied", reason, caller, database, table, requestCode, null);
  }

  // Không bao giờ ghi keyPrefix, token hay secret.
  private static void audit(String result, int status, String error, String denyReason, Caller caller, String database,
                            String table, String requestCode, Long keyVersion) {
    AUDIT.info("DEID_KEY_FETCH result={} httpStatus={} error={} denyReason={} tenant={} workspace={} clientId={} team={} "
            + "database={} table={} requestCode={} keyVersion={}",
        result, status, error, denyReason, caller.tenant(), caller.workspace(), caller.clientId(), caller.team(),
        database, table, requestCode == null ? "-" : requestCode, keyVersion == null ? "-" : keyVersion);
  }
}
