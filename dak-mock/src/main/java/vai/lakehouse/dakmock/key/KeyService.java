package vai.lakehouse.dakmock.key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import vai.lakehouse.dakmock.auth.TokenVerifier.Caller;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Lấy key cho team đã xác minh. Mock KHÔNG kiểm tra grant/thời hạn quyền: team nào có token hợp lệ cũng lấy được
 * mọi key có trong Vault. Tenant/workspace luôn lấy từ {@link Caller}, không từ request.
 */
@Service
public class KeyService {

  private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");

  /** Response 200 của API lấy key (docs/DAK_API_SPEC.md mục 5.2). */
  public record KeyResponse(String database, String table, String keyPrefix, long keyVersion) {}

  private final VaultKeyReader vault;

  public KeyService(VaultKeyReader vault) {
    this.vault = vault;
  }

  public KeyResponse fetch(Caller caller, String database, String table) {
    try {
      VaultKeyReader.KeyMaterial key = vault.read(caller.tenant(), caller.workspace(), database, table);
      audit("ALLOWED", 200, "-", "-", caller, database, table, key.version());
      return new KeyResponse(database, table, key.keyPrefix(), key.version());
    } catch (VaultKeyReader.KeyNotFoundException e) {
      // Bảng không có key -> cùng 403 như "không có quyền", để không dò được bảng của nơi khác.
      audit("DENIED", 403, "access_denied", "KEY_NOT_FOUND", caller, database, table, null);
      throw DakApiException.accessDenied(database, table);
    } catch (DakApiException e) {
      audit("ERROR", e.status().value(), e.error(), "-", caller, database, table, null);
      throw e;
    }
  }

  // Không bao giờ ghi keyPrefix, token hay secret.
  private static void audit(String result, int status, String error, String denyReason, Caller caller,
                            String database, String table, Long keyVersion) {
    AUDIT.info("KEY_FETCH result={} httpStatus={} error={} denyReason={} realm={} tenant={} workspace={} "
            + "clientId={} team={} database={} table={} keyVersion={}",
        result, status, error, denyReason, caller.realm(), caller.tenant(), caller.workspace(),
        caller.clientId(), caller.team(), database, table, keyVersion == null ? "-" : keyVersion);
  }
}
