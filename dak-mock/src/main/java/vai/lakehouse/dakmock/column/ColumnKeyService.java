package vai.lakehouse.dakmock.column;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import vai.lakehouse.dakmock.auth.TokenVerifier.Caller;
import vai.lakehouse.dakmock.column.ColumnKeyResponse.ColumnKey;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.key.VaultKeyReader.KeyMaterial;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Logic của {@code by-column} (docs/dak-mock-api/DAK_MOCK_BY_COLUMN_PLAN.md mục 1). Quyền theo DANH TÍNH client, không theo
 * {@code tag} (bên gọi tự khai): client nội bộ bắt buộc gửi {@code tag=internal}; client team gửi {@code tag=internal} bị 403.
 * <ul>
 *   <li>Client nội bộ: không cần grant, mở YC khi {@code onMissing=request}.</li>
 *   <li>Client team: phải có grant còn hạn trên MỌI cột được hỏi, không thì 403 cả lô — kiểm grant TRƯỚC khi tra key, nên
 *       không dò được cột nào đã có key. Không bao giờ mở YC ({@code onMissing} bị bỏ qua, coi như {@code none}).</li>
 * </ul>
 */
@Service
public class ColumnKeyService {

  private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");

  private final ColumnKeyStore store;
  private final YcRegistry registry;
  private final List<DakProperties.Grant> grants;
  private final DakProperties.Yc yc;
  private final Clock clock;

  @Autowired
  public ColumnKeyService(DakProperties props, ColumnKeyStore store, YcRegistry registry) {
    this(props, store, registry, Clock.systemUTC());
  }

  ColumnKeyService(DakProperties props, ColumnKeyStore store, YcRegistry registry, Clock clock) {
    this.store = store;
    this.registry = registry;
    this.grants = props.grants();
    this.yc = props.yc();
    this.clock = clock;
  }

  /** @param dakCaller giá trị {@code X-Dak-Caller} đã kiểm định dạng, hoặc {@code null}; chỉ để ghi audit */
  public ColumnKeyResponse fetch(Caller caller, ColumnKeyRequest req, String dakCaller) {
    List<ColumnKey> columns;
    if (caller.isInternal()) {
      if (!req.internalTag()) {
        auditError(400, "invalid_tag", caller, dakCaller, req);
        throw DakApiException.invalidTag("internal client must send tag=internal");
      }
      columns = req.columns().stream().map(c -> internalColumn(caller, req.id(c), req.onMissing())).toList();
    } else {
      if (req.internalTag() || !grantedAll(caller.team(), req)) {
        auditError(403, "access_denied", caller, dakCaller, req);
        throw DakApiException.accessDenied(req.database(), req.table());
      }
      columns = req.columns().stream().map(c -> teamColumn(req.id(c))).toList();
    }
    columns.forEach(c -> audit(c, caller, dakCaller, req));
    return ColumnKeyResponse.of(req.database(), req.table(), columns);
  }

  private ColumnKey internalColumn(Caller caller, ColumnId id, OnMissing onMissing) {
    Optional<KeyMaterial> key = store.read(id);
    if (key.isPresent()) {
      return keyState(id, key.get());
    }
    Optional<YcRegistry.Yc> existing = registry.find(id);
    if (existing.isPresent()) {
      return fromYc(existing.get());
    }
    if (onMissing == OnMissing.NONE) {
      return ColumnKey.missing(id.column(), ColumnKeyResponse.NOT_REQUESTED);
    }
    if (yc.autoFulfill()) {
      return keyState(id, store.create(id, ColumnKeyStore.newKeyValue()));
    }
    return registry.open(caller.clientId(), id, yc.maxOpenPerTable())
        .map(this::fromYc)
        .orElseGet(() -> ColumnKey.missing(id.column(), ColumnKeyResponse.YC_LIMIT_REACHED));
  }

  private ColumnKey teamColumn(ColumnId id) {
    return store.read(id)
        .map(key -> keyState(id, key))
        .orElseGet(() -> ColumnKey.missing(id.column(), ColumnKeyResponse.NOT_REQUESTED));
  }

  // Cột bị khoá: key vẫn còn nguyên trong Vault, chỉ không trả ra; mở khoá trả lại đúng key cũ.
  private ColumnKey keyState(ColumnId id, KeyMaterial key) {
    return store.isDisabled(id) ? ColumnKey.disabled(id.column()) : ColumnKey.ready(id.column(), key.keyPrefix(),
        key.version());
  }

  private ColumnKey fromYc(YcRegistry.Yc found) {
    String column = found.column().column();
    return found.state() == YcRegistry.State.REJECTED
        ? ColumnKey.rejected(column, found.requestCode(), found.reasonText())
        : ColumnKey.pending(column, found.requestCode());
  }

  private boolean grantedAll(String team, ColumnKeyRequest req) {
    Instant now = clock.instant();
    return req.columns().stream().allMatch(column -> grants.stream().anyMatch(g ->
        g.team().equals(team) && g.database().equals(req.database()) && g.table().equals(req.table())
            && g.columns().contains(column) && g.activeAt(now)));
  }

  // Không bao giờ ghi keyPrefix, token hay secret.
  private static void audit(ColumnKey c, Caller caller, String dakCaller, ColumnKeyRequest req) {
    AUDIT.info("KEY_FETCH_COLUMN state={} httpStatus=200 clientId={} kind={} team={} caller={} database={} table={} "
            + "column={} requestCode={} reason={}",
        c.state(), caller.clientId(), caller.kind(), orDash(caller.team()), orDash(dakCaller), req.database(),
        req.table(), c.column(), orDash(c.requestCode()), orDash(c.reason()));
  }

  private static void auditError(int status, String error, Caller caller, String dakCaller, ColumnKeyRequest req) {
    AUDIT.info("KEY_FETCH_COLUMN result=ERROR httpStatus={} error={} clientId={} kind={} team={} caller={} database={} "
            + "table={} columns={}",
        status, error, caller.clientId(), caller.kind(), orDash(caller.team()), orDash(dakCaller), req.database(),
        req.table(), String.join(",", req.columns()));
  }

  private static String orDash(String s) {
    return s == null ? "-" : s;
  }
}
