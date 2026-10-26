package vai.lakehouse.dakmock.deid;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.UnaryOperator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.NewRequest;
import vai.lakehouse.dakmock.deid.DeidRequest.State;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Phiếu khử định danh trong bộ nhớ (mất khi pod restart; Deployment chạy 1 replica), thread-safe: mọi thao tác chạy dưới
 * một khoá, nên {@link #transition} là compare-and-set nguyên tử. Hai lời gọi cùng muốn rời một trạng thái (duyệt và huỷ cùng
 * lúc) thì đúng một lời gọi thắng, lời gọi còn lại nhận {@code 409 invalid_state}.
 *
 * <p>Mọi tra cứu của người dùng đi qua {@code (team, mã)}: phiếu của team khác và phiếu không tồn tại trả cùng một lỗi
 * {@code 404}, để không lộ phiếu có tồn tại.
 */
@Component
public class DeidRequestRegistry {

  private static final String NOT_FOUND = "Request not found";

  private final int maxActivePerTeam;
  private final Clock clock;
  private final Map<String, DeidRequest> byCode = new LinkedHashMap<>();
  private long sequence;

  @Autowired
  public DeidRequestRegistry(DakProperties props) {
    this(props.deid().maxActivePerTeam(), Clock.systemUTC());
  }

  DeidRequestRegistry(int maxActivePerTeam, Clock clock) {
    this.maxActivePerTeam = maxActivePerTeam;
    this.clock = clock;
  }

  /**
   * Tạo phiếu {@link State#PENDING_APPROVAL}. Từ chối nếu team đã đủ phiếu đang hoạt động ({@code 429}) hoặc đã có phiếu
   * đang hoạt động trùng {@code (database, table, tập cột)} ({@code 409}). Mã phiếu chỉ được cấp khi tạo thành công.
   */
  public synchronized DeidRequest create(NewRequest request) {
    long active = byCode.values().stream()
        .filter(r -> r.state().isActive() && r.team().equals(request.team()))
        .count();
    if (active >= maxActivePerTeam) {
      throw DakApiException.tooManyActiveRequests();
    }
    String key = duplicateKey(request.team(), request.database(), request.table(), request.columns());
    byCode.values().stream()
        .filter(r -> r.state().isActive() && duplicateKey(r.team(), r.database(), r.table(), r.columns()).equals(key))
        .findFirst()
        .ifPresent(existing -> {
          throw DakApiException.duplicateRequest(existing.requestCode());
        });
    Instant now = clock.instant();
    String code = String.format("PYC-%d-%06d", now.atZone(ZoneOffset.UTC).getYear(), ++sequence);
    DeidRequest created = DeidRequest.created(code, now, request);
    byCode.put(code, created);
    return created;
  }

  /** Phiếu của đúng team; rỗng nếu không tồn tại hoặc thuộc team khác. */
  public synchronized Optional<DeidRequest> find(String team, String code) {
    return Optional.ofNullable(byCode.get(code)).filter(r -> r.team().equals(team));
  }

  public synchronized DeidRequest get(String team, String code) {
    return find(team, code).orElseThrow(() -> DakApiException.notFound(NOT_FOUND));
  }

  /** Tra theo mã, không phân biệt team (cho admin). */
  public synchronized Optional<DeidRequest> findByCode(String code) {
    return Optional.ofNullable(byCode.get(code));
  }

  public synchronized DeidRequest getByCode(String code) {
    return findByCode(code).orElseThrow(() -> DakApiException.notFound(NOT_FOUND));
  }

  /** Phiếu của một team, mới nhất trước. */
  public synchronized List<DeidRequest> listForTeam(String team) {
    List<DeidRequest> mine = new ArrayList<>();
    for (DeidRequest r : byCode.values()) {
      if (r.team().equals(team)) {
        mine.add(r);
      }
    }
    Collections.reverse(mine);
    return List.copyOf(mine);
  }

  /** Mọi phiếu, mới nhất trước. */
  public synchronized List<DeidRequest> listAll() {
    List<DeidRequest> all = new ArrayList<>(byCode.values());
    Collections.reverse(all);
    return List.copyOf(all);
  }

  /**
   * Compare-and-set theo mã: chỉ khi trạng thái hiện tại thuộc {@code from} mới gọi {@code change} và lưu kết quả. Ném
   * {@code 404} nếu không có phiếu, {@code 409 invalid_state} nếu trạng thái hiện tại không thuộc {@code from}.
   */
  public synchronized DeidRequest transition(String code, Set<State> from, UnaryOperator<DeidRequest> change) {
    return apply(code, null, from, change);
  }

  /** Như {@link #transition} nhưng phiếu phải thuộc {@code team}; phiếu của team khác trả {@code 404}. */
  public synchronized DeidRequest transitionForTeam(String team, String code, Set<State> from,
                                                    UnaryOperator<DeidRequest> change) {
    return apply(code, team, from, change);
  }

  private DeidRequest apply(String code, String team, Set<State> from, UnaryOperator<DeidRequest> change) {
    DeidRequest current = byCode.get(code);
    if (current == null || (team != null && !current.team().equals(team))) {
      throw DakApiException.notFound(NOT_FOUND);
    }
    if (!from.contains(current.state())) {
      throw DakApiException.invalidState(current.state().name());
    }
    DeidRequest next = change.apply(current);
    // Kết quả sai là lỗi lập trình của bên gọi: không lưu gì, để phiếu giữ nguyên trạng thái cũ.
    if (next == null || !next.requestCode().equals(current.requestCode()) || !next.team().equals(current.team())) {
      throw new IllegalStateException("A transition must return the same request, got a different one for " + code);
    }
    if (next.state() != current.state() && !current.state().canMoveTo(next.state())) {
      throw new IllegalStateException("Illegal transition " + current.state() + " -> " + next.state() + " for " + code);
    }
    byCode.put(code, next);
    return next;
  }

  /** Khoá chống trùng: team, database, table và tập cột (không phụ thuộc thứ tự cột hay chế độ). */
  private static String duplicateKey(String team, String database, String table, List<ColumnSpec> columns) {
    Set<String> names = new TreeSet<>();
    columns.forEach(c -> names.add(c.column()));
    return team + '\0' + database + '\0' + table + '\0' + String.join(",", names);
  }
}
