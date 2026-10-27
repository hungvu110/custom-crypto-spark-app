package vai.lakehouse.dakmock.silver;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Sổ phiếu chuẩn hoá silver trong bộ nhớ (mất khi pod restart, như phiếu khử định danh). Mã phiếu {@code SLV-<năm>-<6 số>}. Chống trùng:
 * một team không có hai phiếu CHƯA KẾT THÚC cho cùng {@code (source, path, batchId)}.
 */
@Component
public class SilverRequestRegistry {

  private final Clock clock;
  private final Map<String, SilverRequest> byCode = new LinkedHashMap<>();
  private long sequence;

  @Autowired
  public SilverRequestRegistry() {
    this(Clock.systemUTC());
  }

  SilverRequestRegistry(Clock clock) {
    this.clock = clock;
  }

  public Instant now() {
    return clock.instant();
  }

  /** Thêm phiếu mới; trùng phiếu đang hoạt động thì {@code 409 duplicate_request}. */
  public synchronized SilverRequest create(String team, String user, SilverRequestParser.Parsed parsed) {
    byCode.values().stream()
        .filter(r -> !r.state().terminal() && r.team().equals(team) && r.source() == parsed.source()
            && r.path().equals(parsed.path()) && r.batchId().equals(parsed.batchId()))
        .findFirst()
        .ifPresent(r -> {
          throw DakApiException.duplicateBatch(r.code());
        });
    Instant now = clock.instant();
    String code = String.format("SLV-%d-%06d", now.atZone(ZoneOffset.UTC).getYear(), ++sequence);
    SilverRequest request = SilverRequest.pending(code, team, user, parsed, now);
    byCode.put(code, request);
    return request;
  }

  public synchronized Optional<SilverRequest> find(String code) {
    return Optional.ofNullable(byCode.get(code));
  }

  /** Đổi phiếu bằng {@code change} (nguyên tử); không có phiếu thì {@code 404}. */
  public synchronized SilverRequest update(String code, UnaryOperator<SilverRequest> change) {
    SilverRequest current = byCode.get(code);
    if (current == null) {
      throw DakApiException.notFound("Unknown request " + code);
    }
    SilverRequest next = change.apply(current);
    byCode.put(code, next);
    return next;
  }

  /** Mới nhất trước; {@code team} null = mọi team (admin). */
  public synchronized List<SilverRequest> list(String team) {
    return byCode.values().stream()
        .filter(r -> team == null || r.team().equals(team))
        .sorted(Comparator.comparing(SilverRequest::code).reversed())
        .toList();
  }
}
