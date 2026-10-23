package vai.lakehouse.dakmock.column;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * YC của luồng nội bộ, giữ trong bộ nhớ (mất khi pod restart; Deployment chạy 1 replica). Mỗi cột tối đa một YC:
 * gọi lại cùng cột trả lại YC đó, kể cả từ client khác. YC {@code REJECTED} giữ nguyên cho cột để không mở YC mới
 * (vòng 4 ⑤b). YC được xoá khi người có quyền tạo key ({@link #remove}).
 */
@Component
public class YcRegistry {

  public enum State {
    OPEN,
    REJECTED
  }

  /** Một YC. Không chứa key. */
  public record Yc(String requestCode, ColumnId column, State state, String clientId, Instant createdAt,
                   String reasonText) {

    Yc rejected(String reason) {
      return new Yc(requestCode, column, State.REJECTED, clientId, createdAt, reason);
    }
  }

  private final Clock clock;
  private final Map<ColumnId, Yc> byColumn = new LinkedHashMap<>();
  private long sequence;

  public YcRegistry() {
    this(Clock.systemUTC());
  }

  YcRegistry(Clock clock) {
    this.clock = clock;
  }

  public synchronized Optional<Yc> find(ColumnId column) {
    return Optional.ofNullable(byColumn.get(column));
  }

  /**
   * YC của cột nếu đã có; không thì mở YC mới. Rỗng khi {@code clientId} đã có {@code maxOpenPerTable} YC {@code OPEN}
   * trên cùng {@code (database, table)}.
   */
  public synchronized Optional<Yc> open(String clientId, ColumnId column, int maxOpenPerTable) {
    Yc existing = byColumn.get(column);
    if (existing != null) {
      return Optional.of(existing);
    }
    long openForTable = byColumn.values().stream()
        .filter(yc -> yc.state() == State.OPEN && yc.clientId().equals(clientId)
            && yc.column().database().equals(column.database()) && yc.column().table().equals(column.table()))
        .count();
    if (openForTable >= maxOpenPerTable) {
      return Optional.empty();
    }
    Instant now = clock.instant();
    String code = String.format("YC-%d-%06d", now.atZone(ZoneOffset.UTC).getYear(), ++sequence);
    Yc yc = new Yc(code, column, State.OPEN, clientId, now, null);
    byColumn.put(column, yc);
    return Optional.of(yc);
  }

  public synchronized Yc byCode(String requestCode) {
    return byColumn.values().stream()
        .filter(yc -> yc.requestCode().equals(requestCode))
        .findFirst()
        .orElseThrow(() -> DakApiException.notFound("Unknown YC " + requestCode));
  }

  public synchronized Yc reject(String requestCode, String reasonText) {
    Yc rejected = byCode(requestCode).rejected(reasonText);
    byColumn.put(rejected.column(), rejected);
    return rejected;
  }

  /** Xoá YC sau khi cột đã có key. */
  public synchronized Yc remove(String requestCode) {
    Yc yc = byCode(requestCode);
    byColumn.remove(yc.column());
    return yc;
  }

  public synchronized List<Yc> list() {
    return List.copyOf(byColumn.values());
  }
}
