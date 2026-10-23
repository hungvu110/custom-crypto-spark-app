package vai.lakehouse.dakmock.column;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Response {@code 200} của {@code by-column}: mỗi cột một {@code state}; {@code complete = true} khi mọi cột {@code READY}.
 * Field không áp dụng cho trạng thái của cột thì không có mặt trong JSON (không trả {@code null}).
 */
public record ColumnKeyResponse(String database, String table, boolean complete, List<ColumnKey> columns) {

  /** Mã cố định của {@code MISSING}. */
  public static final String NOT_REQUESTED = "not_requested";
  public static final String YC_LIMIT_REACHED = "yc_limit_reached";

  public ColumnKeyResponse {
    columns = List.copyOf(columns);
  }

  public static ColumnKeyResponse of(String database, String table, List<ColumnKey> columns) {
    boolean complete = columns.stream().allMatch(c -> c.state() == State.READY);
    return new ColumnKeyResponse(database, table, complete, columns);
  }

  /** Trạng thái của từng cột (tầng cột, khác trạng thái của YC). */
  public enum State {
    READY,
    PENDING,
    MISSING,
    REJECTED,
    DISABLED
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ColumnKey(String column, State state, String keyPrefix, Long keyVersion, String requestCode,
                          String reason, String reasonText) {

    static ColumnKey ready(String column, String keyPrefix, long keyVersion) {
      return new ColumnKey(column, State.READY, keyPrefix, keyVersion, null, null, null);
    }

    static ColumnKey disabled(String column) {
      return new ColumnKey(column, State.DISABLED, null, null, null, null, null);
    }

    static ColumnKey pending(String column, String requestCode) {
      return new ColumnKey(column, State.PENDING, null, null, requestCode, null, null);
    }

    static ColumnKey rejected(String column, String requestCode, String reasonText) {
      return new ColumnKey(column, State.REJECTED, null, null, requestCode, null, reasonText);
    }

    static ColumnKey missing(String column, String reason) {
      return new ColumnKey(column, State.MISSING, null, null, null, reason, null);
    }
  }
}
