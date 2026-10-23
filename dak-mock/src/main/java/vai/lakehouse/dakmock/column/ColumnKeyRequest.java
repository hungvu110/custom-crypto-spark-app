package vai.lakehouse.dakmock.column;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Tham số đã kiểm của {@code GET /api/v1/keys/by-column}. Kiểm TRƯỚC khi xác minh token, để lỗi định dạng ra {@code 400}
 * mà không cần gọi Keycloak.
 *
 * @param columns tên cột chữ thường, đã gộp trùng, giữ thứ tự xuất hiện đầu tiên
 * @param internalTag bên gọi gửi {@code tag=internal} (luồng nội bộ)
 */
public record ColumnKeyRequest(String database, String table, List<String> columns, OnMissing onMissing,
                               boolean internalTag) {

  public static final int MAX_COLUMNS = 100;
  public static final int MAX_COLUMNS_LENGTH = 4096;
  public static final String INTERNAL_TAG = "internal";

  private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_]{1,128}");

  public ColumnKeyRequest {
    columns = List.copyOf(columns);
  }

  public static ColumnKeyRequest parse(String database, String table, String columns, String onMissing, String tag) {
    String db = tableName(database);
    String tbl = tableName(table);
    List<String> cols = columnNames(columns);
    OnMissing mode = OnMissing.parse(onMissing);
    if (tag != null && !INTERNAL_TAG.equals(tag)) {
      throw DakApiException.invalidTag("tag must be 'internal' or omitted");
    }
    return new ColumnKeyRequest(db, tbl, cols, mode, tag != null);
  }

  /** Kiểm và chuẩn hoá một tên cột đơn lẻ (dùng cho endpoint quản trị). */
  public static String columnName(String raw) {
    if (raw == null || !NAME.matcher(raw).matches()) {
      throw DakApiException.invalidColumn("column names must match " + NAME.pattern());
    }
    return raw.toLowerCase(Locale.ROOT);
  }

  /** Kiểm và chuẩn hoá tên database/table. Hive không phân biệt hoa thường nên luôn chữ thường. */
  public static String tableName(String raw) {
    if (raw == null || !NAME.matcher(raw).matches()) {
      throw DakApiException.invalidTable();
    }
    return raw.toLowerCase(Locale.ROOT);
  }

  public ColumnId id(String column) {
    return new ColumnId(database, table, column);
  }

  private static List<String> columnNames(String raw) {
    if (raw == null || raw.isBlank()) {
      throw DakApiException.invalidColumn("columns must list at least one column");
    }
    Set<String> merged = new LinkedHashSet<>();
    for (String part : raw.split(",", -1)) {
      merged.add(columnName(part.trim()));
    }
    // Giới hạn tính SAU khi gộp trùng (hợp đồng vòng 2 ⑧).
    if (merged.size() > MAX_COLUMNS) {
      throw DakApiException.invalidColumn("at most " + MAX_COLUMNS + " columns per call, got " + merged.size());
    }
    int length = String.join(",", merged).length();
    if (length > MAX_COLUMNS_LENGTH) {
      throw DakApiException.invalidColumn("columns must be at most " + MAX_COLUMNS_LENGTH + " characters after merging, got "
          + length);
    }
    return List.copyOf(merged);
  }
}
