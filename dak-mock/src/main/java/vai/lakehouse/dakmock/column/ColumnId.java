package vai.lakehouse.dakmock.column;

/** Định danh của một key cột: {@code (database, table, column)}, đã kiểm bộ ký tự và chuẩn hoá chữ thường. */
public record ColumnId(String database, String table, String column) {

  /** Dạng {@code database.table.column}, dùng cho log. */
  public String qualified() {
    return database + "." + table + "." + column;
  }
}
