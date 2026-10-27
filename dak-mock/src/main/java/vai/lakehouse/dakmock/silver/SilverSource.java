package vai.lakehouse.dakmock.silver;

import java.util.Locale;
import java.util.Optional;

/**
 * Nguồn dữ liệu của phiếu chuẩn hoá silver và DAG xử lý nguồn đó (airflow/dags/silver_ingest_*.py). Mỗi nguồn xử lý khác nhau
 * (docs/silver-ingest/PLAN.md): VTT chỉ đọc và ghi bảng; VTNET mã hoá các cột người gửi chọn, nên phiếu VTNET phải có
 * {@code encryptColumns} và {@code keyField}.
 */
public enum SilverSource {
  VTT("silver_ingest_vtt", false),
  VTNET("silver_ingest_vtnet", true);

  private final String dagId;
  private final boolean encryptsColumns;

  SilverSource(String dagId, boolean encryptsColumns) {
    this.dagId = dagId;
    this.encryptsColumns = encryptsColumns;
  }

  public String dagId() {
    return dagId;
  }

  /** {@code true}: phiếu phải có {@code encryptColumns} và {@code keyField}; {@code false}: phiếu không được có. */
  public boolean encryptsColumns() {
    return encryptsColumns;
  }

  public static Optional<SilverSource> parse(String raw) {
    if (raw == null) {
      return Optional.empty();
    }
    for (SilverSource s : values()) {
      if (s.name().equals(raw.trim().toUpperCase(Locale.ROOT))) {
        return Optional.of(s);
      }
    }
    return Optional.empty();
  }
}
