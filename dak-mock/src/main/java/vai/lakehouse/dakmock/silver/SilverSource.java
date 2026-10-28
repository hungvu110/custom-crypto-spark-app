package vai.lakehouse.dakmock.silver;

import java.util.Locale;
import java.util.Optional;

/**
 * Source of a silver ingest request. Every request is RECURRING and names one dataset of its source ({@link SilverDataset}); each dataset has
 * its own DAG. Which datasets take columns to encrypt is a property of the dataset, not of the source (docs/silver-ingest/PLAN.md).
 */
public enum SilverSource {
  VTT("vtt_"),
  VTNET("vtnet_");

  private final String tablePrefix;

  SilverSource(String tablePrefix) {
    this.tablePrefix = tablePrefix;
  }

  /** Tiền tố bắt buộc của bảng đích, để một phiếu không ghi vào bảng của nguồn khác (khớp {@code table_prefix} của DAG). */
  public String tablePrefix() {
    return tablePrefix;
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
