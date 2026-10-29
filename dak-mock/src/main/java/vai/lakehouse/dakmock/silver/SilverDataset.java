package vai.lakehouse.dakmock.silver;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A dataset of a source: one Airflow DAG ({@code silver_ingest_<source>_<dataset>}) that runs on the cron of the approved request, one ingest mode, one Airflow Variable
 * of the same name that holds the schedule and the table settings of the approved request. The request {@code path} is the dataset itself,
 * never a day: every run processes the folder of its own day, so a request is approved only once.
 *
 * <p>Mirrors {@code DATASETS} in airflow/dags/silver_ingest_vtt.py and airflow/dags/silver_ingest_vtnet.py:
 * {@code SilverDatasetDagDriftTest} fails when the two drift apart.
 */
public enum SilverDataset {

  /** VTT table: the files are already encrypted by column, so they are written as they are. */
  F001_MPRE_CALL(SilverSource.VTT, "f001-mpre-call", false, true),

  /** Passthrough: files are written as they are, so the request has no columns to encrypt. */
  EBS_3G_4G(SilverSource.VTNET, "ebs_3g_4g", false, true),

  /** Decrypt-encrypt: the whole file is decrypted when DAK has a key for the dataset; the request may list no column to encrypt. */
  NIMS(SilverSource.VTNET, "nims", true, true);

  private final SilverSource source;
  private final String id;
  private final boolean encryptsColumns;
  private final boolean supportsHours;

  SilverDataset(SilverSource source, String id, boolean encryptsColumns, boolean supportsHours) {
    this.source = source;
    this.id = id;
    this.encryptsColumns = encryptsColumns;
    this.supportsHours = supportsHours;
  }

  public SilverSource source() {
    return source;
  }

  /** Dataset name: the request path and the first folder under the landing root. */
  public String id() {
    return id;
  }

  /** DAG id: {@code silver_ingest_<source>_<dataset with "-" turned into "_">}. */
  public String dagId() {
    return "silver_ingest_" + source.name().toLowerCase(Locale.ROOT) + "_" + id.replace('-', '_');
  }

  /** Name of the Airflow Variable that carries the approved request; the same string as the DAG id. */
  public String variableKey() {
    return dagId();
  }

  /** {@code decrypt-encrypt} (may list columns to encrypt) or {@code passthrough} (must not), the names of the DAG modes. */
  public String mode() {
    return encryptsColumns ? "decrypt-encrypt" : "passthrough";
  }

  public boolean encryptsColumns() {
    return encryptsColumns;
  }

  /**
   * {@code true} when the landing folder of a day has an hour layout ({@code hour_glob} in the DAG file), so the dataset can run hourly: each
   * run ingests one hour. Without it, only a daily cron is accepted.
   */
  public boolean supportsHours() {
    return supportsHours;
  }

  /** The datasets of a source, in declaration order. */
  public static List<SilverDataset> of(SilverSource source) {
    return Arrays.stream(values()).filter(d -> d.source == source).toList();
  }

  /** The dataset that {@code path} names; the path must be exactly the dataset name. */
  public static Optional<SilverDataset> of(SilverSource source, String path) {
    return of(source).stream().filter(d -> d.id.equals(path)).findFirst();
  }

  /** Comma separated dataset ids of a source, for messages. */
  public static String ids(SilverSource source) {
    return of(source).stream().map(SilverDataset::id).collect(Collectors.joining(", "));
  }
}
