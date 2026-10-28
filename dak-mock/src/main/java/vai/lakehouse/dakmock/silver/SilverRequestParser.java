package vai.lakehouse.dakmock.silver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import vai.lakehouse.dakmock.web.DakApiException;
import vai.lakehouse.dakmock.web.DakApiException.Problem;

/**
 * Validates the JSON body of a silver ingest request and collects EVERY problem in one pass ({@code 400 invalid_request} with
 * {@code problems[]}, fixed messages that never repeat the value typed by the user). Unknown fields are rejected. Every request is RECURRING
 * for one dataset (approved once, the DAG of the dataset runs every day); the rules match the DAGs (airflow/dags/silver_ingest_common.py):
 * <ul>
 *   <li>{@code source}: {@code VTT} or {@code VTNET}.</li>
 *   <li>{@code path}: exactly the name of a {@link SilverDataset} of the source, never a day or a batch. Canonical form: no leading or
 *       trailing {@code /}, no {@code //}, no {@code ..}, only {@code [A-Za-z0-9_.=+-]} and {@code /}, at most 1024 characters.</li>
 *   <li>{@code batchId}: not allowed, each daily run writes the batch of its own day (yyyyMMdd).</li>
 *   <li>{@code scheduleTime}: required, the daily run time {@code HH:mm} in Asia/Ho_Chi_Minh, from 00:00 to 23:59. Each run ingests the day
 *       before.</li>
 *   <li>{@code table}: target table chosen by the requester, {@code [a-z0-9_]{1,128}}, starting with the prefix of the source
 *       ({@code vtt_}, {@code vtnet_}).</li>
 *   <li>{@code outputMode} (optional): {@code overwrite-partitions} or {@code append}.</li>
 *   <li>{@code encryptColumns} (0 to 100 distinct column names, case-insensitive; empty or missing means the files are only decrypted) and
 *       {@code keyField} (required when there are columns to encrypt, not one of them): only for a dataset in decrypt-encrypt mode.
 *       Other requests must not carry them.</li>
 *   <li>{@code purpose}: required, at most 500 characters.</li>
 * </ul>
 */
public final class SilverRequestParser {

  /** A validated request; {@code dagId} is the DAG of its dataset. */
  public record Parsed(SilverSource source, String path, String scheduleTime, String table, String outputMode, List<String> encryptColumns,
                       String keyField, String purpose, String dagId) {}

  public static final String CODE = "invalid_field";
  static final Set<String> OUTPUT_MODES = Set.of("overwrite-partitions", "append");

  private static final Set<String> FIELDS = Set.of("source", "path", "batchId", "scheduleTime", "table", "outputMode", "encryptColumns",
      "keyField", "purpose");
  private static final Pattern TIME = Pattern.compile("([01]\\d|2[0-3]):[0-5]\\d");
  private static final Pattern PATH = Pattern.compile("[A-Za-z0-9_.=+-]+(/[A-Za-z0-9_.=+-]+)*");
  private static final Pattern COLUMN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,127}");
  private static final Pattern TABLE = Pattern.compile("[a-z0-9_]{1,128}");
  private static final int MAX_PATH = 1024;
  private static final int MAX_COLUMNS = 100;
  private static final int MAX_PURPOSE = 500;

  private SilverRequestParser() {}

  public static Parsed parse(JsonNode body) {
    if (body == null || !body.isObject()) {
      throw DakApiException.invalidRequest("body must be a JSON object", List.of());
    }
    List<Problem> problems = new ArrayList<>();
    for (Iterator<String> it = body.fieldNames(); it.hasNext(); ) {
      String name = it.next();
      if (!FIELDS.contains(name)) {
        problems.add(new Problem(safe(name), "unknown_field", "field is not allowed"));
      }
    }
    SilverSource source = text(body, "source").flatMap(SilverSource::parse).orElse(null);
    if (source == null) {
      problems.add(new Problem("source", CODE, "source must be VTT or VTNET"));
    }
    String path = text(body, "path").orElse(null);
    boolean pathOk = path != null && path.length() <= MAX_PATH && PATH.matcher(path).matches() && !path.contains("..");
    if (!pathOk) {
      problems.add(new Problem("path", CODE, "path must be relative to the landing root of the source: no leading or trailing "
          + "'/', no '//', no '..', only [A-Za-z0-9_.=+-] and '/', at most " + MAX_PATH + " characters"));
    }
    SilverDataset dataset = null;
    if (pathOk && source != null) {
      dataset = SilverDataset.of(source, path).orElse(null);
      if (dataset == null) {
        problems.add(new Problem("path", CODE,
            "path must be exactly the name of a known " + source.name() + " dataset (" + SilverDataset.ids(source) + ")"));
      }
    }
    if (body.has("batchId")) {
      problems.add(new Problem("batchId", CODE, "batchId is not allowed: every scheduled run writes the batch of its own day"));
    }
    String scheduleTime = text(body, "scheduleTime").orElse(null);
    if (source != null && (scheduleTime == null || !TIME.matcher(scheduleTime).matches())) {
      problems.add(new Problem("scheduleTime", CODE, "scheduleTime must be the daily run time HH:mm, from 00:00 to 23:59"));
    }
    String table = text(body, "table").orElse(null);
    if (table == null || !TABLE.matcher(table).matches() || (source != null && !table.startsWith(source.tablePrefix()))) {
      problems.add(new Problem("table", CODE, "table must be lowercase letters, digits and '_' (1 to 128), starting with the prefix of the source"));
    }
    String outputMode = text(body, "outputMode").orElse(null);
    if (body.has("outputMode") && (outputMode == null || !OUTPUT_MODES.contains(outputMode))) {
      problems.add(new Problem("outputMode", CODE, "outputMode must be overwrite-partitions or append"));
    }
    List<String> columns = List.of();
    String keyField = null;
    if (dataset != null && dataset.encryptsColumns()) {
      columns = columns(body, problems);
      keyField = keyField(body, columns, problems);
    } else if (dataset != null) {
      for (String field : List.of("encryptColumns", "keyField")) {
        if (body.has(field)) {
          problems.add(new Problem(field, CODE, field + " is only allowed for a dataset in decrypt-encrypt mode"));
        }
      }
    }
    String purpose = text(body, "purpose").map(String::strip).orElse("");
    if (purpose.isEmpty() || purpose.length() > MAX_PURPOSE || purpose.chars().anyMatch(Character::isISOControl)) {
      problems.add(new Problem("purpose", CODE, "purpose is required, at most " + MAX_PURPOSE + " characters, no control characters"));
    }
    if (!problems.isEmpty()) {
      throw DakApiException.invalidRequest("the request has " + problems.size() + " problem(s)", problems);
    }
    String dagId = dataset.dagId();
    return new Parsed(source, path, scheduleTime, table, outputMode, columns, keyField, purpose, dagId);
  }

  /** Optional list: missing, null or empty means that no column is encrypted. */
  private static List<String> columns(JsonNode body, List<Problem> problems) {
    JsonNode node = body.path("encryptColumns");
    if (node.isMissingNode() || node.isNull()) {
      return List.of();
    }
    List<String> columns = new ArrayList<>();
    boolean ok = node.isArray() && node.size() <= MAX_COLUMNS;
    if (ok) {
      Set<String> seen = new HashSet<>();
      for (JsonNode c : node) {
        if (!c.isTextual() || !COLUMN.matcher(c.asText()).matches()) {
          ok = false;
        } else if (!seen.add(c.asText().toLowerCase(Locale.ROOT))) {
          problems.add(new Problem("encryptColumns", CODE, "encryptColumns has a duplicated column (case-insensitive)"));
        } else {
          columns.add(c.asText());
        }
      }
    }
    if (!ok) {
      problems.add(new Problem("encryptColumns", CODE,
          "encryptColumns must be a list of up to " + MAX_COLUMNS + " column names [A-Za-z_][A-Za-z0-9_]*"));
    }
    return columns;
  }

  /** Required when there are columns to encrypt; an empty or missing keyField is the same as none. */
  private static String keyField(JsonNode body, List<String> columns, List<Problem> problems) {
    JsonNode node = body.path("keyField");
    boolean absent = node.isMissingNode() || node.isNull() || (node.isTextual() && node.asText().isEmpty());
    if (absent) {
      if (!columns.isEmpty()) {
        problems.add(new Problem("keyField", CODE, "keyField is required when encryptColumns is not empty"));
      }
      return null;
    }
    if (!node.isTextual() || !COLUMN.matcher(node.asText()).matches()) {
      problems.add(new Problem("keyField", CODE, "keyField must be a column name [A-Za-z_][A-Za-z0-9_]*"));
      return null;
    }
    if (columns.stream().anyMatch(c -> c.equalsIgnoreCase(node.asText()))) {
      problems.add(new Problem("keyField", CODE, "keyField must not be listed in encryptColumns"));
    }
    return node.asText();
  }

  private static Optional<String> text(JsonNode body, String field) {
    JsonNode node = body.path(field);
    return node.isTextual() ? Optional.of(node.asText()) : Optional.empty();
  }

  /** Tên trường do người dùng đặt: chỉ giữ [A-Za-z0-9_], tối đa 64 ký tự. */
  private static String safe(String name) {
    String s = name.replaceAll("[^A-Za-z0-9_]", "_");
    return s.length() > 64 ? s.substring(0, 64) : s;
  }
}
