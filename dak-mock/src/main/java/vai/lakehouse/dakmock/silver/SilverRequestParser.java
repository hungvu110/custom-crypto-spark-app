package vai.lakehouse.dakmock.silver;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import vai.lakehouse.dakmock.web.DakApiException;
import vai.lakehouse.dakmock.web.DakApiException.Problem;

/**
 * Kiểm thân JSON của phiếu chuẩn hoá silver, gom MỌI lỗi một lượt ({@code 400 invalid_request} kèm {@code problems[]}, thông điệp cố
 * định, không lặp lại giá trị người dùng nhập). Trường lạ bị từ chối. Quy tắc khớp DAG (airflow/dags/silver_ingest_common.py):
 * <ul>
 *   <li>{@code source}: {@code VTT} hoặc {@code VTNET}.</li>
 *   <li>{@code path}: tương đối theo gốc landing của nguồn, dạng chuẩn — không {@code /} đầu/cuối, không {@code //}, không {@code ..},
 *       chỉ {@code [A-Za-z0-9_.=+-]} và {@code /}, tối đa 1024 ký tự. Giữ nguyên hoa thường.</li>
 *   <li>{@code batchId}: {@code yyyyMMddHH} (theo giờ) hoặc {@code yyyyMMddHHmm} (theo batch 20 phút), ngày giờ có thật. Hai lô khác nhau
 *       của cùng một bảng phải có {@code batchId} khác nhau, không thì lô sau ghi đè lô trước.</li>
 *   <li>{@code table}: bảng đích do người gửi chọn, {@code [a-z0-9_]{1,128}}, bắt đầu bằng tiền tố của nguồn ({@code vtt_}, {@code vtnet_}).</li>
 *   <li> {@code outputMode} (tuỳ chọn): {@code overwrite-partitions} hoặc
 *       {@code append}.</li>
 *   <li>VTNET: {@code encryptColumns} (1 đến 100 tên cột, không trùng, không phân biệt hoa thường) và {@code keyField} (không nằm
 *       trong {@code encryptColumns}) bắt buộc. VTT: không được có hai trường này.</li>
 *   <li>{@code purpose}: bắt buộc, tối đa 500 ký tự.</li>
 * </ul>
 */
public final class SilverRequestParser {

  /** Phiếu đã kiểm. */
  public record Parsed(SilverSource source, String path, String batchId, String table, String outputMode, List<String> encryptColumns,
                       String keyField, String purpose) {}

  public static final String CODE = "invalid_field";
  static final Set<String> OUTPUT_MODES = Set.of("overwrite-partitions", "append");

  private static final Set<String> FIELDS = Set.of("source", "path", "batchId", "table", "outputMode", "encryptColumns", "keyField",
      "purpose");
  private static final Pattern PATH = Pattern.compile("[A-Za-z0-9_.=+-]+(/[A-Za-z0-9_.=+-]+)*");
  private static final Pattern COLUMN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,127}");
  private static final Pattern TABLE = Pattern.compile("[a-z0-9_]{1,128}");
    private static final DateTimeFormatter MINUTE = DateTimeFormatter.ofPattern("uuuuMMddHHmm").withResolverStyle(ResolverStyle.STRICT);
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
    if (path == null || path.length() > MAX_PATH || !PATH.matcher(path).matches() || path.contains("..")) {
      problems.add(new Problem("path", CODE, "path must be relative to the landing root of the source: no leading or trailing "
          + "'/', no '//', no '..', only [A-Za-z0-9_.=+-] and '/', at most " + MAX_PATH + " characters"));
    }
    String batchId = text(body, "batchId").orElse(null);
    if (batchId == null || !isDate(batchId)) {
      problems.add(new Problem("batchId", CODE, "batchId must be a real date and hour yyyyMMddHH, or with minutes yyyyMMddHHmm"));
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
    if (source != null && source.encryptsColumns()) {
      columns = columns(body, problems);
      keyField = text(body, "keyField").orElse(null);
      if (keyField == null || !COLUMN.matcher(keyField).matches()) {
        problems.add(new Problem("keyField", CODE, "keyField must be a column name [A-Za-z_][A-Za-z0-9_]*"));
      } else if (columns.stream().anyMatch(c -> c.equalsIgnoreCase(body.path("keyField").asText()))) {
        problems.add(new Problem("keyField", CODE, "keyField must not be listed in encryptColumns"));
      }
    } else if (source != null) {
      for (String field : List.of("encryptColumns", "keyField")) {
        if (body.has(field)) {
          problems.add(new Problem(field, CODE, field + " is only allowed for VTNET"));
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
    return new Parsed(source, path, batchId, table, outputMode, columns, keyField, purpose);
  }

  private static List<String> columns(JsonNode body, List<Problem> problems) {
    JsonNode node = body.path("encryptColumns");
    List<String> columns = new ArrayList<>();
    boolean ok = node.isArray() && node.size() >= 1 && node.size() <= MAX_COLUMNS;
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
          "encryptColumns must be a list of 1 to " + MAX_COLUMNS + " column names [A-Za-z_][A-Za-z0-9_]*"));
    }
    return columns;
  }

  private static java.util.Optional<String> text(JsonNode body, String field) {
    JsonNode node = body.path(field);
    return node.isTextual() ? java.util.Optional.of(node.asText()) : java.util.Optional.empty();
  }

  private static boolean isDate(String value) {
    if (value.length() != 10 && value.length() != 12) {
      return false;
    }
    try {
      java.time.LocalDateTime.parse(value.length() == 10 ? value + "00" : value, MINUTE);
      return true;
    } catch (DateTimeParseException e) {
      return false;
    }
  }

  /** Tên trường do người dùng đặt: chỉ giữ [A-Za-z0-9_], tối đa 64 ký tự. */
  private static String safe(String name) {
    String s = name.replaceAll("[^A-Za-z0-9_]", "_");
    return s.length() > 64 ? s.substring(0, 64) : s;
  }
}
