package vai.lakehouse.dakmock.deid;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.column.ColumnKeyRequest;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.config.DeidProperties.Submitter;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.SourceFile;
import vai.lakehouse.dakmock.web.DakApiException;
import vai.lakehouse.dakmock.web.DakApiException.Problem;

/**
 * Đọc file phiếu người dùng upload (docs/dak-deid-flow/API_CONTRACT.md mục 2, plan mục 4.1).
 *
 * <ul>
 *   <li><b>Cả file hỏng</b> (quá cỡ, không phải UTF-8/JSON, gốc không phải object, khoá trùng, dữ liệu thừa, lồng quá sâu):
 *       {@code 400 invalid_file} với {@code problems} rỗng.
 *   <li><b>Lỗi nội dung</b>: gom MỌI lỗi một lượt thành {@code problems} (có {@code field}, {@code code}), người dùng sửa file
 *       một lần. Trường lạ là lỗi, không bỏ qua im lặng.
 * </ul>
 *
 * <p>Chỉ đọc tối đa {@code max-upload-bytes + 1} byte, không ghi gì xuống đĩa. Mọi thông điệp lỗi là chuỗi cố định: không bao
 * giờ lặp lại giá trị người dùng nhập (kể cả tên DAG, để không dò được DAG nào tồn tại). Duy nhất tên trường lạ được đưa vào
 * {@code field}, sau khi làm sạch.
 */
@Component
public class DeidUploadParser {

  private static final Logger log = LoggerFactory.getLogger(DeidUploadParser.class);

  private static final int MAX_COLUMNS = ColumnKeyRequest.MAX_COLUMNS;
  private static final int MAX_PURPOSE_LENGTH = 500;
  private static final int MAX_FILE_NAME_LENGTH = 255;
  private static final int MAX_FIELD_NAME_LENGTH = 64;
  private static final int MAX_NESTING_DEPTH = 10;
  private static final String DEFAULT_FILE_NAME = "unnamed";

  private static final String INVALID_TABLE = "invalid_table";
  private static final String INVALID_COLUMN = "invalid_column";
  private static final String INVALID_MODE = "invalid_mode";
  private static final String INVALID_DAG = "invalid_dag";
  private static final String INVALID_PURPOSE = "invalid_purpose";
  private static final String INVALID_FILE = "invalid_file";

  private static final Set<String> TOP_LEVEL_FIELDS = Set.of("database", "table", "columns", "dagId", "purpose");
  private static final Set<String> COLUMN_FIELDS = Set.of("column", "mode");

  // Khoá trùng và dữ liệu thừa là lỗi; không đưa đoạn nguồn vào thông điệp lỗi của Jackson; giới hạn độ sâu lồng nhau.
  private static final ObjectMapper MAPPER = new ObjectMapper(JsonFactory.builder()
      .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
      .disable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
      .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(MAX_NESTING_DEPTH).build())
      .build())
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  private final DeidProperties deid;

  @Autowired
  public DeidUploadParser(DakProperties props) {
    this(props.deid());
  }

  public DeidUploadParser(DeidProperties deid) {
    this.deid = deid;
  }

  /**
   * Parse file đã upload.
   *
   * @param originalFilename tên file do trình duyệt gửi; chỉ để hiển thị, được làm sạch
   * @throws DakApiException {@code 400 invalid_file} khi file hỏng hoặc có lỗi nội dung
   */
  public DeidUpload parse(String originalFilename, InputStream content) {
    byte[] bytes = read(content);
    JsonNode root = readTree(bytes);

    List<Problem> problems = new ArrayList<>();
    String database = name(root, "database", problems);
    String table = name(root, "table", problems);
    List<ColumnSpec> columns = columns(root.get("columns"), problems);
    String dagId = dagId(root, problems);
    String purpose = purpose(root, problems);
    unknownFields(root, TOP_LEVEL_FIELDS, "", problems);

    if (!problems.isEmpty()) {
      String noun = problems.size() == 1 ? "problem" : "problems";
      throw DakApiException.invalidFile("The uploaded request has " + problems.size() + " " + noun, problems);
    }
    return new DeidUpload(database, table, columns, dagId, purpose,
        new SourceFile(sanitizeFileName(originalFilename), sha256(bytes)));
  }

  /** Tên file để hiển thị: bỏ đường dẫn, ký tự điều khiển và ký tự định dạng, cắt 255 ký tự; trống thì dùng tên mặc định. */
  public static String sanitizeFileName(String raw) {
    if (raw == null) {
      return DEFAULT_FILE_NAME;
    }
    String name = raw.substring(Math.max(raw.lastIndexOf('/'), raw.lastIndexOf('\\')) + 1);
    StringBuilder kept = new StringBuilder();
    name.codePoints()
        .filter(cp -> !Character.isISOControl(cp) && Character.getType(cp) != Character.FORMAT)
        .forEach(kept::appendCodePoint);
    String cleaned = kept.toString().strip();
    if (cleaned.length() > MAX_FILE_NAME_LENGTH) {
      cleaned = cleaned.substring(0, MAX_FILE_NAME_LENGTH);
      // Không để lại nửa cặp surrogate ở cuối.
      if (Character.isHighSurrogate(cleaned.charAt(cleaned.length() - 1))) {
        cleaned = cleaned.substring(0, cleaned.length() - 1);
      }
    }
    return cleaned.isEmpty() || cleaned.equals(".") || cleaned.equals("..") ? DEFAULT_FILE_NAME : cleaned;
  }

  private byte[] read(InputStream content) {
    try {
      byte[] bytes = content.readNBytes(deid.maxUploadBytes() + 1);
      if (bytes.length > deid.maxUploadBytes()) {
        throw wholeFile("The uploaded file is too large");
      }
      return bytes;
    } catch (IOException e) {
      // Chỉ ghi loại lỗi: thông điệp của IOException có thể chứa đường dẫn nội bộ.
      log.warn("Could not read uploaded file: {}", e.getClass().getSimpleName());
      throw wholeFile("The uploaded file could not be read");
    }
  }

  private static JsonNode readTree(byte[] bytes) {
    JsonNode root;
    try {
      root = MAPPER.readTree(bytes);
    } catch (JsonProcessingException e) {
      throw wholeFile("The uploaded file is not valid JSON");
    } catch (IOException e) {
      throw wholeFile("The uploaded file could not be read");
    }
    if (root == null || !root.isObject()) {
      throw wholeFile("The uploaded file must contain a JSON object");
    }
    return root;
  }

  private static DakApiException wholeFile(String message) {
    return DakApiException.invalidFile(message, List.of());
  }

  private static String name(JsonNode root, String field, List<Problem> problems) {
    JsonNode node = root.get(field);
    if (node == null || node.isNull()) {
      problems.add(new Problem(field, INVALID_TABLE, field + " is required"));
      return null;
    }
    if (!node.isTextual() || !ColumnKeyRequest.isName(node.textValue())) {
      problems.add(new Problem(field, INVALID_TABLE, field + " must match [A-Za-z0-9_]{1,128}"));
      return null;
    }
    return node.textValue().toLowerCase(Locale.ROOT);
  }

  private List<ColumnSpec> columns(JsonNode node, List<Problem> problems) {
    if (node == null || node.isNull()) {
      problems.add(new Problem("columns", INVALID_COLUMN, "columns is required"));
      return List.of();
    }
    if (!node.isArray()) {
      problems.add(new Problem("columns", INVALID_COLUMN, "columns must be an array"));
      return List.of();
    }
    if (node.isEmpty()) {
      problems.add(new Problem("columns", INVALID_COLUMN, "at least one column is required"));
      return List.of();
    }
    if (node.size() > MAX_COLUMNS) {
      problems.add(new Problem("columns", INVALID_COLUMN, "at most " + MAX_COLUMNS + " columns"));
      return List.of();
    }
    // Gộp trùng không phân biệt hoa thường, giữ phần tử xuất hiện đầu tiên.
    Map<String, ColumnSpec> merged = new LinkedHashMap<>();
    for (int i = 0; i < node.size(); i++) {
      ColumnSpec spec = column(node.get(i), "columns[" + i + "]", problems);
      if (spec != null) {
        merged.putIfAbsent(spec.column(), spec);
      }
    }
    return List.copyOf(merged.values());
  }

  private ColumnSpec column(JsonNode element, String path, List<Problem> problems) {
    if (!element.isObject()) {
      problems.add(new Problem(path, INVALID_COLUMN, "each column must be an object with a column name"));
      return null;
    }
    String column = null;
    JsonNode nameNode = element.get("column");
    if (nameNode == null || nameNode.isNull()) {
      problems.add(new Problem(path + ".column", INVALID_COLUMN, "column name is required"));
    } else if (!nameNode.isTextual() || !ColumnKeyRequest.isName(nameNode.textValue())) {
      problems.add(new Problem(path + ".column", INVALID_COLUMN, "column name must match [A-Za-z0-9_]{1,128}"));
    } else {
      column = nameNode.textValue().toLowerCase(Locale.ROOT);
    }
    Mode mode = mode(element.get("mode"), path + ".mode", problems);
    unknownFields(element, COLUMN_FIELDS, path + ".", problems);
    return column == null || mode == null ? null : new ColumnSpec(column, mode);
  }

  private Mode mode(JsonNode node, String path, List<Problem> problems) {
    if (node == null || node.isNull()) {
      return Mode.HASH;
    }
    Mode mode = null;
    if (node.isTextual()) {
      try {
        mode = Mode.valueOf(node.textValue().toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        mode = null;
      }
    }
    if (mode == null || !deid.allowedModes().contains(mode)) {
      problems.add(new Problem(path, INVALID_MODE, "mode must be one of " + deid.allowedModes()));
      return null;
    }
    return mode;
  }

  /**
   * {@code dagId} chỉ có nghĩa khi flow gửi job bằng Airflow: bắt buộc và phải nằm trong danh sách cho phép. Mọi lý do sai
   * (không phải chuỗi, ký tự lạ, ngoài danh sách) dùng CÙNG một thông điệp. Khi gửi job bằng cách khác thì bỏ qua hoàn toàn.
   */
  private String dagId(JsonNode root, List<Problem> problems) {
    if (deid.submitter() != Submitter.AIRFLOW) {
      return null;
    }
    JsonNode node = root.get("dagId");
    if (node == null || node.isNull()) {
      problems.add(new Problem("dagId", INVALID_DAG, "dagId is required"));
      return null;
    }
    if (!node.isTextual() || !deid.airflow().allowedDagIds().contains(node.textValue())) {
      problems.add(new Problem("dagId", INVALID_DAG, "dagId is not allowed"));
      return null;
    }
    return node.textValue();
  }

  private static String purpose(JsonNode root, List<Problem> problems) {
    JsonNode node = root.get("purpose");
    if (node == null || node.isNull()) {
      problems.add(new Problem("purpose", INVALID_PURPOSE, "purpose is required"));
      return null;
    }
    if (!node.isTextual()) {
      problems.add(new Problem("purpose", INVALID_PURPOSE, "purpose must be a string"));
      return null;
    }
    String purpose = node.textValue().strip();
    if (purpose.isEmpty() || purpose.codePointCount(0, purpose.length()) > MAX_PURPOSE_LENGTH) {
      problems.add(new Problem("purpose", INVALID_PURPOSE, "purpose must be 1 to " + MAX_PURPOSE_LENGTH + " characters"));
      return null;
    }
    return purpose;
  }

  private static void unknownFields(JsonNode object, Set<String> known, String prefix, List<Problem> problems) {
    object.fieldNames().forEachRemaining(name -> {
      if (!known.contains(name)) {
        problems.add(new Problem(prefix + safeFieldName(name), INVALID_FILE, "unknown field"));
      }
    });
  }

  /** Tên trường do người dùng đặt: chỉ giữ [A-Za-z0-9_.-], các ký tự khác thành '_', tối đa 64 ký tự. */
  private static String safeFieldName(String name) {
    String safe = name.replaceAll("[^A-Za-z0-9_.\\-]", "_");
    if (safe.length() > MAX_FIELD_NAME_LENGTH) {
      safe = safe.substring(0, MAX_FIELD_NAME_LENGTH);
    }
    return safe.isEmpty() ? "_" : safe;
  }

  private static String sha256(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by every Java platform", e);
    }
  }
}
