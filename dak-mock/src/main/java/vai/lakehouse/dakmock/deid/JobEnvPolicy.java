package vai.lakehouse.dakmock.deid;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;

import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.JobEnv;
import vai.lakehouse.dakmock.web.DakApiException.Problem;

/**
 * Luật cho env của job khử định danh do người gửi phiếu nhập (docs/dak-deid-flow/JOB_ENV_PLAN.md mục 3 đến 5), dùng chung cho
 * parser (lúc gửi phiếu), lúc duyệt và lúc gửi cho Airflow. Danh sách env được phép và quy tắc giá trị lấy từ
 * {@code dak.deid.job-env}.
 *
 * <p>Thông điệp lỗi cố định, không bao giờ lặp lại giá trị người dùng đã nhập; tên env do người dùng đặt chỉ xuất hiện trong
 * {@code field} sau khi làm sạch. Tên không có trong danh sách (kể cả env bị cấm) dùng CÙNG một thông điệp để không lộ danh sách.
 */
public final class JobEnvPolicy {

  public static final String CODE = "invalid_env";
  public static final String OUTPUT_DB = "OUTPUT_DB";
  public static final String OUTPUT_TABLE = "OUTPUT_TABLE";

  private static final int MAX_VALUE_LENGTH = 256;
  private static final int MAX_FIELD_NAME_LENGTH = 64;
  private static final String FIELD_PREFIX = "env.";

  private final JobEnv config;
  private final String configuredOutputDatabase;
  private final Map<String, Pattern> rules = new TreeMap<>();

  public JobEnvPolicy(DeidProperties deid) {
    this.config = deid.jobEnv();
    this.configuredOutputDatabase = deid.outputDatabase();
    config.allowed().forEach(e -> rules.put(e.name(), Pattern.compile(e.pattern())));
  }

  /**
   * Kiểm env người gửi nhập; trả mọi lỗi tìm thấy (rỗng nếu hợp lệ), sắp theo tên env. Giá trị phải là chuỗi, tối đa 256 ký tự,
   * không có ký tự điều khiển và khớp toàn bộ quy tắc của env đó.
   */
  public List<Problem> validate(Map<String, ?> env) {
    List<Problem> problems = new ArrayList<>();
    if (env == null || env.isEmpty()) {
      return problems;
    }
    if (env.size() > config.maxEntries()) {
      problems.add(new Problem("env", CODE, "at most " + config.maxEntries() + " entries are allowed"));
    }
    new TreeMap<String, Object>(env).forEach((name, value) -> checkEntry(name, value).ifPresent(problems::add));
    return problems;
  }

  private Optional<Problem> checkEntry(String name, Object value) {
    String field = FIELD_PREFIX + safeName(name);
    Pattern rule = rules.get(name);
    if (rule == null) {
      return Optional.of(new Problem(field, CODE, "env name is not allowed"));
    }
    if (!(value instanceof String text)) {
      return Optional.of(new Problem(field, CODE, "value must be a string"));
    }
    if (text.length() > MAX_VALUE_LENGTH) {
      return Optional.of(new Problem(field, CODE, "value is too long (at most " + MAX_VALUE_LENGTH + " characters)"));
    }
    if (text.chars().anyMatch(c -> c < 0x20 || c == 0x7F)) {
      return Optional.of(new Problem(field, CODE, "value must not contain control characters"));
    }
    if (!rule.matcher(text).matches()) {
      // Quy tắc là cấu hình của DAK, không phải dữ liệu người dùng nên nêu ra được.
      return Optional.of(new Problem(field, CODE, "value must match " + rule.pattern()));
    }
    return Optional.empty();
  }

  /**
   * Env cuối cùng của phiếu khi duyệt: mặc định tĩnh của danh sách, rồi giá trị người gửi nhập đè lên, rồi bảng kết quả mặc định do
   * mock tính cho {@code OUTPUT_DB} và {@code OUTPUT_TABLE} nếu còn thiếu (bảng kết quả luôn có). Sắp theo tên.
   */
  public Map<String, String> complete(Map<String, String> env, String defaultOutputDb, String defaultOutputTable) {
    Map<String, String> result = new TreeMap<>();
    config.allowed().stream().filter(e -> e.defaultValue() != null).forEach(e -> result.put(e.name(), e.defaultValue()));
    result.putAll(env);
    result.putIfAbsent(OUTPUT_DB, defaultOutputDb);
    result.putIfAbsent(OUTPUT_TABLE, defaultOutputTable);
    return result;
  }

  /**
   * Job ghi vào bảng kết quả nên nó không được là bảng nguồn. Database đích lấy từ {@code OUTPUT_DB}, không có thì từ
   * {@code dak.deid.output-database}, không có nữa thì là database nguồn. Không khai {@code OUTPUT_TABLE} thì tên mặc định luôn
   * khác bảng nguồn nên không có xung đột.
   */
  public Optional<Problem> outputConflict(String sourceDatabase, String sourceTable, Map<String, String> env) {
    String table = env.get(OUTPUT_TABLE);
    if (table == null) {
      return Optional.empty();
    }
    String database = env.get(OUTPUT_DB);
    if (database == null) {
      database = configuredOutputDatabase.isEmpty() ? sourceDatabase : configuredOutputDatabase;
    }
    if (database.equals(sourceDatabase) && table.equals(sourceTable)) {
      return Optional.of(new Problem(FIELD_PREFIX + OUTPUT_TABLE, CODE, "the output table must differ from the source table"));
    }
    return Optional.empty();
  }

  /** Tên env do người dùng đặt: chỉ giữ [A-Za-z0-9_.-], ký tự khác thành '_', tối đa 64 ký tự. */
  private static String safeName(String name) {
    String safe = name.replaceAll("[^A-Za-z0-9_.\\-]", "_");
    return safe.length() > MAX_FIELD_NAME_LENGTH ? safe.substring(0, MAX_FIELD_NAME_LENGTH) : safe;
  }
}
