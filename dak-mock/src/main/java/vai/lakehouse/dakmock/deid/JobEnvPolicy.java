package vai.lakehouse.dakmock.deid;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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
  public static final String OUTPUT_MODE = "OUTPUT_MODE";
  public static final String INPUT_FROM = "INPUT_FROM";
  public static final String INPUT_TO = "INPUT_TO";
  public static final String INPUT_HOURS = "INPUT_HOURS";
  private static final String MODE_OVERWRITE = "overwrite";
  private static final String MODE_OVERWRITE_PARTITIONS = "overwrite-partitions";
  private static final List<String> RANGE_ENV = List.of(INPUT_FROM, INPUT_TO, INPUT_HOURS);

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
   * Kiểm env người gửi nhập; trả mọi lỗi tìm thấy (rỗng nếu hợp lệ), sắp theo tên env, gồm cả luật giữa các env ngày giờ. Giá trị phải là chuỗi, tối đa 256 ký tự,
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
    checkRange(env, problems);
    problems.sort(java.util.Comparator.comparing(Problem::field));
    return problems;
  }

  /**
   * Quy tắc giữa các env ngày giờ (chỉ xét env đã qua kiểm từng mục, để một giá trị sai không sinh thêm lỗi thứ hai): ngày có thật,
   * giờ 00..23, FROM không sau TO, hai số của INPUT_HOURS tăng dần, và overwrite-partitions phải có khoảng. Cùng luật với job.
   */
  private void checkRange(Map<String, ?> env, List<Problem> problems) {
    Map<String, String> ok = new TreeMap<>();
    for (String name : RANGE_ENV) {
      if (env.get(name) instanceof String text && rules.containsKey(name) && rules.get(name).matcher(text).matches()
          && problems.stream().noneMatch(p -> p.field().equals(FIELD_PREFIX + name))) {
        ok.put(name, text);
      }
    }
    boolean validPoints = true;
    for (String name : List.of(INPUT_FROM, INPUT_TO)) {
      String text = ok.get(name);
      if (text == null) {
        continue;
      }
      if (!isRealDate(text.substring(0, 8))) {
        problems.add(new Problem(FIELD_PREFIX + name, CODE, "not a real calendar date"));
        validPoints = false;
      } else if (text.length() == 10 && Integer.parseInt(text.substring(8)) > 23) {
        problems.add(new Problem(FIELD_PREFIX + name, CODE, "hour must be 00..23"));
        validPoints = false;
      }
    }
    if (validPoints && ok.containsKey(INPUT_FROM) && ok.containsKey(INPUT_TO)
        && pointKey(ok.get(INPUT_FROM), 0) > pointKey(ok.get(INPUT_TO), 23)) {
      problems.add(new Problem(FIELD_PREFIX + INPUT_TO, CODE, "must not be earlier than " + INPUT_FROM));
    }
    String hours = ok.get(INPUT_HOURS);
    if (hours != null && hours.contains("-")) {
      String[] parts = hours.split("-");
      if (Integer.parseInt(parts[0]) > Integer.parseInt(parts[1])) {
        problems.add(new Problem(FIELD_PREFIX + INPUT_HOURS, CODE, "the first hour must not be after the last hour"));
      }
    }
    if (MODE_OVERWRITE_PARTITIONS.equals(env.get(OUTPUT_MODE)) && RANGE_ENV.stream().noneMatch(env::containsKey)) {
      problems.add(new Problem(FIELD_PREFIX + OUTPUT_MODE, CODE,
          MODE_OVERWRITE_PARTITIONS + " needs " + INPUT_FROM + ", " + INPUT_TO + " or " + INPUT_HOURS));
    }
  }

  private static boolean isRealDate(String yyyymmdd) {
    try {
      LocalDate.parse(yyyymmdd, DateTimeFormatter.BASIC_ISO_DATE);
      return true;
    } catch (DateTimeParseException e) {
      return false;
    }
  }

  /** yyyyMMddHH as a number; a missing hour counts as {@code defaultHour} (00 for FROM, 23 for TO). */
  private static long pointKey(String value, int defaultHour) {
    long date = Long.parseLong(value.substring(0, 8));
    return date * 100 + (value.length() == 10 ? Integer.parseInt(value.substring(8)) : defaultHour);
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
   * Env cuối cùng của phiếu khi duyệt: mặc định tĩnh của danh sách, rồi giá trị người gửi nhập đè lên, rồi {@code OUTPUT_MODE} theo khoảng ngày giờ nếu còn thiếu, rồi bảng kết quả mặc định do
   * mock tính cho {@code OUTPUT_DB} và {@code OUTPUT_TABLE} nếu còn thiếu (bảng kết quả luôn có). Sắp theo tên.
   */
  public Map<String, String> complete(Map<String, String> env, String defaultOutputDb, String defaultOutputTable) {
    Map<String, String> result = new TreeMap<>();
    config.allowed().stream().filter(e -> e.defaultValue() != null).forEach(e -> result.put(e.name(), e.defaultValue()));
    result.putAll(env);
    if (config.find(OUTPUT_MODE).isPresent()) {
      // Mặc định phụ thuộc phiếu: có khoảng ngày giờ thì chỉ thay các partition của khoảng đó, không thì thay cả bảng.
      result.putIfAbsent(OUTPUT_MODE,
          RANGE_ENV.stream().anyMatch(env::containsKey) ? MODE_OVERWRITE_PARTITIONS : MODE_OVERWRITE);
    }
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
