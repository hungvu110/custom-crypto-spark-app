package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.JobEnv;
import vai.lakehouse.dakmock.web.DakApiException.Problem;

/**
 * E2 (docs/dak-deid-flow/JOB_ENV_PLAN.md mục 5): bộ kiểm env người gửi phiếu nhập, dùng chung cho parser (lúc gửi phiếu), lúc duyệt
 * và lúc gửi cho Airflow. Thông điệp lỗi không bao giờ lặp lại giá trị người dùng đã nhập.
 */
class JobEnvPolicyTest {

  private static DeidProperties config(String outputDatabase, JobEnv jobEnv) {
    return new DeidProperties(true, null, 20, 65536, outputDatabase, null, null, null, null, null, jobEnv);
  }

  private static JobEnvPolicy defaults() {
    return new JobEnvPolicy(DeidProperties.defaults());
  }

  private static Map<String, Object> env(Object... pairs) {
    Map<String, Object> env = new LinkedHashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      env.put((String) pairs[i], pairs[i + 1]);
    }
    return env;
  }

  private static Map<String, String> strings(String... pairs) {
    Map<String, String> env = new HashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      env.put(pairs[i], pairs[i + 1]);
    }
    return env;
  }

  // ---------------------------------------------------------------- validate

  @Test
  @DisplayName("env hợp lệ, rỗng hoặc null: không có lỗi")
  void validate_validEmptyOrNull_noProblems() {
    JobEnvPolicy policy = defaults();

    assertThat(policy.validate(env("OUTPUT_DB", "demo_db", "OUTPUT_TABLE", "t_out", "OUTPUT_MODE", "append",
        "OUTPUT_FORMAT", "iceberg", "ENABLE_HIVE_SUPPORT", "false"))).isEmpty();
    assertThat(policy.validate(Map.of())).isEmpty();
    assertThat(policy.validate(null)).isEmpty();
  }

  @Test
  @DisplayName("tên không có trong danh sách, kể cả env bị cấm: cùng một thông điệp chung, không lộ danh sách")
  void validate_unknownOrForbiddenName_sameGenericMessage() {
    List<Problem> problems = defaults().validate(env("FOO", "x", "DB_NAME", "other_table", "VAULT_ADDR", "http://evil"));

    assertThat(problems).extracting(Problem::field, Problem::code, Problem::message).containsExactly(
        tuple("env.DB_NAME", "invalid_env", "env name is not allowed"),
        tuple("env.FOO", "invalid_env", "env name is not allowed"),
        tuple("env.VAULT_ADDR", "invalid_env", "env name is not allowed"));
  }

  @Test
  @DisplayName("giá trị sai quy tắc: thông điệp nêu quy tắc của cấu hình nhưng không lặp lại giá trị đã nhập")
  void validate_badValue_messageDoesNotEchoTheValue() {
    for (String bad : List.of("replace", "OVERWRITE", "append ", "")) {
      List<Problem> problems = defaults().validate(env("OUTPUT_MODE", bad));

      assertThat(problems).hasSize(1);
      assertThat(problems.get(0).field()).isEqualTo("env.OUTPUT_MODE");
      assertThat(problems.get(0).code()).isEqualTo("invalid_env");
      assertThat(problems.get(0).message()).isEqualTo("value must match overwrite|append|overwrite-partitions");
    }
    for (String bad : List.of("Demo_DB", "a-b", "a b", "../x", "x".repeat(129))) {
      List<Problem> problems = defaults().validate(env("OUTPUT_DB", bad));

      assertThat(problems).hasSize(1);
      assertThat(problems.get(0).message()).startsWith("value must match").doesNotContain(bad.substring(0, Math.min(4, bad.length())));
    }
  }

  @Test
  @DisplayName("giá trị không phải chuỗi (số, boolean, object, mảng, null) bị từ chối")
  void validate_nonStringValue_rejected() {
    for (Object bad : new Object[] {7, true, Map.of("a", 1), List.of("x"), null}) {
      List<Problem> problems = defaults().validate(env("OUTPUT_MODE", bad));

      assertThat(problems).extracting(Problem::field, Problem::message)
          .containsExactly(tuple("env.OUTPUT_MODE", "value must be a string"));
    }
  }

  @Test
  @DisplayName("giá trị dài quá 256 ký tự hoặc có ký tự điều khiển bị từ chối dù quy tắc của env cho phép")
  void validate_longOrControlCharacters_rejectedEvenIfThePatternAllows() {
    JobEnvPolicy permissive = new JobEnvPolicy(config("", new JobEnv(10,
        List.of(new JobEnv.Entry("OUTPUT_FORMAT", "(?s).*", null)))));

    assertThat(permissive.validate(env("OUTPUT_FORMAT", "a\nb"))).extracting(Problem::message)
        .containsExactly("value must not contain control characters");
    assertThat(permissive.validate(env("OUTPUT_FORMAT", "a\u0000b"))).hasSize(1);
    assertThat(permissive.validate(env("OUTPUT_FORMAT", "a\tb"))).hasSize(1);
    assertThat(permissive.validate(env("OUTPUT_FORMAT", "x".repeat(257)))).extracting(Problem::message)
        .containsExactly("value is too long (at most 256 characters)");
    assertThat(permissive.validate(env("OUTPUT_FORMAT", "x".repeat(256)))).isEmpty();
  }

  @Test
  @DisplayName("vượt số mục tối đa bị báo lỗi ở trường env")
  void validate_tooManyEntries_reported() {
    JobEnvPolicy policy = new JobEnvPolicy(config("", new JobEnv(2, DeidProperties.defaults().jobEnv().allowed())));

    List<Problem> problems = policy.validate(env("OUTPUT_DB", "a", "OUTPUT_TABLE", "b", "OUTPUT_MODE", "append"));

    assertThat(problems).extracting(Problem::field, Problem::code, Problem::message)
        .containsExactly(tuple("env", "invalid_env", "at most 2 entries are allowed"));
  }

  @Test
  @DisplayName("tên do người dùng đặt được làm sạch trước khi vào trường field: ký tự lạ thành '_', tối đa 64 ký tự")
  void validate_userNameIsSanitisedInTheFieldPath() {
    List<Problem> problems = defaults().validate(env("A B/../x", "1", "y".repeat(100), "1"));

    assertThat(problems).extracting(Problem::field).containsExactlyInAnyOrder("env.A_B_.._x", "env." + "y".repeat(64));
  }

  @Test
  @DisplayName("nhiều lỗi được gom hết, sắp theo tên env để kết quả ổn định")
  void validate_collectsEveryProblemSortedByName() {
    List<Problem> problems = defaults().validate(env("OUTPUT_TABLE", "A-B", "ZZZ", "x", "OUTPUT_MODE", "nope"));

    assertThat(problems).extracting(Problem::field).containsExactly("env.OUTPUT_MODE", "env.OUTPUT_TABLE", "env.ZZZ");
  }

  // ---------------------------------------------------------------- date and hour range

  private static List<String> messages(JobEnvPolicy policy, Map<String, ?> env) {
    return policy.validate(env).stream().map(p -> p.field() + ": " + p.message()).toList();
  }

  @Test
  @DisplayName("khoảng ngày giờ hợp lệ: ngày hoặc ngày kèm giờ, FROM bằng TO, INPUT_HOURS một giờ hoặc một khoảng")
  void validate_dateHourRange_valid() {
    JobEnvPolicy policy = defaults();

    assertThat(policy.validate(env("INPUT_FROM", "20261012"))).isEmpty();
    assertThat(policy.validate(env("INPUT_FROM", "20261012", "INPUT_TO", "20261014"))).isEmpty();
    assertThat(policy.validate(env("INPUT_FROM", "2026101218", "INPUT_TO", "2026101306"))).isEmpty();
    assertThat(policy.validate(env("INPUT_FROM", "20261012", "INPUT_TO", "20261012"))).isEmpty();
    assertThat(policy.validate(env("INPUT_FROM", "2026101205", "INPUT_TO", "2026101205"))).isEmpty();
    assertThat(policy.validate(env("INPUT_HOURS", "18"))).isEmpty();
    assertThat(policy.validate(env("INPUT_HOURS", "6-18"))).isEmpty();
    assertThat(policy.validate(env("INPUT_HOURS", "0-23", "INPUT_TO", "20261231"))).isEmpty();
    assertThat(policy.validate(env("INPUT_FROM", "20240229"))).isEmpty(); // leap day
  }

  @Test
  @DisplayName("khoảng ngày giờ sai định dạng: dạng phút, tháng 13, giờ 24, giờ nhiều chữ số đều bị từ chối bằng thông điệp cố định")
  void validate_dateHourRange_badFormatOrCalendar() {
    JobEnvPolicy policy = defaults();

    assertThat(messages(policy, env("INPUT_FROM", "202610120030"))).containsExactly("env.INPUT_FROM: value must match [0-9]{8}([0-9]{2})?");
    assertThat(messages(policy, env("INPUT_FROM", "2026-10-12"))).hasSize(1);
    assertThat(messages(policy, env("INPUT_FROM", "20261340"))).containsExactly("env.INPUT_FROM: not a real calendar date");
    assertThat(messages(policy, env("INPUT_TO", "20230229"))).containsExactly("env.INPUT_TO: not a real calendar date");
    assertThat(messages(policy, env("INPUT_FROM", "2026101224"))).containsExactly("env.INPUT_FROM: hour must be 00..23");
    assertThat(messages(policy, env("INPUT_TO", "2026101225"))).containsExactly("env.INPUT_TO: hour must be 00..23");
    assertThat(policy.validate(env("INPUT_HOURS", "24"))).hasSize(1);
    assertThat(policy.validate(env("INPUT_HOURS", "6-"))).hasSize(1);
    assertThat(policy.validate(env("INPUT_HOURS", "-6"))).hasSize(1);
    assertThat(policy.validate(env("INPUT_HOURS", "1-2-3"))).hasSize(1);
    assertThat(policy.validate(env("INPUT_FROM", 20261012))).hasSize(1);
  }

  @Test
  @DisplayName("FROM không được sau TO: ngày thiếu giờ tính FROM là 00 và TO là 23")
  void validate_dateHourRange_fromAfterTo() {
    JobEnvPolicy policy = defaults();

    assertThat(messages(policy, env("INPUT_FROM", "20261014", "INPUT_TO", "20261012")))
        .containsExactly("env.INPUT_TO: must not be earlier than INPUT_FROM");
    assertThat(messages(policy, env("INPUT_FROM", "2026101218", "INPUT_TO", "2026101217")))
        .containsExactly("env.INPUT_TO: must not be earlier than INPUT_FROM");
    assertThat(policy.validate(env("INPUT_FROM", "2026101223", "INPUT_TO", "20261012"))).isEmpty(); // 23 <= 23
    assertThat(policy.validate(env("INPUT_FROM", "20261012", "INPUT_TO", "2026101200"))).isEmpty(); // 00 <= 00
    assertThat(messages(policy, env("INPUT_FROM", "2026101201", "INPUT_TO", "2026101200")))
        .containsExactly("env.INPUT_TO: must not be earlier than INPUT_FROM");
  }

  @Test
  @DisplayName("INPUT_HOURS hai số: số đầu không được lớn hơn số sau")
  void validate_dateHourRange_hoursOrder() {
    JobEnvPolicy policy = defaults();

    assertThat(messages(policy, env("INPUT_HOURS", "18-6")))
        .containsExactly("env.INPUT_HOURS: the first hour must not be after the last hour");
    assertThat(policy.validate(env("INPUT_HOURS", "6-6"))).isEmpty();
    assertThat(policy.validate(env("INPUT_HOURS", "06-18"))).isEmpty();
  }

  @Test
  @DisplayName("OUTPUT_MODE=overwrite-partitions chỉ hợp lệ khi có ít nhất một env ngày giờ; các chế độ khác không đòi")
  void validate_overwritePartitions_requiresARange() {
    JobEnvPolicy policy = defaults();

    assertThat(messages(policy, env("OUTPUT_MODE", "overwrite-partitions")))
        .containsExactly("env.OUTPUT_MODE: overwrite-partitions needs INPUT_FROM, INPUT_TO or INPUT_HOURS");
    assertThat(policy.validate(env("OUTPUT_MODE", "overwrite-partitions", "INPUT_FROM", "20261012"))).isEmpty();
    assertThat(policy.validate(env("OUTPUT_MODE", "overwrite-partitions", "INPUT_TO", "20261012"))).isEmpty();
    assertThat(policy.validate(env("OUTPUT_MODE", "overwrite-partitions", "INPUT_HOURS", "5"))).isEmpty();
    assertThat(policy.validate(env("OUTPUT_MODE", "overwrite", "INPUT_FROM", "20261012"))).isEmpty();
    assertThat(policy.validate(env("OUTPUT_MODE", "append", "INPUT_HOURS", "5"))).isEmpty();
    assertThat(policy.validate(env("OUTPUT_MODE", "overwrite"))).isEmpty();
  }

  @Test
  @DisplayName("INPUT_PATH và các INPUT_* khác vẫn bị từ chối bằng thông điệp chung")
  void validate_otherInputEnv_stillRejected() {
    for (String name : List.of("INPUT_PATH", "INPUT_DATE_KEY", "INPUT_HOUR_KEY", "INPUT_FORMAT", "INPUT_OPTION_X", "INPUT_BAD_FILES")) {
      assertThat(messages(defaults(), env(name, "x"))).containsExactly("env." + name + ": env name is not allowed");
    }
  }

  @Test
  @DisplayName("thông điệp lỗi khoảng ngày giờ không lặp lại giá trị đã nhập")
  void validate_dateHourRange_doesNotEchoValues() {
    List<String> all = messages(defaults(), env("INPUT_FROM", "20261340", "INPUT_TO", "2026101299", "INPUT_HOURS", "99-1"));

    assertThat(String.join(" | ", all)).doesNotContain("20261340").doesNotContain("2026101299").doesNotContain("99-1");
  }

  // ---------------------------------------------------------------- complete

  @Test
  @DisplayName("complete: không có env thì lấy mặc định tĩnh của danh sách và mặc định bảng kết quả do mock tính")
  void complete_noEnv_usesDefaults() {
    Map<String, String> result = defaults().complete(Map.of(), "demo_db", "users_cdr_deid_20261026_1");

    assertThat(result).containsExactly(
        Map.entry("ENABLE_HIVE_SUPPORT", "true"), Map.entry("OUTPUT_DB", "demo_db"), Map.entry("OUTPUT_FORMAT", "delta"),
        Map.entry("OUTPUT_MODE", "overwrite"), Map.entry("OUTPUT_TABLE", "users_cdr_deid_20261026_1"));
  }

  @Test
  @DisplayName("complete: có env ngày giờ thì OUTPUT_MODE mặc định là overwrite-partitions, không có thì overwrite; người gửi chọn thì giữ")
  void complete_outputModeDependsOnTheRange() {
    JobEnvPolicy policy = defaults();

    assertThat(policy.complete(strings("INPUT_FROM", "20261012"), "d", "t")).containsEntry("OUTPUT_MODE", "overwrite-partitions");
    assertThat(policy.complete(strings("INPUT_TO", "20261012"), "d", "t")).containsEntry("OUTPUT_MODE", "overwrite-partitions");
    assertThat(policy.complete(strings("INPUT_HOURS", "5"), "d", "t")).containsEntry("OUTPUT_MODE", "overwrite-partitions");
    assertThat(policy.complete(strings(), "d", "t")).containsEntry("OUTPUT_MODE", "overwrite");
    assertThat(policy.complete(strings("INPUT_FROM", "20261012", "OUTPUT_MODE", "overwrite"), "d", "t"))
        .containsEntry("OUTPUT_MODE", "overwrite");
    assertThat(policy.complete(strings("INPUT_FROM", "20261012", "OUTPUT_MODE", "append"), "d", "t"))
        .containsEntry("OUTPUT_MODE", "append");
    // the range env themselves have no static default: they are only set when the requester gave them
    assertThat(policy.complete(strings(), "d", "t")).doesNotContainKeys("INPUT_FROM", "INPUT_TO", "INPUT_HOURS");
    assertThat(policy.validate(policy.complete(strings("INPUT_FROM", "20261012"), "d", "t"))).isEmpty();
  }

  @Test
  @DisplayName("complete: giá trị người gửi phiếu nhập luôn thắng mặc định; env còn thiếu mới được điền")
  void complete_userValuesWin() {
    Map<String, String> result = defaults().complete(strings("OUTPUT_TABLE", "mine", "OUTPUT_MODE", "append"), "demo_db",
        "ignored_default_table");

    assertThat(result).containsEntry("OUTPUT_TABLE", "mine").containsEntry("OUTPUT_MODE", "append")
        .containsEntry("OUTPUT_DB", "demo_db").containsEntry("OUTPUT_FORMAT", "delta");
  }

  @Test
  @DisplayName("complete: bảng kết quả luôn có dù danh sách cấu hình không liệt kê OUTPUT_DB và OUTPUT_TABLE")
  void complete_alwaysHasTheOutputTarget() {
    JobEnvPolicy onlyFormat = new JobEnvPolicy(config("", new JobEnv(10,
        List.of(new JobEnv.Entry("OUTPUT_FORMAT", "delta|hive", "delta")))));

    Map<String, String> result = onlyFormat.complete(Map.of(), "demo_db", "t_deid_1");

    assertThat(result).containsEntry("OUTPUT_DB", "demo_db").containsEntry("OUTPUT_TABLE", "t_deid_1")
        .containsEntry("OUTPUT_FORMAT", "delta").doesNotContainKey("OUTPUT_MODE");
  }

  // ---------------------------------------------------------------- outputConflict

  @Test
  @DisplayName("đích trùng bảng nguồn: cùng database khai rõ, hoặc database đích mặc định là database nguồn")
  void outputConflict_sameAsSource() {
    JobEnvPolicy policy = defaults();

    assertThat(policy.outputConflict("demo_db", "users_cdr", strings("OUTPUT_DB", "demo_db", "OUTPUT_TABLE", "users_cdr")))
        .get().extracting(Problem::field, Problem::code, Problem::message)
        .containsExactly("env.OUTPUT_TABLE", "invalid_env", "the output table must differ from the source table");
    assertThat(policy.outputConflict("demo_db", "users_cdr", strings("OUTPUT_TABLE", "users_cdr"))).isPresent();
  }

  @Test
  @DisplayName("không trùng khi khác database, khác tên bảng, hoặc không khai OUTPUT_TABLE (tên mặc định luôn khác bảng nguồn)")
  void outputConflict_notASourceClash() {
    JobEnvPolicy policy = defaults();

    assertThat(policy.outputConflict("demo_db", "users_cdr", strings("OUTPUT_DB", "other_db", "OUTPUT_TABLE", "users_cdr")))
        .isEmpty();
    assertThat(policy.outputConflict("demo_db", "users_cdr", strings("OUTPUT_TABLE", "users_cdr_pseudo"))).isEmpty();
    assertThat(policy.outputConflict("demo_db", "users_cdr", strings("OUTPUT_DB", "demo_db"))).isEmpty();
    assertThat(policy.outputConflict("demo_db", "users_cdr", Map.of())).isEmpty();
  }

  @Test
  @DisplayName("database đích theo cấu hình dak.deid.output-database được dùng khi người gửi không khai OUTPUT_DB")
  void outputConflict_usesTheConfiguredOutputDatabase() {
    JobEnvPolicy analytics = new JobEnvPolicy(config("analytics", JobEnv.defaults()));
    JobEnvPolicy sameAsSource = new JobEnvPolicy(config("demo_db", JobEnv.defaults()));

    assertThat(analytics.outputConflict("demo_db", "users_cdr", strings("OUTPUT_TABLE", "users_cdr"))).isEmpty();
    assertThat(sameAsSource.outputConflict("demo_db", "users_cdr", strings("OUTPUT_TABLE", "users_cdr"))).isPresent();
  }
}
