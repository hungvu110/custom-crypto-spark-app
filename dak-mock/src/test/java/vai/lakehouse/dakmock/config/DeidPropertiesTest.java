package vai.lakehouse.dakmock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import vai.lakehouse.dakmock.config.DeidProperties.Airflow;
import vai.lakehouse.dakmock.config.DeidProperties.AirflowAuth;
import vai.lakehouse.dakmock.config.DeidProperties.JobEnv;
import vai.lakehouse.dakmock.config.DeidProperties.Kubernetes;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.config.DeidProperties.Submitter;

/**
 * D1 của flow phiếu khử định danh (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 2 và 5): khối {@code dak.deid},
 * tài khoản giả của portal và việc kiểm tra cấu hình lúc khởi động. Cấu hình sai thì {@link DakProperties} ném
 * {@link IllegalStateException}, tức ứng dụng không khởi động.
 */
class DeidPropertiesTest {

  private static final String TOKEN_A = "a".repeat(24);
  private static final String TOKEN_B = "b".repeat(24);
  private static final String DEFAULT_VAULT_TEMPLATE = "base/{database}.{table}";

  /** Bản nháp có thể sửa của cấu hình hợp lệ; mỗi ca test chỉ đổi đúng một chỗ rồi kiểm lỗi. */
  private static final class Draft {
    boolean enabled = true;
    List<Mode> modes = List.of(Mode.HASH);
    int maxActivePerTeam = 20;
    int maxUploadBytes = 65536;
    String outputDatabase = "";
    String outputTableTemplate = "{table}_deid_{yyyyMMdd}_{seq}";
    Submitter submitter = Submitter.NOOP;
    Kubernetes kubernetes = new Kubernetes("https://kubernetes.default.svc", null, null);
    Airflow airflow = new Airflow(null, List.of(), AirflowAuth.CLIENT_CREDENTIALS, null, null, null, null, null);
    List<PortalUser> users = List.of();
    String vaultTemplate = DEFAULT_VAULT_TEMPLATE;
    JobEnv jobEnv = null;

    Draft k8s(String namespace, String templateFile) {
      kubernetes = new Kubernetes("https://kubernetes.default.svc", namespace, templateFile);
      return this;
    }

    Draft airflowWith(String baseUrl, List<String> dags, AirflowAuth auth, String tokenUrl, String clientId,
                      String clientSecret, String token) {
      airflow = new Airflow(baseUrl, dags, auth, tokenUrl, clientId, "", clientSecret, token);
      return this;
    }

    Draft user(String username, String team, String token) {
      return user(new PortalUser(username, team, null, null, token));
    }

    Draft user(PortalUser u) {
      List<PortalUser> next = new ArrayList<>(users);
      next.add(u);
      users = next;
      return this;
    }

    DeidProperties deid() {
      return new DeidProperties(enabled, modes, maxActivePerTeam, maxUploadBytes, outputDatabase, outputTableTemplate,
          submitter, kubernetes, airflow, users, jobEnv);
    }

    DakProperties props() {
      return new DakProperties(List.of(), List.of(), List.of(), vault(vaultTemplate), new DakProperties.Yc(50, false),
          new DakProperties.Admin(null), Duration.ofSeconds(60), deid());
    }
  }

  private static DakProperties.Vault vault(String pathTemplate) {
    return new DakProperties.Vault("http://vault", "token", "kv", pathTemplate,
        "dak/column-keys/{database}/{table}/{column}", "keyPrefix", Duration.ofSeconds(2), Duration.ofSeconds(5));
  }

  private static Draft valid() {
    return new Draft();
  }

  private static Draft validKubernetes() {
    Draft d = valid().k8s("vlp-ns", "/deid/sparkapplication.yaml");
    d.submitter = Submitter.KUBERNETES;
    return d;
  }

  private static Draft validAirflow() {
    Draft d = valid().k8s("vlp-ns", null).airflowWith("http://airflow:8080", List.of("deid_pseudonymize_table"),
        AirflowAuth.CLIENT_CREDENTIALS, "https://sso/realms/r/protocol/openid-connect/token", "client-airflow",
        "client-secret", null);
    d.submitter = Submitter.AIRFLOW;
    return d;
  }

  /** Áp {@code change} lên cấu hình hợp lệ, mong bị từ chối và thông điệp chứa mọi đoạn {@code fragments}. */
  private static void assertRejected(Draft draft, Consumer<Draft> change, String... fragments) {
    change.accept(draft);
    assertThatThrownBy(draft::props)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Invalid DAK mock configuration")
        .hasMessageContainingAll(fragments);
  }

  private static Airflow basicAirflow(String username, String password) {
    return new Airflow("http://airflow:8080", List.of("deid_pseudonymize_table"), AirflowAuth.BASIC, null, null, null, null,
        null, username, password);
  }

  @Test
  @DisplayName("auth=basic cần username và password; đủ thì hợp lệ và password bị che trong toString")
  void basicAuth_requiresUsernameAndPassword() {
    Draft ok = validAirflow();
    ok.airflow = basicAirflow("dak-trigger", "s3cret-pw");
    assertThat(ok.props().deid().airflow().auth()).isEqualTo(AirflowAuth.BASIC);
    assertThat(ok.props().deid().airflow().toString()).contains("username=dak-trigger").doesNotContain("s3cret-pw");

    for (Airflow bad : List.of(basicAirflow(null, "pw"), basicAirflow("", "pw"), basicAirflow("u", null),
        basicAirflow("u", " "))) {
      Draft d = validAirflow();
      d.airflow = bad;
      assertThatThrownBy(d::props).isInstanceOf(IllegalStateException.class)
          .hasMessageStartingWith("Invalid DAK mock configuration").hasMessageContaining("dak.deid.airflow.");
    }
  }

  @Test
  @DisplayName("nạp auth=basic cùng username và password từ thuộc tính kebab-case")
  void basicAuth_bindsFromProperties() {
    Map<String, String> source = Map.of("dak.deid.airflow.auth", "basic", "dak.deid.airflow.username", "dak-trigger",
        "dak.deid.airflow.password", "pw-from-env");

    DeidProperties deid = new Binder(new MapConfigurationPropertySource(source)).bind("dak.deid", DeidProperties.class).get();

    assertThat(deid.airflow().auth()).isEqualTo(AirflowAuth.BASIC);
    assertThat(deid.airflow().username()).isEqualTo("dak-trigger");
    assertThat(deid.airflow().password()).isEqualTo("pw-from-env");
  }

  private static JobEnv.Entry entry(String name, String pattern, String defaultValue) {
    return new JobEnv.Entry(name, pattern, defaultValue);
  }

  private static JobEnv jobEnv(JobEnv.Entry... entries) {
    return new JobEnv(10, List.of(entries));
  }

  @Nested
  @DisplayName("danh sách env người gửi phiếu được nhập (dak.deid.job-env)")
  class JobEnvConfig {

    @Test
    @DisplayName("không cấu hình thì dùng danh sách mặc định: 5 env, tối đa 10 mục, OUTPUT_DB và OUTPUT_TABLE không có giá trị mặc định tĩnh")
    void defaults() {
      JobEnv jobEnv = DeidProperties.defaults().jobEnv();

      assertThat(jobEnv.maxEntries()).isEqualTo(10);
      assertThat(jobEnv.allowed()).extracting(JobEnv.Entry::name)
          .containsExactly("OUTPUT_DB", "OUTPUT_TABLE", "OUTPUT_MODE", "OUTPUT_FORMAT", "ENABLE_HIVE_SUPPORT");
      assertThat(jobEnv.find("OUTPUT_DB").orElseThrow().defaultValue()).isNull();
      assertThat(jobEnv.find("OUTPUT_TABLE").orElseThrow().defaultValue()).isNull();
      assertThat(jobEnv.find("OUTPUT_MODE").orElseThrow().defaultValue()).isEqualTo("overwrite");
      assertThat(jobEnv.find("OUTPUT_FORMAT").orElseThrow().defaultValue()).isEqualTo("delta");
      assertThat(jobEnv.find("ENABLE_HIVE_SUPPORT").orElseThrow().defaultValue()).isEqualTo("true");
      assertThat(jobEnv.find("NOT_THERE")).isEmpty();
    }

    @Test
    @DisplayName("mặc định hợp lệ: mọi giá trị mặc định khớp quy tắc của chính nó")
    void defaults_areValid() {
      assertThatCode(() -> valid().props()).doesNotThrowAnyException();
      for (JobEnv.Entry e : DeidProperties.defaults().jobEnv().allowed()) {
        if (e.defaultValue() != null) {
          assertThat(e.defaultValue()).matches(e.pattern());
        }
      }
    }

    @Test
    @DisplayName("nạp từ thuộc tính kebab-case, danh sách cấu hình thay hẳn danh sách mặc định")
    void binds_fromKebabCaseProperties() {
      Map<String, String> source = Map.of(
          "dak.deid.job-env.max-entries", "3",
          "dak.deid.job-env.allowed[0].name", "OUTPUT_FORMAT",
          "dak.deid.job-env.allowed[0].pattern", "delta|hive",
          "dak.deid.job-env.allowed[0].default-value", "delta",
          "dak.deid.job-env.allowed[1].name", "OUTPUT_DB",
          "dak.deid.job-env.allowed[1].pattern", "[a-z0-9_]{1,10}");

      DeidProperties deid = new Binder(new MapConfigurationPropertySource(source)).bind("dak.deid", DeidProperties.class).get();

      assertThat(deid.jobEnv().maxEntries()).isEqualTo(3);
      assertThat(deid.jobEnv().allowed()).extracting(JobEnv.Entry::name).containsExactly("OUTPUT_FORMAT", "OUTPUT_DB");
      assertThat(deid.jobEnv().find("OUTPUT_FORMAT").orElseThrow().defaultValue()).isEqualTo("delta");
      assertThat(deid.jobEnv().find("OUTPUT_DB").orElseThrow().defaultValue()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"DB_NAME", "TABLE_NAME", "PSEUDO_COLUMNS", "PSEUDO_MODE", "VERIFY", "WAREHOUSE_PATH",
        "MAPPING_TABLE_PATH", "INPUT_PATH", "INPUT_FORMAT", "INPUT_OPTION_DELIMITER", "CRYPTO_PREFIX_SOURCE",
        "CRYPTO_KEY_PREFIX_DIR", "VAULT_ADDR", "VAULT_TOKEN", "DAK_ADDR", "DAK_CLIENT_ID", "JAVA_TOOL_OPTIONS", "LD_PRELOAD",
        "SPARK_LOCAL_DIRS", "HADOOP_CONF_DIR", "KRB5CCNAME", "KRB5_CONFIG", "MY_TOKEN", "API_SECRET", "DB_PASSWORD",
        "USER_PASSWD", "ENCRYPTION_KEY", "KEYTAB_PATH", "MONKEY"})
    @DisplayName("env bị cấm trong danh sách được phép làm app không khởi động và thông điệp nêu đúng tên")
    void forbiddenName_rejected(String name) {
      Draft d = valid();
      d.jobEnv = jobEnv(entry(name, "[a-z]+", null));

      assertThatThrownBy(d::props).isInstanceOf(IllegalStateException.class)
          .hasMessageStartingWith("Invalid DAK mock configuration")
          .hasMessageContaining("dak.deid.job-env").hasMessageContaining(name).hasMessageContaining("not allowed");
    }

    @ParameterizedTest
    @ValueSource(strings = {"OUTPUT_DB", "OUTPUT_TABLE", "OUTPUT_MODE", "OUTPUT_FORMAT", "ENABLE_HIVE_SUPPORT", "TZ",
        "MALLOC_ARENA_MAX"})
    @DisplayName("env bình thường, kể cả OUTPUT_DB, OUTPUT_TABLE và OUTPUT_MODE, được phép đưa vào danh sách")
    void ordinaryName_accepted(String name) {
      Draft d = valid();
      d.jobEnv = jobEnv(entry(name, "[a-z]+", null));

      assertThat(d.props().deid().jobEnv().find(name)).isPresent();
      assertThat(JobEnv.isForbidden(name)).isFalse();
    }

    @Test
    @DisplayName("tên sai dạng, trùng, quy tắc không biên dịch được, giá trị mặc định không khớp quy tắc: đều bị từ chối")
    void invalidEntries_rejected() {
      for (String badName : new String[] {"lower", "1ABC", "A-B", "A B", "", "A".repeat(65)}) {
        assertRejected(valid(), d -> d.jobEnv = jobEnv(entry(badName, "x", null)), "dak.deid.job-env", "name");
      }
      assertRejected(valid(), d -> d.jobEnv = jobEnv(entry(null, "x", null)), "dak.deid.job-env", "name");
      assertRejected(valid(), d -> d.jobEnv = jobEnv(entry("OUTPUT_MODE", "a", null), entry("OUTPUT_MODE", "b", null)),
          "dak.deid.job-env", "duplicate", "OUTPUT_MODE");
      assertRejected(valid(), d -> d.jobEnv = jobEnv(entry("OUTPUT_MODE", "(", null)), "dak.deid.job-env", "pattern",
          "OUTPUT_MODE");
      assertRejected(valid(), d -> d.jobEnv = jobEnv(entry("OUTPUT_MODE", null, null)), "dak.deid.job-env", "pattern",
          "OUTPUT_MODE");
      assertRejected(valid(), d -> d.jobEnv = jobEnv(entry("OUTPUT_MODE", "overwrite|append", "replace")),
          "dak.deid.job-env", "default-value", "OUTPUT_MODE");
    }

    @Test
    @DisplayName("max-entries phải từ 1 đến 50; danh sách rỗng là hợp lệ (không ai nhập được env nào)")
    void maxEntries_andEmptyList() {
      assertRejected(valid(), d -> d.jobEnv = new JobEnv(0, List.of()), "dak.deid.job-env.max-entries");
      assertRejected(valid(), d -> d.jobEnv = new JobEnv(51, List.of()), "dak.deid.job-env.max-entries");
      Draft ok = valid();
      ok.jobEnv = new JobEnv(1, List.of());
      assertThat(ok.props().deid().jobEnv().allowed()).isEmpty();
    }

    @Test
    @DisplayName("flow tắt thì bỏ qua kiểm danh sách env")
    void disabled_skipsTheCheck() {
      Draft d = valid();
      d.enabled = false;
      d.jobEnv = jobEnv(entry("DB_NAME", "x", null));

      assertThatCode(d::props).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("isForbidden: cấm theo tên đúng, tiền tố và từ khoá; không cấm OUTPUT_*")
    void isForbidden_rules() {
      assertThat(JobEnv.isForbidden("DB_NAME")).isTrue();
      assertThat(JobEnv.isForbidden("INPUT_ANYTHING")).isTrue();
      assertThat(JobEnv.isForbidden("CRYPTO_X")).isTrue();
      assertThat(JobEnv.isForbidden("SOME_API_TOKEN")).isTrue();
      assertThat(JobEnv.isForbidden("INPUTS")).isFalse();
      assertThat(JobEnv.isForbidden("OUTPUT_DB")).isFalse();
      assertThat(JobEnv.isForbidden("OUTPUT_TABLE")).isFalse();
      assertThat(JobEnv.isForbidden("OUTPUT_MODE")).isFalse();
    }
  }

  @Nested
  @DisplayName("giá trị mặc định và nạp cấu hình")
  class Defaults {

    @Test
    @DisplayName("DakProperties dựng bằng 7 tham số cũ vẫn chạy và có cấu hình khử định danh mặc định")
    void legacyConstructor_usesDeidDefaults() {
      DakProperties props = new DakProperties(List.of(), List.of(), List.of(), vault(DEFAULT_VAULT_TEMPLATE),
          new DakProperties.Yc(50, false), new DakProperties.Admin(null), Duration.ofSeconds(60));

      DeidProperties deid = props.deid();
      assertThat(deid.enabled()).isTrue();
      assertThat(deid.allowedModes()).containsExactly(Mode.HASH);
      assertThat(deid.maxActivePerTeam()).isEqualTo(20);
      assertThat(deid.maxUploadBytes()).isEqualTo(65536);
      assertThat(deid.outputDatabase()).isEmpty();
      assertThat(deid.outputTableTemplate()).isEqualTo("{table}_deid_{yyyyMMdd}_{seq}");
      assertThat(deid.submitter()).isEqualTo(Submitter.NOOP);
      assertThat(deid.portalUsers()).isEmpty();
      assertThat(deid.portalEnabled()).isFalse();
    }

    @Test
    @DisplayName("deid null thì dùng mặc định")
    void nullDeid_usesDefaults() {
      DakProperties props = new DakProperties(List.of(), List.of(), List.of(), vault(DEFAULT_VAULT_TEMPLATE),
          new DakProperties.Yc(50, false), new DakProperties.Admin(null), Duration.ofSeconds(60), null);

      assertThat(props.deid()).isEqualTo(DeidProperties.defaults());
    }

    @Test
    @DisplayName("portal chỉ bật khi enabled và có ít nhất một tài khoản")
    void portalEnabled_requiresUsers() {
      assertThat(valid().deid().portalEnabled()).isFalse();
      assertThat(valid().user("hungvt", "teamtscauiy", TOKEN_A).deid().portalEnabled()).isTrue();
      Draft off = valid().user("hungvt", "teamtscauiy", TOKEN_A);
      off.enabled = false;
      assertThat(off.deid().portalEnabled()).isFalse();
    }

    @Test
    @DisplayName("danh sách trong cấu hình được sao chép, sửa danh sách gốc không đổi cấu hình")
    void lists_areDefensivelyCopied() {
      List<Mode> modes = new ArrayList<>(List.of(Mode.HASH));
      Draft d = valid();
      d.modes = modes;
      DeidProperties deid = d.deid();

      modes.add(Mode.ENCRYPT);

      assertThat(deid.allowedModes()).containsExactly(Mode.HASH);
    }

    @Test
    @DisplayName("nạp từ thuộc tính dạng kebab-case của Spring, gồm enum và danh sách")
    void binds_fromKebabCaseProperties() {
      Map<String, String> source = Map.ofEntries(
          Map.entry("dak.deid.submitter", "airflow"),
          Map.entry("dak.deid.allowed-modes[0]", "HASH"),
          Map.entry("dak.deid.max-active-per-team", "5"),
          Map.entry("dak.deid.output-database", "analytics"),
          Map.entry("dak.deid.kubernetes.namespace", "vlp-ns"),
          Map.entry("dak.deid.airflow.base-url", "http://airflow:8080"),
          Map.entry("dak.deid.airflow.allowed-dag-ids[0]", "deid_a"),
          Map.entry("dak.deid.airflow.allowed-dag-ids[1]", "deid_b"),
          Map.entry("dak.deid.airflow.auth", "client-credentials"),
          Map.entry("dak.deid.airflow.token-url", "https://sso/token"),
          Map.entry("dak.deid.airflow.client-id", "client-airflow"),
          Map.entry("dak.deid.airflow.client-secret", "s3cret"),
          Map.entry("dak.deid.portal-users[0].username", "hungvt"),
          Map.entry("dak.deid.portal-users[0].team", "teamtscauiy"),
          Map.entry("dak.deid.portal-users[0].token", TOKEN_A));

      DeidProperties deid = new Binder(new MapConfigurationPropertySource(source))
          .bind("dak.deid", DeidProperties.class).get();

      assertThat(deid.submitter()).isEqualTo(Submitter.AIRFLOW);
      assertThat(deid.maxActivePerTeam()).isEqualTo(5);
      assertThat(deid.outputDatabase()).isEqualTo("analytics");
      assertThat(deid.kubernetes().namespace()).isEqualTo("vlp-ns");
      assertThat(deid.kubernetes().apiServer()).isEqualTo("https://kubernetes.default.svc");
      assertThat(deid.airflow().allowedDagIds()).containsExactly("deid_a", "deid_b");
      assertThat(deid.airflow().auth()).isEqualTo(AirflowAuth.CLIENT_CREDENTIALS);
      assertThat(deid.portalUsers()).extracting(PortalUser::username).containsExactly("hungvt");
      assertThat(deid.allowedModes()).containsExactly(Mode.HASH);
    }

    @Test
    @DisplayName("nạp cả DakProperties qua Spring: không khai dak.deid thì lấy mặc định")
    void bindsWholeDakProperties_withoutDeidBlock_usesDefaults() {
      Map<String, String> source = Map.of("dak.vault.addr", "http://vault", "dak.vault.token", "vault-token");

      DakProperties props = new Binder(new MapConfigurationPropertySource(source)).bind("dak", DakProperties.class).get();

      assertThat(props.deid()).isEqualTo(DeidProperties.defaults());
      assertThat(props.clockSkew()).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    @DisplayName("cấu hình dak.deid sai thì việc nạp thất bại với thông điệp nêu đúng thuộc tính (ứng dụng không khởi động)")
    void bindsWholeDakProperties_withInvalidDeid_failsStartup() {
      Map<String, String> source = Map.of("dak.vault.addr", "http://vault", "dak.vault.token", "vault-token",
          "dak.deid.allowed-modes[0]", "ENCRYPT");

      assertThatThrownBy(() -> new Binder(new MapConfigurationPropertySource(source)).bind("dak", DakProperties.class))
          .hasStackTraceContaining("Invalid DAK mock configuration")
          .hasStackTraceContaining("dak.deid.allowed-modes");
    }

    @Test
    @DisplayName("chỉ khai một thuộc tính thì phần còn lại lấy mặc định")
    void binds_defaultsForMissingProperties() {
      DeidProperties deid = new Binder(new MapConfigurationPropertySource(Map.of("dak.deid.enabled", "true")))
          .bind("dak.deid", DeidProperties.class).get();

      assertThat(deid).isEqualTo(DeidProperties.defaults());
    }
  }

  @Nested
  @DisplayName("cấu hình chung")
  class General {

    @Test
    @DisplayName("cấu hình hợp lệ được chấp nhận")
    void valid_isAccepted() {
      assertThatCode(() -> valid().user("hungvt", "teamtscauiy", TOKEN_A).props()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("allowed-modes rỗng bị từ chối")
    void emptyModes_rejected() {
      assertRejected(valid(), d -> d.modes = List.of(), "dak.deid.allowed-modes");
    }

    @Test
    @DisplayName("ENCRYPT bị từ chối vì chưa hỗ trợ giao key")
    void encryptMode_rejected() {
      assertRejected(valid(), d -> d.modes = List.of(Mode.HASH, Mode.ENCRYPT), "dak.deid.allowed-modes", "ENCRYPT");
    }

    @Test
    @DisplayName("MAPPING bị từ chối vì chưa hỗ trợ giao key")
    void mappingMode_rejected() {
      assertRejected(valid(), d -> d.modes = List.of(Mode.MAPPING), "dak.deid.allowed-modes", "MAPPING");
    }

    @Test
    @DisplayName("max-active-per-team phải dương")
    void maxActivePerTeam_mustBePositive() {
      assertRejected(valid(), d -> d.maxActivePerTeam = 0, "dak.deid.max-active-per-team");
    }

    @Test
    @DisplayName("max-upload-bytes dưới 1 KB bị từ chối")
    void maxUploadBytes_belowMinimum_rejected() {
      assertRejected(valid(), d -> d.maxUploadBytes = 1023, "dak.deid.max-upload-bytes");
    }

    @Test
    @DisplayName("max-upload-bytes trên 1 MB bị từ chối")
    void maxUploadBytes_aboveMaximum_rejected() {
      assertRejected(valid(), d -> d.maxUploadBytes = 1_048_577, "dak.deid.max-upload-bytes");
    }

    @Test
    @DisplayName("max-upload-bytes đúng biên 1 KB và 1 MB được chấp nhận")
    void maxUploadBytes_boundaries_accepted() {
      Draft low = valid();
      low.maxUploadBytes = 1024;
      Draft high = valid();
      high.maxUploadBytes = 1_048_576;

      assertThatCode(low::props).doesNotThrowAnyException();
      assertThatCode(high::props).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("output-table-template thiếu {seq} bị từ chối")
    void template_missingSeq_rejected() {
      assertRejected(valid(), d -> d.outputTableTemplate = "{table}_deid", "dak.deid.output-table-template");
    }

    @Test
    @DisplayName("output-table-template thiếu {table} bị từ chối")
    void template_missingTable_rejected() {
      assertRejected(valid(), d -> d.outputTableTemplate = "deid_{seq}", "dak.deid.output-table-template");
    }

    @Test
    @DisplayName("output-table-template có ký tự ngoài [a-z0-9_] bị từ chối")
    void template_illegalCharacters_rejected() {
      assertRejected(valid(), d -> d.outputTableTemplate = "{table}-deid_{seq}", "dak.deid.output-table-template");
      assertRejected(valid(), d -> d.outputTableTemplate = "{table}_Deid_{seq}", "dak.deid.output-table-template");
      assertRejected(valid(), d -> d.outputTableTemplate = "{table}_{seq};drop", "dak.deid.output-table-template");
    }

    @Test
    @DisplayName("output-table-template có placeholder lạ bị từ chối")
    void template_unknownPlaceholder_rejected() {
      assertRejected(valid(), d -> d.outputTableTemplate = "{table}_{foo}_{seq}", "dak.deid.output-table-template");
    }

    @Test
    @DisplayName("output-database sai ký tự bị từ chối, để trống thì được")
    void outputDatabase_validated() {
      assertRejected(valid(), d -> d.outputDatabase = "Bad-Db", "dak.deid.output-database");
      Draft ok = valid();
      ok.outputDatabase = "analytics_db";
      assertThatCode(ok::props).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("deid tắt thì bỏ qua mọi kiểm tra")
    void disabled_skipsValidation() {
      Draft d = valid();
      d.enabled = false;
      d.maxActivePerTeam = -1;
      d.submitter = Submitter.AIRFLOW;

      assertThatCode(d::props).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("submitter noop không cần cấu hình Kubernetes hay Airflow")
    void noop_needsNoExternalConfig() {
      assertThatCode(() -> valid().user("hungvt", "teamtscauiy", TOKEN_A).props()).doesNotThrowAnyException();
    }
  }

  @Nested
  @DisplayName("tài khoản giả của portal")
  class PortalUsers {

    @Test
    @DisplayName("username sai định dạng bị từ chối")
    void username_invalid_rejected() {
      assertRejected(valid(), d -> d.user("Hung VT", "teamtscauiy", TOKEN_A), "dak.deid.portal-users", "username");
      assertRejected(valid(), d -> d.user("", "teamtscauiy", TOKEN_A), "dak.deid.portal-users", "username");
      assertRejected(valid(), d -> d.user("a".repeat(64), "teamtscauiy", TOKEN_A), "dak.deid.portal-users", "username");
    }

    @Test
    @DisplayName("username trùng bị từ chối")
    void username_duplicate_rejected() {
      assertRejected(valid(), d -> d.user("hungvt", "team-a", TOKEN_A).user("hungvt", "team-b", TOKEN_B),
          "duplicate", "hungvt");
    }

    @Test
    @DisplayName("team sai định dạng bị từ chối")
    void team_invalid_rejected() {
      assertRejected(valid(), d -> d.user("hungvt", "Team A", TOKEN_A), "dak.deid.portal-users[hungvt].team");
      assertRejected(valid(), d -> d.user("hungvt", null, TOKEN_A), "dak.deid.portal-users[hungvt].team");
    }

    @Test
    @DisplayName("token ngắn hơn 24 ký tự bị từ chối và thông điệp không lộ token")
    void token_tooShort_rejected_withoutLeakingIt() {
      String shortToken = "s".repeat(23);
      Draft d = valid();
      d.user("hungvt", "teamtscauiy", shortToken);

      assertThatThrownBy(d::props)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("dak.deid.portal-users[hungvt].token")
          .hasMessageNotContaining(shortToken);
    }

    @Test
    @DisplayName("token thiếu hoặc trống bị từ chối")
    void token_missing_rejected() {
      assertRejected(valid(), d -> d.user("hungvt", "teamtscauiy", null), "dak.deid.portal-users[hungvt].token");
      assertRejected(valid(), d -> d.user("hungvt", "teamtscauiy", "  "), "dak.deid.portal-users[hungvt].token");
    }

    @Test
    @DisplayName("hai tài khoản dùng chung token bị từ chối, thông điệp nêu tên chứ không nêu token")
    void token_duplicate_rejected_withoutLeakingIt() {
      Draft d = valid().user("alice", "team-a", TOKEN_A).user("bob", "team-b", TOKEN_A);

      assertThatThrownBy(d::props)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("alice")
          .hasMessageContaining("bob")
          .hasMessageNotContaining(TOKEN_A);
    }

    @Test
    @DisplayName("nhiều tài khoản hợp lệ, kể cả hai người cùng một team")
    void multipleUsers_sameTeam_accepted() {
      Draft d = valid().user("alice", "team-a", TOKEN_A).user("bob", "team-a", TOKEN_B);

      assertThatCode(d::props).doesNotThrowAnyException();
      assertThat(d.deid().portalUsers()).hasSize(2);
    }

    @Test
    @DisplayName("path-template của Vault có {tenant} thì mọi tài khoản phải có tenant")
    void vaultTemplateWithTenant_requiresTenant() {
      assertRejected(valid(), d -> {
        d.vaultTemplate = "base/{tenant}/{database}.{table}";
        d.user("hungvt", "teamtscauiy", TOKEN_A);
      }, "dak.deid.portal-users[hungvt].tenant");
    }

    @Test
    @DisplayName("path-template của Vault có {workspace} thì mọi tài khoản phải có workspace")
    void vaultTemplateWithWorkspace_requiresWorkspace() {
      assertRejected(valid(), d -> {
        d.vaultTemplate = "base/{workspace}/{database}.{table}";
        d.user("hungvt", "teamtscauiy", TOKEN_A);
      }, "dak.deid.portal-users[hungvt].workspace");
    }

    @Test
    @DisplayName("có tenant và workspace hợp lệ thì chấp nhận")
    void vaultTemplateWithTenantAndWorkspace_accepted() {
      Draft d = valid();
      d.vaultTemplate = "base/{tenant}/{workspace}/{database}.{table}";
      d.user(new PortalUser("hungvt", "teamtscauiy", "vlp-tenant", "wsw7vtwvi", TOKEN_A));

      assertThatCode(d::props).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("tenant sai định dạng bị từ chối")
    void tenant_invalid_rejected() {
      assertRejected(valid(), d -> {
        d.vaultTemplate = "base/{tenant}/{database}.{table}";
        d.user(new PortalUser("hungvt", "teamtscauiy", "Bad Tenant", null, TOKEN_A));
      }, "dak.deid.portal-users[hungvt].tenant");
    }
  }

  @Nested
  @DisplayName("submitter kubernetes")
  class KubernetesSubmitter {

    @Test
    @DisplayName("cấu hình đủ được chấp nhận")
    void valid_accepted() {
      assertThatCode(validKubernetes()::props).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("thiếu namespace bị từ chối")
    void missingNamespace_rejected() {
      assertRejected(validKubernetes(), d -> d.k8s(null, "/deid/sparkapplication.yaml"),
          "dak.deid.kubernetes.namespace");
    }

    @Test
    @DisplayName("namespace sai định dạng DNS-1123 bị từ chối")
    void badNamespace_rejected() {
      assertRejected(validKubernetes(), d -> d.k8s("Bad_NS", "/deid/sparkapplication.yaml"),
          "dak.deid.kubernetes.namespace");
      assertRejected(validKubernetes(), d -> d.k8s("a".repeat(64), "/deid/sparkapplication.yaml"),
          "dak.deid.kubernetes.namespace");
    }

    @Test
    @DisplayName("thiếu template-file bị từ chối")
    void missingTemplateFile_rejected() {
      assertRejected(validKubernetes(), d -> d.k8s("vlp-ns", " "), "dak.deid.kubernetes.template-file");
    }

    @Test
    @DisplayName("thiếu api-server bị từ chối")
    void missingApiServer_rejected() {
      assertRejected(validKubernetes(), d -> d.kubernetes = new Kubernetes("", "vlp-ns", "/deid/x.yaml"),
          "dak.deid.kubernetes.api-server");
    }
  }

  @Nested
  @DisplayName("submitter airflow")
  class AirflowSubmitter {

    @Test
    @DisplayName("cấu hình client-credentials đủ được chấp nhận")
    void clientCredentials_valid_accepted() {
      assertThatCode(validAirflow()::props).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("cấu hình bearer tĩnh đủ được chấp nhận")
    void bearer_valid_accepted() {
      Draft d = validAirflow().airflowWith("http://airflow:8080", List.of("deid_a"), AirflowAuth.BEARER, null, null,
          null, "static-token");

      assertThatCode(d::props).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("airflow cũng cần kubernetes.namespace vì mock tạo Secret key ở đó")
    void airflow_requiresNamespaceForKeySecret() {
      assertRejected(validAirflow(), d -> d.k8s(null, null), "dak.deid.kubernetes.namespace");
    }

    @Test
    @DisplayName("base-url thiếu hoặc không phải http(s) bị từ chối")
    void baseUrl_invalid_rejected() {
      assertRejected(validAirflow(), d -> d.airflowWith(null, List.of("a"), AirflowAuth.BEARER, null, null, null, "t"),
          "dak.deid.airflow.base-url");
      assertRejected(validAirflow(), d -> d.airflowWith("ftp://airflow", List.of("a"), AirflowAuth.BEARER, null, null,
          null, "t"), "dak.deid.airflow.base-url");
      assertRejected(validAirflow(), d -> d.airflowWith("airflow:8080", List.of("a"), AirflowAuth.BEARER, null, null,
          null, "t"), "dak.deid.airflow.base-url");
    }

    @Test
    @DisplayName("allowed-dag-ids rỗng bị từ chối")
    void allowedDagIds_empty_rejected() {
      assertRejected(validAirflow(), d -> d.airflowWith("http://airflow:8080", List.of(), AirflowAuth.BEARER, null, null,
          null, "t"), "dak.deid.airflow.allowed-dag-ids");
    }

    @Test
    @DisplayName("dagId có ký tự nguy hiểm bị từ chối (chống path traversal vào URL Airflow)")
    void allowedDagIds_unsafeCharacters_rejected() {
      for (String bad : List.of("a/b", "..", "a?b", "a#b", "a b", "", "x".repeat(251))) {
        assertRejected(validAirflow(), d -> d.airflowWith("http://airflow:8080", List.of(bad), AirflowAuth.BEARER, null,
            null, null, "t"), "dak.deid.airflow.allowed-dag-ids");
      }
    }

    @Test
    @DisplayName("dagId trùng bị từ chối")
    void allowedDagIds_duplicate_rejected() {
      assertRejected(validAirflow(), d -> d.airflowWith("http://airflow:8080", List.of("a", "a"), AirflowAuth.BEARER,
          null, null, null, "t"), "dak.deid.airflow.allowed-dag-ids", "duplicate");
    }

    @Test
    @DisplayName("dagId hợp lệ có dấu chấm, gạch ngang và chữ hoa được chấp nhận")
    void allowedDagIds_validCharacters_accepted() {
      Draft d = validAirflow().airflowWith("http://airflow:8080", List.of("Deid.Pseudo-nymize_1", "x".repeat(250)),
          AirflowAuth.BEARER, null, null, null, "t");

      assertThatCode(d::props).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("client-credentials thiếu token-url, client-id hoặc client-secret bị từ chối")
    void clientCredentials_missingParts_rejected() {
      List<String> dags = List.of("a");
      assertRejected(validAirflow(), d -> d.airflowWith("http://airflow:8080", dags, AirflowAuth.CLIENT_CREDENTIALS,
          null, "c", "s", null), "dak.deid.airflow.token-url");
      assertRejected(validAirflow(), d -> d.airflowWith("http://airflow:8080", dags, AirflowAuth.CLIENT_CREDENTIALS,
          "https://sso/token", "", "s", null), "dak.deid.airflow.client-id");
      assertRejected(validAirflow(), d -> d.airflowWith("http://airflow:8080", dags, AirflowAuth.CLIENT_CREDENTIALS,
          "https://sso/token", "c", null, null), "dak.deid.airflow.client-secret", "AIRFLOW_CLIENT_SECRET");
    }

    @Test
    @DisplayName("bearer thiếu token bị từ chối")
    void bearer_missingToken_rejected() {
      assertRejected(validAirflow(), d -> d.airflowWith("http://airflow:8080", List.of("a"), AirflowAuth.BEARER, null,
          null, null, null), "dak.deid.airflow.token", "AIRFLOW_TOKEN");
    }
  }

  @Nested
  @DisplayName("không để lộ bí mật")
  class Secrets {

    @Test
    @DisplayName("toString của tài khoản portal che token")
    void portalUser_toString_masksToken() {
      PortalUser u = new PortalUser("hungvt", "teamtscauiy", null, null, TOKEN_A);

      assertThat(u.toString()).contains("hungvt").contains("***").doesNotContain(TOKEN_A);
    }

    @Test
    @DisplayName("toString của khối Airflow che client-secret và token")
    void airflow_toString_masksSecrets() {
      Airflow a = new Airflow("http://airflow:8080", List.of("a"), AirflowAuth.CLIENT_CREDENTIALS, "https://sso/token",
          "client-airflow", "", "very-secret-value", "static-token-value");

      assertThat(a.toString()).contains("http://airflow:8080").contains("***")
          .doesNotContain("very-secret-value").doesNotContain("static-token-value");
    }

    @Test
    @DisplayName("toString của cả khối deid không chứa token nào")
    void deid_toString_containsNoToken() {
      Draft d = validAirflow().user("hungvt", "teamtscauiy", TOKEN_A);

      assertThat(d.deid().toString()).doesNotContain(TOKEN_A).doesNotContain("client-secret");
    }

    @Test
    @DisplayName("toString của DakProperties không chứa token portal")
    void dakProperties_toString_containsNoPortalToken() {
      Draft d = valid().user("hungvt", "teamtscauiy", TOKEN_A);

      assertThat(d.props().toString()).doesNotContain(TOKEN_A);
    }
  }
}
