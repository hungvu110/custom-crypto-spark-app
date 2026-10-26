package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.yaml.snakeyaml.Yaml;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.NewRequest;
import vai.lakehouse.dakmock.deid.DeidRequest.SourceFile;

/**
 * D6 của flow phiếu khử định danh (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 4.5): dựng SparkApplication từ mẫu YAML bằng
 * cách thay placeholder trong GIÁ TRỊ chuỗi của cây đã parse, không nối chuỗi YAML, nên giá trị từ phiếu không thể thêm khoá hay
 * đổi cấu trúc.
 */
class ManifestRendererTest {

  private static final Path REPO_TEMPLATE = Path.of("k8s/deid/sparkapplication.yaml");
  private static final String NAMESPACE = "team-ns";
  private static final String SECRET = "deid-key-2026-000007";
  private static final Instant NOW = Instant.parse("2026-10-23T09:00:00Z");
  private static final String PURPOSE = "PURPOSE-SENTINEL-do-not-leak";
  private static final String PREFIX = "Invalid DAK mock configuration: dak.deid.kubernetes.template-file";

  private static final String MINIMAL = """
      apiVersion: sparkoperator.k8s.io/v1beta2
      kind: SparkApplication
      metadata:
        name: whatever
      spec:
        timeToLiveSeconds: 604800
        driver:
          env:
            - {name: DB_NAME, value: "${DEID_DATABASE}"}
            - {name: TABLE_NAME, value: "${DEID_TABLE}"}
            - {name: PSEUDO_COLUMNS, value: "${DEID_PSEUDO_COLUMNS}"}
            - {name: OUTPUT_DB, value: "${DEID_OUTPUT_DB}"}
            - {name: OUTPUT_TABLE, value: "${DEID_OUTPUT_TABLE}"}
            - {name: OUTPUT_MODE, value: overwrite}
            - {name: VERIFY, value: "true"}
        volumes:
          - name: keys
            secret:
              secretName: "${DEID_KEY_SECRET}"
      """;

  private static DeidRequest request(String code, String database, String table, List<ColumnSpec> columns) {
    NewRequest n = new NewRequest("team-a", "CREATOR-SENTINEL", database, table, columns, "deid_dag", PURPOSE,
        new SourceFile("phieu.json", "ab".repeat(32)));
    return DeidRequest.created(code, NOW, n).approved("admin", NOW, "out_db", "users_cdr_deid_20261023_7");
  }

  private static DeidRequest request() {
    return request("PYC-2026-000007", "demo_db", "users_cdr",
        List.of(new ColumnSpec("msisdn", Mode.HASH), new ColumnSpec("imei", Mode.HASH)));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object o) {
    return (Map<String, Object>) o;
  }

  @SuppressWarnings("unchecked")
  private static List<Object> list(Object o) {
    return (List<Object>) o;
  }

  private static String env(Map<String, Object> manifest, String name) {
    List<Object> env = list(map(map(manifest.get("spec")).get("driver")).get("env"));
    return env.stream().map(ManifestRendererTest::map).filter(e -> name.equals(e.get("name")))
        .map(e -> (String) e.get("value")).findFirst().orElseThrow();
  }

  @Test
  @DisplayName("render with the repo template fills the job parameters, name, namespace and Secret")
  void render_repoTemplate_fillsEverything() {
    ManifestRenderer renderer = ManifestRenderer.fromFile(REPO_TEMPLATE, NAMESPACE);

    Map<String, Object> m = renderer.render(request(), SECRET);

    assertThat(m.get("apiVersion")).isEqualTo("sparkoperator.k8s.io/v1beta2");
    assertThat(m.get("kind")).isEqualTo("SparkApplication");
    Map<String, Object> meta = map(m.get("metadata"));
    assertThat(meta.get("name")).isEqualTo("deid-2026-000007");
    assertThat(meta.get("namespace")).isEqualTo(NAMESPACE);
    assertThat(map(meta.get("labels"))).containsEntry("dak.vai/request-code", "PYC-2026-000007");
    assertThat(env(m, "DB_NAME")).isEqualTo("demo_db");
    assertThat(env(m, "TABLE_NAME")).isEqualTo("users_cdr");
    assertThat(env(m, "PSEUDO_COLUMNS")).isEqualTo("msisdn:HASH,imei:HASH");
    assertThat(env(m, "OUTPUT_DB")).isEqualTo("out_db");
    assertThat(env(m, "OUTPUT_TABLE")).isEqualTo("users_cdr_deid_20261023_7");
    assertThat(env(m, "OUTPUT_MODE")).isEqualTo("overwrite");
    assertThat(env(m, "VERIFY")).isEqualTo("true");
    Map<String, Object> volume = map(list(map(m.get("spec")).get("volumes")).get(0));
    assertThat(map(volume.get("secret")).get("secretName")).isEqualTo(SECRET);
  }

  @Test
  @DisplayName("render never copies purpose, source file, dagId, team or user into the manifest")
  void render_requestMetadata_notInManifest() {
    String yaml = ManifestRenderer.fromFile(REPO_TEMPLATE, NAMESPACE).renderYaml(request(), SECRET);

    assertThat(yaml).doesNotContain(PURPOSE).doesNotContain("phieu.json").doesNotContain("deid_dag")
        .doesNotContain("team-a").doesNotContain("CREATOR-SENTINEL").doesNotContain("abababab");
  }

  @Test
  @DisplayName("renderYaml output parses back to the same tree as render")
  void renderYaml_roundTrips() {
    ManifestRenderer renderer = ManifestRenderer.fromFile(REPO_TEMPLATE, NAMESPACE);

    String yaml = renderer.renderYaml(request(), SECRET);

    assertThat(new Yaml().<Object>load(yaml)).isEqualTo(renderer.render(request(), SECRET));
  }

  @Test
  @DisplayName("render returns a fresh tree each time so callers cannot corrupt later renders")
  void render_calledTwice_independentTrees() {
    ManifestRenderer renderer = new ManifestRenderer(MINIMAL, NAMESPACE);
    Map<String, Object> first = renderer.render(request(), SECRET);

    map(first.get("metadata")).put("name", "hijacked");
    list(map(map(first.get("spec")).get("driver")).get("env")).clear();
    Map<String, Object> second = renderer.render(request(), SECRET);

    assertThat(map(second.get("metadata")).get("name")).isEqualTo("deid-2026-000007");
    assertThat(env(second, "DB_NAME")).isEqualTo("demo_db");
  }

  @Test
  @DisplayName("render overrides name, namespace and labels and drops server-side fields from the template")
  void render_templateMetadata_overridden() {
    String template = MINIMAL.replace("""
        metadata:
          name: whatever
        """, """
        metadata:
          name: attacker-name
          namespace: kube-system
          generateName: x-
          resourceVersion: "5"
          uid: abc
          creationTimestamp: "2020-01-01T00:00:00Z"
          ownerReferences: [{name: o}]
          managedFields: [{manager: m}]
          labels:
            dak.vai/request-code: PYC-1999-000001
            keep: me
        status: {state: COMPLETED}
        """);

    Map<String, Object> m = new ManifestRenderer(template, NAMESPACE).render(request(), SECRET);

    Map<String, Object> meta = map(m.get("metadata"));
    assertThat(meta).containsOnlyKeys("name", "namespace", "labels");
    assertThat(meta.get("name")).isEqualTo("deid-2026-000007");
    assertThat(meta.get("namespace")).isEqualTo(NAMESPACE);
    assertThat(map(meta.get("labels"))).containsEntry("dak.vai/request-code", "PYC-2026-000007").containsEntry("keep", "me");
    assertThat(m).doesNotContainKey("status");
  }

  @Test
  @DisplayName("render substitutes inside longer strings")
  void render_embeddedPlaceholder_substituted() {
    String template = MINIMAL.replace("value: overwrite}", "value: overwrite}\n"
        + "      - {name: NOTE, value: \"job-${DEID_REQUEST_CODE}-x\"}");

    Map<String, Object> m = new ManifestRenderer(template, NAMESPACE).render(request(), SECRET);

    assertThat(env(m, "NOTE")).isEqualTo("job-PYC-2026-000007-x");
  }

  static Stream<Arguments> hostileValues() {
    List<ColumnSpec> ok = List.of(new ColumnSpec("c1", Mode.HASH));
    return Stream.of(
        Arguments.of("table with newline and new key", "demo_db", "t\nkind: Evil", ok),
        Arguments.of("table with quote", "demo_db", "t\"; rm", ok),
        Arguments.of("table with placeholder", "demo_db", "${DEID_KEY_SECRET}", ok),
        Arguments.of("table with dollar brace", "demo_db", "a${b}", ok),
        Arguments.of("table with space", "demo_db", "a b", ok),
        Arguments.of("table with colon", "demo_db", "a: b", ok),
        Arguments.of("empty table", "demo_db", "", ok),
        Arguments.of("table too long", "demo_db", "a".repeat(129), ok),
        Arguments.of("database with dash", "demo-db", "t", ok),
        Arguments.of("database with slash", "../db", "t", ok),
        Arguments.of("column with comma", "demo_db", "t", List.of(new ColumnSpec("a,b", Mode.HASH))),
        Arguments.of("column with colon", "demo_db", "t", List.of(new ColumnSpec("a:ENCRYPT", Mode.HASH))),
        Arguments.of("no columns", "demo_db", "t", List.of()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("hostileValues")
  @DisplayName("render rejects a value outside the placeholder's character set without echoing it")
  void render_hostileValue_rejectedWithFixedMessage(String label, String database, String table, List<ColumnSpec> columns) {
    ManifestRenderer renderer = new ManifestRenderer(MINIMAL, NAMESPACE);
    DeidRequest hostile = request("PYC-2026-000007", database, table, columns);

    assertThatThrownBy(() -> renderer.render(hostile, SECRET)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("invalid value for DEID_[A-Z_]+");
  }

  @ParameterizedTest
  @ValueSource(strings = {"deid-key-2026-00007", "deid-key-26-000007", "other-2026-000007", "deid-key-2026-000007\n", "",
      "deid-key-2026-000007-x"})
  @DisplayName("render rejects a key Secret name that is not deid-key-<year>-<6 digits>")
  void render_badSecretName_rejected(String secret) {
    ManifestRenderer renderer = new ManifestRenderer(MINIMAL, NAMESPACE);

    assertThatThrownBy(() -> renderer.render(request(), secret)).isInstanceOf(IllegalArgumentException.class)
        .hasMessage("invalid value for DEID_KEY_SECRET");
  }

  @Test
  @DisplayName("render rejects a request whose output table is not decided yet")
  void render_notApproved_rejected() {
    DeidRequest pending = DeidRequestTest.created();
    ManifestRenderer renderer = new ManifestRenderer(MINIMAL, NAMESPACE);

    assertThatThrownBy(() -> renderer.render(pending, SECRET)).isInstanceOf(IllegalArgumentException.class)
        .hasMessage("invalid value for DEID_OUTPUT_DB");
  }

  static Stream<Arguments> brokenTemplates() {
    return Stream.of(
        Arguments.of("missing DEID_DATABASE", MINIMAL.replace("${DEID_DATABASE}", "x"), "DEID_DATABASE"),
        Arguments.of("missing DEID_TABLE", MINIMAL.replace("${DEID_TABLE}", "x"), "DEID_TABLE"),
        Arguments.of("missing DEID_PSEUDO_COLUMNS", MINIMAL.replace("${DEID_PSEUDO_COLUMNS}", "x"), "DEID_PSEUDO_COLUMNS"),
        Arguments.of("missing DEID_OUTPUT_DB", MINIMAL.replace("${DEID_OUTPUT_DB}", "x"), "DEID_OUTPUT_DB"),
        Arguments.of("missing DEID_OUTPUT_TABLE", MINIMAL.replace("${DEID_OUTPUT_TABLE}", "x"), "DEID_OUTPUT_TABLE"),
        Arguments.of("missing DEID_KEY_SECRET", MINIMAL.replace("${DEID_KEY_SECRET}", "x"), "DEID_KEY_SECRET"),
        Arguments.of("unknown placeholder",
            MINIMAL.replace("overwrite}", "overwrite}\n      - {name: A, value: \"${DEID_NOPE}\"}"), "DEID_NOPE"),
        Arguments.of("wrong apiVersion", MINIMAL.replace("v1beta2", "v1"), "apiVersion"),
        Arguments.of("wrong kind", MINIMAL.replace("kind: SparkApplication", "kind: Pod"), "kind"),
        Arguments.of("output mode not overwrite", MINIMAL.replace("value: overwrite", "value: append"), "OUTPUT_MODE"),
        Arguments.of("output mode missing", MINIMAL.replace("      - {name: OUTPUT_MODE, value: overwrite}\n", ""),
            "OUTPUT_MODE"),
        Arguments.of("verify false", MINIMAL.replace("{name: VERIFY, value: \"true\"}", "{name: VERIFY, value: \"false\"}"),
            "VERIFY"),
        Arguments.of("verify missing", MINIMAL.replace("      - {name: VERIFY, value: \"true\"}\n", ""), "VERIFY"),
        Arguments.of("ttl too short", MINIMAL.replace("604800", "86400"), "timeToLiveSeconds"),
        Arguments.of("ttl not a number", MINIMAL.replace("604800", "soon"), "timeToLiveSeconds"),
        Arguments.of("literal secret in env",
            MINIMAL.replace("overwrite}", "overwrite}\n      - {name: DB_PASSWORD, value: hunter2}"), "DB_PASSWORD"),
        Arguments.of("literal token in env",
            MINIMAL.replace("overwrite}", "overwrite}\n      - {name: AIRFLOW_TOKEN, value: abc}"), "AIRFLOW_TOKEN"),
        Arguments.of("not a mapping", "- a\n- b\n", "mapping"),
        Arguments.of("empty", "", "mapping"),
        Arguments.of("two documents", MINIMAL + "---\n" + MINIMAL, "one YAML document"),
        Arguments.of("broken yaml", "a: [unclosed\n", "not valid YAML"),
        Arguments.of("custom java tag", MINIMAL.replace("name: whatever", "name: !!java.net.URL \"http://x\""), "not valid YAML"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("brokenTemplates")
  @DisplayName("constructor rejects an unsafe or incomplete template at startup")
  void constructor_brokenTemplate_failsFast(String label, String template, String mention) {
    assertThatThrownBy(() -> new ManifestRenderer(template, NAMESPACE)).isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith(PREFIX).hasMessageContaining(mention);
  }

  @Test
  @DisplayName("constructor accepts a template without timeToLiveSeconds and with secretKeyRef env")
  void constructor_noTtlAndSecretKeyRef_ok() {
    String template = MINIMAL.replace("  timeToLiveSeconds: 604800\n", "").replace("overwrite}", "overwrite}\n"
        + "      - name: DB_PASSWORD\n        valueFrom: {secretKeyRef: {name: s, key: k}}");

    assertThat(new ManifestRenderer(template, NAMESPACE).render(request(), SECRET)).containsKey("spec");
  }

  @Test
  @DisplayName("constructor rejects YAML aliases that could expand into a huge tree")
  void constructor_aliasBomb_rejected() {
    String bomb = """
        apiVersion: sparkoperator.k8s.io/v1beta2
        kind: SparkApplication
        metadata: {name: x}
        spec:
          a: &a [x, x, x, x, x, x, x, x, x]
          b: &b [*a, *a, *a, *a, *a, *a, *a, *a, *a]
          c: &c [*b, *b, *b, *b, *b, *b, *b, *b, *b]
          d: [*c, *c, *c, *c, *c, *c, *c, *c, *c]
        """;

    assertThatThrownBy(() -> new ManifestRenderer(bomb, NAMESPACE)).isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith(PREFIX);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "Team", "a_b", "-a", "a-",
      "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "a b"})
  @DisplayName("constructor rejects a namespace that is not a DNS-1123 label")
  void constructor_badNamespace_failsFast(String namespace) {
    assertThatThrownBy(() -> new ManifestRenderer(MINIMAL, namespace)).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("namespace");
  }

  @Test
  @DisplayName("fromFile reports a missing file with a fixed message")
  void fromFile_missing_failsFast() {
    assertThatThrownBy(() -> ManifestRenderer.fromFile(Path.of("no/such/template.yaml"), NAMESPACE))
        .isInstanceOf(IllegalStateException.class).hasMessageStartingWith(PREFIX).hasMessageContaining("cannot be read");
  }

  @Test
  @DisplayName("fromFile loads a template written to disk")
  void fromFile_tempFile_ok(@TempDir Path dir) throws Exception {
    Path file = Files.writeString(dir.resolve("t.yaml"), MINIMAL);

    assertThat(ManifestRenderer.fromFile(file, NAMESPACE).render(request(), SECRET)).containsKey("metadata");
  }
}
