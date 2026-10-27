package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.Airflow;
import vai.lakehouse.dakmock.config.DeidProperties.AirflowAuth;
import vai.lakehouse.dakmock.config.DeidProperties.JobEnv;
import vai.lakehouse.dakmock.config.DeidProperties.Kubernetes;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.config.DeidProperties.Submitter;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.NewRequest;
import vai.lakehouse.dakmock.deid.DeidRequest.SourceFile;

/**
 * D8 (plan mục 4.7): trigger DAG của người dùng chọn qua Airflow 2.x REST {@code /api/v1}, xác thực bằng token Keycloak
 * ({@code client_credentials}) hoặc token tĩnh. {@code conf} chỉ có TÊN Secret chứa key. Mọi lỗi quy về thông điệp cố định.
 */
class AirflowJobSubmitterTest {

  private static final String SECRET = "deid-key-2026-000007";
  private static final String CLIENT_SECRET = "client-secret-value";
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String TOKEN_OK = "{\"access_token\":\"tok-1\",\"expires_in\":300}";
  private static final String DAG_RUN_OK = "{\"dag_run_id\":\"deid-2026-000007\",\"state\":\"queued\"}";
  private static final JobRef REF = new JobRef(JobRef.Kind.AIRFLOW_DAG_RUN, "deid-2026-000007");

  /** Đồng hồ chỉnh tay để thử hạn token. */
  private static final class MutableClock extends Clock {
    private Instant now = Instant.parse("2026-10-23T09:00:00Z");

    void advance(Duration d) {
      now = now.plus(d);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }

  private MockWebServer server;
  private final Map<String, Deque<MockResponse>> responses = new ConcurrentHashMap<>();
  private final List<RecordedRequest> seen = new CopyOnWriteArrayList<>();
  private final MutableClock clock = new MutableClock();

  @BeforeEach
  void setUp() throws IOException {
    server = new MockWebServer();
    server.setDispatcher(new Dispatcher() {
      @Override
      public MockResponse dispatch(RecordedRequest request) {
        seen.add(request);
        String key = request.getPath().startsWith("/token") ? "token" : "airflow";
        Deque<MockResponse> queue = responses.get(key);
        MockResponse next = queue == null ? null : queue.poll();
        return next == null ? new MockResponse().setResponseCode(599) : next;
      }
    });
    server.start();
  }

  @AfterEach
  void tearDown() throws IOException {
    server.shutdown();
  }

  private void token(MockResponse... r) {
    responses.computeIfAbsent("token", k -> new ArrayDeque<>()).addAll(List.of(r));
  }

  private void airflow(MockResponse... r) {
    responses.computeIfAbsent("airflow", k -> new ArrayDeque<>()).addAll(List.of(r));
  }

  private static MockResponse json(int status, String body) {
    return new MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(body);
  }

  private long count(String prefix) {
    return seen.stream().filter(r -> r.getPath().startsWith(prefix)).count();
  }

  private Airflow config(String scope, List<String> dags) {
    return new Airflow(server.url("/airflow").toString(), dags, AirflowAuth.CLIENT_CREDENTIALS,
        server.url("/token").toString(), "dak-client", scope, CLIENT_SECRET, null);
  }

  private AirflowJobSubmitter submitter(Airflow cfg) {
    return submitter(cfg, null);
  }

  private AirflowJobSubmitter submitter(Airflow cfg, JobEnv jobEnv) {
    DeidProperties deid = new DeidProperties(true, null, 20, 65536, null, null, Submitter.AIRFLOW,
        new Kubernetes(null, "ns", null), cfg, null, jobEnv);
    return new AirflowJobSubmitter(deid, new AirflowTokens(cfg, clock));
  }

  private AirflowJobSubmitter submitter() {
    return submitter(config("openid", List.of("deid_dag", "other_dag")));
  }

  private static DeidRequest request(String table, String dagId) {
    return DeidRequest.created("PYC-2026-000007", Instant.parse("2026-10-23T08:00:00Z"),
            new NewRequest("team-a", "alice", "demo_db", table,
                List.of(new ColumnSpec("msisdn", Mode.HASH), new ColumnSpec("imei", Mode.HASH)), dagId, "purpose",
                new SourceFile("f.json", "ab".repeat(32))))
        .approved("admin", Instant.parse("2026-10-23T09:00:00Z"), "demo_db", "users_cdr_deid_20261023_7");
  }

  private static DeidRequest request() {
    return request("users_cdr", "deid_dag");
  }

  @Test
  @DisplayName("submit gets a Keycloak token then triggers the DAG run with the table parameters and the Secret NAME")
  void submit_ok_triggersDagRun() throws Exception {
    token(json(200, TOKEN_OK));
    airflow(json(200, DAG_RUN_OK));

    JobRef ref = submitter().submit(request(), SECRET);

    assertThat(ref).isEqualTo(REF);
    RecordedRequest tokenReq = seen.get(0);
    assertThat(tokenReq.getMethod()).isEqualTo("POST");
    assertThat(tokenReq.getHeader("Authorization")).isEqualTo("Basic "
        + Base64.getEncoder().encodeToString(("dak-client:" + CLIENT_SECRET).getBytes()));
    assertThat(tokenReq.getHeader("Content-Type")).startsWith("application/x-www-form-urlencoded");
    assertThat(tokenReq.getBody().readUtf8()).isEqualTo("grant_type=client_credentials&scope=openid");
    RecordedRequest run = seen.get(1);
    assertThat(run.getMethod()).isEqualTo("POST");
    assertThat(run.getPath()).isEqualTo("/airflow/api/v1/dags/deid_dag/dagRuns");
    assertThat(run.getHeader("Authorization")).isEqualTo("Bearer tok-1");
    JsonNode body = JSON.readTree(run.getBody().readUtf8());
    assertThat(body.path("dag_run_id").asText()).isEqualTo("deid-2026-000007");
    assertThat(JSON.convertValue(body.path("conf"), Map.class)).isEqualTo(Map.of(
        "requestCode", "PYC-2026-000007", "database", "demo_db", "table", "users_cdr",
        "pseudoColumns", "msisdn:HASH,imei:HASH", "keySecretName", SECRET,
        "env", Map.of("ENABLE_HIVE_SUPPORT", "true", "OUTPUT_DB", "demo_db", "OUTPUT_FORMAT", "delta",
            "OUTPUT_MODE", "overwrite", "OUTPUT_TABLE", "users_cdr_deid_20261023_7")));
    assertThat(body.path("conf").has("outputDb")).isFalse();
    assertThat(body.path("conf").has("outputTable")).isFalse();
  }

  private static DeidRequest requestWithFinalEnv(Map<String, String> env) {
    return DeidRequest.created("PYC-2026-000007", Instant.parse("2026-10-23T08:00:00Z"),
            new NewRequest("team-a", "alice", "demo_db", "users_cdr", List.of(new ColumnSpec("msisdn", Mode.HASH)),
                "deid_dag", "purpose", new SourceFile("f.json", "ab".repeat(32)), env))
        .approved("admin", Instant.parse("2026-10-23T09:00:00Z"), env.getOrDefault("OUTPUT_DB", "demo_db"),
            env.getOrDefault("OUTPUT_TABLE", "users_cdr_deid_20261023_7"), env);
  }

  @Test
  @DisplayName("submit sends the final env of the request as conf.env, without separate outputDb and outputTable keys")
  void submit_sendsTheFinalEnv() throws Exception {
    token(json(200, TOKEN_OK));
    airflow(json(200, DAG_RUN_OK));
    Map<String, String> env = Map.of("OUTPUT_DB", "deid_out", "OUTPUT_TABLE", "users_pseudo", "OUTPUT_MODE", "append",
        "OUTPUT_FORMAT", "iceberg", "ENABLE_HIVE_SUPPORT", "false");

    submitter().submit(requestWithFinalEnv(env), SECRET);

    JsonNode conf = JSON.readTree(seen.get(1).getBody().readUtf8()).path("conf");
    assertThat(JSON.convertValue(conf.path("env"), Map.class)).isEqualTo(env);
    assertThat(conf.has("outputDb")).isFalse();
    assertThat(conf.toString()).doesNotContain("base64:");
  }

  @Test
  @DisplayName("submit refuses an env that is no longer allowed or is hostile, before any network call")
  void submit_badEnv_noNetwork() {
    JobEnv noMode = new JobEnv(10, List.of(new JobEnv.Entry("OUTPUT_DB", "[a-z0-9_]{1,128}", null),
        new JobEnv.Entry("OUTPUT_TABLE", "[a-z0-9_]{1,128}", null)));
    Airflow cfg = config("openid", List.of("deid_dag"));

    for (Map<String, String> bad : List.of(Map.of("DB_NAME", "other_table"), Map.of("OUTPUT_MODE", "append"),
        Map.of("OUTPUT_FORMAT", "not-a-format"), Map.of("OUTPUT_TABLE", "A B"))) {
      JobEnv allowed = bad.containsKey("OUTPUT_MODE") ? noMode : null;
      assertThatThrownBy(() -> submitter(cfg, allowed).submit(requestWithFinalEnv(bad), SECRET))
          .isInstanceOf(JobSubmitException.class).hasMessage("invalid job parameters");
    }
    assertThat(seen).isEmpty();
  }

  @Test
  @DisplayName("submit refuses an output table equal to the source table, before any network call")
  void submit_outputEqualToSource_noNetwork() {
    assertThatThrownBy(() -> submitter().submit(requestWithFinalEnv(Map.of("OUTPUT_DB", "demo_db",
        "OUTPUT_TABLE", "users_cdr")), SECRET)).isInstanceOf(JobSubmitException.class).hasMessage("invalid job parameters");
    assertThat(seen).isEmpty();
  }

  @Test
  @DisplayName("submit omits scope from the token request when none is configured")
  void submit_noScope_bodyHasOnlyGrantType() throws Exception {
    token(json(200, TOKEN_OK));
    airflow(json(200, DAG_RUN_OK));

    submitter(config(null, List.of("deid_dag"))).submit(request(), SECRET);

    assertThat(seen.get(0).getBody().readUtf8()).isEqualTo("grant_type=client_credentials");
  }

  @Test
  @DisplayName("submit caches the token and fetches a new one 30 seconds before it expires")
  void submit_tokenCachedUntilExpiry() {
    token(json(200, TOKEN_OK), json(200, "{\"access_token\":\"tok-2\",\"expires_in\":300}"));
    airflow(json(200, DAG_RUN_OK), json(200, DAG_RUN_OK), json(200, DAG_RUN_OK));
    AirflowJobSubmitter submitter = submitter();

    submitter.submit(request(), SECRET);
    clock.advance(Duration.ofSeconds(200));
    submitter.submit(request(), SECRET);
    assertThat(count("/token")).isEqualTo(1);

    clock.advance(Duration.ofSeconds(71));
    submitter.submit(request(), SECRET);

    assertThat(count("/token")).isEqualTo(2);
    assertThat(seen.get(seen.size() - 1).getHeader("Authorization")).isEqualTo("Bearer tok-2");
  }

  @Test
  @DisplayName("submit drops the token on a 401, fetches a new one and retries exactly once")
  void submit_unauthorizedOnce_retriesWithNewToken() {
    token(json(200, TOKEN_OK), json(200, "{\"access_token\":\"tok-2\",\"expires_in\":300}"));
    airflow(json(401, "{}"), json(200, DAG_RUN_OK));

    JobRef ref = submitter().submit(request(), SECRET);

    assertThat(ref.name()).isEqualTo("deid-2026-000007");
    assertThat(count("/token")).isEqualTo(2);
    assertThat(count("/airflow")).isEqualTo(2);
    assertThat(seen.get(seen.size() - 1).getHeader("Authorization")).isEqualTo("Bearer tok-2");
  }

  @Test
  @DisplayName("submit reports airflow denied the request after a second 401")
  void submit_unauthorizedTwice_denied() {
    token(json(200, TOKEN_OK), json(200, TOKEN_OK));
    airflow(json(401, "{}"), json(401, "{}"));

    assertThatThrownBy(() -> submitter().submit(request(), SECRET)).isInstanceOf(JobSubmitException.class)
        .hasMessage("airflow denied the request");
    assertThat(count("/airflow")).isEqualTo(2);
  }

  @ParameterizedTest
  @CsvSource({"403,airflow denied the request", "404,airflow dag not found", "400,airflow unavailable",
      "500,airflow unavailable", "503,airflow unavailable"})
  @DisplayName("submit maps Airflow error statuses to fixed messages")
  void submit_errorStatus_fixedMessage(int status, String message) {
    token(json(200, TOKEN_OK));
    airflow(json(status, "{\"detail\":\"" + CLIENT_SECRET + " tok-1\"}"));

    Throwable t = catchThrowable(() -> submitter().submit(request(), SECRET));

    assertThat(t).isInstanceOf(JobSubmitException.class).hasMessage(message);
    assertThat(t.getCause()).isNull();
  }

  @Test
  @DisplayName("submit treats 409 as already submitted and returns the same job reference")
  void submit_conflict_isIdempotent() {
    token(json(200, TOKEN_OK));
    airflow(json(409, "{}"));

    assertThat(submitter().submit(request(), SECRET)).isEqualTo(REF);
  }

  @Test
  @DisplayName("submit refuses a DAG that is no longer in the allowlist before any network call")
  void submit_dagNotAllowed_noNetwork() {
    AirflowJobSubmitter submitter = submitter(config("openid", List.of("other_dag")));

    assertThatThrownBy(() -> submitter.submit(request(), SECRET)).isInstanceOf(JobSubmitException.class)
        .hasMessage("airflow dag not allowed");
    assertThat(seen).isEmpty();
  }

  @Test
  @DisplayName("submit refuses a request without dagId")
  void submit_noDagId_notAllowed() {
    assertThatThrownBy(() -> submitter().submit(request("users_cdr", null), SECRET)).isInstanceOf(JobSubmitException.class)
        .hasMessage("airflow dag not allowed");
    assertThat(seen).isEmpty();
  }

  @Test
  @DisplayName("submit refuses parameters outside the allowed character set before any network call")
  void submit_hostileTable_noNetwork() {
    assertThatThrownBy(() -> submitter().submit(request("t\"}", "deid_dag"), SECRET)).isInstanceOf(JobSubmitException.class)
        .hasMessage("invalid job parameters");
    assertThat(seen).isEmpty();
  }

  @ParameterizedTest
  @CsvSource({"401,airflow denied the request", "400,airflow denied the request", "500,airflow unavailable"})
  @DisplayName("submit maps a failing token endpoint to fixed messages without Airflow being called")
  void submit_tokenEndpointFails_fixedMessage(int status, String message) {
    token(json(status, "{\"error\":\"" + CLIENT_SECRET + "\"}"));

    assertThatThrownBy(() -> submitter().submit(request(), SECRET)).isInstanceOf(JobSubmitException.class)
        .hasMessage(message);
    assertThat(count("/airflow")).isZero();
  }

  @Test
  @DisplayName("submit reports airflow unavailable when the token response has no access_token")
  void submit_tokenWithoutAccessToken_unavailable() {
    token(json(200, "{\"expires_in\":300}"));

    assertThatThrownBy(() -> submitter().submit(request(), SECRET)).isInstanceOf(JobSubmitException.class)
        .hasMessage("airflow unavailable");
  }

  @Test
  @DisplayName("submit reports airflow unavailable when the server is down")
  void submit_serverDown_unavailable() throws Exception {
    AirflowJobSubmitter submitter = submitter();
    server.shutdown();

    assertThatThrownBy(() -> submitter.submit(request(), SECRET)).isInstanceOf(JobSubmitException.class)
        .hasMessage("airflow unavailable");
  }

  @Test
  @DisplayName("a static bearer token is sent as is and no token endpoint is called")
  void submit_bearerAuth_noTokenEndpoint() {
    Airflow cfg = new Airflow(server.url("/airflow").toString(), List.of("deid_dag"), AirflowAuth.BEARER, null, null, null,
        null, "static-token-123");
    airflow(json(200, DAG_RUN_OK));

    submitter(cfg).submit(request(), SECRET);

    assertThat(count("/token")).isZero();
    assertThat(seen.get(0).getHeader("Authorization")).isEqualTo("Bearer static-token-123");
  }

  @Test
  @DisplayName("basic auth sends HTTP Basic with the Airflow user, calls no token endpoint and never retries a 401")
  void submit_basicAuth_sendsBasicAndDoesNotRetry() {
    Airflow cfg = new Airflow(server.url("/airflow").toString(), List.of("deid_pseudonymize_table"), AirflowAuth.BASIC, null,
        null, null, null, null, "dak-trigger", "p@ss word:1");
    airflow(json(200, DAG_RUN_OK), json(401, "{}"), json(200, DAG_RUN_OK));
    AirflowJobSubmitter submitter = submitter(cfg);
    DeidRequest request = request("users_cdr", "deid_pseudonymize_table");

    submitter.submit(request, SECRET);
    assertThat(seen.get(0).getHeader("Authorization")).isEqualTo("Basic "
        + Base64.getEncoder().encodeToString("dak-trigger:p@ss word:1".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    assertThat(count("/token")).isZero();

    assertThatThrownBy(() -> submitter.submit(request, SECRET)).isInstanceOf(JobSubmitException.class)
        .hasMessage("airflow denied the request");
    assertThat(count("/airflow")).isEqualTo(2);
  }

  @ParameterizedTest
  @CsvSource({"queued,RUNNING", "running,RUNNING", "success,SUCCEEDED", "failed,FAILED", "restarting,RUNNING"})
  @DisplayName("status maps the Airflow dag run state")
  void status_mapsStates(String state, JobState expected) {
    token(json(200, TOKEN_OK));
    airflow(json(200, "{\"state\":\"" + state + "\"}"));

    JobState result = submitter().status(request(), REF);

    assertThat(result).isEqualTo(expected);
    RecordedRequest req = seen.get(seen.size() - 1);
    assertThat(req.getMethod()).isEqualTo("GET");
    assertThat(req.getPath()).isEqualTo("/airflow/api/v1/dags/deid_dag/dagRuns/deid-2026-000007");
  }

  @Test
  @DisplayName("status reports UNKNOWN when Airflow no longer knows the dag run")
  void status_notFound_unknown() {
    token(json(200, TOKEN_OK));
    airflow(json(404, "{}"));

    assertThat(submitter().status(request(), REF)).isEqualTo(JobState.UNKNOWN);
  }

  @Test
  @DisplayName("status fails with a fixed message on an Airflow error so the request keeps its state")
  void status_serverError_throws() {
    token(json(200, TOKEN_OK));
    airflow(json(500, "{}"));

    assertThatThrownBy(() -> submitter().status(request(), REF)).isInstanceOf(JobSubmitException.class)
        .hasMessage("airflow unavailable");
  }
}
