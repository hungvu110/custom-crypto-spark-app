package vai.lakehouse.dakmock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Test end-to-end của {@code GET /api/v1/keys/by-column} (docs/dak-mock-api/DAK_MOCK_BY_COLUMN_PLAN.md mục 1) và endpoint
 * quản trị của mock. MockWebServer đóng vai Keycloak (JWKS) và một Vault KV v2 nhỏ có hỗ trợ {@code cas=0}.
 *
 * <p>YC và danh sách khoá sống suốt context Spring, nên mỗi test của luồng nội bộ dùng một bảng riêng.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ColumnKeyApiTest {

  private static final String JWKS_PATH = "/realms/tenant-a/protocol/openid-connect/certs";
  private static final String COLUMN_KEYS = "/v1/kv/data/dak/column-keys/";
  private static final String VAULT_TOKEN = "vault-test-token";
  private static final String ADMIN_TOKEN = "admin-secret";
  private static final String INTERNAL = "dak.internal";
  private static final String TEAM_A = "dak.w1.team-a";
  private static final String TEAM_B = "dak.w1.team-b";

  private static final MockWebServer upstream = new MockWebServer();
  /** Vault giả: path sau {@code /v1/kv/data/} -> keyPrefix. */
  private static final Map<String, String> vault = new ConcurrentHashMap<>();
  private static final List<RecordedRequest> seen = new CopyOnWriteArrayList<>();
  private static final ObjectMapper json = new ObjectMapper();
  private static final RSAKey signingKey;
  private static final String base;

  static {
    try {
      signingKey = new RSAKeyGenerator(2048).keyID("k1").generate();
      upstream.setDispatcher(new Dispatcher() {
        @Override
        public MockResponse dispatch(RecordedRequest request) {
          seen.add(request);
          return route(request);
        }
      });
      upstream.start();
      base = "http://127.0.0.1:" + upstream.getPort();
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry r) {
    r.add("dak.realms[0].realm", () -> "tenant-a");
    r.add("dak.realms[0].tenant", () -> "tenant-a");
    r.add("dak.realms[0].issuer", () -> base + "/realms/tenant-a");
    r.add("dak.realms[0].jwks-uri", () -> base + JWKS_PATH);
    r.add("dak.clients[0].realm", () -> "tenant-a");
    r.add("dak.clients[0].client-id", () -> INTERNAL);
    r.add("dak.clients[0].kind", () -> "internal");
    r.add("dak.clients[1].realm", () -> "tenant-a");
    r.add("dak.clients[1].client-id", () -> TEAM_A);
    r.add("dak.clients[1].workspace", () -> "w1");
    r.add("dak.clients[1].team", () -> "team-a");
    r.add("dak.clients[2].realm", () -> "tenant-a");
    r.add("dak.clients[2].client-id", () -> TEAM_B);
    r.add("dak.clients[2].workspace", () -> "w1");
    r.add("dak.clients[2].team", () -> "team-b");
    r.add("dak.grants[0].team", () -> "team-a");
    r.add("dak.grants[0].database", () -> "demo_db");
    r.add("dak.grants[0].table", () -> "users_cdr");
    r.add("dak.grants[0].columns[0]", () -> "msisdn");
    r.add("dak.grants[0].columns[1]", () -> "IMEI");
    r.add("dak.grants[1].team", () -> "team-a");
    r.add("dak.grants[1].database", () -> "demo_db");
    r.add("dak.grants[1].table", () -> "users_cdr");
    r.add("dak.grants[1].columns[0]", () -> "old_col");
    r.add("dak.grants[1].expires-at", () -> "2020-01-01T00:00:00Z");
    r.add("dak.vault.addr", () -> base);
    r.add("dak.vault.token", () -> VAULT_TOKEN);
    r.add("dak.vault.path-template", () -> "base/{database}.{table}");
    r.add("dak.vault.column-path-template", () -> "dak/column-keys/{database}/{table}/{column}");
    r.add("dak.yc.max-open-per-table", () -> "3");
    r.add("dak.admin.token", () -> ADMIN_TOKEN);
  }

  @AfterAll
  static void stop() throws Exception {
    upstream.shutdown();
  }

  @Autowired
  private TestRestTemplate rest;

  @BeforeEach
  void reset() {
    vault.clear();
    seen.clear();
  }

  // ---------------------------------------------------------------- luồng nội bộ

  @Test
  void internalClientGetsMissingWithoutOpeningAYcWhenNotRequested() throws Exception {
    ResponseEntity<String> res = byColumn(INTERNAL, "demo_db", "t_none", "a", "none", "internal");

    JsonNode body = ok(res);
    assertThat(body.path("database").asText()).isEqualTo("demo_db");
    assertThat(body.path("table").asText()).isEqualTo("t_none");
    assertThat(body.path("complete").asBoolean()).isFalse();
    JsonNode a = body.path("columns").get(0);
    assertThat(a.path("column").asText()).isEqualTo("a");
    assertThat(a.path("state").asText()).isEqualTo("MISSING");
    assertThat(a.path("reason").asText()).isEqualTo("not_requested");
    assertThat(a.has("keyPrefix")).isFalse();
    assertThat(a.has("requestCode")).isFalse();
    assertThat(ycFor("t_none")).isEmpty();
  }

  @Test
  void requestOpensOneYcPerColumnAndFulfillMakesTheColumnReady() throws Exception {
    JsonNode first = ok(byColumn(INTERNAL, "demo_db", "t_flow", "a,b", "request", "internal"));
    String codeA = first.path("columns").get(0).path("requestCode").asText();
    String codeB = first.path("columns").get(1).path("requestCode").asText();
    assertThat(first.path("columns").get(0).path("state").asText()).isEqualTo("PENDING");
    assertThat(codeA).matches("YC-\\d{4}-\\d{6}").isNotEqualTo(codeB);

    JsonNode retry = ok(byColumn(INTERNAL, "demo_db", "t_flow", "a,b", "request", "internal"));
    assertThat(retry.path("columns").get(0).path("requestCode").asText()).isEqualTo(codeA);
    assertThat(ycFor("t_flow")).hasSize(2);

    ResponseEntity<String> fulfilled = admin(HttpMethod.POST, "/mock-admin/yc/" + codeA + "/fulfill", null, ADMIN_TOKEN);
    assertThat(fulfilled.getStatusCode().value()).isEqualTo(200);
    String createdKey = vault.get("dak/column-keys/demo_db/t_flow/a");
    assertThat(createdKey).isNotBlank();
    assertThat(fulfilled.getBody()).doesNotContain(createdKey);
    RecordedRequest write = vaultWrites().get(0);
    assertThat(json.readTree(write.getBody().readUtf8()).path("options").path("cas").asInt(-1)).isZero();
    assertThat(write.getHeader("X-Vault-Token")).isEqualTo(VAULT_TOKEN);

    JsonNode half = ok(byColumn(INTERNAL, "demo_db", "t_flow", "a,b", "request", "internal"));
    assertThat(half.path("complete").asBoolean()).isFalse();
    assertThat(half.path("columns").get(0).path("state").asText()).isEqualTo("READY");
    assertThat(half.path("columns").get(0).path("keyPrefix").asText()).isEqualTo(createdKey);
    assertThat(half.path("columns").get(0).path("keyVersion").asLong()).isEqualTo(1);
    assertThat(half.path("columns").get(1).path("state").asText()).isEqualTo("PENDING");

    admin(HttpMethod.POST, "/mock-admin/yc/" + codeB + "/fulfill", null, ADMIN_TOKEN);
    assertThat(ok(byColumn(INTERNAL, "demo_db", "t_flow", "a,b", "none", "internal")).path("complete").asBoolean())
        .isTrue();
    assertThat(ycFor("t_flow")).isEmpty();
  }

  @Test
  void fulfillUsesTheGivenKeyValueAndNeverOverwritesAnExistingKey() throws Exception {
    JsonNode res = ok(byColumn(INTERNAL, "demo_db", "t_cas", "given,raced", "request", "internal"));
    String given = res.path("columns").get(0).path("requestCode").asText();
    String raced = res.path("columns").get(1).path("requestCode").asText();

    admin(HttpMethod.POST, "/mock-admin/yc/" + given + "/fulfill", "{\"keyValue\":\"given-key\"}", ADMIN_TOKEN);
    // Có key được ghi vào Vault trong lúc YC đang mở: fulfill không được thay key đó.
    vault.put("dak/column-keys/demo_db/t_cas/raced", "existing-key");
    admin(HttpMethod.POST, "/mock-admin/yc/" + raced + "/fulfill", null, ADMIN_TOKEN);

    JsonNode after = ok(byColumn(INTERNAL, "demo_db", "t_cas", "given,raced", "none", "internal"));
    assertThat(after.path("columns").get(0).path("keyPrefix").asText()).isEqualTo("given-key");
    assertThat(after.path("columns").get(1).path("keyPrefix").asText()).isEqualTo("existing-key");
    assertThat(vault.get("dak/column-keys/demo_db/t_cas/raced")).isEqualTo("existing-key");
  }

  @Test
  void rejectedYcIsReturnedAgainAndNoNewYcIsOpened() throws Exception {
    String code = ok(byColumn(INTERNAL, "demo_db", "t_reject", "a", "request", "internal"))
        .path("columns").get(0).path("requestCode").asText();

    ResponseEntity<String> rejected =
        admin(HttpMethod.POST, "/mock-admin/yc/" + code + "/reject", "{\"reasonText\":\"not sensitive\"}", ADMIN_TOKEN);
    assertThat(rejected.getStatusCode().value()).isEqualTo(200);

    JsonNode a = ok(byColumn(INTERNAL, "demo_db", "t_reject", "a", "request", "internal")).path("columns").get(0);
    assertThat(a.path("state").asText()).isEqualTo("REJECTED");
    assertThat(a.path("requestCode").asText()).isEqualTo(code);
    assertThat(a.path("reasonText").asText()).isEqualTo("not sensitive");
    assertThat(ycFor("t_reject")).hasSize(1);
  }

  @Test
  void stopsOpeningYcAtTheLimitPerClientAndTable() throws Exception {
    JsonNode cols = ok(byColumn(INTERNAL, "demo_db", "t_limit", "c1,c2,c3,c4", "request", "internal")).path("columns");

    assertThat(List.of(cols.get(0), cols.get(1), cols.get(2))).allSatisfy(c ->
        assertThat(c.path("state").asText()).isEqualTo("PENDING"));
    assertThat(cols.get(3).path("state").asText()).isEqualTo("MISSING");
    assertThat(cols.get(3).path("reason").asText()).isEqualTo("yc_limit_reached");
  }

  @Test
  void disabledColumnHidesItsKeyAndEnablingReturnsTheSameKey() throws Exception {
    vault.put("dak/column-keys/demo_db/t_lock/a", "lock-key");
    assertThat(ok(byColumn(INTERNAL, "demo_db", "t_lock", "a", "none", "internal"))
        .path("columns").get(0).path("state").asText()).isEqualTo("READY");

    assertThat(admin(HttpMethod.POST, "/mock-admin/columns/demo_db/t_lock/a/disable", null, ADMIN_TOKEN)
        .getStatusCode().value()).isEqualTo(204);
    ResponseEntity<String> locked = byColumn(INTERNAL, "demo_db", "t_lock", "a", "none", "internal");
    assertThat(ok(locked).path("columns").get(0).path("state").asText()).isEqualTo("DISABLED");
    assertThat(locked.getBody()).doesNotContain("lock-key");

    admin(HttpMethod.POST, "/mock-admin/columns/demo_db/t_lock/a/enable", null, ADMIN_TOKEN);
    assertThat(ok(byColumn(INTERNAL, "demo_db", "t_lock", "a", "none", "internal"))
        .path("columns").get(0).path("keyPrefix").asText()).isEqualTo("lock-key");
  }

  @Test
  void mergesDuplicateColumnsAndKeepsFirstOrder() throws Exception {
    vault.put("dak/column-keys/demo_db/t_dup/a", "ka");
    vault.put("dak/column-keys/demo_db/t_dup/b", "kb");

    JsonNode body = ok(byColumn(INTERNAL, "Demo_DB", "T_DUP", "B,a,b", "none", "internal"));

    assertThat(body.path("table").asText()).isEqualTo("t_dup");
    assertThat(body.path("columns")).hasSize(2);
    assertThat(body.path("columns").get(0).path("column").asText()).isEqualTo("b");
    assertThat(body.path("columns").get(1).path("column").asText()).isEqualTo("a");
    assertThat(body.path("complete").asBoolean()).isTrue();
  }

  @Test
  void internalClientMustSendTagInternal() throws Exception {
    assertError(byColumn(INTERNAL, "demo_db", "t_tag", "a", "none", null), 400, "invalid_tag");
  }

  @Test
  void internalClientCannotUseTheTableEndpoint() throws Exception {
    assertError(get("/api/v1/keys/demo_db/users_cdr", INTERNAL, null), 403, "access_denied");
  }

  // ---------------------------------------------------------------- luồng client team

  @Test
  void teamClientGetsKeysOfGrantedColumns() throws Exception {
    vault.put("dak/column-keys/demo_db/users_cdr/msisdn", "k-msisdn");
    vault.put("dak/column-keys/demo_db/users_cdr/imei", "k-imei");

    JsonNode body = ok(byColumn(TEAM_A, "demo_db", "users_cdr", "msisdn,imei", "none", null));

    assertThat(body.path("complete").asBoolean()).isTrue();
    assertThat(body.path("columns").get(0).path("keyPrefix").asText()).isEqualTo("k-msisdn");
    assertThat(body.path("columns").get(1).path("keyPrefix").asText()).isEqualTo("k-imei");
  }

  @Test
  void teamClientSendingTagInternalIsDenied() throws Exception {
    vault.put("dak/column-keys/demo_db/users_cdr/msisdn", "k-msisdn");

    assertError(byColumn(TEAM_A, "demo_db", "users_cdr", "msisdn", "none", "internal"), 403, "access_denied");
  }

  @Test
  void teamClientIsDeniedTheWholeBatchWhenOneColumnIsNotGranted() throws Exception {
    vault.put("dak/column-keys/demo_db/users_cdr/msisdn", "k-msisdn");

    ResponseEntity<String> res = byColumn(TEAM_A, "demo_db", "users_cdr", "msisdn,device_id", "none", null);

    assertError(res, 403, "access_denied");
    assertThat(res.getBody()).doesNotContain("k-msisdn");
  }

  @Test
  void expiredGrantAndTeamWithoutGrantsAreDenied() throws Exception {
    vault.put("dak/column-keys/demo_db/users_cdr/old_col", "k-old");
    vault.put("dak/column-keys/demo_db/users_cdr/msisdn", "k-msisdn");

    assertError(byColumn(TEAM_A, "demo_db", "users_cdr", "old_col", "none", null), 403, "access_denied");
    assertError(byColumn(TEAM_B, "demo_db", "users_cdr", "msisdn", "none", null), 403, "access_denied");
  }

  @Test
  void teamClientNeverOpensAYcEvenWhenAskingForIt() throws Exception {
    JsonNode imei = ok(byColumn(TEAM_A, "demo_db", "users_cdr", "imei", "request", null)).path("columns").get(0);

    assertThat(imei.path("state").asText()).isEqualTo("MISSING");
    assertThat(imei.path("reason").asText()).isEqualTo("not_requested");
    assertThat(ycFor("users_cdr")).isEmpty();
  }

  // ---------------------------------------------------------------- kiểm tham số, header

  @Test
  void rejectsBadParametersBeforeCheckingTheToken() throws Exception {
    assertError(get("/api/v1/keys/by-column?database=demo_db&table=t&columns=start-time", null, null),
        400, "invalid_column");
    assertError(get("/api/v1/keys/by-column?database=demo-db&table=t&columns=a", null, null), 400, "invalid_table");
    assertError(get("/api/v1/keys/by-column?database=demo_db&table=t&columns=a&onMissing=always", null, null),
        400, "invalid_on_missing");
    assertError(get("/api/v1/keys/by-column?database=demo_db&table=t&columns=a&tag=external", null, null),
        400, "invalid_tag");
    assertThat(seen).isEmpty();
  }

  @Test
  void rejectsMissingToken() throws Exception {
    assertError(get("/api/v1/keys/by-column?database=demo_db&table=t&columns=a&tag=internal", null, null),
        401, "invalid_token");
  }

  @Test
  void ignoresAnInvalidCallerHeader() throws Exception {
    HttpHeaders headers = auth(INTERNAL);
    headers.set("X-Dak-Caller", "bad caller!");

    ResponseEntity<String> res = rest.exchange(
        "/api/v1/keys/by-column?database=demo_db&table=t_caller&columns=a&tag=internal", HttpMethod.GET,
        new HttpEntity<>(headers), String.class);

    assertThat(res.getStatusCode().value()).isEqualTo(200);
  }

  // ---------------------------------------------------------------- endpoint quản trị

  @Test
  void adminEndpointsAreHiddenWithoutTheRightToken() {
    assertThat(admin(HttpMethod.GET, "/mock-admin/yc", null, null).getStatusCode().value()).isEqualTo(404);
    assertThat(admin(HttpMethod.GET, "/mock-admin/yc", null, "wrong").getStatusCode().value()).isEqualTo(404);
    assertThat(admin(HttpMethod.POST, "/mock-admin/yc/YC-2026-999999/fulfill", null, ADMIN_TOKEN)
        .getStatusCode().value()).isEqualTo(404);
  }

  // ---------------------------------------------------------------- helpers

  private static MockResponse route(RecordedRequest request) {
    String path = request.getPath() == null ? "" : request.getPath();
    if (path.equals(JWKS_PATH)) {
      return jsonResponse(200, new JWKSet(signingKey.toPublicJWK()).toString());
    }
    if (!path.startsWith(COLUMN_KEYS) || !VAULT_TOKEN.equals(request.getHeader("X-Vault-Token"))) {
      return new MockResponse().setResponseCode(404);
    }
    String key = path.substring("/v1/kv/data/".length());
    if ("GET".equals(request.getMethod())) {
      String prefix = vault.get(key);
      return prefix == null ? new MockResponse().setResponseCode(404)
          : jsonResponse(200, "{\"data\":{\"data\":{\"keyPrefix\":\"" + prefix + "\"},\"metadata\":{\"version\":1}}}");
    }
    try {
      JsonNode body = json.readTree(request.getBody().clone().readUtf8());
      if (body.path("options").path("cas").asInt(-1) == 0 && vault.containsKey(key)) {
        return jsonResponse(400, "{\"errors\":[\"check-and-set parameter did not match the current version\"]}");
      }
      vault.put(key, body.path("data").path("keyPrefix").asText());
      return jsonResponse(200, "{\"data\":{\"version\":1}}");
    } catch (Exception e) {
      return new MockResponse().setResponseCode(500);
    }
  }

  private ResponseEntity<String> byColumn(String clientId, String database, String table, String columns,
                                          String onMissing, String tag) {
    StringBuilder path = new StringBuilder("/api/v1/keys/by-column?database=").append(database)
        .append("&table=").append(table).append("&columns=").append(columns).append("&onMissing=").append(onMissing);
    if (tag != null) {
      path.append("&tag=").append(tag);
    }
    return get(path.toString(), clientId, "req-col");
  }

  private ResponseEntity<String> get(String path, String clientId, String requestId) {
    HttpHeaders headers = clientId == null ? new HttpHeaders() : auth(clientId);
    if (requestId != null) {
      headers.set("X-Request-Id", requestId);
    }
    return rest.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), String.class);
  }

  private ResponseEntity<String> admin(HttpMethod method, String path, String body, String token) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    if (token != null) {
      headers.set("X-Mock-Admin-Token", token);
    }
    return rest.exchange(path, method, new HttpEntity<>(body, headers), String.class);
  }

  private List<JsonNode> ycFor(String table) throws Exception {
    JsonNode list = json.readTree(admin(HttpMethod.GET, "/mock-admin/yc", null, ADMIN_TOKEN).getBody());
    List<JsonNode> out = new ArrayList<>();
    list.forEach(yc -> {
      if (table.equals(yc.path("table").asText())) {
        out.add(yc);
      }
    });
    return out;
  }

  private List<RecordedRequest> vaultWrites() {
    return seen.stream().filter(r -> "POST".equals(r.getMethod())).toList();
  }

  private static JsonNode ok(ResponseEntity<String> res) throws Exception {
    assertThat(res.getStatusCode().value()).as(res.getBody()).isEqualTo(200);
    assertThat(res.getHeaders().getCacheControl()).isEqualTo("no-store");
    assertThat(res.getHeaders().getFirst("X-Request-Id")).isNotBlank();
    return json.readTree(res.getBody());
  }

  private static void assertError(ResponseEntity<String> res, int status, String error) throws Exception {
    assertThat(res.getStatusCode().value()).as(res.getBody()).isEqualTo(status);
    JsonNode body = json.readTree(res.getBody());
    assertThat(body.path("error").asText()).isEqualTo(error);
    assertThat(body.path("requestId").asText()).isEqualTo(res.getHeaders().getFirst("X-Request-Id"));
    assertThat(res.getBody()).doesNotContain(VAULT_TOKEN);
    assertThat(res.getHeaders().getCacheControl()).isEqualTo("no-store");
  }

  private static HttpHeaders auth(String clientId) {
    HttpHeaders headers = new HttpHeaders();
    headers.set(HttpHeaders.AUTHORIZATION, "Bearer " + token(clientId));
    return headers;
  }

  private static String token(String clientId) {
    try {
      Instant now = Instant.now();
      JWTClaimsSet claims = new JWTClaimsSet.Builder()
          .issuer(base + "/realms/tenant-a")
          .issueTime(Date.from(now))
          .expirationTime(Date.from(now.plusSeconds(300)))
          .claim("typ", "Bearer")
          .claim("azp", clientId)
          .claim("client_id", clientId)
          .build();
      SignedJWT jwt = new SignedJWT(
          new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(signingKey.getKeyID()).type(JOSEObjectType.JWT).build(), claims);
      jwt.sign(new RSASSASigner(signingKey));
      return jwt.serialize();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static MockResponse jsonResponse(int status, String body) {
    return new MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(body);
  }
}
