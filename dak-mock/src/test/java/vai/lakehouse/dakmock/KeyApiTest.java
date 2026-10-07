package vai.lakehouse.dakmock;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
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
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Test end-to-end của API lấy key: MockWebServer đóng vai Keycloak (JWKS) và Vault, token ký bằng khoá RSA sinh
 * trong test. Kiểm tra theo docs/DAK_API_SPEC.md mục 4–5.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class KeyApiTest {

  private static final String JWKS_PATH = "/realms/tenant-a/protocol/openid-connect/certs";
  private static final String JWKS_DOWN_PATH = "/realms/tenant-down/protocol/openid-connect/certs";
  private static final String VAULT_KEY_PATH = "/v1/kv/data/base/demo_db.users_cdr";
  private static final String VAULT_TOKEN = "vault-test-token";
  private static final String PREFIX = "9f2c4b-prefix";

  private static final MockWebServer upstream = new MockWebServer();
  private static final Map<String, MockResponse> routes = new ConcurrentHashMap<>();
  private static final List<RecordedRequest> seen = new CopyOnWriteArrayList<>();
  private static final RSAKey signingKey;
  private static final String base;

  static {
    try {
      signingKey = new RSAKeyGenerator(2048).keyID("k1").generate();
      upstream.setDispatcher(new Dispatcher() {
        @Override
        public MockResponse dispatch(RecordedRequest request) {
          seen.add(request);
          return routes.getOrDefault(request.getPath(), new MockResponse().setResponseCode(404));
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
    r.add("dak.realms[1].realm", () -> "tenant-down");
    r.add("dak.realms[1].tenant", () -> "tenant-down");
    r.add("dak.realms[1].issuer", () -> base + "/realms/tenant-down");
    r.add("dak.realms[1].jwks-uri", () -> base + JWKS_DOWN_PATH);
    r.add("dak.clients[0].realm", () -> "tenant-a");
    r.add("dak.clients[0].client-id", () -> "dak.w1.team-a");
    r.add("dak.clients[0].workspace", () -> "w1");
    r.add("dak.clients[0].team", () -> "team-a");
    r.add("dak.clients[1].realm", () -> "tenant-a");
    r.add("dak.clients[1].client-id", () -> "dak.w1.team-off");
    r.add("dak.clients[1].workspace", () -> "w1");
    r.add("dak.clients[1].team", () -> "team-off");
    r.add("dak.clients[1].enabled", () -> "false");
    r.add("dak.clients[2].realm", () -> "tenant-down");
    r.add("dak.clients[2].client-id", () -> "c-down");
    r.add("dak.clients[2].workspace", () -> "w1");
    r.add("dak.clients[2].team", () -> "team-down");
    r.add("dak.vault.addr", () -> base);
    r.add("dak.vault.token", () -> VAULT_TOKEN);
    r.add("dak.vault.path-template", () -> "base/{database}.{table}");
  }

  @AfterAll
  static void stop() throws Exception {
    upstream.shutdown();
  }

  @Autowired
  private TestRestTemplate rest;

  private final ObjectMapper json = new ObjectMapper();

  @BeforeEach
  void reset() {
    routes.clear();
    seen.clear();
    routes.put(JWKS_PATH, jsonResponse(200, new JWKSet(signingKey.toPublicJWK()).toString()));
    routes.put(JWKS_DOWN_PATH, new MockResponse().setResponseCode(500));
    routes.put(VAULT_KEY_PATH, vaultSecret(PREFIX, 3));
  }

  // ---------------------------------------------------------------- 200

  @Test
  void returnsKeyPrefixFromVaultForAValidToken() throws Exception {
    ResponseEntity<String> res = get("/api/v1/keys/demo_db/users_cdr", "Bearer " + token(c -> {}), "req-1");

    assertThat(res.getStatusCode().value()).isEqualTo(200);
    JsonNode body = json.readTree(res.getBody());
    assertThat(body.path("database").asText()).isEqualTo("demo_db");
    assertThat(body.path("table").asText()).isEqualTo("users_cdr");
    assertThat(body.path("keyPrefix").asText()).isEqualTo(PREFIX);
    assertThat(body.path("keyVersion").asLong()).isEqualTo(3);
    assertThat(res.getHeaders().getFirst("X-Request-Id")).isEqualTo("req-1");
    assertThat(res.getHeaders().getCacheControl()).isEqualTo("no-store");

    RecordedRequest vaultCall = vaultRequests().get(0);
    assertThat(vaultCall.getHeader("X-Vault-Token")).isEqualTo(VAULT_TOKEN);
  }

  @Test
  void normalizesDatabaseAndTableToLowerCase() throws Exception {
    ResponseEntity<String> res = get("/api/v1/keys/Demo_DB/Users_CDR", "Bearer " + token(c -> {}), null);

    assertThat(res.getStatusCode().value()).isEqualTo(200);
    assertThat(json.readTree(res.getBody()).path("database").asText()).isEqualTo("demo_db");
    assertThat(vaultRequests()).extracting(RecordedRequest::getPath).containsExactly(VAULT_KEY_PATH);
  }

  // ---------------------------------------------------------------- 400

  @Test
  void rejectsInvalidTableNameBeforeCheckingTheToken() throws Exception {
    ResponseEntity<String> res = get("/api/v1/keys/demo-db/users_cdr", null, null);

    assertError(res, 400, "invalid_table");
    assertThat(vaultRequests()).isEmpty();
  }

  // ---------------------------------------------------------------- 401

  @Test
  void rejectsMissingToken() throws Exception {
    ResponseEntity<String> res = get("/api/v1/keys/demo_db/users_cdr", null, null);

    assertError(res, 401, "invalid_token");
    assertThat(res.getHeaders().getFirst("X-Request-Id")).isNotBlank();
  }

  @Test
  void rejectsTokenSignedWithAnotherKeyUsingTheSameKid() throws Exception {
    RSAKey other = new RSAKeyGenerator(2048).keyID("k1").generate();

    assertError(get("/api/v1/keys/demo_db/users_cdr", "Bearer " + sign(claims(c -> {}), other, null), null),
        401, "invalid_token");
  }

  @Test
  void rejectsUnknownIssuer() throws Exception {
    String token = token(c -> c.issuer("https://evil.example/realms/tenant-a"));

    assertError(get("/api/v1/keys/demo_db/users_cdr", "Bearer " + token, null), 401, "invalid_token");
  }

  @Test
  void rejectsExpiredToken() throws Exception {
    String token = token(c -> c.issueTime(Date.from(Instant.now().minusSeconds(900)))
        .expirationTime(Date.from(Instant.now().minusSeconds(120))));

    assertError(get("/api/v1/keys/demo_db/users_cdr", "Bearer " + token, null), 401, "invalid_token");
  }

  @Test
  void rejectsTokenThatIsNotAnAccessToken() throws Exception {
    String token = token(c -> c.claim("typ", "Refresh"));

    assertError(get("/api/v1/keys/demo_db/users_cdr", "Bearer " + token, null), 401, "invalid_token");
  }

  @Test
  void rejectsUnsignedAndHmacTokens() throws Exception {
    String none = new PlainJWT(claims(c -> {})).serialize();
    SignedJWT hmac = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims(c -> {}));
    hmac.sign(new MACSigner("0123456789abcdef0123456789abcdef".getBytes()));

    assertError(get("/api/v1/keys/demo_db/users_cdr", "Bearer " + none, null), 401, "invalid_token");
    assertError(get("/api/v1/keys/demo_db/users_cdr", "Bearer " + hmac.serialize(), null), 401, "invalid_token");
  }

  @Test
  void rejectsUnregisteredDisabledOrMismatchedClients() throws Exception {
    String unregistered = token(c -> c.claim("azp", "dak.w9.ghost").claim("client_id", "dak.w9.ghost"));
    String disabled = token(c -> c.claim("azp", "dak.w1.team-off").claim("client_id", "dak.w1.team-off"));
    String mismatched = token(c -> c.claim("client_id", "dak.w1.someone-else"));

    for (String t : List.of(unregistered, disabled, mismatched)) {
      assertError(get("/api/v1/keys/demo_db/users_cdr", "Bearer " + t, null), 401, "invalid_token");
    }
    assertThat(vaultRequests()).isEmpty();
  }

  @Test
  void ignoresJkuHeaderAndNeverCallsTheAttackerServer() throws Exception {
    RSAKey attackerKey = new RSAKeyGenerator(2048).keyID("attacker").generate();
    try (MockWebServer attacker = new MockWebServer()) {
      attacker.enqueue(jsonResponse(200, new JWKSet(attackerKey.toPublicJWK()).toString()));
      attacker.start();
      JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("attacker")
          .jwkURL(URI.create(attacker.url("/jwks").toString())).build();

      ResponseEntity<String> res = get("/api/v1/keys/demo_db/users_cdr",
          "Bearer " + sign(claims(c -> {}), attackerKey, header), null);

      assertError(res, 401, "invalid_token");
      assertThat(attacker.getRequestCount()).isZero();
    }
  }

  // ---------------------------------------------------------------- 403 / 500 / 503

  @Test
  void returnsAccessDeniedWhenVaultHasNoSecretForTheTable() throws Exception {
    routes.remove(VAULT_KEY_PATH);

    ResponseEntity<String> res = get("/api/v1/keys/demo_db/users_cdr", "Bearer " + token(c -> {}), null);

    assertError(res, 403, "access_denied");
  }

  @Test
  void returnsInternalErrorWhenTheVaultSecretHasNoKeyPrefix() throws Exception {
    routes.put(VAULT_KEY_PATH, vaultSecret("", 1));

    assertError(get("/api/v1/keys/demo_db/users_cdr", "Bearer " + token(c -> {}), null), 500, "internal_error");
  }

  @Test
  void returnsUpstreamUnavailableWhenVaultRejectsTheDakToken() throws Exception {
    routes.put(VAULT_KEY_PATH, jsonResponse(403, "{\"errors\":[\"permission denied\"]}"));

    assertError(get("/api/v1/keys/demo_db/users_cdr", "Bearer " + token(c -> {}), null), 503, "upstream_unavailable");
  }

  @Test
  void returnsUpstreamUnavailableWhenRealmKeysCannotBeLoaded() throws Exception {
    String token = token(c -> c.issuer(base + "/realms/tenant-down").claim("azp", "c-down").claim("client_id", "c-down"));

    assertError(get("/api/v1/keys/demo_db/users_cdr", "Bearer " + token, null), 503, "upstream_unavailable");
  }

  @Test
  void replacesAnInvalidIncomingRequestId() throws Exception {
    ResponseEntity<String> res = get("/api/v1/keys/demo_db/users_cdr", "Bearer " + token(c -> {}), "bad id!");

    assertThat(res.getHeaders().getFirst("X-Request-Id")).matches("[0-9a-f-]{36}");
  }

  // ---------------------------------------------------------------- helpers

  private ResponseEntity<String> get(String path, String authorization, String requestId) {
    HttpHeaders headers = new HttpHeaders();
    if (authorization != null) {
      headers.set(HttpHeaders.AUTHORIZATION, authorization);
    }
    if (requestId != null) {
      headers.set("X-Request-Id", requestId);
    }
    return rest.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), String.class);
  }

  private void assertError(ResponseEntity<String> res, int status, String error) throws Exception {
    assertThat(res.getStatusCode().value()).isEqualTo(status);
    JsonNode body = json.readTree(res.getBody());
    assertThat(body.path("error").asText()).isEqualTo(error);
    assertThat(body.path("requestId").asText()).isEqualTo(res.getHeaders().getFirst("X-Request-Id"));
    assertThat(res.getBody()).doesNotContain(PREFIX).doesNotContain(VAULT_TOKEN);
    assertThat(res.getHeaders().getCacheControl()).isEqualTo("no-store");
  }

  private List<RecordedRequest> vaultRequests() {
    return seen.stream().filter(r -> r.getPath() != null && r.getPath().startsWith("/v1/")).toList();
  }

  private static JWTClaimsSet claims(Consumer<JWTClaimsSet.Builder> customize) {
    Instant now = Instant.now();
    JWTClaimsSet.Builder b = new JWTClaimsSet.Builder()
        .issuer(base + "/realms/tenant-a")
        .issueTime(Date.from(now))
        .expirationTime(Date.from(now.plusSeconds(300)))
        .audience("dak-api")
        .claim("typ", "Bearer")
        .claim("azp", "dak.w1.team-a")
        .claim("client_id", "dak.w1.team-a");
    customize.accept(b);
    return b.build();
  }

  private static String token(Consumer<JWTClaimsSet.Builder> customize) throws Exception {
    return sign(claims(customize), signingKey, null);
  }

  private static String sign(JWTClaimsSet claims, RSAKey key, JWSHeader header) throws Exception {
    JWSHeader h = header != null ? header
        : new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).type(JOSEObjectType.JWT).build();
    SignedJWT jwt = new SignedJWT(h, claims);
    jwt.sign(new RSASSASigner(key));
    return jwt.serialize();
  }

  private static MockResponse jsonResponse(int status, String body) {
    return new MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(body);
  }

  private static MockResponse vaultSecret(String prefix, int version) {
    return jsonResponse(200, "{\"data\":{\"data\":{\"keyPrefix\":\"" + prefix + "\"},\"metadata\":{\"version\":"
        + version + "}}}");
  }
}
