package vai.lakehouse.dakmock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
 * End-to-end của {@code GET /api/v1/keys/global} (docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md): admin tạo key của vùng, người gửi phiếu silver chọn
 * vùng ({@code globalZone}), admin duyệt, rồi client nội bộ hoặc client của team gửi phiếu lấy được key. MockWebServer đóng vai Keycloak (JWKS) và
 * một Vault KV v2 nhỏ có hỗ trợ {@code cas=0}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GlobalKeyApiTest {

  private static final String JWKS_PATH = "/realms/tenant-a/protocol/openid-connect/certs";
  private static final String GLOBAL_KEYS = "/v1/kv/data/dak/global-keys/";
  private static final String VAULT_TOKEN = "vault-test-token";
  private static final String ADMIN_TOKEN = "admin-secret";
  private static final String ALICE = "alice-token-0123456789abcdef";
  private static final String INTERNAL = "dak.internal";
  private static final String TEAM_A = "dak.w1.team-a";
  private static final String TEAM_B = "dak.w1.team-b";
  private static final String ZONE = "khach_hang";

  private static final MockWebServer upstream = new MockWebServer();
  /** Vault giả: path sau {@code /v1/kv/data/} -> keyPrefix. */
  private static final Map<String, String> vault = new ConcurrentHashMap<>();
  private static final ObjectMapper json = new ObjectMapper();
  private static final RSAKey signingKey;
  private static final String base;

  static {
    try {
      signingKey = new RSAKeyGenerator(2048).keyID("k1").generate();
      upstream.setDispatcher(new Dispatcher() {
        @Override
        public MockResponse dispatch(RecordedRequest request) {
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
    r.add("dak.vault.addr", () -> base);
    r.add("dak.vault.token", () -> VAULT_TOKEN);
    r.add("dak.vault.global-key-template", () -> "dak/global-keys/{zone}");
    r.add("dak.admin.token", () -> ADMIN_TOKEN);
    r.add("dak.deid.portal-users[0].username", () -> "alice");
    r.add("dak.deid.portal-users[0].team", () -> "team-a");
    r.add("dak.deid.portal-users[0].token", () -> ALICE);
  }

  @AfterAll
  static void stop() throws Exception {
    upstream.shutdown();
  }

  @Autowired
  private TestRestTemplate rest;

  @Test
  void aZoneIsReadOnlyWhileAnEffectiveSilverRequestOfTheZoneExists() throws Exception {
    String zone = "zone_flow";
    assertThat(admin(HttpMethod.POST, "/mock-admin/global-keys", "{\"zone\":\"" + zone + "\",\"keyValue\":\"k-global\"}").getStatusCode().value())
        .isEqualTo(200);
    // The key exists but no silver request chose the zone: access_denied, the same answer as for a zone that does not exist.
    ResponseEntity<String> noRequest = global(INTERNAL, zone);
    ResponseEntity<String> unknownZone = global(INTERNAL, "zone_unknown");
    assertError(noRequest, 403, "access_denied");
    assertError(unknownZone, 403, "access_denied");
    assertThat(json.readTree(noRequest.getBody()).path("message")).isEqualTo(json.readTree(unknownZone.getBody()).path("message"));

    String code = submitNims(zone, "vtnet_nims_global_flow");
    assertError(global(INTERNAL, zone), 403, "access_denied"); // not approved yet

    JsonNode approved = json.readTree(post("/mock-admin/silver-requests/" + code + "/approve").getBody());
    assertThat(approved.path("state").asText()).isEqualTo("SCHEDULED");
    assertThat(approved.path("globalZone").asText()).isEqualTo(zone);

    JsonNode internal = ok(global(INTERNAL, zone));
    assertThat(internal.path("zone").asText()).isEqualTo(zone);
    assertThat(internal.path("keyPrefix").asText()).isEqualTo("k-global");
    assertThat(internal.path("keyVersion").asLong()).isEqualTo(1);
    assertThat(ok(global(TEAM_A, zone)).path("keyPrefix").asText()).isEqualTo("k-global"); // the team that sent the request
    assertError(global(TEAM_B, zone), 403, "access_denied"); // another team
    assertThat(vault.keySet()).contains("dak/global-keys/" + zone);

    post("/mock-admin/silver-requests/" + code + "/stop");
    assertError(global(INTERNAL, zone), 403, "access_denied"); // no effective request any more
  }

  @Test
  void aDisabledGlobalKeyIsForbiddenAndEnableRestoresIt() throws Exception {
    String zone = "zone_disabled";
    admin(HttpMethod.POST, "/mock-admin/global-keys", "{\"zone\":\"" + zone + "\",\"keyValue\":\"k-dis\"}");
    String code = submitNims(zone, "vtnet_nims_global_disabled");
    post("/mock-admin/silver-requests/" + code + "/approve");
    assertThat(ok(global(INTERNAL, zone)).path("keyPrefix").asText()).isEqualTo("k-dis");

    assertThat(admin(HttpMethod.POST, "/mock-admin/global-keys/disable", "{\"zone\":\"" + zone + "\"}").getStatusCode().value()).isEqualTo(204);
    assertError(global(INTERNAL, zone), 403, "key_disabled");

    assertThat(admin(HttpMethod.POST, "/mock-admin/global-keys/enable", "{\"zone\":\"" + zone + "\"}").getStatusCode().value()).isEqualTo(204);
    assertThat(ok(global(INTERNAL, zone)).path("keyPrefix").asText()).isEqualTo("k-dis");
    post("/mock-admin/silver-requests/" + code + "/stop");
  }

  @Test
  void theKeyOfAZoneNeverChanges() throws Exception {
    String zone = "zone_fixed";
    admin(HttpMethod.POST, "/mock-admin/global-keys", "{\"zone\":\"" + zone + "\",\"keyValue\":\"k-first\"}");
    ResponseEntity<String> again = admin(HttpMethod.POST, "/mock-admin/global-keys", "{\"zone\":\"" + zone + "\",\"keyValue\":\"k-second\"}");

    assertThat(again.getStatusCode().value()).isEqualTo(200);
    assertThat(again.getBody()).doesNotContain("k-first").doesNotContain("k-second");
    assertThat(vault.get("dak/global-keys/" + zone)).isEqualTo("k-first");
  }

  @Test
  void aBadZoneIsRejectedBeforeTheTokenAndATokenIsRequired() throws Exception {
    assertError(global(INTERNAL, "Bad-Zone"), 400, "invalid_zone");
    assertError(rest.exchange("/api/v1/keys/global", HttpMethod.GET, new HttpEntity<>(auth(INTERNAL)), String.class), 400, "invalid_zone");
    assertError(rest.exchange("/api/v1/keys/global?zone=" + ZONE, HttpMethod.GET, new HttpEntity<>(new HttpHeaders()), String.class), 401,
        "invalid_token");
    assertThat(admin(HttpMethod.POST, "/mock-admin/global-keys", "{\"zone\":\"Bad Zone\"}").getStatusCode().value()).isEqualTo(400);
    assertThat(admin(HttpMethod.POST, "/mock-admin/global-keys/disable", "{\"zone\":\"zone_without_key\"}").getStatusCode().value()).isEqualTo(404);
  }

  @Test
  void theAdminEndpointsAreHiddenWithoutTheToken() {
    HttpHeaders h = new HttpHeaders();
    h.setContentType(MediaType.APPLICATION_JSON);
    assertThat(rest.exchange("/mock-admin/global-keys", HttpMethod.POST, new HttpEntity<>("{\"zone\":\"zone_x\"}", h), String.class)
        .getStatusCode().value()).isEqualTo(404);
  }

  // ---------------------------------------------------------------- helpers

  private String submitNims(String zone, String table) throws Exception {
    String body = "{\"source\":\"VTNET\",\"path\":\"nims\",\"cron\":\"0 2 * * *\",\"table\":\"" + table + "\","
        + "\"encryptColumns\":[\"lng\",\"lat\"],\"keyField\":\"eci\",\"globalZone\":\"" + zone + "\",\"purpose\":\"global key\"}";
    HttpHeaders h = new HttpHeaders();
    h.setContentType(MediaType.APPLICATION_JSON);
    h.set("X-Mock-Portal-Token", ALICE);
    ResponseEntity<String> created = rest.exchange("/mock-portal/silver-requests", HttpMethod.POST, new HttpEntity<>(body, h), String.class);
    assertThat(created.getStatusCode().value()).as(created.getBody()).isEqualTo(201);
    return json.readTree(created.getBody()).path("code").asText();
  }

  private ResponseEntity<String> post(String path) {
    return admin(HttpMethod.POST, path, null);
  }

  private ResponseEntity<String> global(String clientId, String zone) {
    return rest.exchange("/api/v1/keys/global?zone=" + zone, HttpMethod.GET, new HttpEntity<>(auth(clientId)), String.class);
  }

  private ResponseEntity<String> admin(HttpMethod method, String path, String body) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("X-Mock-Admin-Token", ADMIN_TOKEN);
    return rest.exchange(path, method, new HttpEntity<>(body, headers), String.class);
  }

  private static JsonNode ok(ResponseEntity<String> res) throws Exception {
    assertThat(res.getStatusCode().value()).as(res.getBody()).isEqualTo(200);
    assertThat(res.getHeaders().getCacheControl()).isEqualTo("no-store");
    return json.readTree(res.getBody());
  }

  private static void assertError(ResponseEntity<String> res, int status, String error) throws Exception {
    assertThat(res.getStatusCode().value()).as(res.getBody()).isEqualTo(status);
    assertThat(json.readTree(res.getBody()).path("error").asText()).isEqualTo(error);
    assertThat(res.getBody()).doesNotContain(VAULT_TOKEN).doesNotContain("k-global");
  }

  private static MockResponse route(RecordedRequest request) {
    String path = request.getPath() == null ? "" : request.getPath();
    if (path.equals(JWKS_PATH)) {
      return jsonResponse(200, new JWKSet(signingKey.toPublicJWK()).toString());
    }
    if (!path.startsWith(GLOBAL_KEYS) || !VAULT_TOKEN.equals(request.getHeader("X-Vault-Token"))) {
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
