package vai.lakehouse.dakmock;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

/** Phiếu chuẩn hoá silver đầu-cuối qua HTTP với {@code submitter=noop}: gửi → duyệt → dag run (noop: xong ngay). */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MockSilverApiTest {

  private static final String ALICE = "alice-token-0123456789abcdef";
  private static final String CAROL = "carol-token-0123456789abcdef";
  private static final String ADMIN = "admin-secret";
  private static final ObjectMapper JSON = new ObjectMapper();

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry r) {
    r.add("dak.vault.addr", () -> "http://127.0.0.1:1");
    r.add("dak.vault.token", () -> "vault-test-token");
    r.add("dak.admin.token", () -> ADMIN);
    r.add("dak.deid.portal-users[0].username", () -> "alice");
    r.add("dak.deid.portal-users[0].team", () -> "team-a");
    r.add("dak.deid.portal-users[0].token", () -> ALICE);
    r.add("dak.deid.portal-users[1].username", () -> "carol");
    r.add("dak.deid.portal-users[1].team", () -> "team-b");
    r.add("dak.deid.portal-users[1].token", () -> CAROL);
  }

  @Autowired
  private TestRestTemplate rest;

  private ResponseEntity<String> portal(HttpMethod method, String path, String token, String body) {
    HttpHeaders h = new HttpHeaders();
    h.setContentType(MediaType.APPLICATION_JSON);
    h.set("X-Mock-Portal-Token", token);
    return rest.exchange(path, method, new HttpEntity<>(body, h), String.class);
  }

  private ResponseEntity<String> admin(HttpMethod method, String path, String token) {
    HttpHeaders h = new HttpHeaders();
    if (token != null) {
      h.set("X-Mock-Admin-Token", token);
    }
    return rest.exchange(path, method, new HttpEntity<>(h), String.class);
  }

  @Test
  void vtnetRequestIsApprovedTriggeredAndFinished() throws Exception {
    String body = "{\"source\":\"VTNET\",\"path\":\"vtnet_cdr/20261027\",\"batchDate\":\"20261027\","
        + "\"encryptColumns\":[\"msisdn\",\"imei\"],\"keyField\":\"start_time\",\"purpose\":\"nạp lô 27\"}";

    ResponseEntity<String> created = portal(HttpMethod.POST, "/mock-portal/silver-requests", ALICE, body);

    assertThat(created.getStatusCode().value()).isEqualTo(201);
    JsonNode req = JSON.readTree(created.getBody());
    String code = req.path("code").asText();
    assertThat(req.path("state").asText()).isEqualTo("PENDING_APPROVAL");
    assertThat(req.path("dagId").asText()).isEqualTo("silver_ingest_vtnet");

    JsonNode approved = JSON.readTree(admin(HttpMethod.POST, "/mock-admin/silver-requests/" + code + "/approve", ADMIN).getBody());
    assertThat(approved.path("state").asText()).isEqualTo("JOB_SUBMITTED");
    assertThat(approved.path("dagRunId").asText()).isEqualTo(code.toLowerCase());

    JsonNode seen = JSON.readTree(portal(HttpMethod.GET, "/mock-portal/silver-requests/" + code, ALICE, null).getBody());
    assertThat(seen.path("state").asText()).isEqualTo("SUCCEEDED"); // noop: dag run xong ngay
    assertThat(portal(HttpMethod.GET, "/mock-portal/silver-requests/" + code, CAROL, null).getStatusCode().value())
        .isEqualTo(404);
  }

  @Test
  void invalidRequestsListEveryProblem() throws Exception {
    ResponseEntity<String> res = portal(HttpMethod.POST, "/mock-portal/silver-requests", ALICE,
        "{\"source\":\"VTT\",\"path\":\"/abs\",\"batchDate\":\"x\",\"encryptColumns\":[\"a\"],\"purpose\":\"p\"}");

    assertThat(res.getStatusCode().value()).isEqualTo(400);
    JsonNode body = JSON.readTree(res.getBody());
    assertThat(body.path("error").asText()).isEqualTo("invalid_request");
    assertThat(body.path("problems")).extracting(p -> p.path("field").asText())
        .containsExactlyInAnyOrder("path", "batchDate", "encryptColumns");
  }

  @Test
  void adminEndpointsAreHiddenWithoutTheToken() {
    assertThat(admin(HttpMethod.GET, "/mock-admin/silver-requests", null).getStatusCode().value()).isEqualTo(404);
    assertThat(admin(HttpMethod.GET, "/mock-admin/silver-requests", ADMIN).getStatusCode().value()).isEqualTo(200);
  }
}
