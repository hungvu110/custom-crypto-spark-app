package vai.lakehouse.dakmock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

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

  private ResponseEntity<String> adminJson(HttpMethod method, String path, String token, String body) {
    HttpHeaders h = new HttpHeaders();
    if (token != null) {
      h.set("X-Mock-Admin-Token", token);
    }
    h.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);
    return rest.exchange(path, method, new HttpEntity<>(body, h), String.class);
  }

  @Test
  void recurringNimsRequestIsApprovedOnceScheduledAndStopped() throws Exception {
    String body = "{\"source\":\"VTNET\",\"path\":\"nims\",\"cron\":\"0 3 * * *\",\"table\":\"vtnet_nims_infra_cell_4g\","
        + "\"encryptColumns\":[\"lng\",\"lat\"],\"keyField\":\"eci\",\"purpose\":\"nạp hằng ngày\"}";

    ResponseEntity<String> created = portal(HttpMethod.POST, "/mock-portal/silver-requests", ALICE, body);

    assertThat(created.getStatusCode().value()).isEqualTo(201);
    JsonNode req = JSON.readTree(created.getBody());
    String code = req.path("code").asText();
    assertThat(req.path("state").asText()).isEqualTo("PENDING_APPROVAL");
    assertThat(req.path("dagId").asText()).isEqualTo("silver_ingest_vtnet_nims");
    assertThat(req.path("cron").asText()).isEqualTo("0 3 * * *");
    assertThat(req.has("batchId")).isFalse();

    JsonNode approved = JSON.readTree(admin(HttpMethod.POST, "/mock-admin/silver-requests/" + code + "/approve", ADMIN).getBody());
    assertThat(approved.path("state").asText()).isEqualTo("SCHEDULED"); // duyệt một lần: DAG tự chạy mỗi ngày

    JsonNode seen = JSON.readTree(portal(HttpMethod.GET, "/mock-portal/silver-requests/" + code, ALICE, null).getBody());
    assertThat(seen.path("state").asText()).isEqualTo("SCHEDULED");
    assertThat(portal(HttpMethod.GET, "/mock-portal/silver-requests/" + code, CAROL, null).getStatusCode().value())
        .isEqualTo(404);

    ResponseEntity<String> secondOfTheDataset = portal(HttpMethod.POST, "/mock-portal/silver-requests", CAROL, body);
    assertThat(secondOfTheDataset.getStatusCode().value()).isEqualTo(409);

    assertThat(admin(HttpMethod.POST, "/mock-admin/silver-requests/" + code + "/stop", null).getStatusCode().value()).isEqualTo(404);
    JsonNode stopped = JSON.readTree(admin(HttpMethod.POST, "/mock-admin/silver-requests/" + code + "/stop", ADMIN).getBody());
    assertThat(stopped.path("state").asText()).isEqualTo("STOPPED");
    assertThat(admin(HttpMethod.POST, "/mock-admin/silver-requests/" + code + "/stop", ADMIN).getStatusCode().value()).isEqualTo(409);
  }

  @Test
  void anAdminCanRunOneDayOutsideTheSchedule() throws Exception {
    String body = "{\"source\":\"VTNET\",\"path\":\"ebs_3g_4g\",\"cron\":\"30 2 * * *\",\"table\":\"vtnet_ebs_3g_4g\","
        + "\"purpose\":\"write as it is\"}";
    String code = JSON.readTree(portal(HttpMethod.POST, "/mock-portal/silver-requests", ALICE, body).getBody()).path("code").asText();
    String run = "/mock-admin/silver-requests/" + code + "/run";

    assertThat(adminJson(HttpMethod.POST, run, ADMIN, "{\"date\":\"20261012\"}").getStatusCode().value()).isEqualTo(409); // not approved yet
    admin(HttpMethod.POST, "/mock-admin/silver-requests/" + code + "/approve", ADMIN);

    assertThat(adminJson(HttpMethod.POST, run, null, "{\"date\":\"20261012\"}").getStatusCode().value()).isEqualTo(404);
    ResponseEntity<String> ok = adminJson(HttpMethod.POST, run, ADMIN, "{\"date\":\"20261012\"}");
    assertThat(ok.getStatusCode().value()).isEqualTo(200);
    JsonNode started = JSON.readTree(ok.getBody());
    assertThat(started.path("requestCode").asText()).isEqualTo(code);
    assertThat(started.path("dagId").asText()).isEqualTo("silver_ingest_vtnet_ebs_3g_4g");
    assertThat(started.path("date").asText()).isEqualTo("20261012");
    assertThat(started.path("dagRunId").asText()).startsWith(code.toLowerCase() + "-20261012-");
    assertThat(adminJson(HttpMethod.POST, run, ADMIN, "{\"date\":\"2026-10-12\"}").getStatusCode().value()).isEqualTo(400);
    assertThat(adminJson(HttpMethod.POST, run, ADMIN, null).getStatusCode().value()).isEqualTo(400);

    JsonNode seen = JSON.readTree(portal(HttpMethod.GET, "/mock-portal/silver-requests/" + code, ALICE, null).getBody());
    assertThat(seen.path("state").asText()).isEqualTo("SCHEDULED"); // the schedule is untouched
    admin(HttpMethod.POST, "/mock-admin/silver-requests/" + code + "/stop", ADMIN); // the registry is shared by the tests of this class
  }

  @Test
  void vttRequestIsScheduledToo() throws Exception {
    String body = "{\"source\":\"VTT\",\"path\":\"f001-mpre-call\",\"cron\":\"30 1 * * *\","
        + "\"table\":\"vtt_f001_mpre_call\",\"purpose\":\"nạp hằng ngày\"}";
    JsonNode created = JSON.readTree(portal(HttpMethod.POST, "/mock-portal/silver-requests", ALICE, body).getBody());
    String code = created.path("code").asText();
    assertThat(created.path("dagId").asText()).isEqualTo("silver_ingest_vtt_f001_mpre_call");

    JsonNode approved = JSON.readTree(admin(HttpMethod.POST, "/mock-admin/silver-requests/" + code + "/approve", ADMIN).getBody());

    assertThat(approved.path("state").asText()).isEqualTo("SCHEDULED");
    admin(HttpMethod.POST, "/mock-admin/silver-requests/" + code + "/stop", ADMIN); // the registry is shared by the tests of this class
  }

  @Test
  void nimsRequestWithoutColumnsAndPassthroughRequestAreAccepted() throws Exception {
    String nims = "{\"source\":\"VTNET\",\"path\":\"nims\",\"cron\":\"0 3 * * *\",\"table\":\"vtnet_nims_infra_cell_4g\","
        + "\"encryptColumns\":[],\"purpose\":\"decrypt only\"}";
    String ebs = "{\"source\":\"VTNET\",\"path\":\"ebs_3g_4g\",\"cron\":\"30 2 * * *\",\"table\":\"vtnet_ebs_3g_4g\","
        + "\"purpose\":\"write as it is\"}";

    JsonNode decryptOnly = JSON.readTree(portal(HttpMethod.POST, "/mock-portal/silver-requests", ALICE, nims).getBody());
    JsonNode passthrough = JSON.readTree(portal(HttpMethod.POST, "/mock-portal/silver-requests", ALICE, ebs).getBody());

    assertThat(decryptOnly.path("dagId").asText()).isEqualTo("silver_ingest_vtnet_nims");
    assertThat(passthrough.path("dagId").asText()).isEqualTo("silver_ingest_vtnet_ebs_3g_4g");
    // One open request per dataset and the registry is shared by the tests of this class: give the datasets back.
    for (JsonNode created : List.of(decryptOnly, passthrough)) {
      admin(HttpMethod.POST, "/mock-admin/silver-requests/" + created.path("code").asText() + "/reject", ADMIN);
    }
  }

  @Test
  void invalidRequestsListEveryProblem() throws Exception {
    ResponseEntity<String> res = portal(HttpMethod.POST, "/mock-portal/silver-requests", ALICE,
        "{\"source\":\"VTT\",\"path\":\"/abs\",\"batchId\":\"x\",\"table\":\"vtt_t\",\"purpose\":\"p\"}");

    assertThat(res.getStatusCode().value()).isEqualTo(400);
    JsonNode body = JSON.readTree(res.getBody());
    assertThat(body.path("error").asText()).isEqualTo("invalid_request");
    assertThat(body.path("problems")).extracting(p -> p.path("field").asText())
        .containsExactlyInAnyOrder("path", "batchId", "cron");
  }

  @Test
  void adminEndpointsAreHiddenWithoutTheToken() {
    assertThat(admin(HttpMethod.GET, "/mock-admin/silver-requests", null).getStatusCode().value()).isEqualTo(404);
    assertThat(admin(HttpMethod.GET, "/mock-admin/silver-requests", ADMIN).getStatusCode().value()).isEqualTo(200);
  }
}
