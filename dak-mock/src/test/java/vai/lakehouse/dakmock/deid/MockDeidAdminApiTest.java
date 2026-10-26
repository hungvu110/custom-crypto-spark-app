package vai.lakehouse.dakmock.deid;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * D5, đầu-cuối qua HTTP: {@code /mock-admin/deid-requests/**} (docs/dak-deid-flow/API_CONTRACT.md mục 5) cùng portal giả, với
 * {@code submitter=noop}. Phiếu sống suốt context Spring nên mỗi test dùng bảng riêng và chỉ so sánh tương đối.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MockDeidAdminApiTest {

  private static final String ADMIN = "admin-token-0123456789abcdef";
  private static final String ALICE = "alice-token-0123456789abcdef";
  private static final String BOB = "bob---token-0123456789abcdef";
  private static final String CAROL = "carol-token-0123456789abcdef";
  private static final List<String> TOKENS = List.of(ADMIN, ALICE, BOB, CAROL);
  private static final String PURPOSE = "ly do rieng tu cua team";
  private static final String UNKNOWN_CODE = "PYC-2026-999999";
  private static final String BASE = "/mock-admin/deid-requests";

  private static final ObjectMapper json = new ObjectMapper();
  private static final AtomicInteger tableCounter = new AtomicInteger();
  private static final ListAppender<ILoggingEvent> everything = new ListAppender<>();

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry r) {
    r.add("dak.vault.addr", () -> "http://127.0.0.1:1");
    r.add("dak.vault.token", () -> "vault-test-token");
    r.add("dak.admin.token", () -> ADMIN);
    r.add("dak.deid.max-active-per-team", () -> "100");
    r.add("dak.deid.portal-users[0].username", () -> "alice");
    r.add("dak.deid.portal-users[0].team", () -> "team-a");
    r.add("dak.deid.portal-users[0].token", () -> ALICE);
    r.add("dak.deid.portal-users[1].username", () -> "bob");
    r.add("dak.deid.portal-users[1].team", () -> "team-a");
    r.add("dak.deid.portal-users[1].token", () -> BOB);
    r.add("dak.deid.portal-users[2].username", () -> "carol");
    r.add("dak.deid.portal-users[2].team", () -> "team-b");
    r.add("dak.deid.portal-users[2].token", () -> CAROL);
  }

  @Autowired
  private TestRestTemplate rest;

  private ListAppender<ILoggingEvent> audit;

  @BeforeAll
  static void captureEveryLog() {
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    everything.start();
    root.addAppender(everything);
  }

  @AfterAll
  static void tokensNeverAppearInAnyLog() {
    ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).detachAppender(everything);
    List<String> messages = everything.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    for (String token : TOKENS) {
      assertThat(messages).as("log lines containing a token").noneMatch(m -> m.contains(token));
    }
    assertThat(messages).as("log lines containing a reject reason").noneMatch(m -> m.contains("ly do tu choi rieng"));
  }

  @BeforeEach
  void captureAudit() {
    audit = new ListAppender<>();
    audit.start();
    ((Logger) LoggerFactory.getLogger("AUDIT")).addAppender(audit);
  }

  @AfterEach
  void stopCapturingAudit() {
    ((Logger) LoggerFactory.getLogger("AUDIT")).detachAppender(audit);
  }

  // ---------------------------------------------------------------- tiện ích

  private static String table() {
    return "t" + tableCounter.incrementAndGet();
  }

  private static String phieu(String table) {
    return "{\"database\":\"demo_db\",\"table\":\"" + table + "\",\"columns\":[{\"column\":\"msisdn\"},{\"column\":\"imei\"}],"
        + "\"purpose\":\"" + PURPOSE + "\"}";
  }

  private HttpHeaders headers(String headerName, String token, MediaType contentType) {
    HttpHeaders h = new HttpHeaders();
    if (token != null) {
      h.set(headerName, token);
    }
    if (contentType != null) {
      h.setContentType(contentType);
    }
    return h;
  }

  private ResponseEntity<String> upload(String portalToken, String phieuJson) {
    MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
    parts.add("file", new ByteArrayResource(phieuJson.getBytes(UTF_8)) {
      @Override
      public String getFilename() {
        return "phieu.json";
      }
    });
    return rest.exchange("/mock-portal/deid-requests", HttpMethod.POST,
        new HttpEntity<>(parts, headers("X-Mock-Portal-Token", portalToken, MediaType.MULTIPART_FORM_DATA)), String.class);
  }

  private String createdCode(String portalToken, String phieuJson) {
    ResponseEntity<String> response = upload(portalToken, phieuJson);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    return body(response).get("requestCode").asText();
  }

  private ResponseEntity<String> portalGet(String portalToken, String code) {
    return rest.exchange("/mock-portal/deid-requests/" + code, HttpMethod.GET,
        new HttpEntity<>(headers("X-Mock-Portal-Token", portalToken, null)), String.class);
  }

  private ResponseEntity<String> portalCancel(String portalToken, String code) {
    return rest.exchange("/mock-portal/deid-requests/" + code + "/cancel", HttpMethod.POST,
        new HttpEntity<>(headers("X-Mock-Portal-Token", portalToken, null)), String.class);
  }

  private ResponseEntity<String> adminGet(String adminToken, String path) {
    return rest.exchange(path, HttpMethod.GET, new HttpEntity<>(headers("X-Mock-Admin-Token", adminToken, null)),
        String.class);
  }

  private ResponseEntity<String> adminPost(String adminToken, String path) {
    return rest.exchange(path, HttpMethod.POST, new HttpEntity<>(headers("X-Mock-Admin-Token", adminToken, null)),
        String.class);
  }

  private ResponseEntity<String> adminPostJson(String adminToken, String path, String rawBody) {
    return rest.exchange(path, HttpMethod.POST,
        new HttpEntity<>(rawBody, headers("X-Mock-Admin-Token", adminToken, MediaType.APPLICATION_JSON)), String.class);
  }

  private ResponseEntity<String> reject(String code, String reasonText) {
    return adminPostJson(ADMIN, BASE + "/" + code + "/reject", "{\"reasonText\":\"" + reasonText + "\"}");
  }

  private static JsonNode body(ResponseEntity<String> response) {
    try {
      return json.readTree(response.getBody());
    } catch (Exception e) {
      throw new AssertionError("Body is not JSON: " + response.getBody(), e);
    }
  }

  private static List<String> codes(JsonNode requests) {
    List<String> codes = new ArrayList<>();
    requests.forEach(r -> codes.add(r.get("requestCode").asText()));
    return codes;
  }

  private static void assertError(ResponseEntity<String> response, HttpStatus status, String error) {
    assertThat(response.getStatusCode()).isEqualTo(status);
    assertThat(body(response).get("error").asText()).isEqualTo(error);
  }

  private String stateOf(String code) {
    return body(portalGet(ALICE, code)).get("state").asText();
  }

  // ---------------------------------------------------------------- xác thực

  @Test
  void adminEndpoints_withoutTheAdminToken_are404_andChangeNothing() {
    String code = createdCode(ALICE, phieu(table()));
    List<ResponseEntity<String>> hidden = new ArrayList<>();
    for (String token : new String[] {null, "", ALICE, "wrong-admin-token-wrong-admin"}) {
      hidden.add(adminGet(token, BASE));
      hidden.add(adminGet(token, BASE + "/" + code));
      hidden.add(adminPost(token, BASE + "/" + code + "/approve"));
      hidden.add(adminPostJson(token, BASE + "/" + code + "/reject", "{\"reasonText\":\"x\"}"));
      hidden.add(adminPost(token, BASE + "/" + code + "/retry-submit"));
    }

    for (ResponseEntity<String> response : hidden) {
      assertError(response, HttpStatus.NOT_FOUND, "not_found");
      assertThat(body(response).get("message").asText()).isEqualTo("Unknown endpoint");
      assertThat(response.getBody()).doesNotContain(code).doesNotContain(ALICE);
    }
    assertThat(stateOf(code)).isEqualTo("PENDING_APPROVAL");
  }

  @Test
  void adminToken_isNotAPortalToken() {
    ResponseEntity<String> response = rest.exchange("/mock-portal/deid-requests", HttpMethod.GET,
        new HttpEntity<>(headers("X-Mock-Portal-Token", ADMIN, null)), String.class);

    assertError(response, HttpStatus.UNAUTHORIZED, "invalid_credentials");
  }

  // ---------------------------------------------------------------- duyệt

  @Test
  void approve_noop_movesToJobSubmitted_andTheTeamSeesIt() {
    String t = table();
    String code = createdCode(ALICE, phieu(t));

    ResponseEntity<String> approved = adminPost(ADMIN, BASE + "/" + code + "/approve");

    assertThat(approved.getStatusCode()).isEqualTo(HttpStatus.OK);
    JsonNode r = body(approved);
    assertThat(r.get("state").asText()).isEqualTo("JOB_SUBMITTED");
    assertThat(r.get("decidedBy").asText()).isEqualTo("admin");
    assertThat(r.get("outputDatabase").asText()).isEqualTo("demo_db");
    assertThat(r.get("outputTable").asText()).matches(t + "_deid_\\d{8}_\\d+");
    assertThat(r.get("jobRef").get("kind").asText()).isEqualTo("SparkApplication");
    assertThat(r.get("jobRef").get("name").asText()).matches("deid-\\d{4}-\\d{6}");
    assertThat(r.has("keyVersion")).as("noop does not read any key").isFalse();
    assertThat(r.has("submitError")).isFalse();
    ResponseEntity<String> seenByTeam = portalGet(BOB, code);
    // Xem phiếu làm mới trạng thái job (D9) và noop luôn báo job đã xong, nên lần xem này thấy SUCCEEDED.
    assertThat(body(seenByTeam).get("state").asText()).isEqualTo("SUCCEEDED");
    assertThat(body(seenByTeam).get("outputTable").asText()).isEqualTo(r.get("outputTable").asText());
    for (ResponseEntity<String> response : List.of(approved, seenByTeam)) {
      assertThat(response.getBody()).doesNotContain("deid-key-");
      assertThat(response.getHeaders().getFirst("X-Request-Id")).isNotBlank();
      assertThat(response.getHeaders().getFirst(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
    }
  }

  @Test
  void approve_isIdempotent() {
    String code = createdCode(ALICE, phieu(table()));
    ResponseEntity<String> first = adminPost(ADMIN, BASE + "/" + code + "/approve");

    ResponseEntity<String> again = adminPost(ADMIN, BASE + "/" + code + "/approve");

    assertThat(again.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(body(again)).isEqualTo(body(first));
  }

  @Test
  void approve_afterTheUserCancelled_is409_andNothingHappens() {
    String code = createdCode(ALICE, phieu(table()));
    portalCancel(BOB, code);

    ResponseEntity<String> response = adminPost(ADMIN, BASE + "/" + code + "/approve");

    assertError(response, HttpStatus.CONFLICT, "invalid_state");
    assertThat(body(response).get("message").asText()).contains("CANCELLED");
    assertThat(stateOf(code)).isEqualTo("CANCELLED");
  }

  @Test
  void cancel_afterApproval_is409() {
    String code = createdCode(ALICE, phieu(table()));
    adminPost(ADMIN, BASE + "/" + code + "/approve");

    ResponseEntity<String> response = portalCancel(ALICE, code);

    assertError(response, HttpStatus.CONFLICT, "invalid_state");
    // Phiếu đã duyệt không còn huỷ được; noop báo job đã xong nên lần xem sau thấy SUCCEEDED (không phải CANCELLED).
    assertThat(stateOf(code)).isEqualTo("SUCCEEDED");
  }

  @Test
  void unknownAndMalformedCodes_are404_forEveryAction() {
    for (String code : List.of(UNKNOWN_CODE, "not-a-code", "PYC-1")) {
      assertError(adminGet(ADMIN, BASE + "/" + code), HttpStatus.NOT_FOUND, "not_found");
      assertError(adminPost(ADMIN, BASE + "/" + code + "/approve"), HttpStatus.NOT_FOUND, "not_found");
      assertError(adminPost(ADMIN, BASE + "/" + code + "/retry-submit"), HttpStatus.NOT_FOUND, "not_found");
      assertError(reject(code, "x"), HttpStatus.NOT_FOUND, "not_found");
    }
  }

  // ---------------------------------------------------------------- từ chối

  @Test
  void reject_requiresAReason_andMalformedJsonIs400() {
    String code = createdCode(ALICE, phieu(table()));
    String url = BASE + "/" + code + "/reject";

    assertError(adminPost(ADMIN, url), HttpStatus.BAD_REQUEST, "invalid_request");
    assertError(adminPostJson(ADMIN, url, "{}"), HttpStatus.BAD_REQUEST, "invalid_request");
    assertError(reject(code, ""), HttpStatus.BAD_REQUEST, "invalid_request");
    assertError(reject(code, "   "), HttpStatus.BAD_REQUEST, "invalid_request");
    ResponseEntity<String> malformed = adminPostJson(ADMIN, url, "{not json SECRET-BODY");
    assertError(malformed, HttpStatus.BAD_REQUEST, "invalid_request");
    assertThat(malformed.getBody()).doesNotContain("SECRET-BODY");
    assertThat(stateOf(code)).isEqualTo("PENDING_APPROVAL");
  }

  @Test
  void reject_recordsTheReason_theTeamSeesIt_andABlockedTableCanBeUploadedAgain() {
    String t = table();
    String code = createdCode(ALICE, phieu(t));

    ResponseEntity<String> rejected = reject(code, "ly do tu choi rieng");

    assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(body(rejected).get("state").asText()).isEqualTo("REJECTED");
    assertThat(body(rejected).get("reasonText").asText()).isEqualTo("ly do tu choi rieng");
    assertThat(body(rejected).get("decidedBy").asText()).isEqualTo("admin");
    JsonNode seenByTeam = body(portalGet(BOB, code));
    assertThat(seenByTeam.get("state").asText()).isEqualTo("REJECTED");
    assertThat(seenByTeam.get("reasonText").asText()).isEqualTo("ly do tu choi rieng");
    assertError(reject(code, "lan hai"), HttpStatus.CONFLICT, "invalid_state");
    assertError(adminPost(ADMIN, BASE + "/" + code + "/approve"), HttpStatus.CONFLICT, "invalid_state");
    assertThat(upload(ALICE, phieu(t)).getStatusCode()).isEqualTo(HttpStatus.CREATED);
  }

  @Test
  void retrySubmit_onAPhieuThatDidNotFail_is409() {
    String pending = createdCode(ALICE, phieu(table()));
    String submitted = createdCode(ALICE, phieu(table()));
    adminPost(ADMIN, BASE + "/" + submitted + "/approve");

    assertError(adminPost(ADMIN, BASE + "/" + pending + "/retry-submit"), HttpStatus.CONFLICT, "invalid_state");
    assertError(adminPost(ADMIN, BASE + "/" + submitted + "/retry-submit"), HttpStatus.CONFLICT, "invalid_state");
    assertThat(stateOf(pending)).isEqualTo("PENDING_APPROVAL");
  }

  // ---------------------------------------------------------------- xem, liệt kê

  @Test
  void get_returnsAnyTeamsRequest() {
    String code = createdCode(CAROL, phieu(table()));

    ResponseEntity<String> response = adminGet(ADMIN, BASE + "/" + code);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(body(response).get("team").asText()).isEqualTo("team-b");
    assertThat(body(response).get("purpose").asText()).isEqualTo(PURPOSE);
  }

  @Test
  void list_coversEveryTeam_filtersByState_andValidatesParameters() {
    String a = createdCode(ALICE, phieu(table()));
    String c = createdCode(CAROL, phieu(table()));
    String rejected = createdCode(ALICE, phieu(table()));
    reject(rejected, "khong");

    JsonNode all = body(adminGet(ADMIN, BASE)).get("requests");
    JsonNode onlyRejected = body(adminGet(ADMIN, BASE + "?state=rejected")).get("requests");
    JsonNode page1 = body(adminGet(ADMIN, BASE + "?limit=2"));
    JsonNode page2 = body(adminGet(ADMIN, BASE + "?limit=2&cursor=" + page1.get("nextCursor").asText()));

    assertThat(codes(all)).startsWith(rejected, c, a);
    assertThat(codes(onlyRejected)).first().isEqualTo(rejected);
    onlyRejected.forEach(r -> assertThat(r.get("state").asText()).isEqualTo("REJECTED"));
    assertThat(codes(page1.get("requests"))).containsExactly(rejected, c);
    assertThat(codes(page2.get("requests"))).first().isEqualTo(a);
    for (String query : List.of("limit=0", "limit=101", "limit=abc", "state=BOGUS", "cursor=nope", "cursor=" + UNKNOWN_CODE)) {
      assertError(adminGet(ADMIN, BASE + "?" + query), HttpStatus.BAD_REQUEST, "invalid_request");
    }
  }

  @Test
  void existingYcEndpoints_stillWorkWithTheSameToken() {
    ResponseEntity<String> ok = adminGet(ADMIN, "/mock-admin/yc");
    ResponseEntity<String> hidden = adminGet(null, "/mock-admin/yc");

    assertThat(ok.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(body(ok).isArray()).isTrue();
    assertError(hidden, HttpStatus.NOT_FOUND, "not_found");
  }

  // ---------------------------------------------------------------- audit

  @Test
  void audit_recordsAdminActionsWithoutTokensOrReasons() {
    String code = createdCode(ALICE, phieu(table()));
    String toReject = createdCode(ALICE, phieu(table()));
    adminPost(ADMIN, BASE + "/" + code + "/approve");
    reject(toReject, "ly do tu choi rieng");
    adminPost(ADMIN, BASE + "/" + code + "/retry-submit");

    List<String> lines = audit.list.stream().map(ILoggingEvent::getFormattedMessage).toList();

    assertThat(lines).anyMatch(l -> l.startsWith("DEID_TRANSITION") && l.contains("requestCode=" + code)
        && l.contains("from=PENDING_APPROVAL") && l.contains("to=APPROVED") && l.contains("actor=admin"));
    assertThat(lines).anyMatch(l -> l.startsWith("DEID_TRANSITION") && l.contains("requestCode=" + code)
        && l.contains("from=APPROVED") && l.contains("to=JOB_SUBMITTED"));
    assertThat(lines).anyMatch(l -> l.contains("requestCode=" + toReject) && l.contains("to=REJECTED"));
    assertThat(lines).anyMatch(l -> l.contains("action=retry-submit") && l.contains("result=ERROR")
        && l.contains("error=invalid_state") && l.contains("actor=admin"));
    assertThat(lines).noneMatch(l -> l.contains("ly do tu choi rieng"));
    for (String token : TOKENS) {
      assertThat(lines).noneMatch(l -> l.contains(token));
    }
  }
}
