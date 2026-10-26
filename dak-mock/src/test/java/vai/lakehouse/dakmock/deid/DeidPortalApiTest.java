package vai.lakehouse.dakmock.deid;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
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
import vai.lakehouse.dakmock.deid.DeidRequest.State;

/**
 * D4, đầu-cuối qua HTTP: portal giả {@code /mock-portal/**} (docs/dak-deid-flow/API_CONTRACT.md mục 4 và 8). Chạy với
 * {@code submitter=noop}; không cần Keycloak, Vault hay Kubernetes. Phiếu sống suốt context Spring nên mỗi test dùng bảng
 * riêng và dọn phiếu còn chờ duyệt sau khi xong.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DeidPortalApiTest {

  private static final String ALICE = "alice-token-0123456789abcdef";
  private static final String BOB = "bob---token-0123456789abcdef";
  private static final String CAROL = "carol-token-0123456789abcdef";
  private static final List<String> TOKENS = List.of(ALICE, BOB, CAROL);
  private static final String PURPOSE = "thu nghiem portal";
  private static final String UNKNOWN_CODE = "PYC-2026-999999";

  private static final ObjectMapper json = new ObjectMapper();
  private static final AtomicInteger tableCounter = new AtomicInteger();
  private static final ListAppender<ILoggingEvent> everything = new ListAppender<>();

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry r) {
    r.add("dak.vault.addr", () -> "http://127.0.0.1:1");
    r.add("dak.vault.token", () -> "vault-test-token");
    r.add("dak.deid.max-active-per-team", () -> "3");
    r.add("dak.deid.max-upload-bytes", () -> "4096");
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
  @Autowired
  private DeidRequestRegistry registry;

  private ListAppender<ILoggingEvent> audit;

  @BeforeAll
  static void captureEveryLog() {
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    everything.start();
    root.addAppender(everything);
  }

  @AfterAll
  static void tokensNeverAppearInAnyLog() {
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.detachAppender(everything);
    List<String> messages = everything.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    for (String token : TOKENS) {
      assertThat(messages).as("log lines containing a portal token").noneMatch(m -> m.contains(token));
    }
  }

  @BeforeEach
  void captureAudit() {
    audit = new ListAppender<>();
    audit.start();
    ((Logger) LoggerFactory.getLogger("AUDIT")).addAppender(audit);
  }

  @AfterEach
  void cleanUp() {
    ((Logger) LoggerFactory.getLogger("AUDIT")).detachAppender(audit);
    for (DeidRequest r : registry.listAll()) {
      if (r.state() == State.PENDING_APPROVAL) {
        registry.transition(r.requestCode(), Set.of(State.PENDING_APPROVAL), DeidRequest::cancelled);
      }
    }
  }

  // ---------------------------------------------------------------- tiện ích

  private static String table() {
    return "t" + tableCounter.incrementAndGet();
  }

  private static String phieu(String table, String... columns) {
    List<String> items = new ArrayList<>();
    for (String column : columns) {
      items.add("{\"column\":\"" + column + "\"}");
    }
    return "{\"database\":\"demo_db\",\"table\":\"" + table + "\",\"columns\":[" + String.join(",", items)
        + "],\"purpose\":\"" + PURPOSE + "\"}";
  }

  private static String phieu(String table) {
    return phieu(table, "msisdn", "imei");
  }

  private HttpHeaders headers(String token, MediaType contentType) {
    HttpHeaders h = new HttpHeaders();
    if (token != null) {
      h.set("X-Mock-Portal-Token", token);
    }
    if (contentType != null) {
      h.setContentType(contentType);
    }
    return h;
  }

  private ResponseEntity<String> upload(String token, String filename, byte[] content) {
    MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
    parts.add("file", new ByteArrayResource(content) {
      @Override
      public String getFilename() {
        return filename;
      }
    });
    return rest.exchange("/mock-portal/deid-requests", HttpMethod.POST,
        new HttpEntity<>(parts, headers(token, MediaType.MULTIPART_FORM_DATA)), String.class);
  }

  private ResponseEntity<String> upload(String token, String phieuJson) {
    return upload(token, "phieu.json", phieuJson.getBytes(UTF_8));
  }

  private ResponseEntity<String> get(String token, String path) {
    return rest.exchange(path, HttpMethod.GET, new HttpEntity<>(headers(token, null)), String.class);
  }

  private ResponseEntity<String> cancel(String token, String code) {
    return rest.exchange("/mock-portal/deid-requests/" + code + "/cancel", HttpMethod.POST,
        new HttpEntity<>(headers(token, null)), String.class);
  }

  private static JsonNode body(ResponseEntity<String> response) {
    try {
      return json.readTree(response.getBody());
    } catch (Exception e) {
      throw new AssertionError("Body is not JSON: " + response.getBody(), e);
    }
  }

  /** Mã các phiếu trong một mảng {@code requests} của response danh sách, giữ nguyên thứ tự. */
  private static List<String> codes(JsonNode requests) {
    List<String> codes = new ArrayList<>();
    requests.forEach(r -> codes.add(r.get("requestCode").asText()));
    return codes;
  }

  private String createdCode(String token, String phieuJson) {
    ResponseEntity<String> response = upload(token, phieuJson);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    return body(response).get("requestCode").asText();
  }

  private static void assertError(ResponseEntity<String> response, HttpStatus status, String error) {
    assertThat(response.getStatusCode()).isEqualTo(status);
    assertThat(body(response).get("error").asText()).isEqualTo(error);
  }

  // ---------------------------------------------------------------- upload

  @Test
  void upload_validFile_createsPendingRequestOfTheUsersTeam() throws Exception {
    String t = table();
    String phieu = phieu(t);

    ResponseEntity<String> response = upload(ALICE, "phieu-khu-dinh-danh.json", phieu.getBytes(UTF_8));

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    JsonNode r = body(response);
    assertThat(r.get("requestCode").asText()).matches("PYC-\\d{4}-\\d{6}");
    assertThat(r.get("state").asText()).isEqualTo("PENDING_APPROVAL");
    assertThat(r.get("database").asText()).isEqualTo("demo_db");
    assertThat(r.get("table").asText()).isEqualTo(t);
    assertThat(r.get("columns")).hasSize(2);
    assertThat(r.get("columns").get(0).get("column").asText()).isEqualTo("msisdn");
    assertThat(r.get("columns").get(0).get("mode").asText()).isEqualTo("HASH");
    assertThat(r.get("purpose").asText()).isEqualTo(PURPOSE);
    assertThat(r.get("team").asText()).isEqualTo("team-a");
    assertThat(r.get("createdBy").asText()).isEqualTo("alice");
    assertThat(Instant.parse(r.get("createdAt").asText())).isNotNull();
    assertThat(r.get("sourceFile").get("name").asText()).isEqualTo("phieu-khu-dinh-danh.json");
    String expectedSha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(phieu.getBytes(UTF_8)));
    assertThat(r.get("sourceFile").get("sha256").asText()).isEqualTo(expectedSha);
    // Trường của các bước sau không có mặt (không trả null).
    for (String absent : List.of("decidedAt", "decidedBy", "reasonText", "outputDatabase", "outputTable", "keyVersion",
        "jobRef", "submitError", "jobState", "jobFinishedAt", "dagId")) {
      assertThat(r.has(absent)).as(absent).isFalse();
    }
    for (String token : TOKENS) {
      assertThat(response.getBody()).doesNotContain(token);
    }
  }

  @Test
  void upload_responsesCarryRequestIdAndNoStore() {
    ResponseEntity<String> ok = upload(ALICE, phieu(table()));
    ResponseEntity<String> denied = upload("wrong-token-wrong-token-wrong", phieu(table()));

    for (ResponseEntity<String> response : List.of(ok, denied)) {
      assertThat(response.getHeaders().getFirst("X-Request-Id")).isNotBlank();
      assertThat(response.getHeaders().getFirst(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
    }
  }

  @Test
  void upload_invalidFile_is400WithProblems_andCreatesNothing() {
    String t = table();
    String bad = "{\"database\":\"bad db\",\"table\":\"" + t + "\",\"columns\":[{\"column\":\"ok\"},{\"column\":\"no good\"}],"
        + "\"purpose\":\"\"}";

    int before = registry.listAll().size();
    ResponseEntity<String> response = upload(ALICE, bad);

    assertError(response, HttpStatus.BAD_REQUEST, "invalid_file");
    JsonNode problems = body(response).get("problems");
    assertThat(problems).hasSize(3);
    assertThat(problems.get(0).get("field").asText()).isEqualTo("database");
    assertThat(problems.get(0).get("code").asText()).isEqualTo("invalid_table");
    assertThat(problems.get(1).get("field").asText()).isEqualTo("columns[1].column");
    assertThat(problems.get(2).get("field").asText()).isEqualTo("purpose");
    assertThat(registry.listAll()).hasSize(before);
  }

  @Test
  void upload_missingFilePart_andNonMultipartBody_are400() {
    int before = registry.listAll().size();
    MultiValueMap<String, Object> other = new LinkedMultiValueMap<>();
    other.add("not-the-file", "x");
    ResponseEntity<String> noFilePart = rest.exchange("/mock-portal/deid-requests", HttpMethod.POST,
        new HttpEntity<>(other, headers(ALICE, MediaType.MULTIPART_FORM_DATA)), String.class);
    ResponseEntity<String> jsonBody = rest.exchange("/mock-portal/deid-requests", HttpMethod.POST,
        new HttpEntity<>("{}", headers(ALICE, MediaType.APPLICATION_JSON)), String.class);

    assertError(noFilePart, HttpStatus.BAD_REQUEST, "invalid_file");
    assertError(jsonBody, HttpStatus.BAD_REQUEST, "invalid_file");
    assertThat(registry.listAll()).hasSize(before);
  }

  @Test
  void upload_nonMultipartBodyWithoutToken_is401NotA400() {
    ResponseEntity<String> response = rest.exchange("/mock-portal/deid-requests", HttpMethod.POST,
        new HttpEntity<>("{}", headers(null, MediaType.APPLICATION_JSON)), String.class);

    assertError(response, HttpStatus.UNAUTHORIZED, "invalid_credentials");
  }

  @Test
  void upload_overTheParserLimit_is400WithoutProblems() {
    byte[] oversized = new byte[5000];
    Arrays.fill(oversized, (byte) ' ');

    ResponseEntity<String> response = upload(ALICE, "phieu.json", oversized);

    assertError(response, HttpStatus.BAD_REQUEST, "invalid_file");
    assertThat(body(response).has("problems")).isFalse();
    assertThat(body(response).get("message").asText()).contains("too large");
  }

  @Test
  void upload_overTheSpringMultipartLimit_is400TooLarge() {
    byte[] huge = new byte[1_200_000];
    Arrays.fill(huge, (byte) ' ');

    ResponseEntity<String> response = upload(ALICE, "phieu.json", huge);

    assertError(response, HttpStatus.BAD_REQUEST, "invalid_file");
    assertThat(body(response).get("message").asText()).contains("too large");
  }

  @Test
  void upload_duplicate_is409WithTheExistingCode_andOtherTeamIsUnaffected() {
    String t = table();
    String first = createdCode(ALICE, phieu(t));

    ResponseEntity<String> sameTeam = upload(BOB, phieu(t));
    ResponseEntity<String> otherTeam = upload(CAROL, phieu(t));

    assertError(sameTeam, HttpStatus.CONFLICT, "duplicate_request");
    assertThat(body(sameTeam).get("message").asText()).contains(first);
    assertThat(otherTeam.getStatusCode()).isEqualTo(HttpStatus.CREATED);
  }

  @Test
  void upload_overTheActiveLimit_is429_andCancellingFreesASlot() {
    String first = createdCode(ALICE, phieu(table()));
    createdCode(ALICE, phieu(table()));
    createdCode(BOB, phieu(table()));

    assertError(upload(ALICE, phieu(table())), HttpStatus.TOO_MANY_REQUESTS, "too_many_active_requests");
    assertThat(upload(CAROL, phieu(table())).getStatusCode()).isEqualTo(HttpStatus.CREATED);

    assertThat(cancel(ALICE, first).getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(upload(ALICE, phieu(table())).getStatusCode()).isEqualTo(HttpStatus.CREATED);
  }

  // ---------------------------------------------------------------- xác thực

  @Test
  void everyEndpoint_withoutAValidToken_is401WithTheSameBody() {
    String code = createdCode(ALICE, phieu(table()));
    String wrong = "wrong-token-should-not-be-echoed";
    List<ResponseEntity<String>> denied = new ArrayList<>();
    for (String token : new String[] {null, "", wrong, ALICE + "x"}) {
      denied.add(upload(token, phieu(table())));
      denied.add(get(token, "/mock-portal/deid-requests"));
      denied.add(get(token, "/mock-portal/deid-requests/" + code));
      denied.add(cancel(token, code));
      denied.add(get(token, "/mock-portal/deid-dags"));
    }

    for (ResponseEntity<String> response : denied) {
      assertError(response, HttpStatus.UNAUTHORIZED, "invalid_credentials");
      assertThat(body(response).get("message").asText()).isEqualTo(body(denied.get(0)).get("message").asText());
      assertThat(response.getBody()).doesNotContain(wrong).doesNotContain(ALICE);
    }
    assertThat(body(get(ALICE, "/mock-portal/deid-requests/" + code)).get("state").asText()).isEqualTo("PENDING_APPROVAL");
  }

  // ---------------------------------------------------------------- huỷ, xem, cách ly team

  @Test
  void cancel_movesToCancelled_isIdempotent_andAllowsReupload() {
    String t = table();
    String code = createdCode(ALICE, phieu(t));

    ResponseEntity<String> first = cancel(ALICE, code);
    ResponseEntity<String> again = cancel(BOB, code);

    assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(body(first).get("state").asText()).isEqualTo("CANCELLED");
    assertThat(again.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(body(again).get("state").asText()).isEqualTo("CANCELLED");
    ResponseEntity<String> reupload = upload(ALICE, phieu(t));
    assertThat(reupload.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(body(reupload).get("requestCode").asText()).isNotEqualTo(code);
  }

  @Test
  void cancel_afterApproval_is409_andTheRequestIsUnchanged() {
    String code = createdCode(ALICE, phieu(table()));
    registry.transition(code, Set.of(State.PENDING_APPROVAL), r -> r.approved("admin", Instant.now(), "demo_db", "out"));
    try {
      ResponseEntity<String> response = cancel(ALICE, code);

      assertError(response, HttpStatus.CONFLICT, "invalid_state");
      assertThat(body(response).get("message").asText()).contains("APPROVED");
      assertThat(body(get(ALICE, "/mock-portal/deid-requests/" + code)).get("state").asText()).isEqualTo("APPROVED");
    } finally {
      registry.transition(code, Set.of(State.APPROVED), r -> r.submitted(new JobRef(JobRef.Kind.SPARK_APPLICATION, "x"), 1L));
      registry.transition(code, Set.of(State.JOB_SUBMITTED), r -> r.jobProgress(JobState.SUCCEEDED, Instant.now()));
    }
  }

  @Test
  void teamMembers_shareRequests() {
    String code = createdCode(ALICE, phieu(table()));

    ResponseEntity<String> seenByBob = get(BOB, "/mock-portal/deid-requests/" + code);

    assertThat(seenByBob.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(body(seenByBob).get("createdBy").asText()).isEqualTo("alice");
    assertThat(codes(body(get(BOB, "/mock-portal/deid-requests")).get("requests"))).first().isEqualTo(code);
  }

  @Test
  void otherTeam_getsA404IdenticalToAnUnknownCode_andCannotCancel() {
    String code = createdCode(ALICE, phieu(table()));

    ResponseEntity<String> foreignView = get(CAROL, "/mock-portal/deid-requests/" + code);
    ResponseEntity<String> unknownView = get(CAROL, "/mock-portal/deid-requests/" + UNKNOWN_CODE);
    ResponseEntity<String> foreignCancel = cancel(CAROL, code);
    ResponseEntity<String> unknownCancel = cancel(CAROL, UNKNOWN_CODE);

    assertError(foreignView, HttpStatus.NOT_FOUND, "not_found");
    assertError(foreignCancel, HttpStatus.NOT_FOUND, "not_found");
    assertThat(body(foreignView).get("message").asText()).isEqualTo(body(unknownView).get("message").asText());
    assertThat(body(foreignCancel).get("message").asText()).isEqualTo(body(unknownCancel).get("message").asText());
    assertThat(foreignView.getBody()).doesNotContain(code);
    assertThat(body(get(ALICE, "/mock-portal/deid-requests/" + code)).get("state").asText()).isEqualTo("PENDING_APPROVAL");
    assertThat(codes(body(get(CAROL, "/mock-portal/deid-requests")).get("requests"))).doesNotContain(code);
  }

  @Test
  void unknownAndMalformedCodes_are404WithTheSameBody() {
    ResponseEntity<String> unknown = get(ALICE, "/mock-portal/deid-requests/" + UNKNOWN_CODE);

    for (String bad : List.of("not-a-code", "PYC-1", "pyc-2026-000001")) {
      ResponseEntity<String> response = get(ALICE, "/mock-portal/deid-requests/" + bad);
      assertError(response, HttpStatus.NOT_FOUND, "not_found");
      assertThat(body(response).get("message").asText()).isEqualTo(body(unknown).get("message").asText());
    }
  }

  // ---------------------------------------------------------------- danh sách

  @Test
  void list_isNewestFirst_andOnlyOwnTeam() {
    String first = createdCode(ALICE, phieu(table()));
    String second = createdCode(BOB, phieu(table()));
    createdCode(CAROL, phieu(table()));

    JsonNode mine = body(get(ALICE, "/mock-portal/deid-requests")).get("requests");

    assertThat(codes(mine)).startsWith(second, first);
    assertThat(mine.get(0).get("requestCode").asText()).isEqualTo(second);
    assertThat(mine.get(1).get("requestCode").asText()).isEqualTo(first);
    JsonNode carols = body(get(CAROL, "/mock-portal/deid-requests")).get("requests");
    assertThat(codes(carols)).doesNotContain(first, second);
    carols.forEach(r -> assertThat(r.get("team").asText()).isEqualTo("team-b"));
    mine.forEach(r -> assertThat(r.get("team").asText()).isEqualTo("team-a"));
  }

  @Test
  void list_filtersByState_andPagesWithCursor() {
    String a = createdCode(ALICE, phieu(table()));
    String b = createdCode(ALICE, phieu(table()));
    String c = createdCode(ALICE, phieu(table()));
    cancel(ALICE, b);

    JsonNode cancelled = body(get(ALICE, "/mock-portal/deid-requests?state=cancelled")).get("requests");
    JsonNode page1 = body(get(ALICE, "/mock-portal/deid-requests?limit=2"));
    JsonNode page2 = body(get(ALICE, "/mock-portal/deid-requests?limit=2&cursor=" + page1.get("nextCursor").asText()));

    // Registry dùng chung cả class: các phiếu của test này luôn là mới nhất, nên kiểm theo vị trí đầu danh sách.
    assertThat(codes(cancelled)).first().isEqualTo(b);
    cancelled.forEach(r -> assertThat(r.get("state").asText()).isEqualTo("CANCELLED"));
    assertThat(codes(page1.get("requests"))).containsExactly(c, b);
    assertThat(page1.get("nextCursor").asText()).isEqualTo(b);
    assertThat(codes(page2.get("requests"))).first().isEqualTo(a);
    assertThat(page2.get("requests").size()).isLessThanOrEqualTo(2);
  }

  @Test
  void list_withInvalidParameters_is400() {
    for (String query : List.of("limit=0", "limit=101", "limit=abc", "state=BOGUS", "cursor=nope", "cursor=" + UNKNOWN_CODE)) {
      assertError(get(ALICE, "/mock-portal/deid-requests?" + query), HttpStatus.BAD_REQUEST, "invalid_request");
    }
  }

  @Test
  void deidDags_isEmptyWhenTheSubmitterIsNotAirflow() {
    ResponseEntity<String> response = get(ALICE, "/mock-portal/deid-dags");

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(body(response).get("dagIds")).isEmpty();
  }

  // ---------------------------------------------------------------- audit

  @Test
  void audit_recordsUploadAndRejectionsWithoutPurposeOrTokens() {
    String t = table();
    createdCode(ALICE, phieu(t));
    upload(BOB, phieu(t));
    upload("wrong-token-wrong-token-wrong", phieu(table()));

    List<String> lines = audit.list.stream().map(ILoggingEvent::getFormattedMessage).toList();

    assertThat(lines).anyMatch(l -> l.startsWith("DEID_TRANSITION") && l.contains("to=PENDING_APPROVAL")
        && l.contains("actor=alice") && l.contains("team=team-a") && l.contains("table=" + t) && l.contains("fileSha256=")
        && l.contains("result=OK"));
    assertThat(lines).anyMatch(l -> l.contains("result=ERROR") && l.contains("error=duplicate_request")
        && l.contains("actor=bob"));
    assertThat(lines).noneMatch(l -> l.contains(PURPOSE));
    for (String token : TOKENS) {
      assertThat(lines).noneMatch(l -> l.contains(token));
    }
  }
}
