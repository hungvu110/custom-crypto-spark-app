package vai.lakehouse.dakmock.deid;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
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
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import vai.lakehouse.dakmock.deid.KeyDelivery.KeyHandle;

/**
 * D5: đường lỗi của duyệt, đầu-cuối qua HTTP. {@link KeyDelivery} và {@link JobSubmitter} được thay bằng đối tượng giả để thử
 * các lỗi lấy key và gửi job, rồi {@code retry-submit} (plan mục 4.3). Lỗi này KHÔNG là lỗi HTTP: {@code approve} vẫn trả
 * {@code 200}, lỗi nằm ở trạng thái phiếu (API_CONTRACT mục 5).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MockDeidAdminFailureApiTest {

  private static final String ADMIN = "admin-token-0123456789abcdef";
  private static final String ALICE = "alice-token-0123456789abcdef";
  private static final String BASE = "/mock-admin/deid-requests";
  private static final ObjectMapper json = new ObjectMapper();
  private static final AtomicInteger tableCounter = new AtomicInteger();

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry r) {
    r.add("dak.vault.addr", () -> "http://127.0.0.1:1");
    r.add("dak.vault.token", () -> "vault-test-token");
    r.add("dak.admin.token", () -> ADMIN);
    r.add("dak.deid.max-active-per-team", () -> "100");
    r.add("dak.deid.portal-users[0].username", () -> "alice");
    r.add("dak.deid.portal-users[0].team", () -> "team-a");
    r.add("dak.deid.portal-users[0].token", () -> ALICE);
  }

  @MockitoBean
  private KeyDelivery keyDelivery;
  @MockitoBean
  private JobSubmitter submitter;

  @Autowired
  private TestRestTemplate rest;

  private String uploadedCode() {
    String phieu = "{\"database\":\"demo_db\",\"table\":\"f" + tableCounter.incrementAndGet()
        + "\",\"columns\":[{\"column\":\"msisdn\"}],\"purpose\":\"thu loi\"}";
    MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
    parts.add("file", new ByteArrayResource(phieu.getBytes(UTF_8)) {
      @Override
      public String getFilename() {
        return "phieu.json";
      }
    });
    HttpHeaders h = new HttpHeaders();
    h.set("X-Mock-Portal-Token", ALICE);
    h.setContentType(MediaType.MULTIPART_FORM_DATA);
    ResponseEntity<String> response = rest.exchange("/mock-portal/deid-requests", HttpMethod.POST,
        new HttpEntity<>(parts, h), String.class);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    return body(response).get("requestCode").asText();
  }

  private ResponseEntity<String> admin(String code, String action) {
    HttpHeaders h = new HttpHeaders();
    h.set("X-Mock-Admin-Token", ADMIN);
    return rest.exchange(BASE + "/" + code + "/" + action, HttpMethod.POST, new HttpEntity<>(h), String.class);
  }

  private ResponseEntity<String> portalGet(String code) {
    HttpHeaders h = new HttpHeaders();
    h.set("X-Mock-Portal-Token", ALICE);
    return rest.exchange("/mock-portal/deid-requests/" + code, HttpMethod.GET, new HttpEntity<>(h), String.class);
  }

  private ResponseEntity<String> portalCancel(String code) {
    HttpHeaders h = new HttpHeaders();
    h.set("X-Mock-Portal-Token", ALICE);
    return rest.exchange("/mock-portal/deid-requests/" + code + "/cancel", HttpMethod.POST, new HttpEntity<>(h),
        String.class);
  }

  private static JsonNode body(ResponseEntity<String> response) {
    try {
      return json.readTree(response.getBody());
    } catch (Exception e) {
      throw new AssertionError("Body is not JSON: " + response.getBody(), e);
    }
  }

  @Test
  void keyFailure_leavesTheRequestSubmitFailed_thenRetrySucceeds() {
    when(keyDelivery.deliver(any()))
        .thenThrow(new KeyDeliveryException("table key not found"))
        .thenReturn(new KeyHandle("deid-key-x", 5L));
    when(submitter.submit(any(), anyString())).thenReturn(new JobRef(JobRef.Kind.SPARK_APPLICATION, "deid-2026-000001"));
    String code = uploadedCode();

    ResponseEntity<String> approved = admin(code, "approve");

    assertThat(approved.getStatusCode()).as("approve itself succeeded").isEqualTo(HttpStatus.OK);
    assertThat(body(approved).get("state").asText()).isEqualTo("SUBMIT_FAILED");
    assertThat(body(approved).get("submitError").asText()).isEqualTo("table key not found");
    assertThat(body(approved).get("outputTable").asText()).isNotBlank();
    verify(submitter, never()).submit(any(), anyString());
    assertThat(body(portalGet(code)).get("state").asText()).isEqualTo("SUBMIT_FAILED");
    assertThat(portalCancel(code).getStatusCode()).as("cannot cancel after approval").isEqualTo(HttpStatus.CONFLICT);

    ResponseEntity<String> retried = admin(code, "retry-submit");

    assertThat(retried.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(body(retried).get("state").asText()).isEqualTo("JOB_SUBMITTED");
    assertThat(body(retried).has("submitError")).isFalse();
    assertThat(body(retried).get("keyVersion").asLong()).isEqualTo(5L);
    assertThat(body(retried).get("jobRef").get("kind").asText()).isEqualTo("SparkApplication");
    verify(keyDelivery, times(2)).deliver(any());
    verify(submitter, times(1)).submit(any(), anyString());
  }

  @Test
  void submitFailure_releasesTheKey_andLeavesTheRequestSubmitFailed() {
    when(keyDelivery.deliver(any())).thenReturn(new KeyHandle("deid-key-x", 2L));
    when(submitter.submit(any(), anyString())).thenThrow(new JobSubmitException("kubernetes unavailable"));
    String code = uploadedCode();

    ResponseEntity<String> approved = admin(code, "approve");

    assertThat(approved.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(body(approved).get("state").asText()).isEqualTo("SUBMIT_FAILED");
    assertThat(body(approved).get("submitError").asText()).isEqualTo("kubernetes unavailable");
    verify(keyDelivery, times(1)).release(any());
  }

  @Test
  void unexpectedFailure_neverLeaksIntoTheResponse() {
    when(keyDelivery.deliver(any())).thenThrow(new IllegalStateException("exploded with SECRET-VALUE"));
    String code = uploadedCode();

    ResponseEntity<String> approved = admin(code, "approve");

    assertThat(approved.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(body(approved).get("submitError").asText()).isEqualTo("internal error while delivering the key");
    assertThat(approved.getBody()).doesNotContain("SECRET-VALUE");
    assertThat(portalGet(code).getBody()).doesNotContain("SECRET-VALUE");
  }

  @Test
  void approvingTwice_neverDeliversOrSubmitsTwice() {
    when(keyDelivery.deliver(any())).thenReturn(new KeyHandle("deid-key-x", 1L));
    when(submitter.submit(any(), anyString())).thenReturn(new JobRef(JobRef.Kind.SPARK_APPLICATION, "deid-2026-000001"));
    String code = uploadedCode();

    admin(code, "approve");
    ResponseEntity<String> again = admin(code, "approve");

    assertThat(again.getStatusCode()).isEqualTo(HttpStatus.OK);
    verify(keyDelivery, times(1)).deliver(any());
    verify(submitter, times(1)).submit(any(), anyString());
  }
}
