package vai.lakehouse.dakmock.deid;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

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
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * Khi chưa khai tài khoản giả nào, portal tắt hẳn: mọi đường dẫn {@code /mock-portal/**} trả {@code 404} như không tồn tại
 * (giống endpoint quản trị khi chưa có token), kể cả khi gửi một token bất kỳ và kể cả khi không gửi token.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DeidPortalDisabledApiTest {

  private static final ObjectMapper json = new ObjectMapper();

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry r) {
    r.add("dak.vault.addr", () -> "http://127.0.0.1:1");
    r.add("dak.vault.token", () -> "vault-test-token");
  }

  @Autowired
  private TestRestTemplate rest;

  private ResponseEntity<String> get(String path, String token) {
    HttpHeaders h = new HttpHeaders();
    if (token != null) {
      h.set("X-Mock-Portal-Token", token);
    }
    return rest.exchange(path, HttpMethod.GET, new HttpEntity<>(h), String.class);
  }

  @Test
  void everyPortalEndpoint_is404_withOrWithoutAToken() throws Exception {
    for (String token : new String[] {null, "any-token-any-token-any-token"}) {
      for (String path : List.of("/mock-portal/deid-requests", "/mock-portal/deid-requests/PYC-2026-000001",
          "/mock-portal/deid-dags")) {
        ResponseEntity<String> response = get(path, token);

        assertThat(response.getStatusCode()).as(path).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(json.readTree(response.getBody()).get("error").asText()).isEqualTo("not_found");
      }
    }
  }

  @Test
  void adminDeidEndpoints_are404_whenNoAdminTokenIsConfigured() throws Exception {
    for (String path : List.of("/mock-admin/deid-requests", "/mock-admin/deid-requests/PYC-2026-000001")) {
      HttpHeaders h = new HttpHeaders();
      h.set("X-Mock-Admin-Token", "any-token-any-token-any-token");

      ResponseEntity<String> response = rest.exchange(path, HttpMethod.GET, new HttpEntity<>(h), String.class);

      assertThat(response.getStatusCode()).as(path).isEqualTo(HttpStatus.NOT_FOUND);
      assertThat(json.readTree(response.getBody()).get("error").asText()).isEqualTo("not_found");
    }
    HttpHeaders h = new HttpHeaders();
    h.set("X-Mock-Admin-Token", "any-token-any-token-any-token");
    ResponseEntity<String> approve = rest.exchange("/mock-admin/deid-requests/PYC-2026-000001/approve", HttpMethod.POST,
        new HttpEntity<>(h), String.class);
    assertThat(approve.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  void upload_is404WhenDisabled() {
    MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
    parts.add("file", new ByteArrayResource("{}".getBytes(UTF_8)) {
      @Override
      public String getFilename() {
        return "phieu.json";
      }
    });
    HttpHeaders h = new HttpHeaders();
    h.setContentType(MediaType.MULTIPART_FORM_DATA);
    h.set("X-Mock-Portal-Token", "any-token-any-token-any-token");

    ResponseEntity<String> response = rest.exchange("/mock-portal/deid-requests", HttpMethod.POST,
        new HttpEntity<>(parts, h), String.class);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }
}
