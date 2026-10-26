package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.tls.HandshakeCertificates;
import okhttp3.tls.HeldCertificate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpMethod;

/**
 * D6b: client gọi Kubernetes API bằng ServiceAccount token (đọc lại ở MỖI lời gọi, vì token được xoay) và CA của cluster riêng,
 * không dùng truststore chung của JVM. Không bao giờ ném thông điệp chứa URL, token hay nội dung response.
 */
class KubernetesApiClientTest {

  private MockWebServer server;
  @TempDir
  Path dir;
  private Path tokenFile;

  @BeforeEach
  void setUp() throws IOException {
    server = new MockWebServer();
    tokenFile = Files.writeString(dir.resolve("token"), "sa-token-1\n");
  }

  @AfterEach
  void tearDown() throws IOException {
    server.shutdown();
  }

  private KubernetesApiClient plain() throws IOException {
    server.start();
    return new KubernetesApiClient(server.url("/").toString(), tokenFile, null);
  }

  @Test
  @DisplayName("exchange sends method, path, Bearer token and JSON body, and returns the status")
  void exchange_post_sendsBearerAndBody() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(201).setBody("{\"kind\":\"Secret\"}"));
    KubernetesApiClient client = plain();

    KubernetesApiClient.Response r = client.exchange(HttpMethod.POST, "/api/v1/namespaces/ns/secrets",
        Map.of("kind", "Secret"));

    RecordedRequest req = server.takeRequest();
    assertThat(r.status()).isEqualTo(201);
    assertThat(req.getMethod()).isEqualTo("POST");
    assertThat(req.getPath()).isEqualTo("/api/v1/namespaces/ns/secrets");
    assertThat(req.getHeader("Authorization")).isEqualTo("Bearer sa-token-1");
    assertThat(req.getHeader("Content-Type")).startsWith("application/json");
    assertThat(req.getBody().readUtf8()).isEqualTo("{\"kind\":\"Secret\"}");
  }

  @Test
  @DisplayName("exchange re-reads the token file on every call")
  void exchange_tokenRotated_usesNewToken() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200));
    server.enqueue(new MockResponse().setResponseCode(200));
    KubernetesApiClient client = plain();

    client.exchange(HttpMethod.DELETE, "/api/v1/namespaces/ns/secrets/a", null);
    Files.writeString(tokenFile, "sa-token-2");
    client.exchange(HttpMethod.DELETE, "/api/v1/namespaces/ns/secrets/a", null);

    assertThat(server.takeRequest().getHeader("Authorization")).isEqualTo("Bearer sa-token-1");
    assertThat(server.takeRequest().getHeader("Authorization")).isEqualTo("Bearer sa-token-2");
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 404, 409, 422, 500, 503})
  @DisplayName("exchange returns an error status instead of throwing")
  void exchange_errorStatus_returned(int status) throws Exception {
    server.enqueue(new MockResponse().setResponseCode(status).setBody("{\"message\":\"secret-detail\"}"));

    assertThat(plain().exchange(HttpMethod.GET, "/api/v1/x", null).status()).isEqualTo(status);
  }

  @Test
  @DisplayName("exchange parses a JSON response body")
  void exchange_get_parsesBody() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"status\":{\"x\":1}}"));

    KubernetesApiClient.Response r = plain().exchange(HttpMethod.GET, "/api/v1/x", null);

    assertThat(r.body().path("status").path("x").asInt()).isEqualTo(1);
  }

  @Test
  @DisplayName("exchange tolerates a non-JSON body")
  void exchange_nonJsonBody_noBody() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200).setBody("not json"));

    KubernetesApiClient.Response r = plain().exchange(HttpMethod.GET, "/api/v1/x", null);

    assertThat(r.status()).isEqualTo(200);
    assertThat(r.body()).isNull();
  }

  @Test
  @DisplayName("exchange fails with a fixed message when the server is unreachable")
  void exchange_unreachable_fixedMessage() throws Exception {
    KubernetesApiClient client = plain();
    server.shutdown();

    assertThatThrownBy(() -> client.exchange(HttpMethod.GET, "/api/v1/x", null))
        .isInstanceOf(KubernetesApiClient.UnavailableException.class).hasMessage("kubernetes unavailable");
  }

  @Test
  @DisplayName("exchange fails with the same fixed message when the token file is missing")
  void exchange_noTokenFile_fixedMessage() throws Exception {
    server.start();
    KubernetesApiClient client = new KubernetesApiClient(server.url("/").toString(), dir.resolve("nope"), null);

    assertThatThrownBy(() -> client.exchange(HttpMethod.GET, "/api/v1/x", null))
        .isInstanceOf(KubernetesApiClient.UnavailableException.class).hasMessage("kubernetes unavailable");
    assertThat(server.getRequestCount()).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = {"api/v1/x", "/api/../x", "/a b", "/a?x=1", "/a#f", "/a\nb", "", "//evil.example/x"})
  @DisplayName("exchange rejects a path that could change the target")
  void exchange_badPath_rejected(String path) throws Exception {
    KubernetesApiClient client = plain();

    assertThatThrownBy(() -> client.exchange(HttpMethod.GET, path, null)).isInstanceOf(IllegalArgumentException.class);
    assertThat(server.getRequestCount()).isZero();
  }

  @Test
  @DisplayName("exchange trusts the cluster CA file and rejects a server signed by another CA")
  void exchange_tls_usesOwnCa() throws Exception {
    HeldCertificate ca = new HeldCertificate.Builder().certificateAuthority(0).commonName("cluster-ca").build();
    HeldCertificate serverCert = new HeldCertificate.Builder().commonName("localhost").addSubjectAlternativeName("localhost")
        .signedBy(ca).build();
    server.useHttps(new HandshakeCertificates.Builder().heldCertificate(serverCert, ca.certificate()).build()
        .sslSocketFactory(), false);
    server.start();
    server.enqueue(new MockResponse().setResponseCode(200));
    server.enqueue(new MockResponse().setResponseCode(200));
    String url = "https://localhost:" + server.getPort();
    Path caFile = Files.writeString(dir.resolve("ca.crt"), ca.certificatePem());
    HeldCertificate other = new HeldCertificate.Builder().certificateAuthority(0).commonName("other-ca").build();
    Path otherFile = Files.writeString(dir.resolve("other.crt"), other.certificatePem());

    assertThat(new KubernetesApiClient(url, tokenFile, caFile).exchange(HttpMethod.GET, "/api/v1/x", null).status())
        .isEqualTo(200);
    assertThatThrownBy(() -> new KubernetesApiClient(url, tokenFile, otherFile).exchange(HttpMethod.GET, "/api/v1/x", null))
        .isInstanceOf(KubernetesApiClient.UnavailableException.class).hasMessage("kubernetes unavailable");
  }

  @Test
  @DisplayName("an unreadable CA file fails at construction")
  void constructor_badCaFile_failsFast() throws Exception {
    Path bad = Files.writeString(dir.resolve("bad.crt"), "not a certificate");

    assertThatThrownBy(() -> new KubernetesApiClient("https://localhost:1", tokenFile, bad))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("dak.deid.kubernetes");
  }
}
