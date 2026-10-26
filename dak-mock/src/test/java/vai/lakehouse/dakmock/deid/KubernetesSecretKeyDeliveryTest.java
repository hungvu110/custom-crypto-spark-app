package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.NewRequest;
import vai.lakehouse.dakmock.deid.DeidRequest.SourceFile;
import vai.lakehouse.dakmock.deid.KeyDelivery.KeyHandle;
import vai.lakehouse.dakmock.key.VaultKeyReader;
import vai.lakehouse.dakmock.key.VaultKeyReader.KeyMaterial;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * D6b (plan mục 4.6b): đọc key bảng từ Vault rồi tạo Secret {@code deid-key-<mã>} qua Kubernetes API; job chỉ nhận TÊN Secret. Mọi
 * lỗi quy về đúng một trong 5 thông điệp cố định và không thông điệp nào, log nào chứa key.
 */
@ExtendWith(MockitoExtension.class)
class KubernetesSecretKeyDeliveryTest {

  private static final String NS = "team-ns";
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String KEY = Base64.getEncoder().encodeToString(filled(32));
  private static final Instant T0 = Instant.parse("2026-10-23T08:00:00Z");

  @Mock
  private VaultKeyReader vault;
  private MockWebServer server;
  @TempDir
  Path dir;
  private KubernetesSecretKeyDelivery delivery;

  private static byte[] filled(int n) {
    byte[] b = new byte[n];
    Arrays.fill(b, (byte) 7);
    return b;
  }

  @BeforeEach
  void setUp() throws IOException {
    server = new MockWebServer();
    server.start();
    Path token = Files.writeString(dir.resolve("token"), "sa-token");
    KubernetesApiClient client = new KubernetesApiClient(server.url("/").toString(), token, null);
    DeidProperties deid = new DeidProperties(true, null, 20, 65536, null, null, null, null, null,
        List.of(new PortalUser("u1", "team-a", "ten", "ws", "t".repeat(30)),
            new PortalUser("u2", "team-b", null, null, "u".repeat(30))));
    delivery = new KubernetesSecretKeyDelivery(vault, client, NS, deid);
  }

  @AfterEach
  void tearDown() throws IOException {
    server.shutdown();
  }

  private static NewRequest newRequest(String team) {
    return new NewRequest(team, "u1", "demo_db", "users_cdr", List.of(new ColumnSpec("msisdn", Mode.HASH)), "dag",
        "purpose", new SourceFile("f.json", "ab".repeat(32)));
  }

  private static DeidRequest request(String team) {
    return DeidRequest.created("PYC-2026-000007", T0, newRequest(team))
        .approved("admin", Instant.parse("2026-10-23T09:00:00Z"), "out_db", "users_cdr_deid_20261023_7");
  }

  private void vaultReturns(String keyPrefix) {
    when(vault.read("ten", "ws", "demo_db", "users_cdr")).thenReturn(new KeyMaterial(keyPrefix, 3));
  }

  @Test
  @DisplayName("deliver creates the Secret with the key file and returns only its name and version")
  void deliver_ok_createsSecret() throws Exception {
    vaultReturns(KEY);
    server.enqueue(new MockResponse().setResponseCode(201));

    KeyHandle handle = delivery.deliver(request("team-a"));

    RecordedRequest req = server.takeRequest();
    assertThat(handle).isEqualTo(new KeyHandle("deid-key-2026-000007", 3L));
    assertThat(req.getMethod()).isEqualTo("POST");
    assertThat(req.getPath()).isEqualTo("/api/v1/namespaces/team-ns/secrets");
    JsonNode body = JSON.readTree(req.getBody().readUtf8());
    assertThat(body.path("apiVersion").asText()).isEqualTo("v1");
    assertThat(body.path("kind").asText()).isEqualTo("Secret");
    assertThat(body.path("type").asText()).isEqualTo("Opaque");
    assertThat(body.path("metadata").path("name").asText()).isEqualTo("deid-key-2026-000007");
    assertThat(body.path("metadata").path("labels").path("dak.vai/request-code").asText()).isEqualTo("PYC-2026-000007");
    assertThat(body.path("metadata").path("labels").path("dak.vai/kind").asText()).isEqualTo("deid-key");
    assertThat(body.path("data").size()).isEqualTo(1);
    String stored = new String(Base64.getDecoder().decode(body.path("data").path("pseudonym.hmac_v1").asText()),
        StandardCharsets.UTF_8);
    assertThat(stored).isEqualTo("base64:" + KEY);
  }

  @Test
  @DisplayName("deliver uses empty tenant and workspace when the team's account has none")
  void deliver_teamWithoutTenant_usesEmpty() {
    when(vault.read("", "", "demo_db", "users_cdr")).thenReturn(new KeyMaterial(KEY, 1));
    server.enqueue(new MockResponse().setResponseCode(201));

    assertThat(delivery.deliver(request("team-b")).keyVersion()).isEqualTo(1L);
  }

  @Test
  @DisplayName("deliver uses empty tenant and workspace for a team with no account at all")
  void deliver_unknownTeam_usesEmpty() {
    when(vault.read("", "", "demo_db", "users_cdr")).thenReturn(new KeyMaterial(KEY, 1));
    server.enqueue(new MockResponse().setResponseCode(201));

    assertThat(delivery.deliver(request("team-zzz")).secretName()).isEqualTo("deid-key-2026-000007");
  }

  @Test
  @DisplayName("deliver reports table key not found and calls nothing else when Vault has no key")
  void deliver_vaultNotFound_fixedMessage() {
    when(vault.read(anyString(), anyString(), anyString(), anyString()))
        .thenThrow(new VaultKeyReader.KeyNotFoundException());

    assertThatThrownBy(() -> delivery.deliver(request("team-a"))).isInstanceOf(KeyDeliveryException.class)
        .hasMessage("table key not found");
    assertThat(server.getRequestCount()).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = {"not base64!!", "", "AAAA", "QUJD", "-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_",
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="})
  @DisplayName("deliver rejects a key that is not standard base64 of exactly 32 bytes")
  void deliver_badFormat_fixedMessage(String keyPrefix) {
    vaultReturns(keyPrefix);

    assertThatThrownBy(() -> delivery.deliver(request("team-a"))).isInstanceOf(KeyDeliveryException.class)
        .hasMessage("table key has unsupported format");
    assertThat(server.getRequestCount()).isZero();
  }

  @Test
  @DisplayName("deliver reports vault unavailable for upstream and internal Vault errors")
  void deliver_vaultErrors_fixedMessage() {
    when(vault.read(anyString(), anyString(), anyString(), anyString()))
        .thenThrow(DakApiException.upstream("Vault returned HTTP 503 secret-detail"))
        .thenThrow(DakApiException.internal("Vault secret x has no field"));

    for (int i = 0; i < 2; i++) {
      assertThatThrownBy(() -> delivery.deliver(request("team-a"))).isInstanceOf(KeyDeliveryException.class)
          .hasMessage("vault unavailable");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {401, 403})
  @DisplayName("deliver reports kubernetes denied the request on 401 and 403")
  void deliver_forbidden_fixedMessage(int status) {
    vaultReturns(KEY);
    server.enqueue(new MockResponse().setResponseCode(status).setBody("{\"message\":\"" + KEY + "\"}"));

    assertThatThrownBy(() -> delivery.deliver(request("team-a"))).isInstanceOf(KeyDeliveryException.class)
        .hasMessage("kubernetes denied the request");
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 404, 422, 500, 503})
  @DisplayName("deliver reports kubernetes unavailable on any other failure status")
  void deliver_otherStatus_fixedMessage(int status) {
    vaultReturns(KEY);
    server.enqueue(new MockResponse().setResponseCode(status));

    assertThatThrownBy(() -> delivery.deliver(request("team-a"))).isInstanceOf(KeyDeliveryException.class)
        .hasMessage("kubernetes unavailable");
  }

  @Test
  @DisplayName("deliver reports kubernetes unavailable when the API server is down")
  void deliver_serverDown_fixedMessage() throws Exception {
    vaultReturns(KEY);
    server.shutdown();

    assertThatThrownBy(() -> delivery.deliver(request("team-a"))).isInstanceOf(KeyDeliveryException.class)
        .hasMessage("kubernetes unavailable");
  }

  @Test
  @DisplayName("deliver replaces a leftover Secret on 409: DELETE then POST exactly once")
  void deliver_conflict_deletesThenCreatesOnce() throws Exception {
    vaultReturns(KEY);
    server.enqueue(new MockResponse().setResponseCode(409));
    server.enqueue(new MockResponse().setResponseCode(200));
    server.enqueue(new MockResponse().setResponseCode(201));

    KeyHandle handle = delivery.deliver(request("team-a"));

    assertThat(handle.secretName()).isEqualTo("deid-key-2026-000007");
    assertThat(server.getRequestCount()).isEqualTo(3);
    assertThat(server.takeRequest().getMethod()).isEqualTo("POST");
    RecordedRequest delete = server.takeRequest();
    assertThat(delete.getMethod()).isEqualTo("DELETE");
    assertThat(delete.getPath()).isEqualTo("/api/v1/namespaces/team-ns/secrets/deid-key-2026-000007");
    assertThat(server.takeRequest().getMethod()).isEqualTo("POST");
  }

  @Test
  @DisplayName("deliver gives up after a second 409 and does not loop")
  void deliver_conflictTwice_fails() {
    vaultReturns(KEY);
    server.enqueue(new MockResponse().setResponseCode(409));
    server.enqueue(new MockResponse().setResponseCode(200));
    server.enqueue(new MockResponse().setResponseCode(409));

    assertThatThrownBy(() -> delivery.deliver(request("team-a"))).isInstanceOf(KeyDeliveryException.class)
        .hasMessage("kubernetes unavailable");
    assertThat(server.getRequestCount()).isEqualTo(3);
  }

  @Test
  @DisplayName("deliver fails when deleting the leftover Secret is refused")
  void deliver_conflictDeleteDenied_fails() {
    vaultReturns(KEY);
    server.enqueue(new MockResponse().setResponseCode(409));
    server.enqueue(new MockResponse().setResponseCode(403));

    assertThatThrownBy(() -> delivery.deliver(request("team-a"))).isInstanceOf(KeyDeliveryException.class)
        .hasMessage("kubernetes denied the request");
    assertThat(server.getRequestCount()).isEqualTo(2);
  }

  @Test
  @DisplayName("no exception message or KeyHandle text ever contains the key")
  void failures_neverLeakKey() {
    vaultReturns(KEY);
    server.enqueue(new MockResponse().setResponseCode(500).setBody(KEY));

    Throwable t = catchThrowable(() -> delivery.deliver(request("team-a")));

    assertThat(String.valueOf(t.getMessage())).doesNotContain(KEY);
    assertThat(t.getCause()).isNull();
    assertThat(new KeyHandle("deid-key-2026-000007", 3L).toString()).doesNotContain(KEY);
  }

  @Test
  @DisplayName("release deletes the Secret and treats 404 as already gone")
  void release_deletes_notFoundOk() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200));
    server.enqueue(new MockResponse().setResponseCode(404));

    delivery.release(request("team-a"));
    delivery.release(request("team-a"));

    RecordedRequest first = server.takeRequest();
    assertThat(first.getMethod()).isEqualTo("DELETE");
    assertThat(first.getPath()).isEqualTo("/api/v1/namespaces/team-ns/secrets/deid-key-2026-000007");
    verify(vault, never()).read(anyString(), anyString(), anyString(), anyString());
  }

  @Test
  @DisplayName("release reports a refused or failed delete with a fixed message")
  void release_failure_fixedMessage() {
    server.enqueue(new MockResponse().setResponseCode(403));
    server.enqueue(new MockResponse().setResponseCode(500));

    assertThatThrownBy(() -> delivery.release(request("team-a"))).isInstanceOf(KeyDeliveryException.class)
        .hasMessage("kubernetes denied the request");
    assertThatThrownBy(() -> delivery.release(request("team-a"))).isInstanceOf(KeyDeliveryException.class)
        .hasMessage("kubernetes unavailable");
  }

  @Test
  @DisplayName("release never deletes a Secret whose name is not deid-key-<year>-<6 digits>")
  void release_foreignName_noCall() {
    DeidRequest weird = DeidRequest.created("PYC-../../kube-system", T0, newRequest("team-a"));

    delivery.release(weird);

    assertThat(server.getRequestCount()).isZero();
  }

  @Test
  @DisplayName("deliver refuses a request whose Secret name is not deid-key-<year>-<6 digits>")
  void deliver_foreignName_noCall() {
    DeidRequest weird = DeidRequest.created("PYC-../../kube-system", T0, newRequest("team-a"));

    assertThatThrownBy(() -> delivery.deliver(weird)).isInstanceOf(KeyDeliveryException.class);
    assertThat(server.getRequestCount()).isZero();
  }
}
