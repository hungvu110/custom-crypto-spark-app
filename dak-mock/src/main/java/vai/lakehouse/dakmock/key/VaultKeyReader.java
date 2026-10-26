package vai.lakehouse.dakmock.key;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Đọc keyPrefix từ Vault KV v2 bằng token tĩnh — cùng cách luồng column/cdr crypto cũ đang đọc:
 * {@code GET {addr}/v1/{kvMount}/data/{path}}, header {@code X-Vault-Token}, field {@code keyField}.
 * Mock đọc version mới nhất và trả version đó về ({@code data.metadata.version}). Key cột được TẠO bằng
 * {@link #createIfAbsent} ({@code cas=0}), không bao giờ ghi đè.
 */
@Component
public class VaultKeyReader {

  /** keyPrefix và version KV đã đọc. */
  public record KeyMaterial(String keyPrefix, long version) {}

  /** Vault không có secret cho bảng này. */
  public static final class KeyNotFoundException extends RuntimeException {
    public KeyNotFoundException() {
      super(null, null, false, false);
    }
  }

  private final DakProperties.Vault vault;
  private final RestClient client;

  public VaultKeyReader(DakProperties props, RestClient.Builder builder) {
    this.vault = props.vault();
    JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(
        HttpClient.newBuilder().connectTimeout(vault.connectTimeout()).build());
    factory.setReadTimeout(vault.readTimeout());
    this.client = builder.requestFactory(factory).build();
  }

  public KeyMaterial read(String tenant, String workspace, String database, String table) {
    String path = vault.pathTemplate()
        .replace("{tenant}", tenant)
        .replace("{workspace}", workspace)
        .replace("{database}", database)
        .replace("{table}", table);
    // Mọi thành phần đã được kiểm tra bộ ký tự (database/table ở controller, tenant/workspace ở cấu hình).
    return readAt(path).orElseThrow(KeyNotFoundException::new);
  }

  /** Đọc secret ở {@code path} (sau {@code {kvMount}/data/}); rỗng nếu Vault trả 404. */
  public Optional<KeyMaterial> readAt(String path) {
    try {
      return client.get().uri(dataUri(path))
          .header("X-Vault-Token", vault.token())
          .accept(MediaType.APPLICATION_JSON)
          .exchange((request, response) -> {
            int status = response.getStatusCode().value();
            if (status == 404) {
              return Optional.<KeyMaterial>empty();
            }
            failOnError(status, response.getStatusCode().is2xxSuccessful());
            return Optional.of(toKeyMaterial(response.bodyTo(JsonNode.class), path));
          });
    } catch (ResourceAccessException e) {
      throw DakApiException.upstream("Vault unreachable: " + e.getClass().getSimpleName());
    }
  }

  /**
   * Ghi {@code keyPrefix} vào {@code path} với {@code cas=0}: Vault chỉ nhận khi path CHƯA có secret, nên một key đã có không
   * bao giờ bị ghi đè. Trả {@code false} nếu path đã có secret (không ghi gì).
   */
  public boolean createIfAbsent(String path, String keyPrefix) {
    Map<String, Object> body = Map.of("options", Map.of("cas", 0), "data", Map.of(vault.keyField(), keyPrefix));
    try {
      return client.post().uri(dataUri(path))
          .header("X-Vault-Token", vault.token())
          .contentType(MediaType.APPLICATION_JSON)
          .body(body)
          .exchange((request, response) -> {
            int status = response.getStatusCode().value();
            // KV v2 trả 400 "check-and-set parameter did not match the current version" khi path đã tồn tại.
            if (status == 400) {
              return false;
            }
            failOnError(status, response.getStatusCode().is2xxSuccessful());
            return true;
          });
    } catch (ResourceAccessException e) {
      throw DakApiException.upstream("Vault unreachable: " + e.getClass().getSimpleName());
    }
  }

  private URI dataUri(String path) {
    return URI.create(trimSlashes(vault.addr()) + "/v1/" + trimSlashes(vault.kvMount()) + "/data/" + trimSlashes(path));
  }

  private static void failOnError(int status, boolean success) {
    if (status == 401 || status == 403) {
      throw DakApiException.upstream("Vault rejected the DAK token (HTTP " + status + ")");
    }
    if (!success) {
      throw DakApiException.upstream("Vault returned HTTP " + status);
    }
  }

  private KeyMaterial toKeyMaterial(JsonNode body, String path) {
    JsonNode data = body == null ? null : body.path("data");
    JsonNode field = data == null ? null : data.path("data").path(vault.keyField());
    if (field == null || !field.isTextual() || field.asText().isEmpty()) {
      // Lỗi dữ liệu phía Vault: không trả chuỗi rỗng cho bên gọi. Không log giá trị các field.
      throw DakApiException.internal("Vault secret " + path + " has no non-empty string field " + vault.keyField());
    }
    return new KeyMaterial(field.asText(), data.path("metadata").path("version").asLong(0));
  }

  private static String trimSlashes(String s) {
    return s.replaceAll("^/+|/+$", "");
  }
}
