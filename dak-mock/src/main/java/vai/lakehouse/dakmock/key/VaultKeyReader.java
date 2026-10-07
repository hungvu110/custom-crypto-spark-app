package vai.lakehouse.dakmock.key;

import java.net.URI;
import java.net.http.HttpClient;

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
 * Mock đọc version mới nhất và trả version đó về ({@code data.metadata.version}).
 */
@Component
public class VaultKeyReader {

  /** keyPrefix và version KV đã đọc. */
  public record KeyMaterial(String keyPrefix, long version) {}

  /** Vault không có secret cho bảng này. */
  public static final class KeyNotFoundException extends RuntimeException {
    KeyNotFoundException() {
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
    URI uri = URI.create(trimSlashes(vault.addr()) + "/v1/" + trimSlashes(vault.kvMount()) + "/data/" + trimSlashes(path));
    try {
      return client.get().uri(uri)
          .header("X-Vault-Token", vault.token())
          .accept(MediaType.APPLICATION_JSON)
          .exchange((request, response) -> {
            int status = response.getStatusCode().value();
            if (status == 404) {
              throw new KeyNotFoundException();
            }
            if (status == 401 || status == 403) {
              throw DakApiException.upstream("Vault rejected the DAK token (HTTP " + status + ")");
            }
            if (!response.getStatusCode().is2xxSuccessful()) {
              throw DakApiException.upstream("Vault returned HTTP " + status);
            }
            return toKeyMaterial(response.bodyTo(JsonNode.class), path);
          });
    } catch (ResourceAccessException e) {
      throw DakApiException.upstream("Vault unreachable: " + e.getClass().getSimpleName());
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
