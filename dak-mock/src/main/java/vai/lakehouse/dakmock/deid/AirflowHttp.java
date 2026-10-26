package vai.lakehouse.dakmock.deid;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Lời gọi HTTP dùng chung của phía Airflow (lấy token Keycloak và gọi REST API của Airflow): có timeout, trả status kèm body JSON,
 * và mọi lỗi mạng hoặc TLS ném {@link JobSubmitException} với thông điệp cố định. Không log hay đưa URL, header, body vào lỗi, vì
 * chúng chứa token và client secret.
 */
final class AirflowHttp {

  static final String UNAVAILABLE = "airflow unavailable";

  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);
  private static final ObjectMapper JSON = new ObjectMapper();

  /** Status HTTP và body JSON (null nếu rỗng hoặc không phải JSON). */
  record Result(int status, JsonNode body) {}

  private final RestClient client;

  AirflowHttp() {
    JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(
        HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build());
    factory.setReadTimeout(READ_TIMEOUT);
    this.client = RestClient.builder().requestFactory(factory).build();
  }

  Result exchange(HttpMethod method, URI uri, Consumer<HttpHeaders> headers, MediaType bodyType, Object body) {
    try {
      RestClient.RequestBodySpec spec = client.method(method).uri(uri).headers(headers).accept(MediaType.APPLICATION_JSON);
      if (body != null) {
        spec = spec.contentType(bodyType).body(body);
      }
      return spec.exchange((request, response) -> {
        int status = response.getStatusCode().value();
        try {
          String text = response.bodyTo(String.class);
          return new Result(status, text == null || text.isBlank() ? null : JSON.readTree(text));
        } catch (IOException | RuntimeException e) {
          return new Result(status, null);
        }
      });
    } catch (RestClientException e) {
      throw new JobSubmitException(UNAVAILABLE);
    }
  }
}
