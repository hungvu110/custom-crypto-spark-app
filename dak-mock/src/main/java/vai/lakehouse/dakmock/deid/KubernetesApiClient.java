package vai.lakehouse.dakmock.deid;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Client mỏng gọi Kubernetes API bằng ServiceAccount của pod. Token được đọc lại ở MỖI lời gọi (Kubernetes xoay token của
 * ServiceAccount); CA của cluster được nạp vào SSLContext riêng, không đụng truststore chung của JVM (truststore đó đã được dựng
 * thêm CA Keycloak). Status lỗi (4xx/5xx) được trả về cho người gọi quyết định; chỉ lỗi mạng, TLS hay thiếu token mới ném
 * {@link UnavailableException} với thông điệp cố định. Không bao giờ log hay ném URL, token hoặc nội dung response, vì response của
 * Kubernetes có thể chứa dữ liệu Secret.
 */
public class KubernetesApiClient {

  public static final Path SERVICE_ACCOUNT_TOKEN = Path.of("/var/run/secrets/kubernetes.io/serviceaccount/token");
  public static final Path SERVICE_ACCOUNT_CA = Path.of("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt");

  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);
  /** Chỉ các đường dẫn ghép từ tên đã kiểm: không khoảng trắng, query, fragment hay dấu chấm chấm. */
  private static final Pattern SAFE_PATH = Pattern.compile("/[A-Za-z0-9._~/-]*");

  /** Kết quả một lời gọi: status HTTP và body JSON nếu có (null nếu body rỗng hoặc không phải JSON). */
  public record Response(int status, JsonNode body) {}

  /** Không gọi được Kubernetes (mạng, TLS, thiếu token). Thông điệp luôn là {@value #MESSAGE}. */
  public static final class UnavailableException extends RuntimeException {
    static final String MESSAGE = "kubernetes unavailable";

    UnavailableException(Throwable cause) {
      super(MESSAGE, cause, false, false);
    }
  }

  private static final ObjectMapper JSON = new ObjectMapper();

  private final String apiServer;
  private final Path tokenFile;
  private final RestClient client;

  /** {@code caFile} null thì dùng truststore mặc định của JVM (chỉ hợp lý cho http hoặc test). */
  public KubernetesApiClient(String apiServer, Path tokenFile, Path caFile) {
    this.apiServer = apiServer.replaceAll("/+$", "");
    this.tokenFile = tokenFile;
    HttpClient.Builder http = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT);
    if (caFile != null) {
      http.sslContext(sslContextTrusting(caFile));
    }
    JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http.build());
    factory.setReadTimeout(READ_TIMEOUT);
    this.client = RestClient.builder().requestFactory(factory).build();
  }

  /** Client dùng token và CA mặc định của ServiceAccount trong pod. */
  public static KubernetesApiClient inCluster(String apiServer) {
    return new KubernetesApiClient(apiServer, SERVICE_ACCOUNT_TOKEN,
        Files.isRegularFile(SERVICE_ACCOUNT_CA) ? SERVICE_ACCOUNT_CA : null);
  }

  /** Gọi {@code method path} với {@code body} (null = không body, ngược lại gửi JSON). */
  public Response exchange(HttpMethod method, String path, Object body) {
    if (path == null || !SAFE_PATH.matcher(path).matches() || path.contains("..") || path.contains("//")) {
      throw new IllegalArgumentException("invalid Kubernetes API path");
    }
    String token = readToken();
    try {
      RestClient.RequestBodySpec spec = client.method(method).uri(URI.create(apiServer + path))
          .header("Authorization", "Bearer " + token).accept(MediaType.APPLICATION_JSON);
      if (body != null) {
        spec = spec.contentType(MediaType.APPLICATION_JSON).body(body);
      }
      return spec.exchange((request, response) -> new Response(response.getStatusCode().value(), readBody(response)));
    } catch (RestClientException e) {
      throw new UnavailableException(e);
    }
  }

  private static JsonNode readBody(RestClient.RequestHeadersSpec.ConvertibleClientHttpResponse response) {
    try {
      // Đọc thành chuỗi rồi tự parse: Kubernetes luôn trả JSON nhưng không dựa vào header Content-Type.
      String text = response.bodyTo(String.class);
      return text == null || text.isBlank() ? null : JSON.readTree(text);
    } catch (IOException | RuntimeException e) {
      return null;
    }
  }

  private String readToken() {
    try {
      String token = Files.readString(tokenFile, StandardCharsets.UTF_8).strip();
      if (token.isEmpty()) {
        throw new UnavailableException(null);
      }
      return token;
    } catch (IOException e) {
      throw new UnavailableException(e);
    }
  }

  private static SSLContext sslContextTrusting(Path caFile) {
    try (InputStream in = Files.newInputStream(caFile)) {
      Collection<? extends Certificate> certs = CertificateFactory.getInstance("X.509").generateCertificates(in);
      if (certs.isEmpty()) {
        throw new GeneralSecurityException("no certificate");
      }
      KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
      store.load(null, null);
      int i = 0;
      for (Certificate cert : certs) {
        store.setCertificateEntry("ca-" + i++, cert);
      }
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(store);
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, tmf.getTrustManagers(), null);
      return context;
    } catch (IOException | GeneralSecurityException e) {
      throw new IllegalStateException("Invalid DAK mock configuration: the Kubernetes CA file of dak.deid.kubernetes "
          + "cannot be loaded", e);
    }
  }
}
