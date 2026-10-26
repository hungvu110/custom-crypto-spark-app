package vai.lakehouse.dakmock.deid;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import vai.lakehouse.dakmock.config.DeidProperties.Airflow;
import vai.lakehouse.dakmock.config.DeidProperties.AirflowAuth;

/**
 * Xác thực với Airflow. Với {@code basic}: HTTP Basic bằng user Airflow, không có token. Phần còn lại: cấp access token để gọi Airflow (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 4.7). {@code client-credentials}: lấy token
 * Keycloak bằng {@code client_credentials} (client xác thực bằng HTTP Basic), giữ trong bộ nhớ đến {@code expires_in − 30s} rồi lấy
 * lại. {@code bearer}: trả nguyên token tĩnh đã cấu hình. Lỗi quy về thông điệp cố định; token và client secret không bao giờ vào
 * log hay thông điệp lỗi.
 */
class AirflowTokens {

  static final String DENIED = "airflow denied the request";

  private static final long EARLY_REFRESH_SECONDS = 30;
  private static final long DEFAULT_LIFETIME_SECONDS = 60;

  private final Airflow config;
  private final Clock clock;
  private final AirflowHttp http = new AirflowHttp();
  private String cached;
  private Instant validUntil = Instant.MIN;

  AirflowTokens(Airflow config, Clock clock) {
    this.config = config;
    this.clock = clock;
  }

  /** Gắn thông tin xác thực vào request: Basic với user Airflow, hoặc Bearer với token còn hạn. */
  void authorize(HttpHeaders headers) {
    if (config.auth() == AirflowAuth.BASIC) {
      headers.setBasicAuth(config.username(), config.password(), StandardCharsets.UTF_8);
    } else {
      headers.setBearerAuth(token());
    }
  }

  /** {@code true} nếu 401 có thể chữa bằng cách lấy token mới (chỉ {@code client-credentials}). */
  boolean refreshable() {
    return config.auth() == AirflowAuth.CLIENT_CREDENTIALS;
  }

  /** Token còn hạn; lấy mới nếu chưa có hoặc sắp hết hạn. */
  synchronized String token() {
    if (config.auth() == AirflowAuth.BEARER) {
      return config.token();
    }
    if (cached == null || !clock.instant().isBefore(validUntil)) {
      fetch();
    }
    return cached;
  }

  /** Bỏ token đang giữ (Airflow vừa trả 401): lần {@link #token()} sau lấy token mới. */
  synchronized void invalidate() {
    cached = null;
  }

  private void fetch() {
    String form = "grant_type=client_credentials";
    if (config.scope() != null && !config.scope().isBlank()) {
      form += "&scope=" + URLEncoder.encode(config.scope(), StandardCharsets.UTF_8);
    }
    AirflowHttp.Result r = http.exchange(HttpMethod.POST, URI.create(config.tokenUrl()),
        h -> h.setBasicAuth(config.clientId(), config.clientSecret(), StandardCharsets.UTF_8),
        MediaType.APPLICATION_FORM_URLENCODED, form);
    if (r.status() == 400 || r.status() == 401 || r.status() == 403) {
      throw new JobSubmitException(DENIED);
    }
    JsonNode token = r.body() == null ? null : r.body().get("access_token");
    if (r.status() < 200 || r.status() >= 300 || token == null || !token.isTextual() || token.asText().isEmpty()) {
      throw new JobSubmitException(AirflowHttp.UNAVAILABLE);
    }
    long lifetime = r.body().path("expires_in").asLong(DEFAULT_LIFETIME_SECONDS);
    cached = token.asText();
    validUntil = clock.instant().plusSeconds(Math.max(0, lifetime - EARLY_REFRESH_SECONDS));
  }
}
