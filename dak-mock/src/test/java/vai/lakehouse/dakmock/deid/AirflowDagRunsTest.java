package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.config.DeidProperties.Airflow;
import vai.lakehouse.dakmock.config.DeidProperties.AirflowAuth;

class AirflowDagRunsTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private MockWebServer server;

  @BeforeEach
  void setUp() throws IOException {
    server = new MockWebServer();
    server.start();
  }

  @AfterEach
  void tearDown() throws IOException {
    server.shutdown();
  }

  private AirflowDagRuns runs() {
    Airflow cfg = new Airflow(server.url("/airflow").toString(), List.of(), AirflowAuth.BASIC, null, null, null, null, null,
        "dak-trigger", "pw");
    return new AirflowDagRuns(cfg, Clock.systemUTC());
  }

  private static MockResponse json(int status, String body) {
    return new MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(body);
  }

  @Test
  void triggersTheDagRunWithBasicAuthAndConf() throws Exception {
    server.enqueue(json(200, "{\"state\":\"queued\"}"));

    runs().trigger("silver_ingest_vtnet", "slv-2026-000001", Map.of("path", "vtnet_cdr/20261027"));

    RecordedRequest req = server.takeRequest();
    assertThat(req.getMethod()).isEqualTo("POST");
    assertThat(req.getPath()).isEqualTo("/airflow/api/v1/dags/silver_ingest_vtnet/dagRuns");
    assertThat(req.getHeader("Authorization")).startsWith("Basic ");
    JsonNode body = JSON.readTree(req.getBody().readUtf8());
    assertThat(body.path("dag_run_id").asText()).isEqualTo("slv-2026-000001");
    assertThat(body.path("conf").path("path").asText()).isEqualTo("vtnet_cdr/20261027");
  }

  @Test
  void aSecondTriggerOfTheSameRunIsFineAndFailuresUseFixedMessages() {
    server.enqueue(json(409, "{}"));
    runs().trigger("d", "r", Map.of());

    server.enqueue(json(404, "{\"detail\":\"secret\"}"));
    assertThatThrownBy(() -> runs().trigger("d", "r", Map.of())).hasMessage(AirflowDagRuns.NOT_FOUND);
    server.enqueue(json(403, "{}"));
    assertThatThrownBy(() -> runs().trigger("d", "r", Map.of())).hasMessage(AirflowDagRuns.DENIED);
    server.enqueue(json(500, "{\"detail\":\"secret\"}"));
    assertThatThrownBy(() -> runs().trigger("d", "r", Map.of())).hasMessage(AirflowDagRuns.UNAVAILABLE);
  }

  @Test
  void mapsTheDagRunState() {
    server.enqueue(json(200, "{\"state\":\"success\"}"));
    server.enqueue(json(200, "{\"state\":\"failed\"}"));
    server.enqueue(json(200, "{\"state\":\"running\"}"));
    server.enqueue(json(404, "{}"));

    assertThat(runs().state("d", "r")).isEqualTo(JobState.SUCCEEDED);
    assertThat(runs().state("d", "r")).isEqualTo(JobState.FAILED);
    assertThat(runs().state("d", "r")).isEqualTo(JobState.RUNNING);
    assertThat(runs().state("d", "r")).isEqualTo(JobState.UNKNOWN);
  }

  @Test
  void rejectsUnsafeIdsBeforeCallingAirflow() {
    assertThatThrownBy(() -> runs().trigger("../x", "r", Map.of())).isInstanceOf(IllegalArgumentException.class);
    assertThat(server.getRequestCount()).isZero();
  }
}
