package vai.lakehouse.dakmock.deid;

import java.net.URI;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import vai.lakehouse.dakmock.config.DeidProperties.Airflow;

/**
 * Trigger và đọc trạng thái một dag run của Airflow 2.x qua REST {@code /api/v1}, dùng chung cấu hình {@code dak.deid.airflow} (địa
 * chỉ, cách xác thực). Dùng cho phiếu chuẩn hoá silver; luồng khử định danh vẫn đi qua {@link AirflowJobSubmitter}.
 *
 * <p>Gửi lần hai cùng {@code dag_run_id} ({@code 409}) được coi là đã gửi. Lỗi quy về thông điệp cố định ({@link #NOT_FOUND},
 * {@link #DENIED}, {@link #UNAVAILABLE}); nội dung response của Airflow không bao giờ được đưa vào.
 */
public class AirflowDagRuns {

  public static final String NOT_FOUND = "airflow dag not found";
  public static final String DENIED = AirflowTokens.DENIED;
  public static final String UNAVAILABLE = AirflowHttp.UNAVAILABLE;

  private static final Pattern DAG_ID = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9_.-]{0,249}");
  private static final Pattern RUN_ID = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9_.-]{0,249}");

  private final AirflowTokens tokens;
  private final AirflowHttp http = new AirflowHttp();
  private final String runsBase;

  public AirflowDagRuns(Airflow config, Clock clock) {
    this.tokens = new AirflowTokens(config, clock);
    this.runsBase = config.baseUrl().replaceAll("/+$", "") + "/api/v1/dags/";
  }

  /** Trigger dag run {@code runId} của {@code dagId} với {@code conf}; lỗi là {@link JobSubmitException} thông điệp cố định. */
  public void trigger(String dagId, String runId, Map<String, Object> conf) {
    requireIds(dagId, runId);
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("dag_run_id", runId);
    body.put("conf", conf);
    int status = call(HttpMethod.POST, URI.create(runsBase + dagId + "/dagRuns"), body).status();
    if (status >= 200 && status < 300 || status == 409) {
      return;
    }
    throw new JobSubmitException(status == 404 ? NOT_FOUND : failure(status));
  }

  /** Trạng thái dag run: {@code success} → SUCCEEDED, {@code failed} → FAILED, khác → RUNNING; không tìm thấy → UNKNOWN. */
  public JobState state(String dagId, String runId) {
    requireIds(dagId, runId);
    AirflowHttp.Result result = call(HttpMethod.GET, URI.create(runsBase + dagId + "/dagRuns/" + runId), null);
    int status = result.status();
    if (status == 404) {
      return JobState.UNKNOWN;
    }
    if (status < 200 || status >= 300 || result.body() == null || !result.body().path("state").isTextual()) {
      throw new JobSubmitException(failure(status));
    }
    return switch (result.body().path("state").asText()) {
      case "success" -> JobState.SUCCEEDED;
      case "failed" -> JobState.FAILED;
      default -> JobState.RUNNING;
    };
  }

  private static void requireIds(String dagId, String runId) {
    if (dagId == null || !DAG_ID.matcher(dagId).matches() || runId == null || !RUN_ID.matcher(runId).matches()) {
      throw new IllegalArgumentException("invalid dag id or dag run id");
    }
  }

  /** Gặp 401 thì bỏ token đang giữ, lấy token mới và thử lại đúng một lần. */
  private AirflowHttp.Result call(HttpMethod method, URI uri, Object body) {
    AirflowHttp.Result result = send(method, uri, body);
    if (result.status() == 401 && tokens.refreshable()) {
      tokens.invalidate();
      result = send(method, uri, body);
    }
    return result;
  }

  private AirflowHttp.Result send(HttpMethod method, URI uri, Object body) {
    return http.exchange(method, uri, tokens::authorize, MediaType.APPLICATION_JSON, body);
  }

  private static String failure(int status) {
    return status == 401 || status == 403 ? DENIED : UNAVAILABLE;
  }
}
