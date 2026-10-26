package vai.lakehouse.dakmock.deid;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import vai.lakehouse.dakmock.config.DeidProperties.Airflow;

/**
 * Gửi job khử định danh bằng cách trigger một dag run của Airflow 2.x qua REST {@code /api/v1} (docs/dak-deid-flow/
 * MOCK_IMPLEMENTATION_PLAN.md mục 4.7). {@code dagId} lấy từ phiếu (người dùng chọn) và được kiểm lại với allowlist ngay trước khi
 * gọi. {@code conf} chỉ chứa tham số bảng và TÊN Secret chứa key, không bao giờ chứa key (Airflow lưu {@code conf} trong DB và hiện
 * trên giao diện). {@code dag_run_id} là tên job suy ra từ mã phiếu nên gửi lần hai là {@code 409} và được coi là đã gửi.
 *
 * <p>Lỗi quy về thông điệp cố định: {@value #NOT_ALLOWED}, {@value #BAD_PARAMS}, {@value #NOT_FOUND}, {@value #DENIED} và
 * {@value AirflowHttp#UNAVAILABLE}; nội dung response của Airflow không bao giờ được đưa vào.
 */
public class AirflowJobSubmitter implements JobSubmitter {

  static final String NOT_ALLOWED = "airflow dag not allowed";
  static final String BAD_PARAMS = "invalid job parameters";
  static final String NOT_FOUND = "airflow dag not found";
  static final String DENIED = AirflowTokens.DENIED;

  private static final Pattern DAG_ID = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9_.-]{0,249}");
  private static final Pattern RUN_ID = Pattern.compile("deid-\\d{4}-\\d{6}");

  private final Airflow config;
  private final AirflowTokens tokens;
  private final AirflowHttp http = new AirflowHttp();
  private final String runsBase;

  AirflowJobSubmitter(Airflow config, AirflowTokens tokens) {
    this.config = config;
    this.tokens = tokens;
    this.runsBase = config.baseUrl().replaceAll("/+$", "") + "/api/v1/dags/";
  }

  @Override
  public JobRef submit(DeidRequest request, String keySecretName) {
    String dagId = request.dagId();
    if (dagId == null || !config.allowedDagIds().contains(dagId)) {
      throw new JobSubmitException(NOT_ALLOWED);
    }
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("dag_run_id", request.jobName());
    body.put("conf", parameters(request, keySecretName));
    AirflowHttp.Result result = call(HttpMethod.POST, URI.create(runsBase + dagId + "/dagRuns"), body);
    int status = result.status();
    if (status >= 200 && status < 300 || status == 409) {
      return new JobRef(JobRef.Kind.AIRFLOW_DAG_RUN, request.jobName());
    }
    throw new JobSubmitException(status == 404 ? NOT_FOUND : failure(status));
  }

  @Override
  public JobState status(DeidRequest request, JobRef ref) {
    String dagId = request.dagId();
    if (dagId == null || !DAG_ID.matcher(dagId).matches() || !RUN_ID.matcher(ref.name()).matches()) {
      return JobState.UNKNOWN;
    }
    AirflowHttp.Result result = call(HttpMethod.GET, URI.create(runsBase + dagId + "/dagRuns/" + ref.name()), null);
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

  private static Map<String, String> parameters(DeidRequest request, String keySecretName) {
    try {
      return ManifestRenderer.jobParameters(request, keySecretName);
    } catch (IllegalArgumentException e) {
      throw new JobSubmitException(BAD_PARAMS);
    }
  }

  /** Gọi Airflow; gặp 401 thì bỏ token đang giữ, lấy token mới và thử lại đúng một lần. */
  private AirflowHttp.Result call(HttpMethod method, URI uri, Object body) {
    AirflowHttp.Result result = send(method, uri, body);
    if (result.status() == 401) {
      tokens.invalidate();
      result = send(method, uri, body);
    }
    return result;
  }

  private AirflowHttp.Result send(HttpMethod method, URI uri, Object body) {
    String token = tokens.token();
    return http.exchange(method, uri, h -> h.setBearerAuth(token), MediaType.APPLICATION_JSON, body);
  }

  private static String failure(int status) {
    return status == 401 || status == 403 ? DENIED : AirflowHttp.UNAVAILABLE;
  }
}
