package vai.lakehouse.dakmock.silver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.deid.JobState;
import vai.lakehouse.dakmock.deid.JobSubmitException;
import vai.lakehouse.dakmock.web.DakApiException;

class SilverRequestServiceTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final PortalUser ALICE = new PortalUser("alice", "team-a", null, null, "t-a");
  private static final PortalUser CAROL = new PortalUser("carol", "team-b", null, null, "t-c");

  /** Trigger giả: ghi lại conf, lỗi theo yêu cầu, trạng thái dag run đặt tay. */
  private static final class FakeTrigger implements SilverDagTrigger {
    final List<SilverRequest> triggered = new ArrayList<>();
    String failWith;
    JobState state = JobState.RUNNING;

    @Override
    public void trigger(SilverRequest request) {
      if (failWith != null) {
        throw new JobSubmitException(failWith);
      }
      triggered.add(request);
    }

    @Override
    public JobState state(SilverRequest request) {
      return state;
    }
  }

  private final FakeTrigger trigger = new FakeTrigger();
  private final SilverRequestService service = new SilverRequestService(new SilverRequestRegistry(), trigger);

  private static SilverRequestParser.Parsed vtnet(String path) {
    ObjectNode n = JSON.createObjectNode();
    n.put("source", "VTNET");
    n.put("path", path);
    n.put("batchId", "202610121520");
    n.put("table", "vtnet_ebs_3g_4g");
    n.putArray("encryptColumns").add("msisdn");
    n.put("keyField", "start_time");
    n.put("purpose", "p");
    return SilverRequestParser.parse(n);
  }

  @Test
  @DisplayName("gửi → duyệt → trigger DAG của nguồn với conf của phiếu → trạng thái theo dag run")
  void happyPath() {
    SilverRequest created = service.submit(ALICE, vtnet("ebs_3g_4g/dt=20261012/batch=1520"));
    assertThat(created.table()).isEqualTo("vtnet_ebs_3g_4g");
    assertThat(created.code()).matches("SLV-\\d{4}-\\d{6}");
    assertThat(created.state()).isEqualTo(SilverState.PENDING_APPROVAL);
    assertThat(created.dagId()).isEqualTo("silver_ingest_vtnet");

    SilverRequest submitted = service.approve(created.code());

    assertThat(submitted.state()).isEqualTo(SilverState.JOB_SUBMITTED);
    assertThat(submitted.dagRunId()).isEqualTo(created.code().toLowerCase());
    assertThat(SilverDagTrigger.conf(trigger.triggered.get(0))).containsExactly(
        java.util.Map.entry("path", "ebs_3g_4g/dt=20261012/batch=1520"), java.util.Map.entry("batchId", "202610121520"),
        java.util.Map.entry("table", "vtnet_ebs_3g_4g"),
        java.util.Map.entry("encryptColumns", List.of("msisdn")), java.util.Map.entry("keyField", "start_time"));
    assertThat(service.get(ALICE, created.code()).state()).isEqualTo(SilverState.RUNNING);
    trigger.state = JobState.SUCCEEDED;
    assertThat(service.get(ALICE, created.code()).state()).isEqualTo(SilverState.SUCCEEDED);
    trigger.state = JobState.FAILED; // đã kết thúc thì không đổi nữa
    assertThat(service.get(ALICE, created.code()).state()).isEqualTo(SilverState.SUCCEEDED);
  }

  @Test
  @DisplayName("trigger lỗi → SUBMIT_FAILED với thông điệp cố định → sửa rồi retry-submit")
  void submitFailureAndRetry() {
    String code = service.submit(ALICE, vtnet("ebs_3g_4g/dt=20261012/batch=a")).code();
    trigger.failWith = "airflow unavailable";

    SilverRequest failed = service.approve(code);

    assertThat(failed.state()).isEqualTo(SilverState.SUBMIT_FAILED);
    assertThat(failed.submitError()).isEqualTo("airflow unavailable");
    trigger.failWith = null;
    SilverRequest retried = service.retrySubmit(code);
    assertThat(retried.state()).isEqualTo(SilverState.JOB_SUBMITTED);
    assertThat(retried.submitError()).isNull();
  }

  @Test
  @DisplayName("chống trùng theo (team, source, path, batchId) khi phiếu cũ chưa kết thúc")
  void duplicates() {
    String code = service.submit(ALICE, vtnet("ebs_3g_4g/dt=20261012/batch=dup")).code();

    DakApiException e = catchThrowableOfType(() -> service.submit(ALICE, vtnet("ebs_3g_4g/dt=20261012/batch=dup")), DakApiException.class);

    assertThat(e.error()).isEqualTo("duplicate_request");
    assertThat(e.getMessage()).contains(code);
    service.submit(CAROL, vtnet("ebs_3g_4g/dt=20261012/batch=dup")); // team khác: không trùng
    service.reject(code, "sai lô");
    service.submit(ALICE, vtnet("ebs_3g_4g/dt=20261012/batch=dup")); // phiếu cũ đã kết thúc
  }

  @Test
  @DisplayName("phiếu team khác là 404; huỷ/duyệt/từ chối sai trạng thái là 409")
  void isolationAndStates() {
    String code = service.submit(ALICE, vtnet("ebs_3g_4g/dt=20261012/batch=iso")).code();

    assertThat(catchThrowableOfType(() -> service.get(CAROL, code), DakApiException.class).error()).isEqualTo("not_found");
    assertThat(service.cancel(ALICE, code).state()).isEqualTo(SilverState.CANCELLED);
    assertThat(catchThrowableOfType(() -> service.approve(code), DakApiException.class).error()).isEqualTo("invalid_state");
    assertThat(catchThrowableOfType(() -> service.retrySubmit(code), DakApiException.class).error()).isEqualTo("invalid_state");
    assertThat(trigger.triggered).isEmpty();
  }
}
