package vai.lakehouse.dakmock.silver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

  /** Trigger giả: ghi lại các lần gọi, lỗi theo yêu cầu, dag run gần nhất đặt tay. */
  private static final class FakeTrigger implements SilverDagTrigger {
    final List<SilverRequest> scheduled = new ArrayList<>();
    final List<SilverRequest> unscheduled = new ArrayList<>();
    String failWith;
    JobState lastRun = JobState.UNKNOWN;

    private void failIfAsked() {
      if (failWith != null) {
        throw new JobSubmitException(failWith);
      }
    }

    @Override
    public void schedule(SilverRequest request) {
      failIfAsked();
      scheduled.add(request);
    }

    @Override
    public void unschedule(SilverRequest request) {
      failIfAsked();
      unscheduled.add(request);
    }

    @Override
    public JobState lastRun(SilverRequest request) {
      return lastRun;
    }
  }

  private final FakeTrigger trigger = new FakeTrigger();
  private final SilverRequestService service = new SilverRequestService(new SilverRequestRegistry(), trigger);

  /** NIMS: a request for the whole dataset, decrypt-encrypt mode, one column to encrypt. */
  private static SilverRequestParser.Parsed nims() {
    ObjectNode n = JSON.createObjectNode();
    n.put("source", "VTNET");
    n.put("path", "nims");
    n.put("scheduleTime", "03:05");
    n.put("table", "vtnet_nims_infra_cell_4g");
    n.putArray("encryptColumns").add("lng");
    n.put("keyField", "eci");
    n.put("purpose", "p");
    return SilverRequestParser.parse(n);
  }

  private static SilverRequestParser.Parsed ebs() {
    ObjectNode n = JSON.createObjectNode();
    n.put("source", "VTNET");
    n.put("path", "ebs_3g_4g");
    n.put("scheduleTime", "02:30");
    n.put("table", "vtnet_ebs_3g_4g");
    n.put("purpose", "p");
    return SilverRequestParser.parse(n);
  }

  private static SilverRequestParser.Parsed vtt() {
    ObjectNode n = JSON.createObjectNode();
    n.put("source", "VTT");
    n.put("path", "f001-mpre-call");
    n.put("scheduleTime", "01:30");
    n.put("table", "vtt_f001_mpre_call");
    n.put("purpose", "p");
    return SilverRequestParser.parse(n);
  }

  @Test
  @DisplayName("duyệt MỘT lần thì đăng ký lịch ở Airflow, phiếu ở SCHEDULED")
  void requestIsApprovedOnceAndScheduled() {
    SilverRequest created = service.submit(ALICE, nims());
    assertThat(created.code()).matches("SLV-\\d{4}-\\d{6}");
    assertThat(created.state()).isEqualTo(SilverState.PENDING_APPROVAL);
    assertThat(created.dagId()).isEqualTo("silver_ingest_vtnet_nims");
    assertThat(created.scheduleTime()).isEqualTo("03:05");

    SilverRequest scheduled = service.approve(created.code());

    assertThat(scheduled.state()).isEqualTo(SilverState.SCHEDULED);
    assertThat(scheduled.submitError()).isNull();
    assertThat(trigger.scheduled).hasSize(1);
    assertThat(SilverDagTrigger.scheduleConfig(trigger.scheduled.get(0))).containsExactly(
        Map.entry("ticket", created.code()), Map.entry("table", "vtnet_nims_infra_cell_4g"), Map.entry("cron", "5 3 * * *"),
        Map.entry("encryptColumns", List.of("lng")), Map.entry("keyField", "eci"));
  }

  @Test
  @DisplayName("phiếu VTT cũng là phiếu định kỳ: DAG của dataset VTT, cấu hình lịch không có cột mã hoá")
  void vttRequestIsAlsoScheduled() {
    SilverRequest created = service.submit(ALICE, vtt());
    assertThat(created.dagId()).isEqualTo("silver_ingest_vtt_f001_mpre_call");

    assertThat(service.approve(created.code()).state()).isEqualTo(SilverState.SCHEDULED);

    assertThat(SilverDagTrigger.scheduleConfig(trigger.scheduled.get(0))).containsExactly(
        Map.entry("ticket", created.code()), Map.entry("table", "vtt_f001_mpre_call"), Map.entry("cron", "30 1 * * *"));
  }

  @Test
  @DisplayName("phiếu không đổi trạng thái theo dag run; xem phiếu thấy dag run gần nhất (lastRun)")
  void scheduledRequestShowsTheLatestRun() {
    String code = service.submit(ALICE, nims()).code();
    service.approve(code);
    assertThat(service.get(ALICE, code).lastRun()).isNull(); // chưa có dag run nào

    trigger.lastRun = JobState.RUNNING;
    assertThat(service.get(ALICE, code).lastRun()).isEqualTo("RUNNING");
    trigger.lastRun = JobState.FAILED;
    SilverRequest failedRun = service.get(ALICE, code);
    assertThat(failedRun.lastRun()).isEqualTo("FAILED");
    assertThat(failedRun.state()).isEqualTo(SilverState.SCHEDULED); // một ngày lỗi không làm phiếu thành lỗi
    trigger.lastRun = JobState.SUCCEEDED;
    assertThat(service.adminGet(code).lastRun()).isEqualTo("SUCCEEDED");
    assertThat(service.adminGet(code).state()).isEqualTo(SilverState.SCHEDULED);
  }

  @Test
  @DisplayName("admin stop phiếu SCHEDULED: gỡ lịch, phiếu thành STOPPED và không còn chặn phiếu mới của dataset")
  void stopRemovesTheSchedule() {
    String code = service.submit(ALICE, nims()).code();
    service.approve(code);

    SilverRequest stopped = service.stop(code);

    assertThat(stopped.state()).isEqualTo(SilverState.STOPPED);
    assertThat(trigger.unscheduled).hasSize(1);
    assertThat(catchThrowableOfType(() -> service.stop(code), DakApiException.class).error()).isEqualTo("invalid_state");
    service.submit(CAROL, nims()); // dataset đã được giải phóng
  }

  @Test
  @DisplayName("gỡ lịch lỗi: báo lỗi, phiếu vẫn SCHEDULED để thử lại")
  void stopFailureKeepsTheRequestScheduled() {
    String code = service.submit(ALICE, nims()).code();
    service.approve(code);
    trigger.failWith = "airflow unavailable";

    DakApiException e = catchThrowableOfType(() -> service.stop(code), DakApiException.class);

    assertThat(e).isNotNull();
    assertThat(service.adminGet(code).state()).isEqualTo(SilverState.SCHEDULED);
    trigger.failWith = null;
    assertThat(service.stop(code).state()).isEqualTo(SilverState.STOPPED);
  }

  @Test
  @DisplayName("stop chỉ dùng cho phiếu đang SCHEDULED")
  void stopNeedsAScheduledRequest() {
    String pending = service.submit(ALICE, nims()).code();
    assertThat(catchThrowableOfType(() -> service.stop(pending), DakApiException.class).error()).isEqualTo("invalid_state");
  }

  @Test
  @DisplayName("dataset passthrough: DAG của dataset, cấu hình lịch không có cột mã hoá")
  void passthroughRequestIsScheduled() {
    SilverRequest created = service.submit(ALICE, ebs());
    assertThat(created.dagId()).isEqualTo("silver_ingest_vtnet_ebs_3g_4g");

    assertThat(service.approve(created.code()).state()).isEqualTo(SilverState.SCHEDULED);

    assertThat(SilverDagTrigger.scheduleConfig(trigger.scheduled.get(0))).containsOnlyKeys("ticket", "table", "cron");
    assertThat(SilverDagTrigger.scheduleConfig(trigger.scheduled.get(0))).containsEntry("cron", "30 2 * * *");
  }

  @Test
  @DisplayName("dataset decrypt-encrypt không chọn cột: cấu hình lịch có encryptColumns rỗng và không có keyField")
  void decryptOnlyConfig() {
    ObjectNode n = JSON.createObjectNode();
    n.put("source", "VTNET");
    n.put("path", "nims");
    n.put("scheduleTime", "00:00");
    n.put("table", "vtnet_nims_infra_cell_4g");
    n.putArray("encryptColumns");
    n.put("purpose", "p");
    SilverRequest created = service.submit(ALICE, SilverRequestParser.parse(n));

    service.approve(created.code());

    assertThat(SilverDagTrigger.scheduleConfig(trigger.scheduled.get(0))).containsEntry("encryptColumns", List.of())
        .containsEntry("cron", "0 0 * * *").doesNotContainKey("keyField");
  }

  @Test
  @DisplayName("đăng ký lịch lỗi → SUBMIT_FAILED với thông điệp cố định → sửa rồi retry-submit")
  void submitFailureAndRetry() {
    String nimsCode = service.submit(ALICE, nims()).code();
    String vttCode = service.submit(ALICE, vtt()).code();
    trigger.failWith = "airflow unavailable";

    for (String code : List.of(nimsCode, vttCode)) {
      SilverRequest failed = service.approve(code);
      assertThat(failed.state()).isEqualTo(SilverState.SUBMIT_FAILED);
      assertThat(failed.submitError()).isEqualTo("airflow unavailable");
    }
    trigger.failWith = null;
    assertThat(service.retrySubmit(nimsCode).state()).isEqualTo(SilverState.SCHEDULED);
    assertThat(service.retrySubmit(vttCode).state()).isEqualTo(SilverState.SCHEDULED);
    assertThat(service.adminGet(nimsCode).submitError()).isNull();
  }

  @Test
  @DisplayName("chống trùng: cả dataset chỉ một phiếu chưa kết thúc, của mọi team; dataset khác không trùng")
  void duplicates() {
    String code = service.submit(ALICE, nims()).code();

    DakApiException same = catchThrowableOfType(() -> service.submit(ALICE, nims()), DakApiException.class);
    DakApiException otherTeam = catchThrowableOfType(() -> service.submit(CAROL, nims()), DakApiException.class);

    assertThat(same.error()).isEqualTo("duplicate_request");
    assertThat(otherTeam.error()).isEqualTo("duplicate_request");
    assertThat(otherTeam.getMessage()).contains(code);
    service.submit(ALICE, ebs()); // dataset khác: không trùng
    service.submit(ALICE, vtt());
    assertThat(catchThrowableOfType(() -> service.submit(CAROL, vtt()), DakApiException.class).error()).isEqualTo("duplicate_request");
    service.approve(code);
    assertThat(catchThrowableOfType(() -> service.submit(CAROL, nims()), DakApiException.class).error()).isEqualTo("duplicate_request");
  }

  @Test
  @DisplayName("phiếu bị từ chối giải phóng dataset")
  void rejectedRequestFreesTheDataset() {
    String code = service.submit(ALICE, nims()).code();
    service.reject(code, "sai bảng");
    assertThat(service.adminGet(code).state()).isEqualTo(SilverState.REJECTED);
    service.submit(ALICE, nims());
  }

  @Test
  @DisplayName("phiếu team khác là 404; huỷ/duyệt/từ chối sai trạng thái là 409")
  void isolationAndStates() {
    String code = service.submit(ALICE, nims()).code();

    assertThat(catchThrowableOfType(() -> service.get(CAROL, code), DakApiException.class).error()).isEqualTo("not_found");
    assertThat(service.cancel(ALICE, code).state()).isEqualTo(SilverState.CANCELLED);
    assertThat(catchThrowableOfType(() -> service.approve(code), DakApiException.class).error()).isEqualTo("invalid_state");
    assertThat(catchThrowableOfType(() -> service.retrySubmit(code), DakApiException.class).error()).isEqualTo("invalid_state");
    assertThat(trigger.scheduled).isEmpty();
  }

  @Test
  @DisplayName("cron: HH:mm thành '<phút> <giờ> * * *'")
  void cronOfTheScheduleTime() {
    assertThat(SilverDagTrigger.cron("03:00")).isEqualTo("0 3 * * *");
    assertThat(SilverDagTrigger.cron("03:05")).isEqualTo("5 3 * * *");
    assertThat(SilverDagTrigger.cron("23:59")).isEqualTo("59 23 * * *");
    assertThat(SilverDagTrigger.cron("00:00")).isEqualTo("0 0 * * *");
  }
}
