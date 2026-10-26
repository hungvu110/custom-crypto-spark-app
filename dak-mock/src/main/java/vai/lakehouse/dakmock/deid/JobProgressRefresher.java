package vai.lakehouse.dakmock.deid;

import java.time.Clock;
import java.time.Instant;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.deid.DeidRequest.State;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Làm mới trạng thái job khi có người xem phiếu (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 4.4); mock không có tiến trình
 * nền. Phiếu {@code JOB_SUBMITTED}/{@code RUNNING} được hỏi trạng thái qua {@link JobSubmitter#status}, kết quả ghi vào phiếu.
 * Khi job xong (hoặc không còn thấy job) thì thu hồi Secret key bằng {@link KeyDelivery#release}, chỉ do lần xem ghi được trạng
 * thái cuối, nên mỗi phiếu chỉ thu hồi một lần dù nhiều người xem cùng lúc.
 *
 * <p>Xem phiếu không bao giờ hỏng vì Airflow hay Kubernetes chập chờn: lỗi hỏi trạng thái hoặc thu hồi chỉ ghi {@code WARN} (loại
 * lỗi, không kèm nội dung) và phiếu giữ nguyên.
 */
@Component
public class JobProgressRefresher {

  private static final Logger log = LoggerFactory.getLogger(JobProgressRefresher.class);
  private static final String SYSTEM = "system";
  private static final Set<State> RUNNING_STATES = Set.of(State.JOB_SUBMITTED, State.RUNNING);

  private final DeidRequestRegistry registry;
  private final JobSubmitter submitter;
  private final KeyDelivery keyDelivery;
  private final Clock clock;

  @Autowired
  public JobProgressRefresher(DeidRequestRegistry registry, JobSubmitter submitter, KeyDelivery keyDelivery) {
    this(registry, submitter, keyDelivery, Clock.systemUTC());
  }

  JobProgressRefresher(DeidRequestRegistry registry, JobSubmitter submitter, KeyDelivery keyDelivery, Clock clock) {
    this.registry = registry;
    this.submitter = submitter;
    this.keyDelivery = keyDelivery;
    this.clock = clock;
  }

  /** Bản không làm gì, cho những chỗ không cần làm mới (test của service). */
  static JobProgressRefresher disabled() {
    return new JobProgressRefresher(null, null, null, Clock.systemUTC());
  }

  /** Phiếu sau khi làm mới; không phải phiếu đang chạy job thì trả nguyên đối tượng nhận vào. */
  public DeidRequest refresh(DeidRequest request) {
    if (submitter == null || !RUNNING_STATES.contains(request.state()) || request.jobRef() == null
        || request.jobState() == JobState.UNKNOWN) {
      return request;
    }
    JobState progress = askStatus(request);
    if (progress == null) {
      return request;
    }
    Instant now = clock.instant();
    DeidRequest updated;
    try {
      updated = registry.transition(request.requestCode(), RUNNING_STATES, r -> r.jobProgress(progress, now));
    } catch (DakApiException e) {
      // Lần xem khác đã ghi trạng thái trước: lấy bản hiện tại, không ghi đè và không thu hồi lần hai.
      return registry.getByCode(request.requestCode());
    }
    if (updated.state() != request.state()) {
      DeidAudit.transition(updated, request.state().name(), updated.state(), SYSTEM, "");
    }
    if (progress != JobState.RUNNING) {
      release(updated);
    }
    return updated;
  }

  private JobState askStatus(DeidRequest request) {
    try {
      return submitter.status(request, request.jobRef());
    } catch (RuntimeException e) {
      log.warn("Could not refresh the job status of {}: {}", request.requestCode(), e.getClass().getName());
      return null;
    }
  }

  private void release(DeidRequest request) {
    try {
      keyDelivery.release(request);
    } catch (RuntimeException e) {
      // Secret sót có nhãn dak.vai/request-code để quét dọn (plan mục 6).
      log.warn("Could not release the key of {}: {}", request.requestCode(), e.getClass().getName());
    }
  }
}
