package vai.lakehouse.dakmock.silver;

import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.deid.JobState;
import vai.lakehouse.dakmock.deid.JobSubmitException;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Luồng phiếu chuẩn hoá silver: team gửi phiếu cho một dataset → admin duyệt MỘT lần (hoặc từ chối) → DAK đăng ký lịch hằng ngày với Airflow
 * ({@code SCHEDULED}) cho tới khi admin {@code stop}; xem phiếu thì thấy dag run gần nhất ({@code lastRun}). Phiếu của team khác trả
 * {@code 404} như phiếu không tồn tại. Đăng ký lịch lỗi thì phiếu vào {@code SUBMIT_FAILED} kèm thông điệp cố định, admin sửa nguyên nhân
 * rồi {@code retry-submit}.
 */
@Service
public class SilverRequestService {

  private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");
  private static final String ADMIN = "admin";
  private static final String INTERNAL_ERROR = "internal error while scheduling the dag";

  private final SilverRequestRegistry registry;
  private final SilverDagTrigger trigger;

  public SilverRequestService(SilverRequestRegistry registry, SilverDagTrigger trigger) {
    this.registry = registry;
    this.trigger = trigger;
  }

  public SilverRequest submit(PortalUser user, SilverRequestParser.Parsed parsed) {
    SilverRequest created = registry.create(user.team(), user.username(), parsed);
    audit(created, null, user.username());
    return created;
  }

  public List<SilverRequest> list(PortalUser user) {
    return registry.list(user.team()).stream().map(this::refresh).toList();
  }

  public SilverRequest get(PortalUser user, String code) {
    return refresh(registry.find(code).filter(r -> r.team().equals(user.team()))
        .orElseThrow(() -> DakApiException.notFound("Unknown request " + code)));
  }

  /** Người gửi huỷ phiếu chưa duyệt. */
  public SilverRequest cancel(PortalUser user, String code) {
    get(user, code);
    return transition(code, SilverState.PENDING_APPROVAL, r -> r.decided(SilverState.CANCELLED, null, null, registry.now()),
        user.username());
  }

  public List<SilverRequest> adminList() {
    return registry.list(null).stream().map(this::refresh).toList();
  }

  public SilverRequest adminGet(String code) {
    return refresh(registry.find(code).orElseThrow(() -> DakApiException.notFound("Unknown request " + code)));
  }

  /** Duyệt MỘT lần rồi đăng ký lịch hằng ngày với Airflow ngay. */
  public SilverRequest approve(String code) {
    transition(code, SilverState.PENDING_APPROVAL, r -> r.decided(SilverState.APPROVED, ADMIN, null, registry.now()), ADMIN);
    return schedule(code);
  }

  public SilverRequest reject(String code, String reason) {
    String text = reason == null ? "" : reason.strip();
    String kept = text.length() > 500 ? text.substring(0, 500) : text;
    return transition(code, SilverState.PENDING_APPROVAL, r -> r.decided(SilverState.REJECTED, ADMIN, kept, registry.now()),
        ADMIN);
  }

  /** Gửi lại phiếu {@code SUBMIT_FAILED} sau khi đã sửa nguyên nhân. */
  public SilverRequest retrySubmit(String code) {
    transition(code, SilverState.SUBMIT_FAILED, r -> r.withState(SilverState.APPROVED, registry.now()), ADMIN);
    return schedule(code);
  }

  /** Admin gỡ lịch của phiếu đang {@code SCHEDULED}: DAG ngừng chạy. Gỡ lịch lỗi thì phiếu vẫn {@code SCHEDULED}. */
  public SilverRequest stop(String code) {
    SilverRequest current = adminGet(code);
    if (current.state() != SilverState.SCHEDULED) {
      throw DakApiException.invalidState(current.state().name());
    }
    try {
      trigger.unschedule(current);
    } catch (JobSubmitException e) {
      throw DakApiException.upstream(e.getMessage());
    }
    return transition(code, SilverState.SCHEDULED, r -> r.stopped(ADMIN, registry.now()), ADMIN);
  }

  private SilverRequest schedule(String code) {
    SilverRequest approved = adminGet(code);
    SilverRequest next;
    try {
      trigger.schedule(approved);
      next = registry.update(code, r -> r.scheduled(registry.now()));
    } catch (JobSubmitException e) {
      String error = e.getMessage();
      next = registry.update(code, r -> r.submitFailed(error, registry.now()));
    } catch (RuntimeException e) {
      LoggerFactory.getLogger(getClass()).error("Unexpected failure while scheduling {}: {}", code, e.getClass().getName());
      next = registry.update(code, r -> r.submitFailed(INTERNAL_ERROR, registry.now()));
    }
    audit(next, SilverState.APPROVED, ADMIN);
    return next;
  }

  /** Phiếu đang {@code SCHEDULED}: cập nhật dag run gần nhất ({@code lastRun}); lỗi đọc thì giữ nguyên phiếu. */
  private SilverRequest refresh(SilverRequest r) {
    if (r.state() != SilverState.SCHEDULED) {
      return r;
    }
    JobState run;
    try {
      run = trigger.lastRun(r);
    } catch (JobSubmitException e) {
      return r;
    }
    String name = run == JobState.UNKNOWN ? null : run.name();
    if (Objects.equals(name, r.lastRun())) {
      return r;
    }
    return registry.update(r.code(), current -> current.withLastRun(name));
  }

  private SilverRequest transition(String code, SilverState from, UnaryOperator<SilverRequest> change, String actor) {
    SilverRequest next = registry.update(code, r -> {
      if (r.state() != from) {
        throw DakApiException.invalidState(r.state().name());
      }
      return change.apply(r);
    });
    audit(next, from, actor);
    return next;
  }

  // Không ghi purpose (người dùng nhập) hay bất kỳ key nào.
  private static void audit(SilverRequest r, SilverState from, String actor) {
    AUDIT.info("SILVER_TRANSITION requestCode={} from={} to={} actor={} team={} source={} path={} scheduleTime={} table={} dagId={} "
            + "encryptColumns={} submitError={}",
        r.code(), from == null ? "-" : from, r.state(), actor, r.team(), r.source(), r.path(), r.scheduleTime(), r.table(), r.dagId(),
        r.encryptColumns().isEmpty() ? "-" : String.join(",", r.encryptColumns()),
        r.submitError() == null ? "-" : r.submitError());
  }
}
