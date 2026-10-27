package vai.lakehouse.dakmock.silver;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.deid.JobState;
import vai.lakehouse.dakmock.deid.JobSubmitException;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Luồng phiếu chuẩn hoá silver: team gửi phiếu → admin duyệt (hoặc từ chối) → DAK trigger DAG của nguồn → trạng thái dag run được cập
 * nhật khi có người xem phiếu. Phiếu của team khác trả {@code 404} như phiếu không tồn tại. Trigger lỗi thì phiếu vào
 * {@code SUBMIT_FAILED} kèm thông điệp cố định, admin sửa nguyên nhân rồi {@code retry-submit}.
 */
@Service
public class SilverRequestService {

  private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");
  private static final String ADMIN = "admin";
  private static final String INTERNAL_ERROR = "internal error while triggering the dag";

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

  /** Duyệt rồi trigger DAG ngay. */
  public SilverRequest approve(String code) {
    transition(code, SilverState.PENDING_APPROVAL, r -> r.decided(SilverState.APPROVED, ADMIN, null, registry.now()), ADMIN);
    return submit(code);
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
    return submit(code);
  }

  private SilverRequest submit(String code) {
    SilverRequest approved = adminGet(code);
    SilverRequest next;
    try {
      trigger.trigger(approved);
      next = registry.update(code, r -> r.submitted(registry.now()));
    } catch (JobSubmitException e) {
      String error = e.getMessage();
      next = registry.update(code, r -> r.submitFailed(error, registry.now()));
    } catch (RuntimeException e) {
      LoggerFactory.getLogger(getClass()).error("Unexpected failure while triggering {}: {}", code, e.getClass().getName());
      next = registry.update(code, r -> r.submitFailed(INTERNAL_ERROR, registry.now()));
    }
    audit(next, SilverState.APPROVED, ADMIN);
    return next;
  }

  /** Cập nhật trạng thái dag run khi phiếu đang chạy; lỗi đọc trạng thái thì giữ nguyên phiếu. */
  private SilverRequest refresh(SilverRequest r) {
    if (r.state() != SilverState.JOB_SUBMITTED && r.state() != SilverState.RUNNING) {
      return r;
    }
    JobState state;
    try {
      state = trigger.state(r);
    } catch (JobSubmitException e) {
      return r;
    }
    SilverState next = switch (state) {
      case SUCCEEDED -> SilverState.SUCCEEDED;
      case FAILED -> SilverState.FAILED;
      case RUNNING -> SilverState.RUNNING;
      case UNKNOWN -> r.state();
    };
    if (next == r.state()) {
      return r;
    }
    SilverRequest updated = registry.update(r.code(), current -> current.withState(next, registry.now()));
    audit(updated, r.state(), "system");
    return updated;
  }

  private SilverRequest transition(String code, SilverState from, java.util.function.UnaryOperator<SilverRequest> change,
                                   String actor) {
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
    AUDIT.info("SILVER_TRANSITION requestCode={} from={} to={} actor={} team={} source={} path={} batchId={} table={} dagId={} "
            + "encryptColumns={} submitError={}",
        r.code(), from == null ? "-" : from, r.state(), actor, r.team(), r.source(), r.path(), r.batchId(), r.table(), r.dagId(),
        r.encryptColumns().isEmpty() ? "-" : String.join(",", r.encryptColumns()),
        r.submitError() == null ? "-" : r.submitError());
  }
}
