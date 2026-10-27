package vai.lakehouse.dakmock.deid;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.deid.DeidRequest.State;
import vai.lakehouse.dakmock.deid.KeyDelivery.KeyHandle;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Nghiệp vụ phía admin của flow phiếu khử định danh (docs/dak-deid-flow/API_CONTRACT.md mục 5, plan mục 4.3): duyệt, từ chối,
 * gửi lại và xem mọi team. Admin hành động dưới tên cố định {@value #ADMIN} (mock dùng một token chung).
 *
 * <p><b>Duyệt</b> chạy theo thứ tự cố định: (1) chuyển {@code PENDING_APPROVAL → APPROVED} nguyên tử và chốt bảng đích; (2) giao
 * key cho job; (3) gửi job. Bước (2) và (3) gọi mạng nên chạy NGOÀI khoá của registry, và chỉ người thắng bước (1) mới chạy
 * chúng, nên key được giao và job được gửi đúng một lần dù duyệt đồng thời. Lỗi ở (2) hoặc (3) KHÔNG làm duyệt thất bại: phiếu vào
 * {@code SUBMIT_FAILED} kèm thông điệp cố định, admin {@link #retrySubmit gửi lại} sau khi khắc phục.
 */
@Component
public class DeidAdminService {

  public static final String ADMIN = "admin";

  private static final Logger log = LoggerFactory.getLogger(DeidAdminService.class);
  private static final int MAX_REASON_LENGTH = 500;
  private static final String NOT_FOUND = "Request not found";
  private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
  /** Phiếu đã qua bước duyệt: duyệt lặp lại trả lại phiếu hiện tại, không làm gì thêm. */
  private static final Set<State> ALREADY_APPROVED = EnumSet.of(State.APPROVED, State.SUBMIT_FAILED, State.JOB_SUBMITTED,
      State.RUNNING, State.SUCCEEDED, State.FAILED);

  private final DeidProperties deid;
  private final DeidRequestRegistry registry;
  private final KeyDelivery keyDelivery;
  private final JobSubmitter submitter;
  private final Clock clock;
  private final JobProgressRefresher refresher;
  private final JobEnvPolicy envPolicy;

  @Autowired
  public DeidAdminService(DakProperties props, DeidRequestRegistry registry, KeyDelivery keyDelivery,
                          JobSubmitter submitter, JobProgressRefresher refresher) {
    this(props.deid(), registry, keyDelivery, submitter, Clock.systemUTC(), refresher);
  }

  DeidAdminService(DeidProperties deid, DeidRequestRegistry registry, KeyDelivery keyDelivery, JobSubmitter submitter,
                   Clock clock) {
    this(deid, registry, keyDelivery, submitter, clock, JobProgressRefresher.disabled());
  }

  DeidAdminService(DeidProperties deid, DeidRequestRegistry registry, KeyDelivery keyDelivery, JobSubmitter submitter,
                   Clock clock, JobProgressRefresher refresher) {
    this.deid = deid;
    this.registry = registry;
    this.keyDelivery = keyDelivery;
    this.submitter = submitter;
    this.clock = clock;
    this.refresher = refresher;
    this.envPolicy = new JobEnvPolicy(deid);
  }

  /**
   * Duyệt phiếu. Phiếu đã duyệt (ở bất kỳ trạng thái sau đó) thì trả lại phiếu hiện tại; phiếu đã huỷ hoặc đã từ chối thì
   * {@code 409 invalid_state}.
   */
  public DeidRequest approve(String code) {
    String checked = checkedCode("approve", code);
    DeidRequest approved;
    try {
      Instant now = clock.instant();
      approved = registry.transition(checked, Set.of(State.PENDING_APPROVAL), r -> {
        // Env cuối cùng = env người gửi nhập + mặc định; bảng kết quả không được trùng bảng nguồn (cấu hình có thể đã đổi).
        Map<String, String> finalEnv = envPolicy.complete(r.env(), outputDatabase(r), outputTable(r, now));
        envPolicy.outputConflict(r.database(), r.table(), finalEnv).ifPresent(problem -> {
          throw DakApiException.invalidRequest(problem.message());
        });
        return r.approved(ADMIN, now, finalEnv.get(JobEnvPolicy.OUTPUT_DB), finalEnv.get(JobEnvPolicy.OUTPUT_TABLE), finalEnv);
      });
    } catch (DakApiException e) {
      if ("invalid_state".equals(e.error())) {
        DeidRequest current = registry.getByCode(checked);
        if (ALREADY_APPROVED.contains(current.state())) {
          return current;
        }
      }
      DeidAudit.error("approve", ADMIN, DeidAudit.NONE, checked, e.error());
      throw e;
    }
    DeidAudit.transition(approved, State.PENDING_APPROVAL.name(), State.APPROVED, ADMIN, DeidAudit.envSummary(approved.env()));
    return deliverAndSubmit(approved);
  }

  /** Từ chối kèm lý do bắt buộc (1 đến 500 ký tự sau khi strip). Chỉ phiếu đang chờ duyệt mới từ chối được. */
  public DeidRequest reject(String code, String reasonText) {
    String checked = checkedCode("reject", code);
    try {
      String reason = checkedReason(reasonText);
      Instant now = clock.instant();
      DeidRequest rejected = registry.transition(checked, Set.of(State.PENDING_APPROVAL),
          r -> r.rejected(ADMIN, now, reason));
      // Lý do là văn bản tự do của admin: không đưa vào audit.
      DeidAudit.transition(rejected, State.PENDING_APPROVAL.name(), State.REJECTED, ADMIN, "");
      return rejected;
    } catch (DakApiException e) {
      DeidAudit.error("reject", ADMIN, DeidAudit.NONE, checked, e.error());
      throw e;
    }
  }

  /** Làm lại bước giao key và gửi job cho phiếu {@code SUBMIT_FAILED}. Bảng đích đã chốt lúc duyệt được giữ nguyên. */
  public DeidRequest retrySubmit(String code) {
    String checked = checkedCode("retry-submit", code);
    DeidRequest approved;
    try {
      approved = registry.transition(checked, Set.of(State.SUBMIT_FAILED), DeidRequest::retrying);
    } catch (DakApiException e) {
      DeidAudit.error("retry-submit", ADMIN, DeidAudit.NONE, checked, e.error());
      throw e;
    }
    DeidAudit.transition(approved, State.SUBMIT_FAILED.name(), State.APPROVED, ADMIN, "");
    return deliverAndSubmit(approved);
  }

  /** Xem một phiếu bất kỳ, không phân biệt team. */
  public DeidRequest get(String code) {
    String checked = checkedCode("get", code);
    try {
      return refresher.refresh(registry.getByCode(checked));
    } catch (DakApiException e) {
      DeidAudit.error("get", ADMIN, DeidAudit.NONE, checked, e.error());
      throw e;
    }
  }

  /** Phiếu của mọi team, mới nhất trước; xem {@link RequestListing} về {@code state}, {@code limit}, {@code cursor}. */
  public DeidRequestService.Page list(String state, String limit, String cursor) {
    return RequestListing.page(registry.listAll(), state, limit, cursor);
  }

  /** Giao key rồi gửi job cho phiếu đang {@code APPROVED}; mọi lỗi quy về {@code SUBMIT_FAILED} với thông điệp cố định. */
  private DeidRequest deliverAndSubmit(DeidRequest approved) {
    KeyHandle key;
    try {
      key = keyDelivery.deliver(approved);
      DeidAudit.keyDelivery(approved, key.keyVersion(), true);
    } catch (KeyDeliveryException e) {
      DeidAudit.keyDelivery(approved, null, false);
      return submitFailed(approved, e.getMessage());
    } catch (RuntimeException e) {
      // Chỉ ghi loại lỗi: nội dung exception không được phép lọt vào log hay phiếu.
      log.error("Unexpected failure while delivering the key of {}: {}", approved.requestCode(), e.getClass().getName());
      DeidAudit.keyDelivery(approved, null, false);
      return submitFailed(approved, "internal error while delivering the key");
    }

    JobRef ref;
    try {
      ref = submitter.submit(approved, key.secretName());
    } catch (JobSubmitException e) {
      release(approved);
      return submitFailed(approved, e.getMessage());
    } catch (RuntimeException e) {
      log.error("Unexpected failure while submitting the job of {}: {}", approved.requestCode(), e.getClass().getName());
      release(approved);
      return submitFailed(approved, "internal error while submitting the job");
    }

    DeidRequest submitted = registry.transition(approved.requestCode(), Set.of(State.APPROVED),
        r -> r.submitted(ref, key.keyVersion()));
    DeidAudit.transition(submitted, State.APPROVED.name(), State.JOB_SUBMITTED, ADMIN, "");
    return submitted;
  }

  private DeidRequest submitFailed(DeidRequest approved, String message) {
    DeidRequest failed = registry.transition(approved.requestCode(), Set.of(State.APPROVED), r -> r.submitFailed(message));
    DeidAudit.transition(failed, State.APPROVED.name(), State.SUBMIT_FAILED, ADMIN,
        " submitError=" + DeidAudit.clean(message));
    return failed;
  }

  private void release(DeidRequest request) {
    try {
      keyDelivery.release(request);
    } catch (RuntimeException e) {
      // Thu hồi lỗi không được che mất lỗi gốc; Secret sót được quét dọn theo nhãn (plan mục 6).
      log.warn("Could not release the key of {}: {}", request.requestCode(), e.getClass().getName());
    }
  }

  private String checkedCode(String action, String code) {
    if (!DeidRequest.isCode(code)) {
      DeidAudit.error(action, ADMIN, DeidAudit.NONE, DeidAudit.NONE, "not_found");
      throw DakApiException.notFound(NOT_FOUND);
    }
    return code;
  }

  private static String checkedReason(String reasonText) {
    String reason = reasonText == null ? "" : reasonText.strip();
    if (reason.isEmpty() || reason.codePointCount(0, reason.length()) > MAX_REASON_LENGTH) {
      throw DakApiException.invalidRequest("reasonText is required (1 to " + MAX_REASON_LENGTH + " characters)");
    }
    return reason;
  }

  private String outputDatabase(DeidRequest r) {
    return deid.outputDatabase().isEmpty() ? r.database() : deid.outputDatabase();
  }

  /** Tên bảng đích theo mẫu cấu hình: {@code {table}}, ngày duyệt {@code {yyyyMMdd}} (UTC), {@code {seq}} là số cuối của mã phiếu. */
  private String outputTable(DeidRequest r, Instant now) {
    String code = r.requestCode();
    String seq = String.valueOf(Long.parseLong(code.substring(code.lastIndexOf('-') + 1)));
    return deid.outputTableTemplate()
        .replace("{table}", r.table())
        .replace("{yyyyMMdd}", DAY.format(now))
        .replace("{seq}", seq);
  }
}
