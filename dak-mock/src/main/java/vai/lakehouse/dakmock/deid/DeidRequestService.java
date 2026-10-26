package vai.lakehouse.dakmock.deid;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.config.DeidProperties.Submitter;
import vai.lakehouse.dakmock.deid.DeidRequest.NewRequest;
import vai.lakehouse.dakmock.deid.DeidRequest.State;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Nghiệp vụ phía người dùng portal của flow phiếu khử định danh (plan mục 4.1, 4.2, 4.9): upload, huỷ, xem, danh sách phiếu
 * của team và danh sách {@code dagId} được phép. Mọi quy tắc nằm ở đây; lớp HTTP chỉ là vỏ mỏng.
 *
 * <p>Phiếu thuộc TEAM của người đăng nhập: mọi tra cứu đi qua {@code (team, mã)}, phiếu của team khác trả đúng lỗi 404 như phiếu
 * không tồn tại. Mã phiếu sai định dạng cũng trả 404 mà không chạm registry và không bao giờ được ghi vào audit.
 */
@Component
public class DeidRequestService {

  /** Một trang danh sách; {@code nextCursor} là mã phiếu cuối trang, chỉ có khi còn phiếu phía sau. */
  public record Page(List<DeidRequest> requests, @JsonInclude(JsonInclude.Include.NON_NULL) String nextCursor) {}

  private static final String NOT_FOUND = "Request not found";

  private final DeidProperties deid;
  private final DeidUploadParser parser;
  private final DeidRequestRegistry registry;
  private final JobProgressRefresher refresher;

  @Autowired
  public DeidRequestService(DakProperties props, DeidUploadParser parser, DeidRequestRegistry registry,
                            JobProgressRefresher refresher) {
    this(props.deid(), parser, registry, refresher);
  }

  DeidRequestService(DeidProperties deid, DeidUploadParser parser, DeidRequestRegistry registry) {
    this(deid, parser, registry, JobProgressRefresher.disabled());
  }

  DeidRequestService(DeidProperties deid, DeidUploadParser parser, DeidRequestRegistry registry,
                     JobProgressRefresher refresher) {
    this.deid = deid;
    this.parser = parser;
    this.registry = registry;
    this.refresher = refresher;
  }

  /** Upload một file phiếu: hợp lệ thì tạo phiếu {@link State#PENDING_APPROVAL} của team người upload. */
  public DeidRequest upload(PortalUser user, String originalFilename, InputStream content) {
    try {
      DeidUpload upload = parser.parse(originalFilename, content);
      DeidRequest created = registry.create(new NewRequest(user.team(), user.username(), upload.database(),
          upload.table(), upload.columns(), upload.dagId(), upload.purpose(), upload.sourceFile()));
      DeidAudit.transition(created, DeidAudit.NONE, State.PENDING_APPROVAL, user.username(),
          " fileSha256=" + created.sourceFile().sha256());
      return created;
    } catch (DakApiException e) {
      DeidAudit.error("upload", user.username(), user.team(), DeidAudit.NONE, e.error());
      throw e;
    }
  }

  /** Huỷ phiếu đang chờ duyệt. Huỷ lặp lại phiếu đã huỷ không phải lỗi: trả lại phiếu và không ghi audit thêm. */
  public DeidRequest cancel(PortalUser user, String code) {
    String checked = checkedCode(user, "cancel", code);
    try {
      DeidRequest cancelled = registry.transitionForTeam(user.team(), checked, Set.of(State.PENDING_APPROVAL),
          DeidRequest::cancelled);
      DeidAudit.transition(cancelled, State.PENDING_APPROVAL.name(), State.CANCELLED, user.username(), "");
      return cancelled;
    } catch (DakApiException e) {
      if ("invalid_state".equals(e.error())) {
        DeidRequest current = registry.get(user.team(), checked);
        if (current.state() == State.CANCELLED) {
          return current;
        }
      }
      DeidAudit.error("cancel", user.username(), user.team(), checked, e.error());
      throw e;
    }
  }

  public DeidRequest get(PortalUser user, String code) {
    String checked = checkedCode(user, "get", code);
    try {
      return refresher.refresh(registry.get(user.team(), checked));
    } catch (DakApiException e) {
      DeidAudit.error("get", user.username(), user.team(), checked, e.error());
      throw e;
    }
  }

  /** Phiếu của team, mới nhất trước; xem {@link RequestListing} về {@code state}, {@code limit}, {@code cursor}. */
  public Page list(PortalUser user, String state, String limit, String cursor) {
    return RequestListing.page(registry.listForTeam(user.team()), state, limit, cursor);
  }

  /** {@code dagId} người dùng được chọn: chỉ có khi gửi job bằng Airflow, không thì rỗng. Không sửa được từ ngoài. */
  public List<String> allowedDagIds() {
    return deid.submitter() == Submitter.AIRFLOW ? deid.airflow().allowedDagIds() : List.of();
  }

  private String checkedCode(PortalUser user, String action, String code) {
    if (!DeidRequest.isCode(code)) {
      // Không ghi mã sai định dạng vào audit: đó là dữ liệu người dùng tự do.
      DeidAudit.error(action, user.username(), user.team(), DeidAudit.NONE, "not_found");
      throw DakApiException.notFound(NOT_FOUND);
    }
    return code;
  }
}
