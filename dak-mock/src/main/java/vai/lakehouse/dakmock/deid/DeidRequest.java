package vai.lakehouse.dakmock.deid;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonInclude;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;

/**
 * Phiếu yêu cầu khử định danh (PYC), bất biến: mỗi bước chuyển trạng thái trả bản MỚI (docs/dak-deid-flow/API_CONTRACT.md
 * mục 1). Trường của các bước sau là {@code null} cho đến khi bước đó xảy ra. Phiếu không chứa key hay bí mật nào.
 *
 * <p>Hàm chuyển trạng thái ({@link #approved}, {@link #cancelled}...) tự kiểm trạng thái nguồn và ném
 * {@link IllegalStateException} nếu sai: đó là lỗi lập trình, còn việc từ chối thao tác của người dùng (409) do
 * {@link DeidRequestRegistry} đảm nhiệm trước khi gọi các hàm này.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DeidRequest(
    String requestCode,
    State state,
    String database,
    String table,
    List<ColumnSpec> columns,
    String dagId,
    String purpose,
    String team,
    String createdBy,
    Instant createdAt,
    SourceFile sourceFile,
    Instant decidedAt,
    String decidedBy,
    String reasonText,
    String outputDatabase,
    String outputTable,
    Long keyVersion,
    JobRef jobRef,
    String submitError,
    JobState jobState,
    Instant jobFinishedAt) {

  private static final Pattern CODE = Pattern.compile("PYC-\\d{4}-\\d{6}");

  /** Trạng thái phiếu và các cạnh hợp lệ (docs/dak-deid-flow/README.md mục 2). */
  public enum State {
    PENDING_APPROVAL,
    APPROVED,
    SUBMIT_FAILED,
    JOB_SUBMITTED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    REJECTED,
    CANCELLED;

    /** Trạng thái cuối: phiếu không đổi nữa, muốn làm lại phải tạo phiếu mới. */
    public boolean isFinal() {
      return this == REJECTED || this == CANCELLED || this == SUCCEEDED || this == FAILED;
    }

    /** Phiếu còn đang xử lý: tính vào giới hạn phiếu hoạt động của team và vào việc chống trùng. */
    public boolean isActive() {
      return !isFinal();
    }

    /**
     * Cạnh hợp lệ. Ngoài các cạnh trong README còn có SUBMIT_FAILED → APPROVED: {@code retry-submit} đưa phiếu về APPROVED
     * trước khi lấy key và gửi job lại (plan mục 4.3).
     */
    public boolean canMoveTo(State next) {
      return switch (this) {
        case PENDING_APPROVAL -> next == CANCELLED || next == REJECTED || next == APPROVED;
        case APPROVED -> next == JOB_SUBMITTED || next == SUBMIT_FAILED;
        case SUBMIT_FAILED -> next == APPROVED || next == JOB_SUBMITTED;
        case JOB_SUBMITTED -> next == RUNNING || next == SUCCEEDED || next == FAILED;
        case RUNNING -> next == SUCCEEDED || next == FAILED;
        case REJECTED, CANCELLED, SUCCEEDED, FAILED -> false;
      };
    }
  }

  /** Một cột cần khử định danh và chế độ của nó. */
  public record ColumnSpec(String column, Mode mode) {

    public ColumnSpec {
      Objects.requireNonNull(column, "column");
      Objects.requireNonNull(mode, "mode");
    }
  }

  /** File người dùng đã upload: chỉ giữ tên (đã làm sạch, để hiển thị) và SHA-256 của đúng các byte đã đọc. */
  public record SourceFile(String name, String sha256) {}

  /** Dữ liệu một phiếu mới, đã qua kiểm tra ở bước upload; mã phiếu và thời điểm tạo do registry cấp. */
  public record NewRequest(String team, String createdBy, String database, String table, List<ColumnSpec> columns,
                           String dagId, String purpose, SourceFile sourceFile) {}

  public DeidRequest {
    Objects.requireNonNull(requestCode, "requestCode");
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(database, "database");
    Objects.requireNonNull(table, "table");
    Objects.requireNonNull(team, "team");
    columns = List.copyOf(columns);
  }

  /** Phiếu mới ở {@link State#PENDING_APPROVAL}. */
  public static DeidRequest created(String requestCode, Instant createdAt, NewRequest n) {
    return new DeidRequest(requestCode, State.PENDING_APPROVAL, n.database(), n.table(), n.columns(), n.dagId(),
        n.purpose(), n.team(), n.createdBy(), createdAt, n.sourceFile(), null, null, null, null, null, null, null, null,
        null, null);
  }

  /** Mã phiếu hợp lệ: {@code PYC-<năm 4 số>-<6 số>}. Dùng để kiểm mã người dùng gửi lên TRƯỚC khi tra cứu hay ghi log. */
  public static boolean isCode(String value) {
    return value != null && CODE.matcher(value).matches();
  }

  /** Hậu tố của mã phiếu ({@code 2026-000007}), dùng chung cho tên job và tên Secret. */
  private String codeSuffix() {
    return requestCode.substring("PYC-".length());
  }

  /** Tên job suy ra từ mã phiếu (chữ thường, hợp DNS-1123; Airflow dùng làm {@code dag_run_id}): {@code deid-2026-000007}. */
  public String jobName() {
    return "deid-" + codeSuffix();
  }

  /** Tên Secret chứa key của phiếu: {@code deid-key-2026-000007}. */
  public String secretName() {
    return "deid-key-" + codeSuffix();
  }

  /** Admin duyệt: chốt người duyệt, thời điểm và bảng đích. */
  public DeidRequest approved(String by, Instant at, String outputDb, String outputTbl) {
    requireMove(State.APPROVED, State.PENDING_APPROVAL);
    return copy(State.APPROVED, at, by, reasonText, outputDb, outputTbl, keyVersion, jobRef, submitError, jobState,
        jobFinishedAt);
  }

  /** Admin từ chối kèm lý do. */
  public DeidRequest rejected(String by, Instant at, String reason) {
    requireMove(State.REJECTED, State.PENDING_APPROVAL);
    return copy(State.REJECTED, at, by, reason, outputDatabase, outputTable, keyVersion, jobRef, submitError, jobState,
        jobFinishedAt);
  }

  /** Người dùng huỷ phiếu đang chờ duyệt. */
  public DeidRequest cancelled() {
    requireMove(State.CANCELLED, State.PENDING_APPROVAL);
    return copy(State.CANCELLED, decidedAt, decidedBy, reasonText, outputDatabase, outputTable, keyVersion, jobRef,
        submitError, jobState, jobFinishedAt);
  }

  /** Lấy key và gửi job thành công; xoá lỗi gửi job cũ (nếu có). */
  public DeidRequest submitted(JobRef ref, Long keyVer) {
    requireMove(State.JOB_SUBMITTED, State.APPROVED, State.SUBMIT_FAILED);
    return copy(State.JOB_SUBMITTED, decidedAt, decidedBy, reasonText, outputDatabase, outputTable, keyVer, ref, null,
        jobState, jobFinishedAt);
  }

  /** Lấy key hoặc gửi job lỗi; {@code error} là thông điệp đã lọc, không chứa bí mật. */
  public DeidRequest submitFailed(String error) {
    requireMove(State.SUBMIT_FAILED, State.APPROVED);
    return copy(State.SUBMIT_FAILED, decidedAt, decidedBy, reasonText, outputDatabase, outputTable, keyVersion, jobRef,
        error, jobState, jobFinishedAt);
  }

  /** {@code retry-submit}: đưa phiếu về APPROVED để lấy key và gửi job lại, xoá lỗi cũ. */
  public DeidRequest retrying() {
    requireMove(State.APPROVED, State.SUBMIT_FAILED);
    return copy(State.APPROVED, decidedAt, decidedBy, reasonText, outputDatabase, outputTable, keyVersion, jobRef, null,
        jobState, jobFinishedAt);
  }

  /**
   * Áp tiến độ job. {@link JobState#UNKNOWN} chỉ ghi {@code jobState}, giữ nguyên trạng thái phiếu; RUNNING lặp lại không
   * phải lỗi; SUCCEEDED và FAILED là trạng thái cuối và ghi giờ kết thúc.
   */
  public DeidRequest jobProgress(JobState progress, Instant now) {
    if (state != State.JOB_SUBMITTED && state != State.RUNNING) {
      throw new IllegalStateException("Job progress does not apply in state " + state);
    }
    return switch (progress) {
      case UNKNOWN -> copy(state, decidedAt, decidedBy, reasonText, outputDatabase, outputTable, keyVersion, jobRef,
          submitError, JobState.UNKNOWN, jobFinishedAt);
      case RUNNING -> copy(State.RUNNING, decidedAt, decidedBy, reasonText, outputDatabase, outputTable, keyVersion,
          jobRef, submitError, JobState.RUNNING, jobFinishedAt);
      case SUCCEEDED -> copy(State.SUCCEEDED, decidedAt, decidedBy, reasonText, outputDatabase, outputTable, keyVersion,
          jobRef, submitError, JobState.SUCCEEDED, now);
      case FAILED -> copy(State.FAILED, decidedAt, decidedBy, reasonText, outputDatabase, outputTable, keyVersion,
          jobRef, submitError, JobState.FAILED, now);
    };
  }

  private void requireMove(State next, State... sources) {
    boolean fromAllowedSource = false;
    for (State source : sources) {
      fromAllowedSource |= state == source;
    }
    if (!fromAllowedSource || !state.canMoveTo(next)) {
      throw new IllegalStateException("Cannot move request " + requestCode + " from " + state + " to " + next);
    }
  }

  /** Bản sao với các trường thay đổi theo bước; các trường định danh phiếu (mã, team, nội dung người dùng gửi) giữ nguyên. */
  private DeidRequest copy(State newState, Instant newDecidedAt, String newDecidedBy, String newReasonText,
                           String newOutputDatabase, String newOutputTable, Long newKeyVersion, JobRef newJobRef,
                           String newSubmitError, JobState newJobState, Instant newJobFinishedAt) {
    return new DeidRequest(requestCode, newState, database, table, columns, dagId, purpose, team, createdBy, createdAt,
        sourceFile, newDecidedAt, newDecidedBy, newReasonText, newOutputDatabase, newOutputTable, newKeyVersion,
        newJobRef, newSubmitError, newJobState, newJobFinishedAt);
  }
}
