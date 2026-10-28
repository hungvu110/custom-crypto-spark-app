package vai.lakehouse.dakmock.silver;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Phiếu chuẩn hoá silver: phiếu ĐỊNH KỲ cho cả một dataset, duyệt một lần, DAG của dataset chạy mỗi ngày lúc {@code scheduleTime} cho dữ liệu
 * của ngày hôm trước. Bất biến: mỗi lần đổi trạng thái tạo bản mới.
 *
 * @param path           tên dataset của nguồn (DAG của dataset, key {@code by-path} của cả dataset), không phải một ngày hay một lô
 * @param scheduleTime   giờ chạy hằng ngày {@code HH:mm} (Asia/Ho_Chi_Minh)
 * @param table          bảng đích trong database silver do người gửi chọn: chữ thường, số, {@code _}, bắt đầu bằng tiền tố của nguồn
 * @param outputMode     {@code null} = mặc định của DAG ({@code overwrite-partitions})
 * @param encryptColumns cột job mã hoá bằng cdr-crypto trước khi ghi; rỗng nếu dataset không mã hoá cột hoặc phiếu không chọn cột nào
 * @param keyField       cột giữ plaintext làm nguyên liệu key của từng dòng; {@code null} khi không có cột mã hoá
 * @param lastRun        trạng thái dag run gần nhất ({@code SUCCEEDED}, {@code FAILED}, {@code RUNNING}), {@code null} nếu chưa chạy
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SilverRequest(
    String code,
    String team,
    String submittedBy,
    SilverSource source,
    String path,
    String scheduleTime,
    String table,
    String outputMode,
    List<String> encryptColumns,
    String keyField,
    String purpose,
    SilverState state,
    Instant createdAt,
    Instant updatedAt,
    String decidedBy,
    String rejectReason,
    String dagId,
    String submitError,
    String lastRun) {

  public SilverRequest {
    encryptColumns = encryptColumns == null ? List.of() : List.copyOf(encryptColumns);
  }

  /** Phiếu mới, chờ duyệt. */
  static SilverRequest pending(String code, String team, String user, SilverRequestParser.Parsed p, Instant now) {
    return new SilverRequest(code, team, user, p.source(), p.path(), p.scheduleTime(), p.table(), p.outputMode(), p.encryptColumns(),
        p.keyField(), p.purpose(), SilverState.PENDING_APPROVAL, now, now, null, null, p.dagId(), null, null);
  }

  /** The dataset of the request. */
  @JsonIgnore
  public SilverDataset dataset() {
    return SilverDataset.of(source, path).orElseThrow(() -> new IllegalStateException("unknown dataset"));
  }

  /**
   * {@code true} when the dataset of the request runs in decrypt-encrypt mode, so the DAG takes {@code encryptColumns} (possibly empty) and
   * {@code keyField}. Passthrough datasets never carry them.
   */
  @JsonIgnore
  public boolean encryptsColumns() {
    return dataset().encryptsColumns();
  }

  SilverRequest decided(SilverState next, String admin, String reason, Instant now) {
    return copy(next, now, admin, reason, submitError, lastRun);
  }

  /** Lịch đã được đăng ký với Airflow. */
  SilverRequest scheduled(Instant now) {
    return copy(SilverState.SCHEDULED, now, decidedBy, rejectReason, null, lastRun);
  }

  /** Admin đã dừng lịch. */
  SilverRequest stopped(String admin, Instant now) {
    return copy(SilverState.STOPPED, now, admin, rejectReason, null, lastRun);
  }

  SilverRequest submitFailed(String error, Instant now) {
    return copy(SilverState.SUBMIT_FAILED, now, decidedBy, rejectReason, error, lastRun);
  }

  SilverRequest withState(SilverState next, Instant now) {
    return copy(next, now, decidedBy, rejectReason, submitError, lastRun);
  }

  /** Chỉ cập nhật thông tin theo dõi, không coi là một lần đổi phiếu ({@code updatedAt} giữ nguyên). */
  SilverRequest withLastRun(String run) {
    return copy(state, updatedAt, decidedBy, rejectReason, submitError, run);
  }

  private SilverRequest copy(SilverState nextState, Instant nextUpdatedAt, String nextDecidedBy, String nextRejectReason,
                             String nextSubmitError, String nextLastRun) {
    return new SilverRequest(code, team, submittedBy, source, path, scheduleTime, table, outputMode, encryptColumns, keyField, purpose,
        nextState, createdAt, nextUpdatedAt, nextDecidedBy, nextRejectReason, dagId, nextSubmitError, nextLastRun);
  }
}
