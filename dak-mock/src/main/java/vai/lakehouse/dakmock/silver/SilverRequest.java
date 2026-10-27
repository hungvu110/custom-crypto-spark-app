package vai.lakehouse.dakmock.silver;

import java.time.Instant;
import java.util.List;
import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Phiếu chuẩn hoá một lô file vào silver. Bất biến: mỗi lần đổi trạng thái tạo bản mới.
 *
 * @param table          bảng đích trong database silver do người gửi chọn: chữ thường, số, {@code _}, bắt đầu bằng tiền tố của nguồn
 * @param batchId        mã lô, {@code yyyyMMddHH} hoặc {@code yyyyMMddHHmm}: giá trị cột partition của lô trong bảng đích
 * @param path           đường dẫn của lô, TƯƠNG ĐỐI theo gốc landing của nguồn, dạng chuẩn (docs/key-access/TRA-LOI-DAK-BY-PATH.md);
 *                       với VTNET cũng là đường dẫn job gửi {@code by-path}
 * @param outputMode     {@code null} = mặc định của DAG ({@code overwrite-partitions})
 * @param encryptColumns chỉ VTNET: cột job mã hoá bằng cdr-crypto trước khi ghi; rỗng với VTT
 * @param keyField       chỉ VTNET: cột giữ plaintext làm nguyên liệu key của từng dòng
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SilverRequest(
    String code,
    String team,
    String submittedBy,
    SilverSource source,
    String path,
    String batchId,
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
    String dagRunId,
    String submitError) {

  public SilverRequest {
    encryptColumns = encryptColumns == null ? List.of() : List.copyOf(encryptColumns);
  }

  /** Phiếu mới, chờ duyệt. */
  static SilverRequest pending(String code, String team, String user, SilverRequestParser.Parsed p, Instant now) {
    return new SilverRequest(code, team, user, p.source(), p.path(), p.batchId(), p.table(), p.outputMode(), p.encryptColumns(),
        p.keyField(), p.purpose(), SilverState.PENDING_APPROVAL, now, now, null, null, p.source().dagId(), null, null);
  }

  /** {@code dag_run_id} của phiếu, suy ra từ mã phiếu: gửi lại là cùng một dag run. */
  @JsonIgnore
  public String runId() {
    return code.toLowerCase(Locale.ROOT);
  }

  SilverRequest decided(SilverState next, String admin, String reason, Instant now) {
    return new SilverRequest(code, team, submittedBy, source, path, batchId, table, outputMode, encryptColumns, keyField, purpose, next,
        createdAt, now, admin, reason, dagId, dagRunId, submitError);
  }

  SilverRequest submitted(Instant now) {
    return new SilverRequest(code, team, submittedBy, source, path, batchId, table, outputMode, encryptColumns, keyField, purpose,
        SilverState.JOB_SUBMITTED, createdAt, now, decidedBy, rejectReason, dagId, runId(), null);
  }

  SilverRequest submitFailed(String error, Instant now) {
    return new SilverRequest(code, team, submittedBy, source, path, batchId, table, outputMode, encryptColumns, keyField, purpose,
        SilverState.SUBMIT_FAILED, createdAt, now, decidedBy, rejectReason, dagId, null, error);
  }

  SilverRequest withState(SilverState next, Instant now) {
    return new SilverRequest(code, team, submittedBy, source, path, batchId, table, outputMode, encryptColumns, keyField, purpose, next,
        createdAt, now, decidedBy, rejectReason, dagId, dagRunId, submitError);
  }
}
