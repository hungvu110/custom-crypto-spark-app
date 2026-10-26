package vai.lakehouse.dakmock.deid;

/**
 * Gửi job khử định danh sau khi duyệt phiếu và hỏi trạng thái job (docs/dak-deid-flow/API_CONTRACT.md mục 7, plan mục 4.6, 4.7).
 * Có ba hiện thực chọn bằng {@code dak.deid.submitter}: {@code noop}, {@code kubernetes}, {@code airflow}.
 *
 * <p>Gửi job phải idempotent theo mã phiếu: gửi lần hai cho cùng phiếu không tạo job thứ hai (tên job suy ra từ mã phiếu).
 */
public interface JobSubmitter {

  /**
   * Gửi job cho phiếu đã duyệt; {@code keySecretName} là tên Secret chứa key (không phải key). Lỗi ném
   * {@link JobSubmitException} với thông điệp cố định.
   */
  JobRef submit(DeidRequest request, String keySecretName);

  /** Trạng thái hiện tại của job đã gửi cho {@code request} (Airflow cần {@code dagId} của phiếu để hỏi dag run). */
  JobState status(DeidRequest request, JobRef ref);
}
