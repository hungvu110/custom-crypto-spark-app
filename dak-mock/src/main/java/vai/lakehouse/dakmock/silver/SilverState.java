package vai.lakehouse.dakmock.silver;

/**
 * Trạng thái phiếu chuẩn hoá silver. Mọi phiếu là phiếu ĐỊNH KỲ cho cả một dataset: duyệt MỘT lần, DAG của dataset chạy mỗi ngày theo giờ trên
 * phiếu ({@code SCHEDULED}) cho tới khi admin {@code stop} ({@code STOPPED}). {@code APPROVED} chỉ thoáng qua: duyệt xong là đăng ký lịch với
 * Airflow ngay. Kết quả từng lần chạy không nằm ở trạng thái phiếu mà ở {@code lastRun} của phiếu.
 */
public enum SilverState {
  PENDING_APPROVAL,
  APPROVED,
  SUBMIT_FAILED,
  SCHEDULED,
  REJECTED,
  CANCELLED,
  STOPPED;

  /** Phiếu đã kết thúc: không đổi nữa, không tính vào chống trùng. */
  public boolean terminal() {
    return this == REJECTED || this == CANCELLED || this == STOPPED;
  }
}
