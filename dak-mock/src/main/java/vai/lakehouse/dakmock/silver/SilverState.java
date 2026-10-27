package vai.lakehouse.dakmock.silver;

/** Trạng thái phiếu chuẩn hoá silver. {@code APPROVED} chỉ thoáng qua: duyệt xong là trigger DAG ngay. */
public enum SilverState {
  PENDING_APPROVAL,
  APPROVED,
  SUBMIT_FAILED,
  JOB_SUBMITTED,
  RUNNING,
  SUCCEEDED,
  FAILED,
  REJECTED,
  CANCELLED;

  /** Phiếu đã kết thúc: không đổi nữa, không tính vào chống trùng. */
  public boolean terminal() {
    return this == SUCCEEDED || this == FAILED || this == REJECTED || this == CANCELLED;
  }
}
