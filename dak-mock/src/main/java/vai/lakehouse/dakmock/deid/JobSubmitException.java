package vai.lakehouse.dakmock.deid;

/**
 * Gửi job thất bại. Thông điệp được ghi nguyên văn vào {@code submitError} của phiếu và hiện cho người dùng, nên PHẢI là một
 * chuỗi cố định (vd {@code kubernetes unavailable}), tuyệt đối không chứa token, thân lỗi của Kubernetes/Airflow hay secret.
 */
public class JobSubmitException extends RuntimeException {

  public JobSubmitException(String fixedMessage) {
    super(fixedMessage, null, false, false);
  }
}
