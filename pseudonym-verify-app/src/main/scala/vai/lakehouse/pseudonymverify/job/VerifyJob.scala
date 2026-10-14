package vai.lakehouse.pseudonymverify.job

import vai.lakehouse.pseudonymverify.config.RunMode
import vai.lakehouse.pseudonymverify.report.Verdict

/**
 * Một lần chạy của app theo `RUN_MODE`. Mỗi chế độ là 1 job độc lập, nhận [[VerifySession]] (mọi thứ dùng chung) và trả
 * về [[Verdict]]; `main` chỉ chọn job rồi đổi verdict thành exit code.
 */
trait VerifyJob {
  def run(session: VerifySession): Verdict
}

object VerifyJob {

  /** Job tương ứng `RUN_MODE`. `match` kín trên sealed [[RunMode]] — thêm chế độ mới mà quên job là lỗi biên dịch. */
  def forMode(mode: RunMode): VerifyJob = mode match {
    case RunMode.Full     => FullJob
    case RunMode.Selftest => SelftestJob
    case RunMode.Guard    => GuardJob
  }
}
