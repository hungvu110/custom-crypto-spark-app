package vai.lakehouse.dakmock.deid;

import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;

/**
 * Gửi job kiểu {@code noop}: KHÔNG gọi Kubernetes hay Airflow. Chỉ ghi log các tham số job SẼ nhận (không có {@code purpose}) và
 * trả tên job suy ra từ mã phiếu; trạng thái job luôn là đã xong. Dùng để chạy thử luồng phiếu mà không cần cluster.
 */
public class NoopJobSubmitter implements JobSubmitter {

  private static final Logger log = LoggerFactory.getLogger(NoopJobSubmitter.class);

  @Override
  public JobRef submit(DeidRequest request, String keySecretName) {
    String pseudoColumns = request.columns().stream()
        .map((ColumnSpec c) -> c.column() + ":" + c.mode())
        .collect(Collectors.joining(","));
    // Mọi giá trị đã qua kiểm tra bộ ký tự ở bước upload; keySecret chỉ là TÊN Secret, không phải key.
    log.info("NOOP_JOB requestCode={} job={} database={} table={} pseudoColumns={} outputDb={} outputTable={} keySecret={}",
        request.requestCode(), request.jobName(), request.database(), request.table(), pseudoColumns,
        request.outputDatabase(), request.outputTable(), keySecretName);
    return new JobRef(JobRef.Kind.SPARK_APPLICATION, request.jobName());
  }

  @Override
  public JobState status(DeidRequest request, JobRef ref) {
    return JobState.SUCCEEDED;
  }
}
