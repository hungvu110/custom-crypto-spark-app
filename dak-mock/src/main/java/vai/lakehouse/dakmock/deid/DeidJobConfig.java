package vai.lakehouse.dakmock.deid;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.Submitter;
import vai.lakehouse.dakmock.key.VaultKeyReader;

/**
 * Chọn hiện thực giao key và gửi job theo {@code dak.deid.submitter}: {@code noop} (không gọi gì) hoặc {@code airflow} (đọc key từ
 * Vault, tạo Secret trong namespace của job rồi trigger dag run). {@code kubernetes} (áp SparkApplication trực tiếp) không còn trong
 * phạm vi vì DAG của Airflow tự tạo SparkApplication; cấu hình đó làm ứng dụng không khởi động, thay vì để phiếu duyệt xong mà
 * không có gì được gửi. Khi flow tắt thì dùng {@code noop} và không kiểm.
 */
@Configuration
public class DeidJobConfig {

  @Bean
  public KeyDelivery keyDelivery(DakProperties props, VaultKeyReader vault) {
    DeidProperties deid = props.deid();
    if (!isActive(deid)) {
      return new NoopKeyDelivery();
    }
    return new KubernetesSecretKeyDelivery(vault, KubernetesApiClient.inCluster(deid.kubernetes().apiServer()),
        deid.kubernetes().namespace(), deid);
  }

  @Bean
  public JobSubmitter jobSubmitter(DakProperties props) {
    DeidProperties deid = props.deid();
    if (!isActive(deid)) {
      return new NoopJobSubmitter();
    }
    return new AirflowJobSubmitter(deid, new AirflowTokens(deid.airflow(), Clock.systemUTC()));
  }

  /** {@code true} khi flow bật và dùng Airflow; {@code false} cho noop; ném lỗi khi submitter không được hỗ trợ. */
  private static boolean isActive(DeidProperties deid) {
    if (!deid.enabled() || deid.submitter() == Submitter.NOOP) {
      return false;
    }
    if (deid.submitter() == Submitter.AIRFLOW) {
      return true;
    }
    throw new IllegalStateException("Invalid DAK mock configuration: dak.deid.submitter="
        + deid.submitter().name().toLowerCase() + " is not supported, use airflow or noop");
  }
}
