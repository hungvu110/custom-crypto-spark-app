package vai.lakehouse.dakmock.silver;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.Submitter;
import vai.lakehouse.dakmock.deid.AirflowDagRuns;
import vai.lakehouse.dakmock.deid.JobState;

/**
 * Chọn cách trigger DAG chuẩn hoá silver, cùng công tắc với phiếu khử định danh ({@code dak.deid.submitter}, {@code dak.deid.airflow}):
 * {@code airflow} gọi REST của Airflow; {@code noop} chỉ ghi log và coi dag run đã xong (để thử luồng phiếu không cần hạ tầng).
 */
@Configuration
public class SilverConfig {

  @Bean
  public SilverDagTrigger silverDagTrigger(DakProperties props) {
    DeidProperties deid = props.deid();
    if (deid.enabled() && deid.submitter() == Submitter.AIRFLOW) {
      AirflowDagRuns runs = new AirflowDagRuns(deid.airflow(), Clock.systemUTC());
      return new SilverDagTrigger() {
        @Override
        public void trigger(SilverRequest request) {
          runs.trigger(request.dagId(), request.runId(), SilverDagTrigger.conf(request));
        }

        @Override
        public JobState state(SilverRequest request) {
          return runs.state(request.dagId(), request.runId());
        }
      };
    }
    return new NoopSilverDagTrigger();
  }

  /** Không gọi gì: log {@code conf} (không có bí mật) và báo dag run đã xong. */
  static final class NoopSilverDagTrigger implements SilverDagTrigger {

    final AtomicInteger triggered = new AtomicInteger();

    @Override
    public void trigger(SilverRequest request) {
      triggered.incrementAndGet();
      LoggerFactory.getLogger(SilverConfig.class).info("NOOP silver dag trigger dagId={} runId={} conf={}", request.dagId(),
          request.runId(), SilverDagTrigger.conf(request));
    }

    @Override
    public JobState state(SilverRequest request) {
      return JobState.SUCCEEDED;
    }
  }
}
