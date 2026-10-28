package vai.lakehouse.dakmock.silver;

import java.time.Clock;
import java.util.Map;
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
 * Chọn cách đặt lịch cho phiếu chuẩn hoá silver, cùng công tắc với phiếu khử định danh ({@code dak.deid.submitter}, {@code dak.deid.airflow}):
 * {@code airflow} ghi Variable và bật DAG qua REST của Airflow; {@code noop} chỉ ghi log và coi lịch đã đặt (để thử luồng phiếu không cần hạ tầng).
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
        public void schedule(SilverRequest request) {
          // The config first, then the switch: a DAG that is switched on always finds its settings.
          runs.putVariable(request.dataset().variableKey(), SilverDagTrigger.scheduleConfig(request));
          runs.setPaused(request.dagId(), false);
        }

        @Override
        public void unschedule(SilverRequest request) {
          runs.setPaused(request.dagId(), true);
          runs.deleteVariable(request.dataset().variableKey());
        }

        @Override
        public void runDay(SilverRequest request, String date, String dagRunId) {
          runs.trigger(request.dagId(), dagRunId, Map.of("date", date));
        }

        @Override
        public JobState lastRun(SilverRequest request) {
          return runs.latestRunState(request.dagId());
        }
      };
    }
    return new NoopSilverDagTrigger();
  }

  /** Không gọi gì: log cấu hình lịch (không có bí mật) và coi như đã đặt. */
  static final class NoopSilverDagTrigger implements SilverDagTrigger {

    final AtomicInteger scheduled = new AtomicInteger();

    @Override
    public void schedule(SilverRequest request) {
      scheduled.incrementAndGet();
      LoggerFactory.getLogger(SilverConfig.class).info("NOOP silver dag schedule dagId={} config={}", request.dagId(),
          SilverDagTrigger.scheduleConfig(request));
    }

    @Override
    public void unschedule(SilverRequest request) {
      LoggerFactory.getLogger(SilverConfig.class).info("NOOP silver dag unschedule dagId={}", request.dagId());
    }

    @Override
    public void runDay(SilverRequest request, String date, String dagRunId) {
      LoggerFactory.getLogger(SilverConfig.class).info("NOOP silver dag run dagId={} date={} dagRunId={}", request.dagId(), date, dagRunId);
    }

    @Override
    public JobState lastRun(SilverRequest request) {
      return JobState.UNKNOWN;
    }
  }
}
