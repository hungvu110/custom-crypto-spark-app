package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Instant;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import vai.lakehouse.dakmock.config.DeidProperties.Mode;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.NewRequest;
import vai.lakehouse.dakmock.deid.DeidRequest.SourceFile;
import vai.lakehouse.dakmock.deid.KeyDelivery.KeyHandle;

/**
 * Cách gửi job {@code noop} (plan mục 4.3, 4.6b): không gọi Vault, Kubernetes hay Airflow. Chỉ trả tên job và tên Secret suy ra
 * từ mã phiếu và ghi log những gì SẼ được gửi, để chạy thử toàn bộ luồng phiếu mà không cần hạ tầng.
 */
class NoopJobAndKeyDeliveryTest {

  private static final Instant T = Instant.parse("2026-10-23T08:00:00Z");

  private ListAppender<ILoggingEvent> logs;
  private Logger jobLogger;

  @BeforeEach
  void captureLogs() {
    logs = new ListAppender<>();
    logs.start();
    jobLogger = (Logger) LoggerFactory.getLogger(NoopJobSubmitter.class);
    jobLogger.setLevel(Level.INFO);
    jobLogger.addAppender(logs);
  }

  @AfterEach
  void stopCapturing() {
    jobLogger.detachAppender(logs);
  }

  private static DeidRequest approved() {
    return DeidRequest.created("PYC-2026-000007", T, new NewRequest("team-a", "alice", "demo_db", "users_cdr",
            List.of(new ColumnSpec("msisdn", Mode.HASH), new ColumnSpec("imei", Mode.HASH)), null, "ly do rieng tu",
            new SourceFile("phieu.json", "ab".repeat(32))))
        .approved("admin", T, "demo_db", "users_cdr_deid_20261023_7");
  }

  @Test
  @DisplayName("noop giao key: trả tên Secret suy ra từ mã phiếu, không có keyVersion, không đọc gì")
  void keyDelivery_returnsADerivedSecretName() {
    KeyHandle handle = new NoopKeyDelivery().deliver(approved());

    assertThat(handle.secretName()).isEqualTo("deid-key-2026-000007");
    assertThat(handle.keyVersion()).isNull();
  }

  @Test
  @DisplayName("noop thu hồi key không làm gì và không lỗi")
  void keyDelivery_releaseIsANoop() {
    assertThatCode(() -> new NoopKeyDelivery().release(approved())).doesNotThrowAnyException();
  }

  @Test
  @DisplayName("noop gửi job: trả tên job suy ra từ mã phiếu")
  void submit_returnsADerivedJobName() {
    JobRef ref = new NoopJobSubmitter().submit(approved(), "deid-key-2026-000007");

    assertThat(ref.kind()).isEqualTo(JobRef.Kind.SPARK_APPLICATION);
    assertThat(ref.name()).isEqualTo("deid-2026-000007");
  }

  @Test
  @DisplayName("noop gửi job ghi log các tham số job sẽ nhận, không có purpose")
  void submit_logsTheJobParameters() {
    new NoopJobSubmitter().submit(approved(), "deid-key-2026-000007");

    assertThat(logs.list).hasSize(1);
    String line = logs.list.get(0).getFormattedMessage();
    assertThat(line).contains("requestCode=PYC-2026-000007").contains("job=deid-2026-000007").contains("database=demo_db")
        .contains("table=users_cdr").contains("pseudoColumns=msisdn:HASH,imei:HASH").contains("outputDb=demo_db")
        .contains("outputTable=users_cdr_deid_20261023_7").contains("keySecret=deid-key-2026-000007");
    assertThat(line).doesNotContain("ly do rieng tu").doesNotContain("\n");
  }

  @Test
  @DisplayName("noop báo job luôn đã chạy xong")
  void status_isAlwaysSucceeded() {
    assertThat(new NoopJobSubmitter().status(approved(), new JobRef(JobRef.Kind.SPARK_APPLICATION, "deid-2026-000007")))
        .isEqualTo(JobState.SUCCEEDED);
  }
}
