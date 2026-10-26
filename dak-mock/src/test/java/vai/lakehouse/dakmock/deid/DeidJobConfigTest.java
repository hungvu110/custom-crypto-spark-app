package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import vai.lakehouse.dakmock.DakMockApplication;

/** D6b/D8: {@code dak.deid.submitter} chọn hiện thực giao key và gửi job; cấu hình không được hỗ trợ thì không khởi động. */
class DeidJobConfigTest {

  private static final String[] BASE = {"dak.vault.addr=http://127.0.0.1:1", "dak.vault.token=vault-test-token"};

  private static ConfigurableApplicationContext start(String... extra) {
    String[] all = new String[BASE.length + extra.length];
    System.arraycopy(BASE, 0, all, 0, BASE.length);
    System.arraycopy(extra, 0, all, BASE.length, extra.length);
    // Tham số dòng lệnh thắng application.yml; properties() chỉ là mặc định nên bị ${VAULT_TOKEN:} trong yml đè.
    String[] args = java.util.Arrays.stream(all).map(p -> "--" + p).toArray(String[]::new);
    return new SpringApplicationBuilder(DakMockApplication.class).web(WebApplicationType.NONE).run(args);
  }

  @Test
  @DisplayName("noop is the default and uses the noop key delivery and job submitter")
  void default_isNoop() {
    try (ConfigurableApplicationContext ctx = start()) {
      assertThat(ctx.getBean(KeyDelivery.class)).isInstanceOf(NoopKeyDelivery.class);
      assertThat(ctx.getBean(JobSubmitter.class)).isInstanceOf(NoopJobSubmitter.class);
    }
  }

  @Test
  @DisplayName("airflow uses the Kubernetes Secret key delivery and the Airflow job submitter")
  void airflow_usesSecretDeliveryAndAirflowSubmitter() {
    try (ConfigurableApplicationContext ctx = start("dak.deid.submitter=airflow", "dak.deid.kubernetes.namespace=team-ns",
        "dak.deid.airflow.base-url=http://airflow.test", "dak.deid.airflow.allowed-dag-ids=deid_dag",
        "dak.deid.airflow.token-url=http://sso.test/token", "dak.deid.airflow.client-id=dak",
        "dak.deid.airflow.client-secret=s3cret-s3cret")) {
      assertThat(ctx.getBean(KeyDelivery.class)).isInstanceOf(KubernetesSecretKeyDelivery.class);
      assertThat(ctx.getBean(JobSubmitter.class)).isInstanceOf(AirflowJobSubmitter.class);
    }
  }

  @Test
  @DisplayName("submitter=kubernetes is not supported: the app does not start instead of approving without a job")
  void kubernetes_notSupported_failsFast() {
    assertThatThrownBy(() -> start("dak.deid.submitter=kubernetes", "dak.deid.kubernetes.namespace=team-ns",
        "dak.deid.kubernetes.template-file=k8s/deid/sparkapplication.yaml"))
        .hasStackTraceContaining("dak.deid.submitter=kubernetes is not supported");
  }

  @Test
  @DisplayName("a disabled flow starts with noop whatever the submitter says")
  void disabled_skipsTheCheck() {
    try (ConfigurableApplicationContext ctx = start("dak.deid.enabled=false", "dak.deid.submitter=kubernetes")) {
      assertThat(ctx.getBean(JobSubmitter.class)).isInstanceOf(NoopJobSubmitter.class);
    }
  }
}
