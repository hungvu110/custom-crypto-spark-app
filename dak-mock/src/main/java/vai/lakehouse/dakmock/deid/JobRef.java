package vai.lakehouse.dakmock.deid;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Tham chiếu tới job đã gửi cho một phiếu: tên SparkApplication hoặc {@code dag_run_id} của Airflow. */
public record JobRef(Kind kind, String name) {

  /** Hệ thống chạy job; tên trong JSON theo hợp đồng (docs/dak-deid-flow/API_CONTRACT.md mục 1). */
  public enum Kind {
    @JsonProperty("SparkApplication")
    SPARK_APPLICATION,
    @JsonProperty("AirflowDagRun")
    AIRFLOW_DAG_RUN
  }

  public JobRef {
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(name, "name");
  }
}
