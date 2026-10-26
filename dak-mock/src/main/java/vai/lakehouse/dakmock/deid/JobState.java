package vai.lakehouse.dakmock.deid;

/**
 * Trạng thái job khử định danh như hệ thống chạy job (SparkApplication hoặc Airflow) báo về, đã quy về bốn giá trị
 * (docs/dak-deid-flow/API_CONTRACT.md mục 7.5). {@link #UNKNOWN}: không còn thấy job (vd SparkApplication đã bị xoá do TTL).
 */
public enum JobState {
  RUNNING,
  SUCCEEDED,
  FAILED,
  UNKNOWN
}
