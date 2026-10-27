package vai.lakehouse.dakmock.silver;

import java.util.LinkedHashMap;
import java.util.Map;

import vai.lakehouse.dakmock.deid.JobState;

/** Trigger DAG chuẩn hoá silver cho một phiếu đã duyệt, và đọc trạng thái dag run. */
public interface SilverDagTrigger {

  /** Ném {@code JobSubmitException} (thông điệp cố định) nếu không trigger được. */
  void trigger(SilverRequest request);

  JobState state(SilverRequest request);

  /**
   * {@code conf} của dag run, đúng khoá DAG nhận (airflow/dags/silver_ingest_common.py): {@code path}, {@code batchDate},
   * {@code outputMode} nếu người gửi chọn, và với VTNET {@code encryptColumns}, {@code keyField}. Không có key hay bí mật nào.
   */
  static Map<String, Object> conf(SilverRequest r) {
    Map<String, Object> conf = new LinkedHashMap<>();
    conf.put("path", r.path());
    conf.put("batchDate", r.batchDate());
    if (r.outputMode() != null) {
      conf.put("outputMode", r.outputMode());
    }
    if (r.source().encryptsColumns()) {
      conf.put("encryptColumns", r.encryptColumns());
      conf.put("keyField", r.keyField());
    }
    return conf;
  }
}
