package vai.lakehouse.dakmock.silver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import vai.lakehouse.dakmock.deid.JobState;

/**
 * Cách DAK đặt lịch cho phiếu chuẩn hoá silver ở Airflow, và đọc lại kết quả. Mọi phiếu là phiếu định kỳ: duyệt một lần, {@link #schedule}
 * ghi {@link #scheduleConfig} vào Airflow Variable của dataset và bật DAG; {@link #unschedule} gỡ lịch; {@link #lastRun} đọc dag run gần
 * nhất.
 */
public interface SilverDagTrigger {

  /** Đăng ký lịch chạy hằng ngày của phiếu; ném {@code JobSubmitException} (thông điệp cố định) nếu không được. */
  void schedule(SilverRequest request);

  /** Gỡ lịch của phiếu (tắt DAG và xoá cấu hình); ném {@code JobSubmitException} nếu không được. */
  void unschedule(SilverRequest request);

  /** Trạng thái dag run gần nhất của phiếu; {@link JobState#UNKNOWN} nếu chưa có dag run nào. */
  JobState lastRun(SilverRequest request);

  /**
   * Nội dung Airflow Variable của dataset, đúng khoá DAG factory đọc (airflow/dags/silver_ingest_factory.py): {@code ticket}, {@code table},
   * {@code cron}, {@code outputMode} nếu người gửi chọn, và với dataset decrypt-encrypt {@code encryptColumns} (có thể rỗng) cùng
   * {@code keyField} (chỉ khi có cột mã hoá). Không có key hay bí mật nào.
   */
  static Map<String, Object> scheduleConfig(SilverRequest r) {
    Map<String, Object> config = new LinkedHashMap<>();
    config.put("ticket", r.code());
    config.put("table", r.table());
    config.put("cron", cron(r.scheduleTime()));
    if (r.outputMode() != null) {
      config.put("outputMode", r.outputMode());
    }
    if (r.encryptsColumns()) {
      config.put("encryptColumns", new ArrayList<>(r.encryptColumns()));
      if (r.keyField() != null) {
        config.put("keyField", r.keyField());
      }
    }
    return config;
  }

  /** Cron chạy hằng ngày lúc {@code HH:mm} (múi giờ do DAG đặt): {@code 03:05} thành {@code 5 3 * * *}. */
  static String cron(String scheduleTime) {
    int hour = Integer.parseInt(scheduleTime.substring(0, 2));
    int minute = Integer.parseInt(scheduleTime.substring(3, 5));
    return minute + " " + hour + " * * *";
  }
}
