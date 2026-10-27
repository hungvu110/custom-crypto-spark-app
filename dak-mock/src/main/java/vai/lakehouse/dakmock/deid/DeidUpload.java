package vai.lakehouse.dakmock.deid;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.SourceFile;

/**
 * Nội dung một file phiếu đã được {@link DeidUploadParser} kiểm: tên chữ thường, cột đã gộp trùng, {@code dagId} chỉ có khi
 * flow gửi job bằng Airflow (còn lại là {@code null}), {@code purpose} đã strip, {@code env} là env của job do người gửi nhập (đã kiểm với danh sách được phép, rỗng nếu không nhập). Chưa gắn team và người upload.
 */
public record DeidUpload(String database, String table, List<ColumnSpec> columns, String dagId, String purpose,
                         SourceFile sourceFile, Map<String, String> env) {

  public DeidUpload {
    columns = List.copyOf(columns);
    env = env == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(env));
  }
}
