package vai.lakehouse.dakmock.deid;

import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;
import vai.lakehouse.dakmock.deid.DeidRequest.State;

/**
 * Audit của flow phiếu khử định danh (docs/dak-deid-flow/API_CONTRACT.md mục 9), logger {@code AUDIT}, một dòng cho mỗi chuyển
 * trạng thái, mỗi lần giao key và mỗi thao tác bị từ chối. Không bao giờ ghi token, key, tên Secret hay {@code purpose}/lý do từ
 * chối (văn bản tự do của người dùng).
 */
final class DeidAudit {

  static final String NONE = "-";
  private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");

  private DeidAudit() {}

  /** Một chuyển trạng thái thành công. {@code extra} là các trường nối thêm (bắt đầu bằng dấu cách) hoặc rỗng. */
  static void transition(DeidRequest r, String from, State to, String actor, String extra) {
    String columns = r.columns().stream().map(ColumnSpec::column).collect(Collectors.joining(","));
    AUDIT.info("DEID_TRANSITION requestCode={} from={} to={} actor={} team={} database={} table={} columns={} dagId={}{} "
            + "result=OK", clean(r.requestCode()), from, to, clean(actor), clean(r.team()), clean(r.database()),
        clean(r.table()), clean(columns), r.dagId() == null ? NONE : clean(r.dagId()), extra);
  }

  /** Một thao tác bị từ chối. {@code code} là mã phiếu đã qua kiểm tra định dạng, hoặc {@link #NONE}. */
  static void error(String action, String actor, String team, String code, String error) {
    AUDIT.info("DEID_REQUEST action={} result=ERROR error={} actor={} team={} requestCode={}", action, error, clean(actor),
        clean(team), clean(code));
  }

  /** Kết quả giao key; {@code keyVersion} là version key bảng, không bao giờ là giá trị key. */
  static void keyDelivery(DeidRequest r, Long keyVersion, boolean ok) {
    AUDIT.info("KEY_DELIVERY requestCode={} keyVersion={} result={}", clean(r.requestCode()),
        keyVersion == null ? NONE : keyVersion, ok ? "OK" : "ERROR");
  }

  /** Mọi giá trị đưa vào audit đã qua kiểm tra bộ ký tự; vẫn thay xuống dòng để một dòng audit không giả được dòng khác. */
  /** Tên env và số lượng (không ghi giá trị) để nối vào dòng audit; rỗng nếu phiếu không có env. */
  static String envSummary(java.util.Map<String, String> env) {
    if (env.isEmpty()) {
      return "";
    }
    return " envNames=" + clean(String.join(",", new java.util.TreeSet<>(env.keySet()))) + " envCount=" + env.size();
  }

  static String clean(String value) {
    return value.replace('\r', ' ').replace('\n', ' ');
  }
}
