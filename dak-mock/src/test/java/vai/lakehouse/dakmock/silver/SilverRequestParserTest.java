package vai.lakehouse.dakmock.silver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.web.DakApiException;
import vai.lakehouse.dakmock.web.DakApiException.Problem;

class SilverRequestParserTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static ObjectNode vtt() {
    ObjectNode n = JSON.createObjectNode();
    n.put("source", "VTT");
    n.put("path", "f001-mpre-call/date=20261024/hour=12");
    n.put("batchId", "2026102412");
    n.put("table", "vtt_f001_mpre_call");
    n.put("purpose", "nạp lô ngày 27");
    return n;
  }

  private static ObjectNode vtnet() {
    ObjectNode n = vtt();
    n.put("source", "vtnet");
    n.put("path", "ebs_3g_4g/dt=20261012/batch=1520");
    n.put("batchId", "202610121520");
    n.put("table", "vtnet_ebs_3g_4g");
    n.putArray("encryptColumns").add("msisdn").add("imei");
    n.put("keyField", "start_time");
    return n;
  }

  private static List<String> fields(ObjectNode body) {
    DakApiException e = catchThrowableOfType(() -> SilverRequestParser.parse(body), DakApiException.class);
    assertThat(e).isNotNull();
    assertThat(e.error()).isEqualTo("invalid_request");
    return e.problems().stream().map(Problem::field).toList();
  }

  @Test
  @DisplayName("phiếu VTT hợp lệ: không có cột mã hoá; outputMode tuỳ chọn")
  void validVtt() {
    SilverRequestParser.Parsed p = SilverRequestParser.parse(vtt().put("outputMode", "append"));

    assertThat(p.source()).isEqualTo(SilverSource.VTT);
    assertThat(p.path()).isEqualTo("f001-mpre-call/date=20261024/hour=12");
    assertThat(p.outputMode()).isEqualTo("append");
    assertThat(p.encryptColumns()).isEmpty();
    assertThat(p.keyField()).isNull();
    assertThat(SilverRequestParser.parse(vtt()).outputMode()).isNull();
  }

  @Test
  @DisplayName("phiếu VTNET hợp lệ: có cột mã hoá và keyField do người gửi chọn, source không phân biệt hoa thường")
  void validVtnet() {
    SilverRequestParser.Parsed p = SilverRequestParser.parse(vtnet());

    assertThat(p.source()).isEqualTo(SilverSource.VTNET);
    assertThat(p.encryptColumns()).containsExactly("msisdn", "imei");
    assertThat(p.keyField()).isEqualTo("start_time");
  }

  @Test
  @DisplayName("path phải tương đối, dạng chuẩn, giữ hoa thường")
  void pathRules() {
    for (String bad : List.of("", "/vtt/x", "vtt/x/", "vtt//x", "hdfs://nn/vtt/x", "a/../b", "a b", "x".repeat(1025))) {
      assertThat(fields(vtt().put("path", bad))).as(bad).containsExactly("path");
    }
    assertThat(SilverRequestParser.parse(vtt().put("path", "VTT_cdr/File-1.csv")).path()).isEqualTo("VTT_cdr/File-1.csv");
  }

  @Test
  @DisplayName("batchId: yyyyMMddHH hoặc yyyyMMddHHmm, ngày giờ phút có thật; ngày không có giờ bị từ chối")
  void batchIdRules() {
    for (String bad : List.of("", "20261027", "2026-10-27", "2026102425", "202610271260", "202610271", "2026102412345", "20261340AB")) {
      assertThat(fields(vtt().put("batchId", bad))).as(bad).containsExactly("batchId");
    }
    assertThat(SilverRequestParser.parse(vtt().put("batchId", "2026102400")).batchId()).isEqualTo("2026102400");
    assertThat(SilverRequestParser.parse(vtnet().put("batchId", "202610121540")).batchId()).isEqualTo("202610121540");
  }

  @Test
  @DisplayName("table: do người gửi chọn, chữ thường/số/_, bắt đầu bằng tiền tố của nguồn")
  void tableRules() {
    for (String bad : List.of("", "VTT_x", "vtt-x", "vtnet_x", "other", "vtt_" + "x".repeat(130), "vtt_x y")) {
      assertThat(fields(vtt().put("table", bad))).as(bad).containsExactly("table");
    }
    assertThat(fields(vtnet().put("table", "vtt_x"))).containsExactly("table");
    assertThat(fields((ObjectNode) vtt().without("table"))).containsExactly("table");
  }

  @Test
  @DisplayName("gom mọi lỗi một lượt, kể cả trường lạ; thông điệp không lặp lại giá trị")
  void allProblemsAtOnce() {
    ObjectNode body = JSON.createObjectNode();
    body.put("source", "VTX");
    body.put("path", "/abs/secret-value");
    body.put("batchId", "20261340");
    body.put("outputMode", "overwrite");
    body.put("evil\nfield", 1);

    DakApiException e = catchThrowableOfType(() -> SilverRequestParser.parse(body), DakApiException.class);

    assertThat(e.problems()).extracting(Problem::field)
        .containsExactlyInAnyOrder("evil_field", "source", "path", "batchId", "table", "outputMode", "purpose");
    assertThat(e.problems().toString()).doesNotContain("secret-value").doesNotContain("VTX");
  }

  @Test
  @DisplayName("VTNET bắt buộc encryptColumns và keyField hợp lệ; VTT không được có hai trường này")
  void columnRules() {
    assertThat(fields((ObjectNode) vtnet().without("encryptColumns"))).containsExactly("encryptColumns");
    assertThat(fields((ObjectNode) vtnet().without("keyField"))).containsExactly("keyField");
    ObjectNode empty = vtnet();
    empty.putArray("encryptColumns");
    assertThat(fields(empty)).containsExactly("encryptColumns");
    ObjectNode dup = vtnet();
    dup.putArray("encryptColumns").add("msisdn").add("MSISDN");
    assertThat(fields(dup)).containsExactly("encryptColumns");
    ObjectNode bad = vtnet();
    bad.putArray("encryptColumns").add("bad-col");
    assertThat(fields(bad)).containsExactly("encryptColumns");
    assertThat(fields(vtnet().put("keyField", "IMEI"))).containsExactly("keyField");
    assertThat(fields(vtnet().put("encryptColumns", "msisdn,imei"))).containsExactly("encryptColumns");
    ObjectNode vttWithColumns = vtt();
    vttWithColumns.putArray("encryptColumns").add("msisdn");
    vttWithColumns.put("keyField", "start_time");
    assertThat(fields(vttWithColumns)).containsExactlyInAnyOrder("encryptColumns", "keyField");
  }

  @Test
  @DisplayName("thân không phải object thì 400, problems rỗng")
  void notAnObject() {
    DakApiException e = catchThrowableOfType(() -> SilverRequestParser.parse(JSON.createArrayNode()), DakApiException.class);
    assertThat(e.error()).isEqualTo("invalid_request");
    assertThat(e.problems()).isEmpty();
  }
}
