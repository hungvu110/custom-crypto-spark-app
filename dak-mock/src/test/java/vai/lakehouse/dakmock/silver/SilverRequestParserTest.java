package vai.lakehouse.dakmock.silver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.web.DakApiException;
import vai.lakehouse.dakmock.web.DakApiException.Problem;

class SilverRequestParserTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** VTT: recurring request for the whole dataset (a table of the VTT landing root), passthrough mode. */
  private static ObjectNode vtt() {
    ObjectNode n = JSON.createObjectNode();
    n.put("source", "VTT");
    n.put("path", "f001-mpre-call");
    n.put("cron", "30 1 * * *");
    n.put("table", "vtt_f001_mpre_call");
    n.put("purpose", "nạp hằng ngày");
    return n;
  }

  /** NIMS: recurring request for the whole dataset, decrypt-encrypt mode, the requester chooses the columns to encrypt. */
  private static ObjectNode nims() {
    ObjectNode n = JSON.createObjectNode();
    n.put("source", "vtnet");
    n.put("path", "nims");
    n.put("cron", "0 3 * * *");
    n.put("table", "vtnet_nims_infra_cell_4g");
    n.putArray("encryptColumns").add("msisdn").add("imei");
    n.put("keyField", "start_time");
    n.put("purpose", "nạp hằng ngày");
    return n;
  }

  /** EBS 3G/4G: recurring request, passthrough mode, no columns to encrypt. */
  private static ObjectNode ebs() {
    ObjectNode n = JSON.createObjectNode();
    n.put("source", "VTNET");
    n.put("path", "ebs_3g_4g");
    n.put("cron", "30 2 * * *");
    n.put("table", "vtnet_ebs_3g_4g");
    n.put("purpose", "nạp hằng ngày");
    return n;
  }

  private static List<String> fields(ObjectNode body) {
    DakApiException e = catchThrowableOfType(() -> SilverRequestParser.parse(body), DakApiException.class);
    assertThat(e).isNotNull();
    assertThat(e.error()).isEqualTo("invalid_request");
    return e.problems().stream().map(Problem::field).toList();
  }

  @Test
  @DisplayName("phiếu VTT hợp lệ: path là dataset, có giờ chạy, không có batchId và cột mã hoá; outputMode tuỳ chọn; chạy DAG của dataset")
  void validVtt() {
    SilverRequestParser.Parsed p = SilverRequestParser.parse(vtt().put("outputMode", "append"));

    assertThat(p.source()).isEqualTo(SilverSource.VTT);
    assertThat(p.path()).isEqualTo("f001-mpre-call");
    assertThat(p.cron()).isEqualTo("30 1 * * *");
    assertThat(p.outputMode()).isEqualTo("append");
    assertThat(p.encryptColumns()).isEmpty();
    assertThat(p.keyField()).isNull();
    assertThat(p.dagId()).isEqualTo("silver_ingest_vtt_f001_mpre_call");
    assertThat(SilverRequestParser.parse(vtt()).outputMode()).isNull();
  }

  @Test
  @DisplayName("phiếu NIMS hợp lệ: path là dataset, có giờ chạy, cột mã hoá và keyField do người gửi chọn, source không phân biệt hoa thường")
  void validNims() {
    SilverRequestParser.Parsed p = SilverRequestParser.parse(nims());

    assertThat(p.source()).isEqualTo(SilverSource.VTNET);
    assertThat(p.path()).isEqualTo("nims");
    assertThat(p.cron()).isEqualTo("0 3 * * *");
    assertThat(p.encryptColumns()).containsExactly("msisdn", "imei");
    assertThat(p.keyField()).isEqualTo("start_time");
    assertThat(p.dagId()).isEqualTo("silver_ingest_vtnet_nims");
  }

  @Test
  @DisplayName("NIMS không chọn cột nào để mã hoá: encryptColumns thiếu, null hoặc rỗng đều hợp lệ, keyField không bắt buộc")
  void nimsWithoutColumns() {
    ObjectNode missing = nims();
    missing.remove(List.of("encryptColumns", "keyField"));
    ObjectNode empty = nims();
    empty.putArray("encryptColumns");
    empty.put("keyField", "");
    ObjectNode nulls = nims();
    nulls.putNull("encryptColumns");
    nulls.putNull("keyField");

    for (ObjectNode body : List.of(missing, empty, nulls)) {
      SilverRequestParser.Parsed p = SilverRequestParser.parse(body);
      assertThat(p.encryptColumns()).isEmpty();
      assertThat(p.keyField()).isNull();
      assertThat(p.dagId()).isEqualTo("silver_ingest_vtnet_nims");
    }
    ObjectNode keyFieldOnly = nims();
    keyFieldOnly.putArray("encryptColumns");
    assertThat(SilverRequestParser.parse(keyFieldOnly).keyField()).isEqualTo("start_time"); // unused, but valid
  }

  @Test
  @DisplayName("dataset passthrough hợp lệ: không có cột mã hoá, chạy DAG của dataset")
  void validEbs() {
    SilverRequestParser.Parsed p = SilverRequestParser.parse(ebs());

    assertThat(p.encryptColumns()).isEmpty();
    assertThat(p.keyField()).isNull();
    assertThat(p.cron()).isEqualTo("30 2 * * *");
    assertThat(p.dagId()).isEqualTo("silver_ingest_vtnet_ebs_3g_4g");
  }

  @Test
  @DisplayName("path phải tương đối, dạng chuẩn, giữ hoa thường")
  void pathRules() {
    for (String bad : List.of("", "/vtt/x", "vtt/x/", "vtt//x", "hdfs://nn/vtt/x", "a/../b", "a b", "x".repeat(1025))) {
      assertThat(fields(vtt().put("path", bad))).as(bad).containsExactly("path");
    }
  }

  @Test
  @DisplayName("path phải đúng là tên một dataset của nguồn, không phải một ngày hay một lô, và không lẫn dataset của nguồn khác")
  void pathIsTheDatasetOfTheSource() {
    for (String bad : List.of("other", "f001_mpre_call", "F001-MPRE-CALL", "f001-mpre-call/date=20261024", "f001-mpre-call/date=20261024/hour=12",
        "nims", "ebs_3g_4g")) {
      assertThat(fields(vtt().put("path", bad))).as(bad).containsExactly("path");
    }
    for (String bad : List.of("other", "nims_x", "NIMS", "nims/dt=20261013", "nims/dt=20261013/batch=1", "ebs_3g_4g/dt=20261012/batch=1520",
        "nims/", "f001-mpre-call")) {
      assertThat(fields(nims().put("path", bad))).as(bad).containsExactly("path");
    }
  }

  @Test
  @DisplayName("không có batchId: mỗi lần chạy tự lấy mã ngày")
  void batchIdIsNotAllowed() {
    assertThat(fields(vtt().put("batchId", "2026102412"))).containsExactly("batchId");
    assertThat(fields(nims().put("batchId", "20261013"))).containsExactly("batchId");
    assertThat(fields(ebs().put("batchId", "202610121520"))).containsExactly("batchId");
  }

  @Test
  @DisplayName("cron: bắt buộc với mọi phiếu, chỉ '<phút> <giờ> * * *' (hằng ngày) hoặc '<phút> * * * *' (hằng giờ), ghi nguyên văn")
  void cronRules() {
    for (ObjectNode request : List.of(vtt(), nims(), ebs())) {
      for (String bad : List.of("", "03:00", "*/5 * * * *", "* * * * *", "*/2 3 * * *", "0 */2 * * *", "0 3 * * 1", "0 3 1 * *", "60 3 * * *",
          "0 24 * * *", "-1 3 * * *", "0  3 * * *", "0 3 * * * ", " 0 3 * * *", "0 3 * * *;x", "@daily", "0 3 * *", "0 3 * * * *")) {
        assertThat(fields(request.deepCopy().put("cron", bad))).as(bad).containsExactly("cron");
      }
      assertThat(fields((ObjectNode) request.deepCopy().without("cron"))).containsExactly("cron");
      assertThat(fields(request.deepCopy().put("cron", 300))).containsExactly("cron");
      assertThat(fields(request.deepCopy().put("scheduleTime", "03:00"))).containsExactlyInAnyOrder("scheduleTime");
      for (String good : List.of("0 0 * * *", "5 3 * * *", "30 12 * * *", "59 23 * * *", "05 03 * * *", "0 * * * *", "5 * * * *", "59 * * * *")) {
        assertThat(SilverRequestParser.parse(request.deepCopy().put("cron", good)).cron()).isEqualTo(good);
      }
    }
  }

  @Test
  @DisplayName("cron hằng giờ cần dataset có thư mục giờ; nhận ra dạng hằng giờ")
  void hourlyCron() {
    assertThat(SilverRequestParser.isHourly("5 * * * *")).isTrue();
    assertThat(SilverRequestParser.isHourly("5 3 * * *")).isFalse();
    for (SilverDataset dataset : SilverDataset.values()) {
      assertThat(dataset.supportsHours()).as(dataset.id()).isTrue(); // every dataset has an hour_glob in its DAG file
    }
    for (ObjectNode request : List.of(vtt(), nims(), ebs())) {
      assertThat(SilverRequestParser.parse(request.deepCopy().put("cron", "5 * * * *")).cron()).isEqualTo("5 * * * *");
    }
  }

  @Test
  @DisplayName("table: do người gửi chọn, chữ thường/số/_, bắt đầu bằng tiền tố của nguồn")
  void tableRules() {
    for (String bad : List.of("", "VTT_x", "vtt-x", "vtnet_x", "other", "vtt_" + "x".repeat(130), "vtt_x y")) {
      assertThat(fields(vtt().put("table", bad))).as(bad).containsExactly("table");
    }
    assertThat(fields(nims().put("table", "vtt_x"))).containsExactly("table");
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
  @DisplayName("dataset decrypt-encrypt: keyField bắt buộc khi có cột mã hoá, các cột hợp lệ và không trùng, keyField không nằm trong cột")
  void columnRules() {
    assertThat(fields((ObjectNode) nims().without("keyField"))).containsExactly("keyField");
    assertThat(fields(nims().put("keyField", ""))).containsExactly("keyField");
    ObjectNode dup = nims();
    dup.putArray("encryptColumns").add("msisdn").add("MSISDN");
    assertThat(fields(dup)).containsExactly("encryptColumns");
    ObjectNode bad = nims();
    bad.putArray("encryptColumns").add("bad-col");
    assertThat(fields(bad)).containsExactly("encryptColumns");
    ObjectNode tooMany = nims();
    ArrayNode many = tooMany.putArray("encryptColumns");
    for (int i = 0; i < 101; i++) {
      many.add("c" + i);
    }
    assertThat(fields(tooMany)).containsExactly("encryptColumns");
    assertThat(fields(nims().put("keyField", "IMEI"))).containsExactly("keyField");
    assertThat(fields(nims().put("keyField", "bad col"))).containsExactly("keyField");
    assertThat(fields(nims().put("encryptColumns", "msisdn,imei"))).containsExactly("encryptColumns");
  }

  @Test
  @DisplayName("dataset passthrough (VTT, ebs_3g_4g) không được có encryptColumns, keyField")
  void columnsAreNotAllowedOutsideDecryptEncrypt() {
    for (ObjectNode request : List.of(vtt(), ebs())) {
      request.putArray("encryptColumns");
      request.put("keyField", "start_time");
      assertThat(fields(request)).containsExactlyInAnyOrder("encryptColumns", "keyField");
    }
  }

  @Test
  @DisplayName("thân không phải object thì 400, problems rỗng")
  void notAnObject() {
    DakApiException e = catchThrowableOfType(() -> SilverRequestParser.parse(JSON.createArrayNode()), DakApiException.class);
    assertThat(e.error()).isEqualTo("invalid_request");
    assertThat(e.problems()).isEmpty();
  }
}
