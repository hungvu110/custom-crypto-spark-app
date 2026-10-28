package vai.lakehouse.dakmock.silver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.web.DakApiException;
import vai.lakehouse.dakmock.web.DakApiException.Problem;

/** Các phiếu mẫu trong {@code k8s/silver/samples/} phải đúng như tên gọi với parser thật: {@code valid-*} hợp lệ, {@code invalid-*} bị từ chối. */
class SilverSampleFilesTest {

  private static final Path SAMPLES = Path.of("k8s/silver/samples");
  private static final ObjectMapper JSON = new ObjectMapper();

  private static SilverRequestParser.Parsed parse(String file) throws IOException {
    return SilverRequestParser.parse(JSON.readTree(Files.readString(SAMPLES.resolve(file))));
  }

  private static List<String> rejectedFields(String file) {
    DakApiException e = catchThrowableOfType(() -> parse(file), DakApiException.class);
    assertThat(e).as(file).isNotNull();
    assertThat(e.error()).isEqualTo("invalid_request");
    return e.problems().stream().map(Problem::field).toList();
  }

  @Test
  @DisplayName("every sample is either valid-* or invalid-*")
  void namingConvention() throws IOException {
    try (var files = Files.list(SAMPLES)) {
      assertThat(files.map(f -> f.getFileName().toString()).toList())
          .isNotEmpty()
          .allMatch(n -> n.matches("(valid|invalid)-[a-z0-9-]+\\.json"));
    }
  }

  @Test
  @DisplayName("the VTT sample is valid: a recurring request for the dataset, no columns to encrypt")
  void vtt() throws IOException {
    SilverRequestParser.Parsed p = parse("valid-vtt-request.json");

    assertThat(p.source()).isEqualTo(SilverSource.VTT);
    assertThat(p.encryptColumns()).isEmpty();
    assertThat(p.path()).isEqualTo("f001-mpre-call");
    assertThat(p.scheduleTime()).isEqualTo("01:30");
    assertThat(p.table()).isEqualTo("vtt_f001_mpre_call");
    assertThat(p.dagId()).isEqualTo("silver_ingest_vtt_f001_mpre_call");
  }

  @Test
  @DisplayName("the NIMS samples are valid: a recurring request for the dataset with the daily run time and the chosen columns")
  void vtnet() throws IOException {
    SilverRequestParser.Parsed p = parse("valid-vtnet-request.json");
    assertThat(p.source()).isEqualTo(SilverSource.VTNET);
    assertThat(p.path()).isEqualTo("nims");
    assertThat(p.scheduleTime()).isEqualTo("03:00");
    assertThat(p.encryptColumns()).containsExactly("lng", "lat");
    assertThat(p.keyField()).isEqualTo("eci");
    assertThat(p.outputMode()).isNull();
    assertThat(p.table()).isEqualTo("vtnet_nims_infra_cell_4g_enc");
    assertThat(p.dagId()).isEqualTo("silver_ingest_vtnet_nims");

    assertThat(parse("valid-vtnet-append-request.json").outputMode()).isEqualTo("append");
    assertThat(parse("valid-vtnet-append-request.json").scheduleTime()).isEqualTo("04:30");
  }

  @Test
  @DisplayName("the NIMS sample without columns only decrypts the file; the passthrough sample goes to its own DAG")
  void noColumnsAndPassthrough() throws IOException {
    SilverRequestParser.Parsed decryptOnly = parse("valid-vtnet-no-columns-request.json");
    assertThat(decryptOnly.encryptColumns()).isEmpty();
    assertThat(decryptOnly.keyField()).isNull();
    assertThat(decryptOnly.dagId()).isEqualTo("silver_ingest_vtnet_nims");

    SilverRequestParser.Parsed passthrough = parse("valid-vtnet-passthrough-request.json");
    assertThat(passthrough.encryptColumns()).isEmpty();
    assertThat(passthrough.dagId()).isEqualTo("silver_ingest_vtnet_ebs_3g_4g");
    assertThat(passthrough.scheduleTime()).isEqualTo("02:30");
  }

  @Test
  @DisplayName("the invalid samples report the expected fields")
  void invalid() {
    assertThat(rejectedFields("invalid-vtt-with-columns.json")).containsExactlyInAnyOrder("encryptColumns", "keyField");
    assertThat(rejectedFields("invalid-vtt-day-in-path.json")).containsExactly("path");
    assertThat(rejectedFields("invalid-vtnet-columns-without-key-field.json")).containsExactly("keyField");
    assertThat(rejectedFields("invalid-vtnet-passthrough-with-columns.json")).containsExactlyInAnyOrder("encryptColumns", "keyField");
    assertThat(rejectedFields("invalid-vtnet-day-in-path.json")).containsExactly("path");
    assertThat(rejectedFields("invalid-vtnet-with-batch-id.json")).containsExactly("batchId");
    assertThat(rejectedFields("invalid-vtnet-missing-schedule-time.json")).containsExactly("scheduleTime");
    assertThat(rejectedFields("invalid-vtnet-unknown-dataset.json")).containsExactly("path");
    assertThat(rejectedFields("invalid-many-errors.json"))
        .containsExactlyInAnyOrder("source", "path", "batchId", "table", "outputMode", "outputDatabase", "purpose");
    assertThat(rejectedFields("invalid-table-of-another-source.json")).containsExactly("table");
  }
}
