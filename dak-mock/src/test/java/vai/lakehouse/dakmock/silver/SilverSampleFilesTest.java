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
  @DisplayName("the VTT sample is valid and has no columns to encrypt")
  void vtt() throws IOException {
    SilverRequestParser.Parsed p = parse("valid-vtt-request.json");

    assertThat(p.source()).isEqualTo(SilverSource.VTT);
    assertThat(p.encryptColumns()).isEmpty();
  }

  @Test
  @DisplayName("the VTNET samples are valid and carry the columns chosen by the requester")
  void vtnet() throws IOException {
    SilverRequestParser.Parsed p = parse("valid-vtnet-request.json");
    assertThat(p.source()).isEqualTo(SilverSource.VTNET);
    assertThat(p.encryptColumns()).containsExactly("msisdn", "imei");
    assertThat(p.keyField()).isEqualTo("start_time");
    assertThat(p.outputMode()).isNull();

    assertThat(parse("valid-vtnet-append-request.json").outputMode()).isEqualTo("append");
  }

  @Test
  @DisplayName("the invalid samples report the expected fields")
  void invalid() {
    assertThat(rejectedFields("invalid-vtt-with-columns.json")).containsExactlyInAnyOrder("encryptColumns", "keyField");
    assertThat(rejectedFields("invalid-vtnet-missing-columns.json")).containsExactlyInAnyOrder("encryptColumns", "keyField");
    assertThat(rejectedFields("invalid-many-errors.json"))
        .containsExactlyInAnyOrder("source", "path", "batchDate", "outputMode", "outputTable", "purpose");
  }
}
