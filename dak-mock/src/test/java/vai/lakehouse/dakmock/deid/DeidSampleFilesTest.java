package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.Airflow;
import vai.lakehouse.dakmock.config.DeidProperties.AirflowAuth;
import vai.lakehouse.dakmock.config.DeidProperties.Kubernetes;
import vai.lakehouse.dakmock.config.DeidProperties.Submitter;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Các file phiếu mẫu trong {@code k8s/deid/samples/} phải đúng như tên gọi với parser thật (mẫu hợp lệ thì được nhận, mẫu lỗi thì
 * bị từ chối), để tài liệu và mẫu không lệch khỏi code.
 */
class DeidSampleFilesTest {

  private static final Path SAMPLES = Path.of("k8s/deid/samples");

  private static DeidUploadParser parser() {
    DeidProperties deid = new DeidProperties(true, null, 20, 65536, null, null, Submitter.AIRFLOW,
        new Kubernetes(null, "ns", null),
        new Airflow("http://airflow:8080", List.of("deid_pseudonymize_table"), AirflowAuth.BEARER, null, null, null, null,
            "t"), null);
    return new DeidUploadParser(deid);
  }

  private static DeidUpload parse(String file) throws IOException {
    try (InputStream in = Files.newInputStream(SAMPLES.resolve(file))) {
      return parser().parse(file, in);
    }
  }

  private static DakApiException rejected(String file) {
    Throwable thrown = catchThrowable(() -> parse(file));
    assertThat(thrown).isInstanceOf(DakApiException.class);
    return (DakApiException) thrown;
  }

  @Test
  @DisplayName("the valid samples are accepted")
  void validSamples_accepted() throws IOException {
    assertThat(parse("phieu-hop-le.json").columns()).hasSize(2);
    assertThat(parse("phieu-mot-cot.json").columns()).hasSize(1);
  }

  @Test
  @DisplayName("the sample with many mistakes reports a problem for each mistaken field")
  void manyMistakes_allReported() {
    DakApiException e = rejected("phieu-loi-nhieu-truong.json");

    assertThat(e.error()).isEqualTo("invalid_file");
    assertThat(e.problems()).extracting(DakApiException.Problem::field)
        .contains("database", "table", "dagId", "purpose");
  }

  @Test
  @DisplayName("the samples with an unknown field or broken JSON are rejected")
  void brokenSamples_rejected() {
    assertThat(rejected("phieu-loi-truong-la.json").error()).isEqualTo("invalid_file");
    assertThat(rejected("phieu-loi-json-hong.json").error()).isEqualTo("invalid_file");
  }
}
