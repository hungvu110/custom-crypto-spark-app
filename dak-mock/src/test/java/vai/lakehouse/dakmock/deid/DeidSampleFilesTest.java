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
    // Không cứng số cột: người dùng sửa phiếu mẫu cho bảng của mình, miễn là vẫn hợp lệ.
    assertThat(parse("valid-request.json").columns()).isNotEmpty();
    assertThat(parse("valid-single-column.json").columns()).hasSize(1);
  }

  @Test
  @DisplayName("the sample with env is accepted and keeps the env the requester entered")
  void sampleWithEnv_keepsTheEnv() throws IOException {
    DeidUpload upload = parse("valid-request-with-env.json");

    assertThat(upload.env()).containsExactly(java.util.Map.entry("OUTPUT_DB", "demo_db"),
        java.util.Map.entry("OUTPUT_FORMAT", "delta"), java.util.Map.entry("OUTPUT_MODE", "overwrite"),
        java.util.Map.entry("OUTPUT_TABLE", "users_cdr_2_pseudonymized"));
  }

  @Test
  @DisplayName("the invalid env sample reports one problem per bad env and echoes no value")
  void invalidEnvSample_reportedPerEnv() {
    DakApiException e = rejected("invalid-env.json");

    assertThat(e.problems()).extracting(DakApiException.Problem::field, DakApiException.Problem::code)
        .containsExactly(org.assertj.core.groups.Tuple.tuple("env.DB_NAME", "invalid_env"),
            org.assertj.core.groups.Tuple.tuple("env.OUTPUT_FORMAT", "invalid_env"),
            org.assertj.core.groups.Tuple.tuple("env.OUTPUT_MODE", "invalid_env"),
            org.assertj.core.groups.Tuple.tuple("env.SOME_UNKNOWN_ENV", "invalid_env"));
    assertThat(e.problems().toString()).doesNotContain("replace-everything").doesNotContain("another_table");
  }

  @Test
  @DisplayName("the sample whose output table equals the source table is rejected")
  void envOutputSameAsSource_rejected() {
    DakApiException e = rejected("invalid-env-output-same-as-source.json");

    assertThat(e.problems()).extracting(DakApiException.Problem::field, DakApiException.Problem::message)
        .containsExactly(org.assertj.core.groups.Tuple.tuple("env.OUTPUT_TABLE",
            "the output table must differ from the source table"));
  }

  @Test
  @DisplayName("the sample with many mistakes reports a problem for each mistaken field")
  void manyMistakes_allReported() {
    DakApiException e = rejected("invalid-many-errors.json");

    assertThat(e.error()).isEqualTo("invalid_file");
    assertThat(e.problems()).extracting(DakApiException.Problem::field)
        .contains("database", "table", "dagId", "purpose");
  }

  @Test
  @DisplayName("the samples with an unknown field or broken JSON are rejected")
  void brokenSamples_rejected() {
    assertThat(rejected("invalid-unknown-field.json").error()).isEqualTo("invalid_file");
    assertThat(rejected("invalid-broken-json.json").error()).isEqualTo("invalid_file");
  }
}
