package vai.lakehouse.dakmock.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.mock.http.MockHttpInputMessage;
import vai.lakehouse.dakmock.web.ApiExceptionHandler.ApiError;
import vai.lakehouse.dakmock.web.DakApiException.Problem;

/**
 * Body lỗi của phiếu khử định danh (docs/dak-deid-flow/API_CONTRACT.md mục 2): lỗi nội dung file mang thêm danh sách
 * {@code problems}; mọi lỗi khác giữ nguyên dạng cũ {@code {error, message, requestId}}, không có trường {@code problems}.
 */
class ApiExceptionHandlerTest {

  private final ApiExceptionHandler handler = new ApiExceptionHandler();
  private final ObjectMapper json = new ObjectMapper();

  @Test
  @DisplayName("invalidFile trả 400 invalid_file kèm problems theo thứ tự")
  void invalidFile_carriesProblems() {
    DakApiException e = DakApiException.invalidFile("The uploaded request has 2 problems", List.of(
        new Problem("columns[1].column", "invalid_column", "name must match [A-Za-z0-9_]{1,128}"),
        new Problem("dagId", "invalid_dag", "dagId is not allowed")));

    ResponseEntity<ApiError> response = handler.handle(e);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    ApiError body = response.getBody();
    assertThat(body).isNotNull();
    assertThat(body.error()).isEqualTo("invalid_file");
    assertThat(body.message()).isEqualTo("The uploaded request has 2 problems");
    assertThat(body.problems()).extracting(Problem::field, Problem::code)
        .containsExactly(tuple("columns[1].column", "invalid_column"), tuple("dagId", "invalid_dag"));
  }

  @Test
  @DisplayName("JSON của lỗi nội dung file có mảng problems với field, code, message")
  void invalidFile_serialisesProblems() throws Exception {
    DakApiException e = DakApiException.invalidFile("The uploaded request has 1 problem",
        List.of(new Problem("purpose", "invalid_purpose", "purpose is required")));

    JsonNode tree = json.readTree(json.writeValueAsString(handler.handle(e).getBody()));

    assertThat(tree.get("error").asText()).isEqualTo("invalid_file");
    assertThat(tree.get("problems")).hasSize(1);
    assertThat(tree.get("problems").get(0).get("field").asText()).isEqualTo("purpose");
    assertThat(tree.get("problems").get(0).get("code").asText()).isEqualTo("invalid_purpose");
    assertThat(tree.get("problems").get(0).get("message").asText()).isEqualTo("purpose is required");
  }

  @Test
  @DisplayName("lỗi cả file (không có problems) không đưa trường problems vào JSON")
  void wholeFileError_hasNoProblemsField() throws Exception {
    DakApiException e = DakApiException.invalidFile("The uploaded file is not valid JSON", List.of());

    JsonNode tree = json.readTree(json.writeValueAsString(handler.handle(e).getBody()));

    assertThat(tree.has("problems")).isFalse();
    assertThat(tree.get("error").asText()).isEqualTo("invalid_file");
  }

  @Test
  @DisplayName("các lỗi cũ giữ nguyên dạng body, không có trường problems")
  void existingErrors_keepTheirShape() throws Exception {
    for (DakApiException e : List.of(DakApiException.notFound("x"), DakApiException.invalidTable(),
        DakApiException.invalidState("CANCELLED"), DakApiException.duplicateRequest("PYC-2026-000001"),
        DakApiException.tooManyActiveRequests())) {
      JsonNode tree = json.readTree(json.writeValueAsString(handler.handle(e).getBody()));

      assertThat(tree.has("problems")).as(e.error()).isFalse();
      assertThat(tree.has("error")).isTrue();
      assertThat(tree.has("message")).isTrue();
    }
  }

  @Test
  @DisplayName("thân request không đọc được thành 400 invalid_request, thông điệp cố định, không lặp lại nội dung")
  void unreadableBody_is400_withAFixedMessage() {
    HttpMessageNotReadableException e = new HttpMessageNotReadableException("JSON parse error near SECRET-BODY",
        new MockHttpInputMessage("{not json SECRET-BODY".getBytes(StandardCharsets.UTF_8)));

    ResponseEntity<ApiError> response = handler.unreadableBody(e);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().error()).isEqualTo("invalid_request");
    assertThat(response.getBody().message()).doesNotContain("SECRET-BODY");
    assertThat(response.getBody().problems()).isEmpty();
  }

  @Test
  @DisplayName("danh sách problems của exception không sửa được từ bên ngoài")
  void problems_areImmutable() {
    DakApiException e = DakApiException.invalidFile("m", new ArrayList<>(List.of(new Problem("a", "invalid_file", "m"))));

    assertThatThrownBy(() -> e.problems().add(new Problem("b", "c", "d")))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(DakApiException.notFound("x").problems()).isEmpty();
  }
}
