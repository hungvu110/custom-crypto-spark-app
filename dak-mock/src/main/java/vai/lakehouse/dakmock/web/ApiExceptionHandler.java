package vai.lakehouse.dakmock.web;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.multipart.support.MissingServletRequestPartException;
import org.springframework.web.servlet.resource.NoResourceFoundException;
import vai.lakehouse.dakmock.web.DakApiException.Problem;

/** Mọi lỗi trả về cùng một dạng body: {@code {"error", "message", "requestId"}}. */
@RestControllerAdvice
public class ApiExceptionHandler {

  private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

  /**
   * Body lỗi (docs/key-access/DAK_API_SPEC.md mục 5.4). {@code problems} chỉ có mặt khi không rỗng (lỗi nội dung file phiếu
   * khử định danh), nên body của mọi lỗi khác giữ nguyên dạng cũ.
   */
  public record ApiError(String error, String message, String requestId,
                         @JsonInclude(JsonInclude.Include.NON_EMPTY) List<Problem> problems) {}

  @ExceptionHandler(DakApiException.class)
  public ResponseEntity<ApiError> handle(DakApiException e) {
    if (e.status().is5xxServerError()) {
      log.error("{} {}: {}", e.status().value(), e.error(), e.internalReason());
    } else {
      log.warn("{} {}: {}", e.status().value(), e.error(), e.internalReason());
    }
    return body(e.status(), e.error(), e.getMessage(), e.problems());
  }

  @ExceptionHandler(NoResourceFoundException.class)
  public ResponseEntity<ApiError> notFound(NoResourceFoundException e) {
    return body(HttpStatus.NOT_FOUND, "not_found", "Unknown endpoint", List.of());
  }

  /** Upload vượt giới hạn multipart của Spring (lớp chặn ngoài; giới hạn chính xác do {@code DeidUploadParser} kiểm). */
  @ExceptionHandler(MaxUploadSizeExceededException.class)
  public ResponseEntity<ApiError> uploadTooLarge(MaxUploadSizeExceededException e) {
    log.warn("400 invalid_file: upload exceeds the multipart limit");
    return body(HttpStatus.BAD_REQUEST, "invalid_file", "The uploaded file is too large", List.of());
  }

  /** Không phải multipart, multipart hỏng hoặc thiếu phần {@code file}. Chỉ ghi loại lỗi, không ghi thông điệp của Spring. */
  @ExceptionHandler({MultipartException.class, MissingServletRequestPartException.class})
  public ResponseEntity<ApiError> notMultipart(Exception e) {
    log.warn("400 invalid_file: {}", e.getClass().getSimpleName());
    return body(HttpStatus.BAD_REQUEST, "invalid_file", "The request must be multipart/form-data with a 'file' part",
        List.of());
  }

  /** Thân request không phải JSON hợp lệ: thông điệp cố định, không lặp lại nội dung thân request. */
  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<ApiError> unreadableBody(HttpMessageNotReadableException e) {
    log.warn("400 invalid_request: unreadable request body");
    return body(HttpStatus.BAD_REQUEST, "invalid_request", "The request body is not valid JSON", List.of());
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ApiError> unexpected(Exception e) {
    log.error("Unexpected error", e);
    return body(HttpStatus.INTERNAL_SERVER_ERROR, "internal_error", "Internal error", List.of());
  }

  private static ResponseEntity<ApiError> body(HttpStatus status, String error, String message, List<Problem> problems) {
    return ResponseEntity.status(status)
        .body(new ApiError(error, message, MDC.get(RequestIdFilter.MDC_KEY), problems));
  }
}
