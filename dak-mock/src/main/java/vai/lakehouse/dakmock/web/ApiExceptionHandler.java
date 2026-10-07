package vai.lakehouse.dakmock.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/** Mọi lỗi trả về cùng một dạng body: {@code {"error", "message", "requestId"}}. */
@RestControllerAdvice
public class ApiExceptionHandler {

  private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

  /** Body lỗi (docs/DAK_API_SPEC.md mục 5.4). */
  public record ApiError(String error, String message, String requestId) {}

  @ExceptionHandler(DakApiException.class)
  public ResponseEntity<ApiError> handle(DakApiException e) {
    if (e.status().is5xxServerError()) {
      log.error("{} {}: {}", e.status().value(), e.error(), e.internalReason());
    } else {
      log.warn("{} {}: {}", e.status().value(), e.error(), e.internalReason());
    }
    return body(e.status(), e.error(), e.getMessage());
  }

  @ExceptionHandler(NoResourceFoundException.class)
  public ResponseEntity<ApiError> notFound(NoResourceFoundException e) {
    return body(HttpStatus.NOT_FOUND, "not_found", "Unknown endpoint");
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ApiError> unexpected(Exception e) {
    log.error("Unexpected error", e);
    return body(HttpStatus.INTERNAL_SERVER_ERROR, "internal_error", "Internal error");
  }

  private static ResponseEntity<ApiError> body(HttpStatus status, String error, String message) {
    return ResponseEntity.status(status).body(new ApiError(error, message, MDC.get(RequestIdFilter.MDC_KEY)));
  }
}
