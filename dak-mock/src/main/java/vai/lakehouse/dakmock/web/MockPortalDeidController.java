package vai.lakehouse.dakmock.web;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.deid.DeidRequest;
import vai.lakehouse.dakmock.deid.DeidRequestService;

/**
 * Portal giả của flow phiếu khử định danh (docs/dak-deid-flow/API_CONTRACT.md mục 4): DAK thật có giao diện đăng nhập + upload,
 * mock mở các endpoint này để chạy thử bằng {@code curl}. Xác thực do {@link PortalAuthInterceptor} làm trước; mọi quy tắc
 * nằm ở {@link DeidRequestService}.
 */
@RestController
@RequestMapping("/mock-portal")
public class MockPortalDeidController {

  /** Danh sách {@code dagId} người dùng được chọn khi điền phiếu. */
  public record DagList(List<String> dagIds) {}

  private final DeidRequestService service;

  public MockPortalDeidController(DeidRequestService service) {
    this.service = service;
  }

  /** Upload phiếu: {@code multipart/form-data}, phần {@code file}. Hợp lệ thì {@code 201}. */
  @PostMapping("/deid-requests")
  public ResponseEntity<DeidRequest> upload(@RequestAttribute(PortalAuthInterceptor.USER_ATTRIBUTE) PortalUser user,
                                            @RequestPart(value = "file", required = false) MultipartFile file) {
    if (file == null) {
      throw DakApiException.invalidFile("The request must be multipart/form-data with a 'file' part", List.of());
    }
    try (InputStream content = file.getInputStream()) {
      return ResponseEntity.status(HttpStatus.CREATED).body(service.upload(user, file.getOriginalFilename(), content));
    } catch (IOException e) {
      throw DakApiException.invalidFile("The uploaded file could not be read", List.of());
    }
  }

  @PostMapping("/deid-requests/{code}/cancel")
  public DeidRequest cancel(@RequestAttribute(PortalAuthInterceptor.USER_ATTRIBUTE) PortalUser user,
                            @PathVariable("code") String code) {
    return service.cancel(user, code);
  }

  @GetMapping("/deid-requests/{code}")
  public DeidRequest get(@RequestAttribute(PortalAuthInterceptor.USER_ATTRIBUTE) PortalUser user,
                         @PathVariable("code") String code) {
    return service.get(user, code);
  }

  @GetMapping("/deid-requests")
  public DeidRequestService.Page list(@RequestAttribute(PortalAuthInterceptor.USER_ATTRIBUTE) PortalUser user,
                                      @RequestParam(value = "state", required = false) String state,
                                      @RequestParam(value = "limit", required = false) String limit,
                                      @RequestParam(value = "cursor", required = false) String cursor) {
    return service.list(user, state, limit, cursor);
  }

  @GetMapping("/deid-dags")
  public DagList dags() {
    return new DagList(service.allowedDagIds());
  }
}
