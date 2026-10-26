package vai.lakehouse.dakmock.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vai.lakehouse.dakmock.deid.DeidAdminService;
import vai.lakehouse.dakmock.deid.DeidRequest;
import vai.lakehouse.dakmock.deid.DeidRequestService;

/**
 * Endpoint admin của flow phiếu khử định danh (docs/dak-deid-flow/API_CONTRACT.md mục 5), cùng token {@code X-Mock-Admin-Token} với
 * {@code /mock-admin/yc/**}: sai, thiếu hoặc chưa bật thì {@code 404}. Ở DAK thật đây là giao diện duyệt phiếu; hành vi và chuyển
 * trạng thái giữ nguyên. Mọi quy tắc nằm ở {@link DeidAdminService}.
 */
@RestController
@RequestMapping("/mock-admin/deid-requests")
public class MockDeidAdminController {

  /** Thân của {@code reject}. {@code reasonText} bắt buộc, 1 đến 500 ký tự. */
  public record RejectRequest(String reasonText) {}

  private final MockAdminAuth auth;
  private final DeidAdminService service;

  public MockDeidAdminController(MockAdminAuth auth, DeidAdminService service) {
    this.auth = auth;
    this.service = service;
  }

  @GetMapping
  public DeidRequestService.Page list(@RequestHeader(value = MockAdminController.TOKEN_HEADER, required = false) String token,
                                      @RequestParam(value = "state", required = false) String state,
                                      @RequestParam(value = "limit", required = false) String limit,
                                      @RequestParam(value = "cursor", required = false) String cursor) {
    auth.authorize(token);
    return service.list(state, limit, cursor);
  }

  @GetMapping("/{code}")
  public DeidRequest get(@RequestHeader(value = MockAdminController.TOKEN_HEADER, required = false) String token,
                         @PathVariable("code") String code) {
    auth.authorize(token);
    return service.get(code);
  }

  /** Duyệt: giao key rồi gửi job. Lỗi giao key hoặc gửi job vẫn trả {@code 200}, lỗi nằm ở {@code state=SUBMIT_FAILED}. */
  @PostMapping("/{code}/approve")
  public DeidRequest approve(@RequestHeader(value = MockAdminController.TOKEN_HEADER, required = false) String token,
                             @PathVariable("code") String code) {
    auth.authorize(token);
    return service.approve(code);
  }

  @PostMapping("/{code}/reject")
  public DeidRequest reject(@RequestHeader(value = MockAdminController.TOKEN_HEADER, required = false) String token,
                            @PathVariable("code") String code,
                            @RequestBody(required = false) RejectRequest body) {
    auth.authorize(token);
    return service.reject(code, body == null ? null : body.reasonText());
  }

  @PostMapping("/{code}/retry-submit")
  public DeidRequest retrySubmit(@RequestHeader(value = MockAdminController.TOKEN_HEADER, required = false) String token,
                                 @PathVariable("code") String code) {
    auth.authorize(token);
    return service.retrySubmit(code);
  }
}
