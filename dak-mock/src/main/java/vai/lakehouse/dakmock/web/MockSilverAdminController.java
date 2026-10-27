package vai.lakehouse.dakmock.web;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import vai.lakehouse.dakmock.silver.SilverRequest;
import vai.lakehouse.dakmock.silver.SilverRequestService;

/** Admin duyệt phiếu chuẩn hoá silver (cùng header {@code X-Mock-Admin-Token}; sai hoặc thiếu thì {@code 404}). */
@RestController
@RequestMapping("/mock-admin/silver-requests")
public class MockSilverAdminController {

  public record RejectRequest(String reasonText) {}

  private final MockAdminAuth auth;
  private final SilverRequestService service;

  public MockSilverAdminController(MockAdminAuth auth, SilverRequestService service) {
    this.auth = auth;
    this.service = service;
  }

  @GetMapping
  public List<SilverRequest> list(@RequestHeader(value = MockAdminController.TOKEN_HEADER, required = false) String token) {
    auth.authorize(token);
    return service.adminList();
  }

  @GetMapping("/{code}")
  public SilverRequest get(@RequestHeader(value = MockAdminController.TOKEN_HEADER, required = false) String token,
                           @PathVariable("code") String code) {
    auth.authorize(token);
    return service.adminGet(code);
  }

  /** Duyệt rồi trigger DAG của nguồn ngay; trigger lỗi thì phiếu {@code SUBMIT_FAILED}. */
  @PostMapping("/{code}/approve")
  public SilverRequest approve(@RequestHeader(value = MockAdminController.TOKEN_HEADER, required = false) String token,
                               @PathVariable("code") String code) {
    auth.authorize(token);
    return service.approve(code);
  }

  @PostMapping("/{code}/reject")
  public SilverRequest reject(@RequestHeader(value = MockAdminController.TOKEN_HEADER, required = false) String token,
                              @PathVariable("code") String code, @RequestBody(required = false) RejectRequest body) {
    auth.authorize(token);
    return service.reject(code, body == null ? null : body.reasonText());
  }

  @PostMapping("/{code}/retry-submit")
  public SilverRequest retrySubmit(@RequestHeader(value = MockAdminController.TOKEN_HEADER, required = false) String token,
                                   @PathVariable("code") String code) {
    auth.authorize(token);
    return service.retrySubmit(code);
  }
}
