package vai.lakehouse.dakmock.web;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.silver.SilverRequest;
import vai.lakehouse.dakmock.silver.SilverRequestParser;
import vai.lakehouse.dakmock.silver.SilverRequestService;

/**
 * Phiếu chuẩn hoá silver phía người gửi (portal giả, cùng tài khoản và header {@code X-Mock-Portal-Token} với phiếu khử định danh).
 * Thân JSON: {@code source}, {@code path}, {@code batchId}, {@code outputMode} (tuỳ chọn), {@code encryptColumns} và {@code keyField}
 * (chỉ VTNET), {@code purpose}. Xem {@link SilverRequestParser}.
 */
@RestController
@RequestMapping("/mock-portal/silver-requests")
public class MockPortalSilverController {

  private final SilverRequestService service;

  public MockPortalSilverController(SilverRequestService service) {
    this.service = service;
  }

  @PostMapping
  public ResponseEntity<SilverRequest> submit(@RequestAttribute(PortalAuthInterceptor.USER_ATTRIBUTE) PortalUser user,
                                              @RequestBody(required = false) JsonNode body) {
    return ResponseEntity.status(HttpStatus.CREATED).body(service.submit(user, SilverRequestParser.parse(body)));
  }

  @GetMapping
  public List<SilverRequest> list(@RequestAttribute(PortalAuthInterceptor.USER_ATTRIBUTE) PortalUser user) {
    return service.list(user);
  }

  @GetMapping("/{code}")
  public SilverRequest get(@RequestAttribute(PortalAuthInterceptor.USER_ATTRIBUTE) PortalUser user,
                           @PathVariable("code") String code) {
    return service.get(user, code);
  }

  @PostMapping("/{code}/cancel")
  public SilverRequest cancel(@RequestAttribute(PortalAuthInterceptor.USER_ATTRIBUTE) PortalUser user,
                              @PathVariable("code") String code) {
    return service.cancel(user, code);
  }
}
