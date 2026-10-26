package vai.lakehouse.dakmock.web;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.deid.PortalUsers;

/**
 * Xác thực portal giả cho mọi đường dẫn {@code /mock-portal/**} TRƯỚC khi controller nhận tham số (nên request không đăng nhập
 * luôn nhận 401, kể cả khi thân request sai định dạng).
 *
 * <ul>
 *   <li>Portal tắt (flow tắt hoặc chưa khai tài khoản nào): {@code 404} như không tồn tại, kể cả khi có token.
 *   <li>Thiếu hoặc sai token: {@code 401 invalid_credentials}, cùng một thông điệp cho mọi nguyên nhân và không lặp lại token.
 * </ul>
 */
@Component
public class PortalAuthInterceptor implements HandlerInterceptor {

  public static final String TOKEN_HEADER = "X-Mock-Portal-Token";
  /** Tên thuộc tính request chứa {@link PortalUser} đã xác thực, đọc bằng {@code @RequestAttribute}. */
  public static final String USER_ATTRIBUTE = "vai.lakehouse.dakmock.portalUser";

  private final PortalUsers users;

  public PortalAuthInterceptor(PortalUsers users) {
    this.users = users;
  }

  @Override
  public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
    if (!users.enabled()) {
      throw DakApiException.notFound("Unknown endpoint");
    }
    PortalUser user = users.authenticate(request.getHeader(TOKEN_HEADER)).orElseThrow(DakApiException::invalidCredentials);
    request.setAttribute(USER_ATTRIBUTE, user);
    return true;
  }
}
