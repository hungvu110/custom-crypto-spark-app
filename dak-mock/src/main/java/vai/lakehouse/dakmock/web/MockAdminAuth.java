package vai.lakehouse.dakmock.web;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.config.DakProperties;

/**
 * Kiểm token của endpoint quản trị {@code /mock-admin/**} (header {@code X-Mock-Admin-Token}), dùng chung cho endpoint YC cũ và
 * endpoint phiếu khử định danh. Chưa cấu hình token, thiếu hoặc sai token đều trả {@code 404 Unknown endpoint} như endpoint không
 * tồn tại, không để lộ là có endpoint. So sánh qua SHA-256 của token bằng {@link MessageDigest#isEqual} (hằng thời gian, độ dài
 * so sánh cố định); thông điệp lỗi không bao giờ lặp lại token.
 */
@Component
public class MockAdminAuth {

  private final byte[] tokenDigest;

  @Autowired
  public MockAdminAuth(DakProperties props) {
    this(props.admin());
  }

  public MockAdminAuth(DakProperties.Admin admin) {
    this.tokenDigest = admin.enabled() ? sha256(admin.token()) : null;
  }

  /** Ném {@code 404} nếu {@code token} không phải token quản trị đã cấu hình. */
  public void authorize(String token) {
    boolean ok = tokenDigest != null && token != null && MessageDigest.isEqual(tokenDigest, sha256(token));
    if (!ok) {
      throw DakApiException.notFound("Unknown endpoint");
    }
  }

  private static byte[] sha256(String token) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by every Java platform", e);
    }
  }
}
