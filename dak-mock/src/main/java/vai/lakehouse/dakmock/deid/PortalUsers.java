package vai.lakehouse.dakmock.deid;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;

/**
 * Tài khoản giả của portal (plan mục 4.8): tra tài khoản theo token. Chỉ là mock; DAK thật dùng phiên đăng nhập của portal.
 *
 * <p>Token được so sánh qua SHA-256 của nó bằng {@link MessageDigest#isEqual} (hằng thời gian, độ dài so sánh luôn cố định),
 * và luôn duyệt hết danh sách, không dừng ở tài khoản khớp đầu tiên, để thời gian xử lý không cho biết token đúng tới đâu hay
 * thuộc tài khoản nào. Không lưu token ở dạng chữ rõ.
 */
@Component
public class PortalUsers {

  private record Entry(PortalUser user, byte[] tokenDigest) {}

  private final boolean enabled;
  private final List<Entry> entries;

  @Autowired
  public PortalUsers(DakProperties props) {
    this(props.deid());
  }

  public PortalUsers(DeidProperties deid) {
    this.enabled = deid.portalEnabled();
    this.entries = deid.portalUsers().stream().map(u -> new Entry(u, digest(u.token()))).toList();
  }

  /** Portal chỉ dùng được khi flow bật và có ít nhất một tài khoản; ngược lại mọi lời gọi {@code /mock-portal/**} trả 404. */
  public boolean enabled() {
    return enabled;
  }

  /** Tài khoản có token khớp chính xác; rỗng nếu token thiếu, sai hoặc portal tắt. */
  public Optional<PortalUser> authenticate(String token) {
    if (!enabled || token == null) {
      return Optional.empty();
    }
    byte[] presented = digest(token);
    PortalUser match = null;
    for (Entry entry : entries) {
      if (MessageDigest.isEqual(entry.tokenDigest(), presented)) {
        match = entry.user();
      }
    }
    return Optional.ofNullable(match);
  }

  private static byte[] digest(String token) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by every Java platform", e);
    }
  }
}
