package vai.lakehouse.dakmock.column;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.key.VaultKeyReader;
import vai.lakehouse.dakmock.key.VaultKeyReader.KeyMaterial;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Key cột: lưu ở Vault KV v2 theo {@code dak.vault.column-path-template}, mỗi phần {@code database}/{@code table}/{@code column}
 * là một đoạn đường dẫn riêng. Key chỉ được TẠO (ghi {@code cas=0}), không bao giờ ghi đè — một cột đã có key thì không bao
 * giờ có key khác (vòng 6 của DAK). Danh sách cột bị khoá giữ trong bộ nhớ; khoá không đụng tới key trong Vault.
 */
@Component
public class ColumnKeyStore {

  private static final int KEY_BYTES = 32;
  private static final SecureRandom RANDOM = new SecureRandom();

  private final VaultKeyReader vault;
  private final String pathTemplate;
  private final Set<ColumnId> disabled = ConcurrentHashMap.newKeySet();

  public ColumnKeyStore(VaultKeyReader vault, DakProperties props) {
    this.vault = vault;
    this.pathTemplate = props.vault().columnPathTemplate();
  }

  public Optional<KeyMaterial> read(ColumnId id) {
    return vault.readAt(path(id));
  }

  /**
   * Tạo key cho cột rồi đọc lại. Nếu cột đã có key (vd được tạo trong lúc YC còn mở) thì giữ key đó, không thay. Trả về key
   * thực sự đang lưu.
   */
  public KeyMaterial create(ColumnId id, String keyValue) {
    vault.createIfAbsent(path(id), keyValue);
    return read(id).orElseThrow(() -> DakApiException.internal("column key " + id.qualified() + " missing after create"));
  }

  /** Giá trị key mới: 32 byte ngẫu nhiên, Base64. */
  public static String newKeyValue() {
    byte[] bytes = new byte[KEY_BYTES];
    RANDOM.nextBytes(bytes);
    return Base64.getEncoder().encodeToString(bytes);
  }

  public void disable(ColumnId id) {
    disabled.add(id);
  }

  public void enable(ColumnId id) {
    disabled.remove(id);
  }

  public boolean isDisabled(ColumnId id) {
    return disabled.contains(id);
  }

  // Cả ba phần đã qua kiểm tra [a-z0-9_] nên đặt thẳng vào đường dẫn, không cần escape.
  private String path(ColumnId id) {
    return pathTemplate
        .replace("{database}", id.database())
        .replace("{table}", id.table())
        .replace("{column}", id.column());
  }
}
