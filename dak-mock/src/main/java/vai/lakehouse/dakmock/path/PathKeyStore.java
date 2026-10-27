package vai.lakehouse.dakmock.path;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;

import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.key.VaultKeyReader;
import vai.lakehouse.dakmock.key.VaultKeyReader.KeyMaterial;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Key của {@code by-path}: lưu ở Vault KV v2 theo {@code dak.vault.path-key-template}, {@code {pathHash}} = SHA-256 hex của
 * đường dẫn (đường dẫn chứa {@code /} và ký tự tuỳ ý nên không đặt thẳng vào path Vault). Key chỉ được TẠO ({@code cas=0}),
 * không bao giờ ghi đè: file đã mã hoá bằng key nào thì chỉ giải mã được bằng đúng key đó.
 */
@Component
public class PathKeyStore {

  private final VaultKeyReader vault;
  private final String pathTemplate;
  private final java.util.Set<String> disabled = java.util.concurrent.ConcurrentHashMap.newKeySet();

  public PathKeyStore(VaultKeyReader vault, DakProperties props) {
    this.vault = vault;
    this.pathTemplate = props.vault().pathKeyTemplate();
  }

  public Optional<KeyMaterial> read(String path) {
    return vault.readAt(vaultPath(path));
  }

  /** Khoá key của đường dẫn đã có key: {@code by-path} trả {@code 403 key_disabled}. Key trong Vault giữ nguyên. */
  public void disable(String path) {
    disabled.add(path);
  }

  public void enable(String path) {
    disabled.remove(path);
  }

  public boolean isDisabled(String path) {
    return disabled.contains(path);
  }

  /** Tạo key cho đường dẫn rồi đọc lại; đã có key thì giữ key đó. Trả về key thực sự đang lưu. */
  public KeyMaterial create(String path, String keyValue) {
    vault.createIfAbsent(vaultPath(path), keyValue);
    return read(path).orElseThrow(() -> DakApiException.internal("path key missing after create"));
  }

  /** Đường dẫn trong Vault (sau {@code {kvMount}/data/}) của key của {@code path}. */
  public String vaultPath(String path) {
    return pathTemplate.replace("{pathHash}", sha256(path));
  }

  static String sha256(String path) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(path.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
