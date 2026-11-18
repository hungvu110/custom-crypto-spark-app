package vai.lakehouse.dakmock.global;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;
import vai.lakehouse.dakmock.config.DakProperties;
import vai.lakehouse.dakmock.key.VaultKeyReader;
import vai.lakehouse.dakmock.key.VaultKeyReader.KeyMaterial;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Key global của {@code GET /api/v1/keys/global}: mỗi VÙNG một key, lưu ở Vault KV v2 theo {@code dak.vault.global-key-template}
 * ({@code {zone}} = tên vùng, đã kiểm {@code [a-z0-9_]{1,64}} nên đặt thẳng vào path Vault được). Key chỉ được TẠO ({@code cas=0}), không bao giờ
 * ghi đè hay xoay: dữ liệu đã mã hoá bằng key nào thì chỉ giải mã được bằng đúng key đó (docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md mục 0).
 */
@Component
public class GlobalKeyStore {

  private final VaultKeyReader vault;
  private final String pathTemplate;
  private final Set<String> disabled = ConcurrentHashMap.newKeySet();

  public GlobalKeyStore(VaultKeyReader vault, DakProperties props) {
    this.vault = vault;
    this.pathTemplate = props.vault().globalKeyTemplate();
  }

  public Optional<KeyMaterial> read(String zone) {
    return vault.readAt(vaultPath(zone));
  }

  /** Khoá key của vùng: {@code GET /api/v1/keys/global} trả {@code 403 key_disabled}. Key trong Vault giữ nguyên. */
  public void disable(String zone) {
    disabled.add(zone);
  }

  public void enable(String zone) {
    disabled.remove(zone);
  }

  public boolean isDisabled(String zone) {
    return disabled.contains(zone);
  }

  /** Tạo key cho vùng rồi đọc lại; đã có key thì giữ key đó. Trả về key thực sự đang lưu. */
  public KeyMaterial create(String zone, String keyValue) {
    vault.createIfAbsent(vaultPath(zone), keyValue);
    return read(zone).orElseThrow(() -> DakApiException.internal("global key missing after create"));
  }

  /** Đường dẫn trong Vault (sau {@code {kvMount}/data/}) của key của {@code zone}. */
  public String vaultPath(String zone) {
    return pathTemplate.replace("{zone}", zone);
  }
}
