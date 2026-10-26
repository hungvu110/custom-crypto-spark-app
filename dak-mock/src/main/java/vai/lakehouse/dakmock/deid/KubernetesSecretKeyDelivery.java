package vai.lakehouse.dakmock.deid;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.http.HttpMethod;
import vai.lakehouse.dakmock.config.DeidProperties;
import vai.lakehouse.dakmock.config.DeidProperties.PortalUser;
import vai.lakehouse.dakmock.key.VaultKeyReader;
import vai.lakehouse.dakmock.key.VaultKeyReader.KeyMaterial;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Giao key khử định danh bằng Secret Kubernetes (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 4.6b): đọc key BẢNG từ Vault
 * của DAK, tạo Secret {@code deid-key-<mã>} trong namespace của job với file {@value #KEY_FILE} chứa {@code base64:<keyPrefix>},
 * và chỉ trả TÊN Secret. Job (DAG Airflow) mount Secret đó; key không đi qua API Airflow, manifest hay log.
 *
 * <p>Mọi lỗi quy về đúng một thông điệp cố định (nằm nguyên văn trong {@code submitError} của phiếu): {@value #NOT_FOUND},
 * {@value #BAD_FORMAT}, {@value #VAULT_DOWN}, {@value #DENIED}, {@value #K8S_DOWN}. Không bao giờ nối nội dung response của Vault
 * hay Kubernetes, hay chính key, vào thông điệp. Quyền {@code secrets create+delete} ở namespace job là cái giá đã chấp nhận
 * (README Q17); vì vậy code chỉ xoá Secret có tên đúng dạng {@code deid-key-\d{4}-\d{6}}.
 */
public class KubernetesSecretKeyDelivery implements KeyDelivery {

  public static final String KEY_FILE = "pseudonym.hmac_v1";

  static final String NOT_FOUND = "table key not found";
  static final String BAD_FORMAT = "table key has unsupported format";
  static final String VAULT_DOWN = "vault unavailable";
  static final String DENIED = "kubernetes denied the request";
  static final String K8S_DOWN = "kubernetes unavailable";
  /** Chỉ khi mã phiếu cho ra tên Secret sai dạng: không thể xảy ra với phiếu do registry tạo. */
  static final String INTERNAL = "internal error while delivering the key";

  private static final Pattern SECRET_NAME = Pattern.compile("deid-key-\\d{4}-\\d{6}");
  /** 32 byte dạng base64 chuẩn có đệm: 43 ký tự rồi một dấu {@code =}. */
  private static final Pattern KEY_32_BYTES = Pattern.compile("[A-Za-z0-9+/]{43}=");

  private final VaultKeyReader vault;
  private final KubernetesApiClient kubernetes;
  private final String secretsPath;
  private final DeidProperties deid;

  public KubernetesSecretKeyDelivery(VaultKeyReader vault, KubernetesApiClient kubernetes, String namespace,
                                     DeidProperties deid) {
    this.vault = vault;
    this.kubernetes = kubernetes;
    this.secretsPath = "/api/v1/namespaces/" + namespace + "/secrets";
    this.deid = deid;
  }

  @Override
  public KeyHandle deliver(DeidRequest request) {
    String name = secretNameOf(request);
    if (name == null) {
      throw new KeyDeliveryException(INTERNAL);
    }
    KeyMaterial key = readKey(request);
    if (!KEY_32_BYTES.matcher(key.keyPrefix()).matches()) {
      throw new KeyDeliveryException(BAD_FORMAT);
    }
    Map<String, Object> secret = secret(request, name, key.keyPrefix());
    try {
      int status = post(secret);
      if (status == 409) {
        // Secret sót từ lần trước (mock restart giữa chừng): thay bằng key hiện tại, đúng một lần, không lặp.
        expect(delete(name), true);
        status = post(secret);
      }
      expect(status, false);
    } catch (KubernetesApiClient.UnavailableException e) {
      throw new KeyDeliveryException(K8S_DOWN);
    }
    return new KeyHandle(name, key.version());
  }

  @Override
  public void release(DeidRequest request) {
    String name = secretNameOf(request);
    if (name == null) {
      return;
    }
    try {
      expect(delete(name), true);
    } catch (KubernetesApiClient.UnavailableException e) {
      throw new KeyDeliveryException(K8S_DOWN);
    }
  }

  private KeyMaterial readKey(DeidRequest request) {
    PortalUser account = deid.portalUsers().stream().filter(u -> request.team().equals(u.team())).findFirst().orElse(null);
    String tenant = account == null || account.tenant() == null ? "" : account.tenant();
    String workspace = account == null || account.workspace() == null ? "" : account.workspace();
    try {
      return vault.read(tenant, workspace, request.database(), request.table());
    } catch (VaultKeyReader.KeyNotFoundException e) {
      throw new KeyDeliveryException(NOT_FOUND);
    } catch (DakApiException e) {
      throw new KeyDeliveryException(VAULT_DOWN);
    }
  }

  private static String secretNameOf(DeidRequest request) {
    String name = request.secretName();
    return SECRET_NAME.matcher(name).matches() ? name : null;
  }

  private static Map<String, Object> secret(DeidRequest request, String name, String keyPrefix) {
    Map<String, Object> labels = new LinkedHashMap<>();
    labels.put("dak.vai/request-code", request.requestCode());
    labels.put("dak.vai/kind", "deid-key");
    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("name", name);
    metadata.put("labels", labels);
    String file = Base64.getEncoder().encodeToString(("base64:" + keyPrefix).getBytes(StandardCharsets.UTF_8));
    Map<String, Object> secret = new LinkedHashMap<>();
    secret.put("apiVersion", "v1");
    secret.put("kind", "Secret");
    secret.put("type", "Opaque");
    secret.put("metadata", metadata);
    secret.put("data", Map.of(KEY_FILE, file));
    return secret;
  }

  private int post(Map<String, Object> secret) {
    return kubernetes.exchange(HttpMethod.POST, secretsPath, secret).status();
  }

  private int delete(String name) {
    return kubernetes.exchange(HttpMethod.DELETE, secretsPath + "/" + name, null).status();
  }

  /** 2xx là đạt; {@code notFoundOk} cho phép 404 (đã xoá rồi); 401/403 là bị từ chối; mọi thứ khác là Kubernetes lỗi. */
  private static void expect(int status, boolean notFoundOk) {
    if (status >= 200 && status < 300 || notFoundOk && status == 404) {
      return;
    }
    throw new KeyDeliveryException(status == 401 || status == 403 ? DENIED : K8S_DOWN);
  }
}
