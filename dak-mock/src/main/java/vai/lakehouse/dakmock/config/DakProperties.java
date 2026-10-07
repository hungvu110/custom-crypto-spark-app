package vai.lakehouse.dakmock.config;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Cấu hình tĩnh của mock (prefix {@code dak}): registry realm, đăng ký client của team, kết nối Vault.
 * Kiểm tra ngay lúc khởi động — cấu hình sai thì ứng dụng không chạy, thay vì lỗi ở request đầu tiên.
 */
@ConfigurationProperties(prefix = "dak")
public record DakProperties(
    List<Realm> realms,
    List<Client> clients,
    Vault vault,
    @DefaultValue("60s") Duration clockSkew) {

  private static final Pattern NAME = Pattern.compile("[a-z0-9][a-z0-9_-]{0,62}");

  public DakProperties {
    realms = realms == null ? List.of() : List.copyOf(realms);
    clients = clients == null ? List.of() : List.copyOf(clients);
    if (vault == null) {
      throw new IllegalStateException("dak.vault must be configured");
    }
    // Static, nhận tham số: trong compact constructor các field của record CHƯA được gán.
    validate(realms, clients, vault);
  }

  /** Một realm Keycloak (một tenant). Token chỉ được tin nếu {@code iss} khớp chính xác {@code issuer}. */
  public record Realm(String realm, String tenant, String issuer, String jwksUri) {}

  /** Đăng ký client của team: {@code (realm, clientId)} -> workspace, team. Client tắt thì token bị 401. */
  public record Client(String realm, String clientId, String workspace, String team,
                       @DefaultValue("true") boolean enabled) {}

  /**
   * Vault KV v2. {@code pathTemplate} nhận {@code {tenant}}, {@code {workspace}}, {@code {database}}, {@code {table}};
   * mặc định trùng path cũ của column/cdr crypto với tên key hai phần ({@code .../<database>.<table>}).
   */
  public record Vault(
      String addr,
      String token,
      @DefaultValue("kv") String kvMount,
      @DefaultValue("hla-datalake/datalake/spark-application/{database}.{table}") String pathTemplate,
      @DefaultValue("keyPrefix") String keyField,
      @DefaultValue("2s") Duration connectTimeout,
      @DefaultValue("5s") Duration readTimeout) {

    @Override
    public String toString() {
      return "Vault[addr=" + addr + ", kvMount=" + kvMount + ", pathTemplate=" + pathTemplate
          + ", keyField=" + keyField + ", token=" + (isBlank(token) ? "<none>" : "***") + "]";
    }
  }

  private static void validate(List<Realm> realmList, List<Client> clientList, Vault vault) {
    require(!isBlank(vault.addr()), "dak.vault.addr is required");
    require(!isBlank(vault.token()), "dak.vault.token is required (set env VAULT_TOKEN)");
    require(vault.pathTemplate().contains("{database}") && vault.pathTemplate().contains("{table}"),
        "dak.vault.path-template must contain {database} and {table}");

    Set<String> issuers = new HashSet<>();
    Set<String> realmNames = new HashSet<>();
    for (Realm r : realmList) {
      require(!isBlank(r.realm()) && !isBlank(r.issuer()) && !isBlank(r.jwksUri()),
          "dak.realms[*] needs realm, issuer and jwks-uri");
      require(r.tenant() != null && NAME.matcher(r.tenant()).matches(),
          "dak.realms[" + r.realm() + "].tenant must match " + NAME.pattern());
      require(issuers.add(r.issuer()) && realmNames.add(r.realm()), "duplicate realm/issuer: " + r.realm());
    }

    Set<String> clientKeys = new HashSet<>();
    for (Client c : clientList) {
      require(realmNames.contains(c.realm()), "dak.clients[" + c.clientId() + "] refers to unknown realm " + c.realm());
      require(!isBlank(c.clientId()), "dak.clients[*].client-id is required");
      require(c.workspace() != null && NAME.matcher(c.workspace()).matches(),
          "dak.clients[" + c.clientId() + "].workspace must match " + NAME.pattern());
      require(!isBlank(c.team()), "dak.clients[" + c.clientId() + "].team is required");
      // client_id chỉ duy nhất TRONG một realm: khoá đăng ký phải gồm cả realm.
      require(clientKeys.add(c.realm() + "\u0000" + c.clientId()), "duplicate client " + c.realm() + "/" + c.clientId());
    }
  }

  private static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }

  private static void require(boolean ok, String message) {
    if (!ok) {
      throw new IllegalStateException("Invalid DAK mock configuration: " + message);
    }
  }
}
