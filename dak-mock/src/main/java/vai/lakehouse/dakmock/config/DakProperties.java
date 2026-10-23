package vai.lakehouse.dakmock.config;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Cấu hình tĩnh của mock (prefix {@code dak}): registry realm, đăng ký client, grant theo cột của client team, kết nối Vault,
 * YC và endpoint quản trị. Kiểm tra ngay lúc khởi động — cấu hình sai thì ứng dụng không chạy, thay vì lỗi ở request đầu tiên.
 */
@ConfigurationProperties(prefix = "dak")
public record DakProperties(
    List<Realm> realms,
    List<Client> clients,
    List<Grant> grants,
    Vault vault,
    Yc yc,
    Admin admin,
    @DefaultValue("60s") Duration clockSkew) {

  private static final Pattern NAME = Pattern.compile("[a-z0-9][a-z0-9_-]{0,62}");
  private static final Pattern HIVE_NAME = Pattern.compile("[A-Za-z0-9_]{1,128}");

  public DakProperties {
    realms = realms == null ? List.of() : List.copyOf(realms);
    clients = clients == null ? List.of() : List.copyOf(clients);
    grants = grants == null ? List.of() : List.copyOf(grants);
    yc = yc == null ? new Yc(Yc.DEFAULT_MAX_OPEN_PER_TABLE, false) : yc;
    admin = admin == null ? new Admin(null) : admin;
    if (vault == null) {
      throw new IllegalStateException("dak.vault must be configured");
    }
    // Static, nhận tham số: trong compact constructor các field của record CHƯA được gán.
    validate(realms, clients, grants, vault, yc);
  }

  /** Một realm Keycloak (một tenant). Token chỉ được tin nếu {@code iss} khớp chính xác {@code issuer}. */
  public record Realm(String realm, String tenant, String issuer, String jwksUri) {}

  /**
   * Đăng ký client: {@code (realm, clientId)}. Client team ánh xạ sang workspace, team; client nội bộ (luồng YC của
   * {@code by-column}) không thuộc team nào. Client tắt thì token bị 401.
   */
  public record Client(String realm, String clientId, String workspace, String team,
                       @DefaultValue("true") boolean enabled,
                       @DefaultValue("team") ClientKind kind) {}

  /**
   * Quyền lấy key theo cột của một team (mock thay cho phiếu được duyệt của DAK). {@code expiresAt} dạng ISO-8601, để trống =
   * không hết hạn. Tên bảng/cột được chuẩn hoá chữ thường.
   */
  public record Grant(String team, String database, String table, List<String> columns, String expiresAt) {

    public Grant {
      database = lower(database);
      table = lower(table);
      columns = columns == null ? List.of() : columns.stream().map(Grant::lower).toList();
    }

    /** Grant còn hiệu lực tại {@code now}. */
    public boolean activeAt(Instant now) {
      return expiresAt == null || expiresAt.isBlank() || Instant.parse(expiresAt).isAfter(now);
    }

    private static String lower(String s) {
      return s == null ? null : s.toLowerCase(Locale.ROOT);
    }
  }

  /**
   * Vault KV v2. {@code pathTemplate} (key cả bảng) nhận {@code {tenant}}, {@code {workspace}}, {@code {database}},
   * {@code {table}}; mặc định trùng path cũ của column/cdr crypto. {@code columnPathTemplate} (key cột) nhận
   * {@code {database}}, {@code {table}}, {@code {column}}, mỗi phần là một đoạn đường dẫn riêng.
   */
  public record Vault(
      String addr,
      String token,
      @DefaultValue("kv") String kvMount,
      @DefaultValue("hla-datalake/datalake/spark-application/{database}.{table}") String pathTemplate,
      @DefaultValue("hla-datalake/datalake/column-keys/{database}/{table}/{column}") String columnPathTemplate,
      @DefaultValue("keyPrefix") String keyField,
      @DefaultValue("2s") Duration connectTimeout,
      @DefaultValue("5s") Duration readTimeout) {

    @Override
    public String toString() {
      return "Vault[addr=" + addr + ", kvMount=" + kvMount + ", pathTemplate=" + pathTemplate
          + ", columnPathTemplate=" + columnPathTemplate + ", keyField=" + keyField
          + ", token=" + (isBlank(token) ? "<none>" : "***") + "]";
    }
  }

  /**
   * YC của luồng nội bộ. {@code maxOpenPerTable}: số YC {@code OPEN} tối đa cho mỗi {@code (client, database, table)}.
   * {@code autoFulfill}: tạo key ngay thay vì mở YC — chỉ để chạy thử đầu-cuối nhanh, DAK thật không làm vậy.
   */
  public record Yc(@DefaultValue("50") int maxOpenPerTable, @DefaultValue("false") boolean autoFulfill) {
    public static final int DEFAULT_MAX_OPEN_PER_TABLE = 50;
  }

  /** Endpoint quản trị của mock ({@code /mock-admin/**}). Token trống = tắt hẳn. */
  public record Admin(String token) {

    public boolean enabled() {
      return !isBlank(token);
    }

    @Override
    public String toString() {
      return "Admin[token=" + (isBlank(token) ? "<none>" : "***") + "]";
    }
  }

  private static void validate(List<Realm> realmList, List<Client> clientList, List<Grant> grantList, Vault vault, Yc yc) {
    require(!isBlank(vault.addr()), "dak.vault.addr is required");
    require(!isBlank(vault.token()), "dak.vault.token is required (set env VAULT_TOKEN)");
    require(vault.pathTemplate().contains("{database}") && vault.pathTemplate().contains("{table}"),
        "dak.vault.path-template must contain {database} and {table}");
    require(vault.columnPathTemplate().contains("{database}") && vault.columnPathTemplate().contains("{table}")
            && vault.columnPathTemplate().contains("{column}"),
        "dak.vault.column-path-template must contain {database}, {table} and {column}");
    require(yc.maxOpenPerTable() > 0, "dak.yc.max-open-per-table must be positive");

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
      if (c.kind() == ClientKind.TEAM) {
        require(c.workspace() != null && NAME.matcher(c.workspace()).matches(),
            "dak.clients[" + c.clientId() + "].workspace must match " + NAME.pattern());
        require(!isBlank(c.team()), "dak.clients[" + c.clientId() + "].team is required");
      }
      // client_id chỉ duy nhất TRONG một realm: khoá đăng ký phải gồm cả realm.
      require(clientKeys.add(c.realm() + "\u0000" + c.clientId()), "duplicate client " + c.realm() + "/" + c.clientId());
    }

    for (Grant g : grantList) {
      String where = "dak.grants[team=" + g.team() + "]";
      require(!isBlank(g.team()), "dak.grants[*].team is required");
      require(g.database() != null && HIVE_NAME.matcher(g.database()).matches()
          && g.table() != null && HIVE_NAME.matcher(g.table()).matches(), where + " needs valid database and table");
      require(!g.columns().isEmpty() && g.columns().stream().allMatch(col -> HIVE_NAME.matcher(col).matches()),
          where + " needs at least one column matching " + HIVE_NAME.pattern());
      if (!isBlank(g.expiresAt())) {
        try {
          Instant.parse(g.expiresAt());
        } catch (DateTimeParseException e) {
          throw new IllegalStateException("Invalid DAK mock configuration: " + where
              + ".expires-at must be an ISO-8601 instant, e.g. 2027-01-01T00:00:00Z");
        }
      }
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
