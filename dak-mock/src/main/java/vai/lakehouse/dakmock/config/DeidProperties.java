package vai.lakehouse.dakmock.config;

import static vai.lakehouse.dakmock.config.DakProperties.isBlank;
import static vai.lakehouse.dakmock.config.DakProperties.require;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Cấu hình flow phiếu khử định danh (PYC), prefix {@code dak.deid}: portal giả, tài khoản giả, cách gửi job
 * (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 2). Kiểm tra ngay lúc khởi động qua {@link #validate}: cấu hình sai thì
 * ứng dụng không chạy, thay vì lỗi ở request đầu tiên. {@code enabled=false} bỏ qua mọi kiểm tra.
 *
 * <p>Bí mật (token tài khoản portal, client secret và token của Airflow) chỉ đến từ biến môi trường hoặc Secret, và
 * {@code toString()} của mọi khối chứa bí mật đều che chúng bằng {@code ***}.
 */
public record DeidProperties(
    @DefaultValue("true") boolean enabled,
    @DefaultValue("HASH") List<Mode> allowedModes,
    @DefaultValue("20") int maxActivePerTeam,
    @DefaultValue("65536") int maxUploadBytes,
    @DefaultValue("") String outputDatabase,
    @DefaultValue(DEFAULT_OUTPUT_TABLE_TEMPLATE) String outputTableTemplate,
    @DefaultValue("noop") Submitter submitter,
    @DefaultValue Kubernetes kubernetes,
    @DefaultValue Airflow airflow,
    List<PortalUser> portalUsers) {

  public static final String DEFAULT_OUTPUT_TABLE_TEMPLATE = "{table}_deid_{yyyyMMdd}_{seq}";
  public static final int MIN_UPLOAD_BYTES = 1024;
  public static final int MAX_UPLOAD_BYTES = 1024 * 1024;
  public static final int MIN_TOKEN_LENGTH = 24;
  public static final int MAX_TOKEN_LENGTH = 256;

  private static final String DEFAULT_KUBERNETES_API_SERVER = "https://kubernetes.default.svc";
  private static final Pattern USERNAME = Pattern.compile("[a-z0-9][a-z0-9_.-]{0,62}");
  private static final Pattern NAME = Pattern.compile("[a-z0-9][a-z0-9_-]{0,62}");
  private static final Pattern TOKEN = Pattern.compile("[\\x21-\\x7E]{" + MIN_TOKEN_LENGTH + "," + MAX_TOKEN_LENGTH + "}");
  private static final Pattern HIVE_NAME = Pattern.compile("[a-z0-9_]{1,128}");
  private static final Pattern OUTPUT_TABLE_TEMPLATE = Pattern.compile("(?:[a-z0-9_]|\\{table}|\\{seq}|\\{yyyyMMdd})+");
  private static final Pattern DNS_LABEL = Pattern.compile("[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?");
  // Bắt đầu bằng chữ/số/gạch dưới: loại "." và ".." (dagId đi vào đường dẫn URL của Airflow).
  private static final Pattern DAG_ID = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9_.-]{0,249}");

  /** Chế độ giả danh của {@code pseudonymize-table-app}. Bản đầu chỉ giao key cho {@link #HASH}. */
  public enum Mode {
    HASH,
    ENCRYPT,
    MAPPING
  }

  /** Cách gửi job sau khi duyệt phiếu. */
  public enum Submitter {
    NOOP,
    KUBERNETES,
    AIRFLOW
  }

  /** Cách xác thực với Airflow REST API. */
  public enum AirflowAuth {
    CLIENT_CREDENTIALS,
    BEARER,
    /** HTTP Basic với một user của Airflow (backend {@code airflow.api.auth.backend.basic_auth}). */
    BASIC
  }

  /**
   * Kubernetes API. {@code namespace} là nơi tạo Secret key của phiếu (và SparkApplication khi {@code submitter=kubernetes});
   * {@code templateFile} chỉ cần khi {@code submitter=kubernetes}.
   */
  public record Kubernetes(@DefaultValue(DEFAULT_KUBERNETES_API_SERVER) String apiServer, String namespace,
                           String templateFile) {

    public Kubernetes {
      apiServer = apiServer == null ? DEFAULT_KUBERNETES_API_SERVER : apiServer;
    }
  }

  /**
   * Airflow 2.x REST API ({@code /api/v1}). {@code allowedDagIds} là danh sách {@code dagId} người dùng được chọn trong phiếu.
   * {@code clientSecret} (env {@code AIRFLOW_CLIENT_SECRET}) và {@code token} (env {@code AIRFLOW_TOKEN}) là bí mật.
   */
  public record Airflow(String baseUrl, List<String> allowedDagIds,
                        @DefaultValue("client-credentials") AirflowAuth auth, String tokenUrl, String clientId,
                        String scope, String clientSecret, String token, String username, String password) {

    /** Cấu hình không dùng {@code basic} (username/password để trống). */
    public Airflow(String baseUrl, List<String> allowedDagIds, AirflowAuth auth, String tokenUrl, String clientId,
                   String scope, String clientSecret, String token) {
      this(baseUrl, allowedDagIds, auth, tokenUrl, clientId, scope, clientSecret, token, null, null);
    }

    @ConstructorBinding
    public Airflow {
      allowedDagIds = allowedDagIds == null ? List.of() : List.copyOf(allowedDagIds);
      auth = auth == null ? AirflowAuth.CLIENT_CREDENTIALS : auth;
    }

    @Override
    public String toString() {
      return "Airflow[baseUrl=" + baseUrl + ", allowedDagIds=" + allowedDagIds + ", auth=" + auth + ", tokenUrl=" + tokenUrl
          + ", clientId=" + clientId + ", scope=" + scope + ", clientSecret=" + mask(clientSecret)
          + ", token=" + mask(token) + ", username=" + username + ", password=" + mask(password) + "]";
    }
  }

  /**
   * Tài khoản giả của portal: một tài khoản thuộc đúng một team, phiếu thuộc team chứ không theo tài khoản.
   * {@code tenant}/{@code workspace} chỉ cần nếu {@code dak.vault.path-template} có {@code {tenant}}/{@code {workspace}}.
   */
  public record PortalUser(String username, String team, String tenant, String workspace, String token) {

    @Override
    public String toString() {
      return "PortalUser[username=" + username + ", team=" + team + ", tenant=" + tenant + ", workspace=" + workspace
          + ", token=" + mask(token) + "]";
    }
  }

  public DeidProperties {
    allowedModes = allowedModes == null ? List.of(Mode.HASH) : List.copyOf(allowedModes);
    outputDatabase = outputDatabase == null ? "" : outputDatabase;
    outputTableTemplate = outputTableTemplate == null ? DEFAULT_OUTPUT_TABLE_TEMPLATE : outputTableTemplate;
    submitter = submitter == null ? Submitter.NOOP : submitter;
    kubernetes = kubernetes == null ? new Kubernetes(null, null, null) : kubernetes;
    airflow = airflow == null ? new Airflow(null, null, null, null, null, null, null, null) : airflow;
    portalUsers = portalUsers == null ? List.of() : List.copyOf(portalUsers);
  }

  /** Cấu hình mặc định: bật, chỉ HASH, gửi job kiểu {@code noop}, chưa có tài khoản nào nên portal chưa dùng được. */
  public static DeidProperties defaults() {
    return new DeidProperties(true, null, 20, 65536, null, null, null, null, null, null);
  }

  /** Portal giả chỉ dùng được khi flow bật và có ít nhất một tài khoản; ngược lại mọi lời gọi trả {@code 404}. */
  public boolean portalEnabled() {
    return enabled && !portalUsers.isEmpty();
  }

  /** Kiểm tra cấu hình; {@code vaultPathTemplate} quyết định tài khoản có cần {@code tenant}/{@code workspace} hay không. */
  void validate(String vaultPathTemplate) {
    if (!enabled) {
      return;
    }
    validateLimits();
    validateOutput();
    validatePortalUsers(vaultPathTemplate);
    switch (submitter) {
      case NOOP -> { }
      case KUBERNETES -> validateKubernetes(true);
      case AIRFLOW -> {
        validateKubernetes(false);
        validateAirflow();
      }
    }
  }

  private void validateLimits() {
    require(!allowedModes.isEmpty(), "dak.deid.allowed-modes must not be empty");
    for (Mode mode : allowedModes) {
      require(mode == Mode.HASH, "dak.deid.allowed-modes: " + mode + " is not supported yet (only HASH; ENCRYPT needs a "
          + "64-byte key and MAPPING needs a mapping table, which one table key cannot provide)");
    }
    require(maxActivePerTeam > 0, "dak.deid.max-active-per-team must be positive");
    require(maxUploadBytes >= MIN_UPLOAD_BYTES && maxUploadBytes <= MAX_UPLOAD_BYTES,
        "dak.deid.max-upload-bytes must be between " + MIN_UPLOAD_BYTES + " and " + MAX_UPLOAD_BYTES);
  }

  private void validateOutput() {
    require(outputDatabase.isEmpty() || HIVE_NAME.matcher(outputDatabase).matches(),
        "dak.deid.output-database must be empty or match " + HIVE_NAME.pattern());
    require(outputTableTemplate.contains("{table}") && outputTableTemplate.contains("{seq}")
            && OUTPUT_TABLE_TEMPLATE.matcher(outputTableTemplate).matches(),
        "dak.deid.output-table-template must contain {table} and {seq} and otherwise only [a-z0-9_] and {yyyyMMdd}");
  }

  private void validatePortalUsers(String vaultPathTemplate) {
    boolean needsTenant = vaultPathTemplate.contains("{tenant}");
    boolean needsWorkspace = vaultPathTemplate.contains("{workspace}");
    Set<String> usernames = new HashSet<>();
    Map<String, String> tokenOwners = new HashMap<>();
    for (PortalUser u : portalUsers) {
      // Không in giá trị username/token sai vào thông điệp: có thể rất dài hoặc là bí mật.
      require(u.username() != null && USERNAME.matcher(u.username()).matches(),
          "dak.deid.portal-users[*].username must match " + USERNAME.pattern());
      String where = "dak.deid.portal-users[" + u.username() + "]";
      require(usernames.add(u.username()), "dak.deid.portal-users: duplicate username " + u.username());
      require(u.team() != null && NAME.matcher(u.team()).matches(), where + ".team must match " + NAME.pattern());
      require(u.token() != null && TOKEN.matcher(u.token()).matches(), where + ".token must be " + MIN_TOKEN_LENGTH + "-"
          + MAX_TOKEN_LENGTH + " visible ASCII characters without spaces");
      String owner = tokenOwners.putIfAbsent(u.token(), u.username());
      require(owner == null, "dak.deid.portal-users: users " + owner + " and " + u.username() + " share the same token");
      if (needsTenant) {
        require(u.tenant() != null && NAME.matcher(u.tenant()).matches(),
            where + ".tenant must match " + NAME.pattern() + " (dak.vault.path-template uses {tenant})");
      }
      if (needsWorkspace) {
        require(u.workspace() != null && NAME.matcher(u.workspace()).matches(),
            where + ".workspace must match " + NAME.pattern() + " (dak.vault.path-template uses {workspace})");
      }
    }
  }

  private void validateKubernetes(boolean needsTemplateFile) {
    require(isHttpUrl(kubernetes.apiServer()), "dak.deid.kubernetes.api-server must be an http(s) URL");
    require(kubernetes.namespace() != null && DNS_LABEL.matcher(kubernetes.namespace()).matches(),
        "dak.deid.kubernetes.namespace must be a DNS-1123 label (it is where the key Secret is created)");
    if (needsTemplateFile) {
      require(!isBlank(kubernetes.templateFile()), "dak.deid.kubernetes.template-file is required when submitter=kubernetes");
    }
  }

  private void validateAirflow() {
    require(isHttpUrl(airflow.baseUrl()), "dak.deid.airflow.base-url must be an http(s) URL");
    require(!airflow.allowedDagIds().isEmpty(), "dak.deid.airflow.allowed-dag-ids must not be empty");
    Set<String> seen = new HashSet<>();
    for (String dagId : airflow.allowedDagIds()) {
      require(dagId != null && DAG_ID.matcher(dagId).matches(),
          "dak.deid.airflow.allowed-dag-ids: every dagId must match " + DAG_ID.pattern());
      require(seen.add(dagId), "dak.deid.airflow.allowed-dag-ids: duplicate dagId " + dagId);
    }
    if (airflow.auth() == AirflowAuth.CLIENT_CREDENTIALS) {
      require(isHttpUrl(airflow.tokenUrl()), "dak.deid.airflow.token-url must be an http(s) URL");
      require(!isBlank(airflow.clientId()), "dak.deid.airflow.client-id is required");
      require(!isBlank(airflow.clientSecret()), "dak.deid.airflow.client-secret is required (set env AIRFLOW_CLIENT_SECRET)");
    } else if (airflow.auth() == AirflowAuth.BASIC) {
      require(!isBlank(airflow.username()), "dak.deid.airflow.username is required when auth=basic");
      require(!isBlank(airflow.password()), "dak.deid.airflow.password is required when auth=basic (set env AIRFLOW_PASSWORD)");
    } else {
      require(!isBlank(airflow.token()), "dak.deid.airflow.token is required (set env AIRFLOW_TOKEN)");
    }
  }

  private static boolean isHttpUrl(String value) {
    if (isBlank(value)) {
      return false;
    }
    try {
      URI uri = new URI(value);
      String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
      return (scheme.equals("http") || scheme.equals("https")) && uri.getHost() != null;
    } catch (URISyntaxException e) {
      return false;
    }
  }

  private static String mask(String secret) {
    return isBlank(secret) ? "<none>" : "***";
  }
}
