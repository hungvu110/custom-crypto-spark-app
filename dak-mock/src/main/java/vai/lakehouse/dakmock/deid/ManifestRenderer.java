package vai.lakehouse.dakmock.deid;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.representer.Representer;
import vai.lakehouse.dakmock.deid.DeidRequest.ColumnSpec;

/**
 * Dựng SparkApplication khử định danh của một phiếu từ mẫu YAML (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 4.5).
 *
 * <p>Mẫu được parse MỘT LẦN lúc khởi động thành cây (SnakeYAML {@code SafeConstructor}: không tạo object tuỳ ý, không alias của
 * collection) và kiểm tra ngay. Mỗi lần dựng, placeholder {@code ${DEID_*}} chỉ được thay trong GIÁ TRỊ chuỗi của một bản sao cây,
 * một lượt (kết quả không bị quét lại), sau khi giá trị đã khớp bộ ký tự riêng của placeholder. Vì thế giá trị lấy từ phiếu không
 * thể thêm khoá, đổi cấu trúc hay chui sang giá trị khác. {@code metadata.name}, {@code namespace} và nhãn
 * {@value #REQUEST_CODE_LABEL} do renderer đặt, các trường do máy chủ cấp bị bỏ. Mục đích, tên file, dagId, team, người tạo của
 * phiếu không có placeholder nên không bao giờ vào manifest.
 */
public final class ManifestRenderer {

  public static final String API_VERSION = "sparkoperator.k8s.io/v1beta2";
  public static final String KIND = "SparkApplication";
  public static final String REQUEST_CODE_LABEL = "dak.vai/request-code";
  /** Một tuần: thời gian tối thiểu giữ SparkApplication đã xong để còn xem log. */
  public static final long MIN_TTL_SECONDS = 604_800;

  private static final String CONFIG = "Invalid DAK mock configuration: ";
  private static final String TEMPLATE = CONFIG + "dak.deid.kubernetes.template-file ";
  private static final int MAX_TEMPLATE_BYTES = 256 * 1024;
  private static final Pattern NAMESPACE = Pattern.compile("[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?");
  private static final Pattern NAME128 = Pattern.compile("[A-Za-z0-9_]{1,128}");
  private static final Pattern NAME200 = Pattern.compile("[A-Za-z0-9_]{1,200}");
  private static final Pattern SECRET_NAME = Pattern.compile("deid-key-\\d{4}-\\d{6}");
  private static final Pattern REQUEST_CODE = Pattern.compile("PYC-\\d{4}-\\d{6}");
  private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{(DEID_[A-Za-z0-9_]*)}");
  private static final Pattern SECRET_LIKE_ENV = Pattern.compile("(?i).*(SECRET|TOKEN|PASSWORD|PASSWD).*");
  /** Trường do máy chủ cấp hoặc do controller quản: không được nằm trong thứ ta gửi đi. */
  private static final Set<String> SERVER_METADATA = Set.of("generateName", "resourceVersion", "uid", "creationTimestamp",
      "ownerReferences", "managedFields", "selfLink", "generation", "deletionTimestamp", "deletionGracePeriodSeconds",
      "finalizers");

  private enum Placeholder {
    DEID_DATABASE(true), DEID_TABLE(true), DEID_PSEUDO_COLUMNS(true), DEID_OUTPUT_DB(true), DEID_OUTPUT_TABLE(true),
    DEID_KEY_SECRET(true), DEID_REQUEST_CODE(false);

    final boolean required;

    Placeholder(boolean required) {
      this.required = required;
    }

    static Placeholder find(String name) {
      for (Placeholder p : values()) {
        if (p.name().equals(name)) {
          return p;
        }
      }
      return null;
    }
  }

  private final Map<String, Object> template;
  private final String namespace;

  public ManifestRenderer(String templateYaml, String namespace) {
    if (namespace == null || !NAMESPACE.matcher(namespace).matches()) {
      throw new IllegalStateException(CONFIG + "dak.deid.kubernetes.namespace must be a DNS-1123 label");
    }
    this.namespace = namespace;
    this.template = parse(templateYaml);
    validate(template);
  }

  /** Đọc mẫu từ file (UTF-8, tối đa 256 KiB). Lỗi đọc báo thông điệp cố định, không kèm nội dung file. */
  public static ManifestRenderer fromFile(Path file, String namespace) {
    try {
      if (Files.size(file) > MAX_TEMPLATE_BYTES) {
        throw new IllegalStateException(TEMPLATE + "is larger than " + MAX_TEMPLATE_BYTES + " bytes");
      }
      return new ManifestRenderer(Files.readString(file, StandardCharsets.UTF_8), namespace);
    } catch (IOException e) {
      throw new IllegalStateException(TEMPLATE + "cannot be read", e);
    }
  }

  /** Cây manifest của phiếu, mới hoàn toàn mỗi lần gọi. Giá trị sai bộ ký tự ném {@code invalid value for DEID_*}. */
  public Map<String, Object> render(DeidRequest request, String keySecretName) {
    Map<Placeholder, String> values = values(request, keySecretName);
    UnaryOperator<String> substitute = text -> substitute(text, values);
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<String, Object> e : template.entrySet()) {
      if ("metadata".equals(e.getKey())) {
        out.put("metadata", metadata(request, substitute));
      } else if (!"status".equals(e.getKey())) {
        out.put(e.getKey(), copy(e.getValue(), substitute));
      }
    }
    out.putIfAbsent("metadata", metadata(request, substitute));
    return out;
  }

  /** {@link #render} dưới dạng YAML kiểu block, để ghi log hoặc gửi đi. */
  public String renderYaml(DeidRequest request, String keySecretName) {
    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    return new Yaml(options).dump(render(request, keySecretName));
  }

  private Map<String, Object> metadata(DeidRequest request, UnaryOperator<String> substitute) {
    Map<String, Object> source = template.get("metadata") instanceof Map<?, ?> m ? castMap(m) : Map.of();
    Map<String, Object> labels = new LinkedHashMap<>();
    if (source.get("labels") instanceof Map<?, ?> l) {
      castMap(l).forEach((k, v) -> labels.put(k, copy(v, substitute)));
    }
    labels.put(REQUEST_CODE_LABEL, request.requestCode());
    Map<String, Object> meta = new LinkedHashMap<>();
    meta.put("name", request.jobName());
    meta.put("namespace", namespace);
    meta.put("labels", labels);
    source.forEach((k, v) -> {
      if (!meta.containsKey(k) && !SERVER_METADATA.contains(k)) {
        meta.put(k, copy(v, substitute));
      }
    });
    return meta;
  }

  /**
   * Tham số job của phiếu đã kiểm bộ ký tự, cho đường Airflow ({@code conf} của dag run). Cùng bộ kiểm với {@link #render}; sai thì
   * ném {@code invalid value for DEID_*}.
   */
  static Map<String, String> jobParameters(DeidRequest r, String keySecretName) {
    Map<Placeholder, String> v = values(r, keySecretName);
    Map<String, String> out = new LinkedHashMap<>();
    out.put("requestCode", v.get(Placeholder.DEID_REQUEST_CODE));
    out.put("database", v.get(Placeholder.DEID_DATABASE));
    out.put("table", v.get(Placeholder.DEID_TABLE));
    out.put("pseudoColumns", v.get(Placeholder.DEID_PSEUDO_COLUMNS));
    out.put("outputDb", v.get(Placeholder.DEID_OUTPUT_DB));
    out.put("outputTable", v.get(Placeholder.DEID_OUTPUT_TABLE));
    out.put("keySecretName", v.get(Placeholder.DEID_KEY_SECRET));
    return out;
  }

  private static Map<Placeholder, String> values(DeidRequest r, String keySecretName) {
    Map<Placeholder, String> values = new EnumMap<>(Placeholder.class);
    values.put(Placeholder.DEID_DATABASE, checked(Placeholder.DEID_DATABASE, r.database(), NAME128));
    values.put(Placeholder.DEID_TABLE, checked(Placeholder.DEID_TABLE, r.table(), NAME128));
    values.put(Placeholder.DEID_PSEUDO_COLUMNS, pseudoColumns(r.columns()));
    values.put(Placeholder.DEID_OUTPUT_DB, checked(Placeholder.DEID_OUTPUT_DB, r.outputDatabase(), NAME200));
    values.put(Placeholder.DEID_OUTPUT_TABLE, checked(Placeholder.DEID_OUTPUT_TABLE, r.outputTable(), NAME200));
    values.put(Placeholder.DEID_KEY_SECRET, checked(Placeholder.DEID_KEY_SECRET, keySecretName, SECRET_NAME));
    values.put(Placeholder.DEID_REQUEST_CODE, checked(Placeholder.DEID_REQUEST_CODE, r.requestCode(), REQUEST_CODE));
    return values;
  }

  private static String pseudoColumns(List<ColumnSpec> specs) {
    boolean ok = specs != null && !specs.isEmpty() && specs.stream().allMatch(c -> c.column() != null
        && NAME128.matcher(c.column()).matches() && c.mode() != null);
    if (!ok) {
      throw invalid(Placeholder.DEID_PSEUDO_COLUMNS);
    }
    return specs.stream().map(c -> c.column() + ":" + c.mode().name()).collect(Collectors.joining(","));
  }

  private static String checked(Placeholder p, String value, Pattern allowed) {
    if (value == null || !allowed.matcher(value).matches()) {
      throw invalid(p);
    }
    return value;
  }

  private static IllegalArgumentException invalid(Placeholder p) {
    // Thông điệp cố định: giá trị sai đến từ phiếu của người dùng, không được lặp lại.
    return new IllegalArgumentException("invalid value for " + p.name());
  }

  private static String substitute(String text, Map<Placeholder, String> values) {
    return PLACEHOLDER.matcher(text).replaceAll(
        m -> Matcher.quoteReplacement(values.get(Placeholder.find(m.group(1)))));
  }

  private static Object copy(Object node, UnaryOperator<String> substitute) {
    if (node instanceof Map<?, ?> map) {
      Map<String, Object> out = new LinkedHashMap<>();
      castMap(map).forEach((k, v) -> out.put(k, copy(v, substitute)));
      return out;
    }
    if (node instanceof List<?> list) {
      List<Object> out = new ArrayList<>(list.size());
      list.forEach(v -> out.add(copy(v, substitute)));
      return out;
    }
    return node instanceof String s ? substitute.apply(s) : node;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> castMap(Map<?, ?> map) {
    return (Map<String, Object>) map;
  }

  private static Map<String, Object> parse(String text) {
    LoaderOptions options = new LoaderOptions();
    options.setMaxAliasesForCollections(0);
    options.setAllowDuplicateKeys(false);
    options.setCodePointLimit(MAX_TEMPLATE_BYTES);
    options.setNestingDepthLimit(40);
    DumperOptions dumper = new DumperOptions();
    Yaml yaml = new Yaml(new SafeConstructor(options), new Representer(dumper), dumper, options);
    List<Object> documents = new ArrayList<>();
    try {
      yaml.loadAll(text == null ? "" : text).forEach(documents::add);
    } catch (YAMLException | ClassCastException e) {
      throw new IllegalStateException(TEMPLATE + "is not valid YAML (aliases, custom tags and duplicate keys are rejected)", e);
    }
    if (documents.size() > 1) {
      throw new IllegalStateException(TEMPLATE + "must contain exactly one YAML document");
    }
    if (documents.isEmpty() || !(documents.get(0) instanceof Map<?, ?> root)) {
      throw new IllegalStateException(TEMPLATE + "must be a YAML mapping");
    }
    return castMap(root);
  }

  private static void validate(Map<String, Object> root) {
    require(API_VERSION.equals(root.get("apiVersion")), "apiVersion must be " + API_VERSION);
    require(KIND.equals(root.get("kind")), "kind must be " + KIND);
    require(root.get("metadata") == null || root.get("metadata") instanceof Map, "metadata must be a mapping");
    Set<Placeholder> used = EnumSet.noneOf(Placeholder.class);
    collectPlaceholders(root, used);
    for (Placeholder p : Placeholder.values()) {
      require(!p.required || used.contains(p), "must use the placeholder ${" + p.name() + "}");
    }
    Map<String, Object> spec = root.get("spec") instanceof Map<?, ?> s ? castMap(s) : Map.of();
    Object ttl = spec.get("timeToLiveSeconds");
    require(ttl == null || (ttl instanceof Number n && n.longValue() >= MIN_TTL_SECONDS),
        "spec.timeToLiveSeconds must be a number of at least " + MIN_TTL_SECONDS + " when present");
    Map<String, String> driverEnv = literalEnv(spec, "driver");
    require("overwrite".equals(driverEnv.get("OUTPUT_MODE")), "spec.driver.env must set OUTPUT_MODE to overwrite");
    require("true".equals(driverEnv.get("VERIFY")), "spec.driver.env must set VERIFY to \"true\"");
    literalEnv(spec, "executor");
  }

  /** Env {@code name -> value} của một vai (driver/executor); từ chối env có tên kiểu bí mật mà mang giá trị viết thẳng. */
  private static Map<String, String> literalEnv(Map<String, Object> spec, String role) {
    Map<String, String> literals = new LinkedHashMap<>();
    if (!(spec.get(role) instanceof Map<?, ?> r) || !(castMap(r).get("env") instanceof List<?> env)) {
      return literals;
    }
    for (Object item : env) {
      if (item instanceof Map<?, ?> e && castMap(e).get("name") instanceof String name
          && castMap(e).get("value") instanceof String v) {
        require(!SECRET_LIKE_ENV.matcher(name).matches(),
            "env " + name + " looks like a secret: use valueFrom.secretKeyRef, not a literal value");
        literals.put(name, v);
      }
    }
    return literals;
  }

  private static void collectPlaceholders(Object node, Set<Placeholder> used) {
    if (node instanceof Map<?, ?> map) {
      map.values().forEach(v -> collectPlaceholders(v, used));
    } else if (node instanceof List<?> list) {
      list.forEach(v -> collectPlaceholders(v, used));
    } else if (node instanceof String s) {
      String rest = PLACEHOLDER.matcher(s).replaceAll(r -> {
        Placeholder p = Placeholder.find(r.group(1));
        require(p != null, "uses the unknown placeholder ${" + r.group(1) + "}");
        used.add(p);
        return "";
      });
      require(!rest.contains("${DEID_"), "contains a malformed ${DEID_...} placeholder");
    }
  }

  private static void require(boolean ok, String message) {
    if (!ok) {
      throw new IllegalStateException(TEMPLATE + message);
    }
  }
}
