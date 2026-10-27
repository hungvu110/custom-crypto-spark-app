"""Phần dùng chung của hai DAG chuẩn hoá silver (silver_ingest_vtt.py, silver_ingest_vtnet.py). Thiết kế: docs/silver-ingest/PLAN.md.

DAK trigger DAG khi phiếu chuẩn hoá được duyệt: POST /api/v1/dags/<dagId>/dagRuns với conf = {"path": "<đường dẫn tương đối theo gốc
landing của nguồn, vd ebs_3g_4g/dt=20261012/batch=1520>", "batchId": "yyyyMMddHH" hoặc "yyyyMMddHHmm", "table": "<bảng đích>",
"outputMode": ...} (+ "encryptColumns", "keyField" với VTNET). DAG ghép INPUT_ROOT + path thành INPUT_PATH, ghi vào OUTPUT_DB.table. Job
thêm cột partition BATCH_COLUMN = batchId vào mọi dòng; mặc định chỉ thay partition của lô đó (chạy lại cùng lô không ghi trùng, lô khác
giữ nguyên). Hai lô khác nhau của cùng một bảng PHẢI có batchId khác nhau, không thì lô sau ghi đè lô trước.
Cấu hình cố định theo nguồn (gốc landing, định dạng file, schema, database đích) là hằng SOURCE trong file DAG của nguồn đó; conf mang
những gì đổi theo từng phiếu. Mọi giá trị trong conf được kiểm ở task đầu `validate_conf` trước khi tạo job.

File này nằm cùng thư mục dags/ nên Airflow import được bằng `import silver_ingest_common` (thư mục dags có sẵn trong sys.path).
Không chứa DAG nào.
"""
import copy
import datetime
import hashlib
import re

# ---------------------------------------------------------------- MÔI TRƯỜNG (sửa theo cụm của bạn)
NAMESPACE = "vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy"
IMAGE = "hub.vtcc.vn:8989/hungvt0110/silver-ingest-app:v0.1"
HADOOP_CONFIG_MAP = "teamtscauiy-hadoop-conf"
SERVICE_ACCOUNT = "spark-application-sa"
HDFS = "hdfs://vailakehouse.datalakehla.viettel.com.vn:8020"
WAREHOUSE_PATH = f"{HDFS}/prod8/warehouse"
EVENT_LOG_DIR = f"{HDFS}/prod8/spark_history"
KERBEROS_PRINCIPAL = "presto@VAILAKEHOUSE.VIETTEL.COM"
KERBEROS_KEYTAB = "/etc/security/keytabs/presto.keytab"
# Keycloak nội bộ (DAK_TOKEN_URL, https): driver cần phân giải tên (hostAliases) và tin CA nội bộ (truststore). Cùng cấu hình với
# k8s/spark-application-cdr.yaml. ConfigMap `keycloak-ca` (key `ca.pem`) phải có trước, cùng namespace:
#   kubectl create configmap keycloak-ca --from-file=ca.pem=<file CA nội bộ> -n <namespace>
KEYCLOAK_HOST = "sso-lakehouse.cyberspace.vn"
KEYCLOAK_IP = "10.221.148.42"
KEYCLOAK_CA_CONFIGMAP = "keycloak-ca"
TRUSTSTORE_DIR = "/opt/truststore"
TRUSTSTORE_FILE = f"{TRUSTSTORE_DIR}/truststore.p12"
TRUSTSTORE_PASSWORD = "changeit"  # truststore tạm trong emptyDir của pod, chứa CA công khai + CA nội bộ, không phải bí mật
SPARK_UID = 185  # user của image apache/spark (USER 185 trong silver-ingest-app/Dockerfile)
LOG4J = "-Dlog4j.configurationFile=file:/opt/app/log4j2.properties"
JARS = ("local:///opt/app/key-prefix-lib-1.0-SNAPSHOT.jar,local:///opt/app/cdr-crypto-udf-1.0-SNAPSHOT.jar,"
        "local:///opt/app/DataLakeSecurity_jv8.jar")

# overwrite-partitions (mặc định): chỉ thay partition của lô. append: ghi nối (chạy lại cùng lô là ghi trùng). Không nhận "overwrite"
# (thay CẢ bảng, xoá mọi lô khác) từ conf để DAK không xoá nhầm dữ liệu.
OUTPUT_MODES = ("overwrite-partitions", "append")
DEFAULT_OUTPUT_MODE = "overwrite-partitions"
# Tên cột partition của lô trong bảng đích (env BATCH_COLUMN của job).
BATCH_COLUMN = "batch_id"
# Mã lô: yyyyMMddHH (theo giờ) hoặc yyyyMMddHHmm (theo batch 20 phút của VTNET, vd dt=20261012/batch=1520 -> 202610121520).
BATCH_ID = re.compile(r"\d{10}(\d{2})?")
# Tên bảng đích do người gửi phiếu chọn: chữ thường, số, "_"; phải bắt đầu bằng tiền tố của nguồn (SOURCE["table_prefix"]) để một phiếu
# không ghi vào bảng của nguồn khác.
TABLE = re.compile(r"[a-z0-9_]{1,128}")
# Đường dẫn tương đối theo gốc của nguồn, ở dạng chuẩn đã thống nhất với DAK (docs/key-access/TRA-LOI-DAK-BY-PATH.md): không "/" đầu/cuối,
# không "//", không scheme. Không có khoảng trắng, dấu nháy hay {{ }} của Jinja.
REL_PATH = re.compile(r"[A-Za-z0-9_.=+-]+(/[A-Za-z0-9_.=+-]+)*")
COLUMN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
MAX_PATH = 1024
MAX_COLUMNS = 100


def check_path(name, value):
    if not isinstance(value, str) or len(value) > MAX_PATH or not REL_PATH.fullmatch(value) or ".." in value:
        raise ValueError(f"conf.{name} thiếu hoặc sai định dạng (tương đối, chỉ [A-Za-z0-9_.=+-] và '/', không '/' đầu/cuối, "
                         f"không '//', không '..', tối đa {MAX_PATH} ký tự)")


def check_columns(conf):
    """encryptColumns (danh sách tên cột, không trùng) và keyField (không nằm trong danh sách) — chỉ nguồn có mã hoá cột."""
    columns = conf.get("encryptColumns")
    if (not isinstance(columns, list) or not columns or len(columns) > MAX_COLUMNS
            or not all(isinstance(c, str) and COLUMN.fullmatch(c) for c in columns)):
        raise ValueError(f"conf.encryptColumns phải là danh sách 1 đến {MAX_COLUMNS} tên cột [A-Za-z_][A-Za-z0-9_]*")
    lower = [c.lower() for c in columns]
    if len(set(lower)) != len(lower):
        raise ValueError("conf.encryptColumns có cột trùng (không phân biệt hoa thường)")
    key_field = conf.get("keyField")
    if not isinstance(key_field, str) or not COLUMN.fullmatch(key_field):
        raise ValueError("conf.keyField thiếu hoặc sai định dạng")
    if key_field.lower() in lower:
        raise ValueError("conf.keyField không được nằm trong encryptColumns (nó giữ plaintext làm nguyên liệu key)")


def check_conf(source, conf):
    """Kiểm conf của dag run, ném ValueError nếu sai. Thông điệp không lặp lại giá trị đã nhận.

    `source` là hằng SOURCE của DAG: `conf_keys` (khoá conf được nhận), `table_prefix` (tiền tố bảng đích) và `encrypts_columns` (nguồn nhận encryptColumns/keyField)."""
    conf = conf or {}
    if not isinstance(conf, dict):
        raise ValueError("conf phải là object")
    unknown = set(conf) - set(source["conf_keys"])
    if unknown:
        raise ValueError(f"conf có khoá không được nhận (chỉ nhận {', '.join(source['conf_keys'])})")
    check_path("path", conf.get("path"))
    batch = conf.get("batchId")
    if not isinstance(batch, str) or not BATCH_ID.fullmatch(batch):
        raise ValueError("conf.batchId thiếu hoặc sai định dạng (yyyyMMddHH hoặc yyyyMMddHHmm)")
    try:
        datetime.datetime.strptime(batch, "%Y%m%d%H" if len(batch) == 10 else "%Y%m%d%H%M")
    except ValueError:
        raise ValueError("conf.batchId không phải ngày giờ có thật") from None
    table = conf.get("table")
    if not isinstance(table, str) or not TABLE.fullmatch(table) or not table.startswith(source["table_prefix"]):
        raise ValueError(f"conf.table thiếu hoặc sai định dạng (chữ thường, số, '_', bắt đầu bằng '{source['table_prefix']}')")
    if "outputMode" in conf and conf["outputMode"] not in OUTPUT_MODES:
        raise ValueError(f"conf.outputMode phải là một trong {', '.join(OUTPUT_MODES)}")
    if source.get("encrypts_columns"):
        check_columns(conf)


def k8s_name(prefix, run_id):
    """Tên SparkApplication hợp lệ DNS-1123 và duy nhất theo dag run (DAK có thể trigger nhiều run gần nhau)."""
    return f"{prefix}-{hashlib.sha1(run_id.encode('utf-8')).hexdigest()[:12]}"


def conf_value(key, default=""):
    """Biểu thức Jinja lấy conf.<key> (đã được validate_conf kiểm)."""
    return "{{ (dag_run.conf or {}).get('" + key + "', '" + default + "') }}"


def keycloak_trust():
    """Phần riêng của driver khi nguồn gọi Keycloak (source["calls_keycloak"]): hostAliases, initContainer dựng truststore (cacerts của JDK
    + CA nội bộ), mount, volume và option JVM. Webhook của Spark Operator chỉ đưa vào pod các volume được mount ở container CHÍNH
    (driver.volumeMounts), nên keycloak-ca phải được mount ở cả hai chỗ."""
    build = f"""cp "$JAVA_HOME/lib/security/cacerts" {TRUSTSTORE_FILE}
awk '/BEGIN CERTIFICATE/ {{ n++ }} n > 0 {{ print > ("{TRUSTSTORE_DIR}/ca-" n ".pem") }}' /ca/ca.pem
ls {TRUSTSTORE_DIR}/ca-*.pem > /dev/null
for f in {TRUSTSTORE_DIR}/ca-*.pem; do
  keytool -importcert -noprompt -alias "internal-$(basename "$f" .pem)" -file "$f" \\
    -keystore {TRUSTSTORE_FILE} -storetype PKCS12 -storepass {TRUSTSTORE_PASSWORD}
done
rm -f {TRUSTSTORE_DIR}/ca-*.pem
"""
    return {
        "hostAliases": [{"ip": KEYCLOAK_IP, "hostnames": [KEYCLOAK_HOST]}],
        "initContainers": [{
            "name": "build-truststore",
            "image": IMAGE,
            "command": ["bash", "-ec"],
            "args": [build],
            "env": [{"name": "JAVA_TOOL_OPTIONS", "value": ""}],
            # ResourceQuota của namespace từ chối container thiếu limits.
            "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"cpu": "500m", "memory": "256Mi"}},
            # Như initContainer build-truststore của dak-mock/k8s/dak-mock.yaml, trừ: có runAsUser (cần mới chạy được) và KHÔNG đặt
            # readOnlyRootFilesystem.
            "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}, "runAsNonRoot": True,
                                "runAsUser": SPARK_UID},
            "volumeMounts": [{"name": "keycloak-ca", "mountPath": "/ca", "readOnly": True},
                             {"name": "truststore", "mountPath": TRUSTSTORE_DIR}],
        }],
        "volumeMounts": [{"name": "truststore", "mountPath": TRUSTSTORE_DIR, "readOnly": True},
                         {"name": "keycloak-ca", "mountPath": "/opt/keycloak-ca", "readOnly": True}],
        "javaOptions": f"-Djavax.net.ssl.trustStore={TRUSTSTORE_FILE} -Djavax.net.ssl.trustStoreType=PKCS12 "
                       f"-Djavax.net.ssl.trustStorePassword={TRUSTSTORE_PASSWORD}",
        "volumes": [{"name": "keycloak-ca", "configMap": {"name": KEYCLOAK_CA_CONFIGMAP}},
                    {"name": "truststore", "emptyDir": {"sizeLimit": "16Mi"}}],
    }


def build_spec(source):
    """SparkApplication chuẩn hoá một lô file của `source`. Env của job: silver-ingest-app/README.md."""
    common_env = [
        {"name": "TZ", "value": "Asia/Ho_Chi_Minh"},
        {"name": "MALLOC_ARENA_MAX", "value": "2"},
    ]
    job_env = [
        {"name": "APP_NAME", "value": f"SilverIngest-{source['name']}"},
        # input_root kết thúc bằng "/", path tương đối không có "/" đầu.
        {"name": "INPUT_PATH", "value": source["input_root"] + conf_value("path")},
        {"name": "OUTPUT_MODE", "value": conf_value("outputMode", DEFAULT_OUTPUT_MODE)},
        {"name": "BATCH_COLUMN", "value": BATCH_COLUMN},
        {"name": "BATCH_VALUE", "value": conf_value("batchId")},
        {"name": "OUTPUT_TABLE", "value": conf_value("table")},
        {"name": "WAREHOUSE_PATH", "value": WAREHOUSE_PATH},
        {"name": "ENABLE_HIVE_SUPPORT", "value": "true"},
    ] + [{"name": k, "value": v} for k, v in source["env"].items()]
    trust = keycloak_trust() if source.get("calls_keycloak") else None
    driver_trust = {k: copy.deepcopy(trust[k]) for k in ("hostAliases", "initContainers", "volumeMounts")} if trust else {}
    spec = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": "{{ k8s_name('silver-" + source["name"] + "', dag_run.run_id) }}", "namespace": NAMESPACE},
        "spec": {
            "type": "Scala",
            "mode": "cluster",
            "sparkVersion": "3.5.1",
            "image": IMAGE,
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local:///opt/app/silver-ingest-app.jar",
            "mainClass": source["main_class"],
            # Lỗi thì xem log rồi chạy lại có chủ đích (append chạy lại là ghi trùng), không tự restart.
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 604800,
            "hadoopConfigMap": HADOOP_CONFIG_MAP,
            "driver": {
                "cores": 1,
                "coreLimit": "1000m",
                "memory": "2048m",
                "memoryOverhead": "512m",
                "labels": {"version": "3.5.1", "app": f"silver-ingest-{source['name']}"},
                "serviceAccount": SERVICE_ACCOUNT,
                "env": common_env + job_env + copy.deepcopy(source.get("driver_extra_env", [])),
                **driver_trust,
            },
            "executor": {
                "instances": 2,
                "cores": 2,
                "coreLimit": "2000m",
                "memory": "4096m",
                "memoryOverhead": "1024m",
                "labels": {"version": "3.5.1", "app": f"silver-ingest-{source['name']}"},
                "serviceAccount": SERVICE_ACCOUNT,
                # Executor KHÔNG cần cấu hình DAK: key cột/key file được lấy ở driver và đi kèm closure.
                "env": copy.deepcopy(common_env),
            },
            "sparkConf": {
                # cdr-crypto (UDF thật) + key-prefix-lib + jar đối tác: executor cũng cần.
                "spark.jars": JARS,
                "spark.kerberos.access.hadoopFileSystems": HDFS,
                "spark.kerberos.principal": KERBEROS_PRINCIPAL,
                "spark.kerberos.keytab": KERBEROS_KEYTAB,
                "spark.kubernetes.kerberos.krb5.path": "/etc/krb5.conf",
                "spark.security.credentials.hadoopfs.enabled": "true",
                "spark.hadoop.hadoop.security.authentication": "kerberos",
                "spark.hadoop.hadoop.security.authorization": "true",
                "spark.hadoop.fs.permissions.umask-mode": "002",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                # BẮT BUỘC false (patch block token qua ServiceLoader).
                "spark.driver.userClassPathFirst": "false",
                "spark.executor.userClassPathFirst": "false",
                # truststore chỉ cho driver (executor không gọi Keycloak/DAK).
                "spark.driver.extraJavaOptions": f"{LOG4J} {trust['javaOptions']}" if trust else LOG4J,
                "spark.executor.extraJavaOptions": LOG4J,
                "spark.jars.ivy": "/tmp",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.adaptive.enabled": "true",
                "spark.dynamicAllocation.enabled": "false",
                "spark.sql.shuffle.partitions": "64",
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": EVENT_LOG_DIR,
                "spark.eventLog.rolling.enabled": "true",
                "spark.eventLog.rolling.maxFileSize": "128m",
            },
        },
    }
    if trust:
        spec["spec"]["volumes"] = copy.deepcopy(trust["volumes"])
    return spec
