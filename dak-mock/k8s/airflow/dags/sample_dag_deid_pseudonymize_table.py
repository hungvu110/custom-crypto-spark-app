"""DAG MẪU nhận dag run do DAK trigger để khử định danh một bảng (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 4.7, bước D11).

Spec SparkApplication ở đây giữ ĐỦ config và env của pseudonymize-table-app/k8s/pseudonymize-table.yaml (cùng nội dung với mẫu
dak-mock/k8s/deid/sparkapplication.yaml): image, hadoopConfigMap, Kerberos, Delta, event log, env của job và của pseudonym-udf. Chỉ
các giá trị theo từng phiếu lấy từ `conf`. Nếu môi trường của bạn khác (vd cụm HDFS `hkh`, warehouse `/prod4`, principal `k8s@...`
như DAG vailakehouse đang chạy) thì sửa các hằng ở khối "MÔI TRƯỜNG" bên dưới.

DAK gọi POST /api/v1/dags/<dagId>/dagRuns với:
  dag_run_id = deid-<năm>-<6 số>              (tên job, cũng là tên SparkApplication)
  conf = {requestCode, database, table, pseudoColumns, keySecretName, env}
`env` là env CUỐI CÙNG của job (env người gửi phiếu nhập cộng giá trị mặc định do DAK điền), gồm OUTPUT_DB, OUTPUT_TABLE,
OUTPUT_MODE, OUTPUT_FORMAT, ENABLE_HIVE_SUPPORT và (nếu người gửi nhập) khoảng ngày giờ INPUT_FROM, INPUT_TO, INPUT_HOURS. Chỉ những env trong bảng JOB_ENV bên dưới mới được nhận, và bảng này PHẢI khớp
`dak.deid.job-env` của dak-mock (có test chống lệch). `keySecretName` là TÊN Secret (cùng namespace với SparkApplication) có file pseudonym.hmac_v1 = "base64:<key bảng>". Key KHÔNG
nằm trong conf. DAG này phải nằm trong dak.deid.airflow.allowed-dag-ids của dak-mock.

TRIGGER TAY ĐỂ THỬ (không cần DAK): bấm Trigger DAG, bỏ trống conf. Khi đó dùng DEFAULTS và MANUAL_OUTPUT bên dưới: khử định danh cột
`name` của demo_db.users_cdr_2 sang bảng demo_db.users_cdr_2_deid_manual, đọc key từ Secret `pseudonym-keys` có sẵn
(pseudonymize-table-app/k8s/pseudonym-keys-secret.yaml). Có thể chỉ đặt một phần, vd conf = {"env": {"OUTPUT_MODE": "append"}}.
Run do DAK tạo (dag_run_id bắt đầu bằng `deid-`) thì BẮT BUỘC có đủ conf, gồm OUTPUT_DB và OUTPUT_TABLE trong `env`, không bao giờ
rơi về giá trị thử.

Mọi giá trị đều được kiểm lại ở task `validate_conf` (hàm `check_conf`) vì DAG trigger tay được; sai thì DAG dừng trước khi tạo job.
Test không cần Airflow: python3 -B -m unittest discover -s dak-mock/k8s/airflow/tests
"""
import copy
import datetime
import re

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

DAG_ID = "deid_pseudonymize_table"  # DEID: phải khớp allowed-dag-ids của dak-mock

# ---------------------------------------------------------------- MÔI TRƯỜNG (sửa theo cụm của bạn)
NAMESPACE = "vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy"  # DEID: namespace có Secret key (dak.deid.kubernetes.namespace)
IMAGE = "hub.vtcc.vn:8989/hungvt0110/pseudonymize-table-app:v0.1"
HADOOP_CONFIG_MAP = "teamtscauiy-hadoop-conf"
SERVICE_ACCOUNT = "spark-application-sa"
HDFS = "hdfs://vailakehouse.datalakehla.viettel.com.vn:8020"
WAREHOUSE_PATH = f"{HDFS}/prod8/warehouse"
EVENT_LOG_DIR = f"{HDFS}/prod8/spark_history"
KERBEROS_PRINCIPAL = "presto@VAILAKEHOUSE.VIETTEL.COM"
KERBEROS_KEYTAB = "/etc/security/keytabs/presto.keytab"
KEY_MOUNT = "/etc/pseudonym-keys"

# ---------------------------------------------------------------- THAM SỐ THEO PHIẾU
# Giá trị mặc định CHỈ cho trigger tay; DAK luôn gửi đủ conf.
DEFAULTS = {
    "requestCode": "PYC-0000-000000",
    "database": "demo_db",
    "table": "users_cdr_2",
    "pseudoColumns": "name:HASH",
    "keySecretName": "pseudonym-keys",
}

# Env của job mà người gửi phiếu được nhập. PHẢI khớp dak.deid.job-env của dak-mock (test Java chống lệch đọc đúng khối này, nên giữ
# nguyên định dạng mỗi dòng: "TÊN": (mặc định hoặc None, r"quy tắc khớp toàn bộ giá trị")). Tên ngoài bảng này luôn bị từ chối.
JOB_ENV = {
    "OUTPUT_DB": (None, r"[a-z0-9_]{1,128}"),
    "OUTPUT_TABLE": (None, r"[a-z0-9_]{1,128}"),
    "OUTPUT_MODE": (None, r"overwrite|append|overwrite-partitions"),
    "OUTPUT_FORMAT": ("delta", r"delta|hive|iceberg"),
    "ENABLE_HIVE_SUPPORT": ("true", r"true|false"),
    "INPUT_FROM": (None, r"[0-9]{8}([0-9]{2})?"),
    "INPUT_TO": (None, r"[0-9]{8}([0-9]{2})?"),
    "INPUT_HOURS": (None, r"([01]?[0-9]|2[0-3])(-([01]?[0-9]|2[0-3]))?"),
}

# Khoảng ngày giờ lọc dữ liệu nguồn (cột date yyyyMMdd và hour HH của bảng). Không có mặc định: không nhập thì driver nhận giá trị rỗng
# (job coi rỗng là không đặt). OUTPUT_MODE cũng không có mặc định tĩnh: có khoảng thì overwrite-partitions, không thì overwrite.
RANGE_ENV = ("INPUT_FROM", "INPUT_TO", "INPUT_HOURS")

# OUTPUT_DB và OUTPUT_TABLE không có mặc định ở JOB_ENV vì DAK luôn gửi; giá trị này CHỈ dùng khi trigger tay.
MANUAL_OUTPUT = {"OUTPUT_DB": "demo_db", "OUTPUT_TABLE": "users_cdr_2_deid_manual"}

NAME = re.compile(r"[A-Za-z0-9_]{1,128}")
PSEUDO_COLUMNS = re.compile(r"[A-Za-z0-9_]{1,128}:HASH(,[A-Za-z0-9_]{1,128}:HASH)*")
SECRET_NAME = re.compile(r"deid-key-\d{4}-\d{6}|pseudonym-keys")
REQUEST_CODE = re.compile(r"PYC-\d{4}-\d{6}")
DAK_RUN_ID = re.compile(r"deid-\d{4}-\d{6}")

CONF_RULES = {
    "requestCode": REQUEST_CODE,
    "database": NAME,
    "table": NAME,
    "pseudoColumns": PSEUDO_COLUMNS,
    "keySecretName": SECRET_NAME,
}


def check_conf(run_id, conf):
    """Kiểm conf của dag run, ném ValueError nếu sai. Thông điệp không lặp lại giá trị đã nhận (có thể do người dùng nhập).

    Run do DAK tạo (dag_run_id bắt đầu bằng `deid-`) phải có đủ conf, không rơi về giá trị mặc định. Run trigger tay thì khoá nào
    thiếu sẽ dùng giá trị thử; env đặt thì vẫn bị kiểm như của DAK."""
    conf = conf or {}
    from_dak = run_id.startswith("deid-")
    if from_dak and not DAK_RUN_ID.fullmatch(run_id):
        raise ValueError("dag_run_id phải dạng deid-<năm>-<6 số> (do DAK đặt)")
    values = {}
    for key, rule in CONF_RULES.items():
        value = DEFAULTS[key] if (key not in conf and not from_dak) else conf.get(key)
        if not isinstance(value, str) or not rule.fullmatch(value):
            raise ValueError(f"conf.{key} thiếu hoặc sai định dạng")
        values[key] = value
    env = conf.get("env")
    env = {} if env is None else env
    if not isinstance(env, dict):
        raise ValueError("conf.env phải là object")
    for name, value in env.items():
        if name not in JOB_ENV:
            raise ValueError("conf.env có env không được phép")
        if not isinstance(value, str) or not re.fullmatch(JOB_ENV[name][1], value):
            raise ValueError(f"conf.env.{name} sai định dạng")
    if from_dak:
        for name in MANUAL_OUTPUT:
            if name not in env:
                raise ValueError(f"conf.env.{name} thiếu (DAK luôn gửi)")
    check_range(env)
    effective = {name: env.get(name, MANUAL_OUTPUT.get(name)) for name in ("OUTPUT_DB", "OUTPUT_TABLE")}
    if effective["OUTPUT_DB"] == values["database"] and effective["OUTPUT_TABLE"] == values["table"]:
        raise ValueError("bảng kết quả phải khác bảng nguồn")


def check_range(env):
    """Luật giữa các env ngày giờ, cùng luật với JobEnvPolicy của dak-mock và job: ngày có thật, giờ 00..23, FROM không sau TO,
    INPUT_HOURS tăng dần, overwrite-partitions phải có khoảng. Env đã qua kiểm định dạng ở check_conf."""
    points = {}
    for name, missing_hour in (("INPUT_FROM", 0), ("INPUT_TO", 23)):
        value = env.get(name)
        if value is None:
            continue
        try:
            datetime.datetime.strptime(value[:8], "%Y%m%d")
        except ValueError:
            raise ValueError(f"conf.env.{name} không phải ngày có thật") from None
        if len(value) == 10 and int(value[8:]) > 23:
            raise ValueError(f"conf.env.{name}: giờ phải từ 00 đến 23")
        points[name] = int(value[:8]) * 100 + (int(value[8:]) if len(value) == 10 else missing_hour)
    if len(points) == 2 and points["INPUT_FROM"] > points["INPUT_TO"]:
        raise ValueError("conf.env.INPUT_TO không được sớm hơn INPUT_FROM")
    hours = env.get("INPUT_HOURS")
    if hours is not None and "-" in hours:
        first, last = (int(part) for part in hours.split("-"))
        if first > last:
            raise ValueError("conf.env.INPUT_HOURS: giờ đầu không được lớn hơn giờ cuối")
    if env.get("OUTPUT_MODE") == "overwrite-partitions" and not any(name in env for name in RANGE_ENV):
        raise ValueError("conf.env.OUTPUT_MODE=overwrite-partitions cần INPUT_FROM, INPUT_TO hoặc INPUT_HOURS")


def validate_conf(**context):
    """Task đầu của DAG: dừng DAG trước khi tạo job nếu conf sai (chống chèn vào env/volume của SparkApplication)."""
    dag_run = context["dag_run"]
    try:
        check_conf(dag_run.run_id, dag_run.conf)
    except ValueError as error:
        raise AirflowFailException(str(error)) from None


def conf_value(key):
    """Biểu thức Jinja lấy conf.<key>, rơi về DEFAULTS khi trigger tay (check_conf đã chặn run của DAK thiếu key)."""
    return "{{ (dag_run.conf or {}).get('" + key + "', '" + DEFAULTS[key] + "') }}"


def env_value(name):
    """Biểu thức Jinja lấy conf.env.<name>; thiếu thì dùng mặc định của JOB_ENV (giá trị thử cho OUTPUT_DB, OUTPUT_TABLE; rỗng cho
    khoảng ngày giờ). OUTPUT_MODE thiếu thì overwrite-partitions khi có khoảng ngày giờ, ngược lại overwrite."""
    env = "((dag_run.conf or {}).get('env') or {})"
    if name == "OUTPUT_MODE":
        has_range = " or ".join(f"{env}.get('{key}')" for key in RANGE_ENV)
        return ("{{ " + env + ".get('OUTPUT_MODE') or ('overwrite-partitions' if (" + has_range + ") else 'overwrite') }}")
    default = JOB_ENV[name][0]
    if default is None:
        default = MANUAL_OUTPUT.get(name, "")
    return "{{ " + env + ".get('" + name + "', '" + default + "') }}"


def job_env_entries():
    """Mỗi env của JOB_ENV luôn có đúng một dòng trong driver.env: danh sách cố định, chỉ giá trị thay đổi."""
    return [{"name": name, "value": env_value(name)} for name in JOB_ENV]


# Tên SparkApplication = dag_run_id của DAK; trigger tay thì sinh tên hợp lệ DNS-1123 từ ts_nodash.
APP_NAME = "{{ dag_run.run_id if dag_run.run_id.startswith('deid-') else 'deid-manual-' ~ (ts_nodash | lower) }}"


def build_spec():
    """SparkApplication khử định danh: cùng nội dung pseudonymize-table.yaml, tham số lấy từ conf."""
    common_env = [
        {"name": "TZ", "value": "Asia/Ho_Chi_Minh"},
        {"name": "MALLOC_ARENA_MAX", "value": "2"},
    ]
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": APP_NAME, "namespace": NAMESPACE},
        "spec": {
            "type": "Scala",
            "mode": "cluster",
            "sparkVersion": "3.5.1",
            "image": IMAGE,
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local:///opt/app/pseudonymize-table-app.jar",
            "mainClass": "vai.lakehouse.pseudonymizetable.app.PseudonymizeTableApp",
            # Job ghi bảng đích: lỗi thì xem log rồi chạy lại có chủ đích, không tự restart. TTL một tuần để còn xem log.
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 604800,
            # ConfigMap core-site.xml + hdfs-site.xml (+ hive-site.xml) của cụm HDFS; phải cùng namespace.
            "hadoopConfigMap": HADOOP_CONFIG_MAP,
            "driver": {
                "cores": 1,
                "coreLimit": "1000m",
                "memory": "2048m",
                "memoryOverhead": "512m",
                "labels": {
                    "version": "3.5.1",
                    "app": "pseudonymize-table",
                    "dak.vai/request-code": conf_value("requestCode"),
                },
                "serviceAccount": SERVICE_ACCOUNT,
                "env": common_env + [
                    # ---- Đầu vào của job (pseudonymize-table-app/README.md mục 3), lấy từ conf do DAK gửi ----
                    {"name": "DB_NAME", "value": conf_value("database")},
                    {"name": "TABLE_NAME", "value": conf_value("table")},
                    {"name": "PSEUDO_COLUMNS", "value": conf_value("pseudoColumns")},  # vd name:HASH
                    {"name": "PSEUDO_MODE", "value": "HASH"},
                    # ---- Env do người gửi phiếu nhập (JOB_ENV): OUTPUT_DB, OUTPUT_TABLE, OUTPUT_MODE, OUTPUT_FORMAT, ENABLE_HIVE_SUPPORT, INPUT_FROM, INPUT_TO, INPUT_HOURS ----
                    *job_env_entries(),
                    # Đối chiếu nguồn/đích sau khi ghi (số dòng, null, số giá trị khác nhau, không sót giá trị gốc). Cố định.
                    {"name": "VERIFY", "value": "true"},
                    {"name": "WAREHOUSE_PATH", "value": WAREHOUSE_PATH},
                    # ---- Env của pseudonym-udf: khoá đọc từ file trong Secret của phiếu (chỉ driver cần) ----
                    {"name": "CRYPTO_PREFIX_SOURCE", "value": "file"},
                    {"name": "CRYPTO_KEY_PREFIX_DIR", "value": KEY_MOUNT},
                ],
                "volumeMounts": [{"name": "pseudonym-keys", "mountPath": KEY_MOUNT, "readOnly": True}],
            },
            "executor": {
                "instances": 2,
                "cores": 2,
                "coreLimit": "2000m",
                "memory": "4096m",
                "memoryOverhead": "1024m",
                "labels": {"version": "3.5.1", "app": "pseudonymize-table"},
                "serviceAccount": SERVICE_ACCOUNT,
                # Executor KHÔNG cần khoá: khoá được đọc ở driver và nằm trong closure của UDF.
                "env": copy.deepcopy(common_env),
            },
            # DEID: Secret key của phiếu do DAK tạo (file pseudonym.hmac_v1). Thiếu Secret thì pod driver không khởi động được.
            "volumes": [{"name": "pseudonym-keys", "secret": {"secretName": conf_value("keySecretName")}}],
            "sparkConf": {
                # Lib giả danh: 2 jar rời trong image, UDF thật nên executor cũng nạp.
                "spark.jars": "local:///opt/app/key-prefix-lib-1.0-SNAPSHOT.jar,local:///opt/app/pseudonym-udf-1.0-SNAPSHOT.jar",
                # Kerberos (vùng storage HDFS). keytab/krb5.conf là path trên pod Spark Operator.
                "spark.kerberos.access.hadoopFileSystems": HDFS,
                "spark.kerberos.principal": KERBEROS_PRINCIPAL,
                "spark.kerberos.keytab": KERBEROS_KEYTAB,
                "spark.kubernetes.kerberos.krb5.path": "/etc/krb5.conf",
                "spark.security.credentials.hadoopfs.enabled": "true",
                # HDFS security: giữ wire encryption.
                "spark.hadoop.hadoop.security.authentication": "kerberos",
                "spark.hadoop.hadoop.security.authorization": "true",
                "spark.hadoop.fs.permissions.umask-mode": "002",
                # Delta Lake (đã có trong fat jar).
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                # BẮT BUỘC false (patch block token qua ServiceLoader).
                "spark.driver.userClassPathFirst": "false",
                "spark.executor.userClassPathFirst": "false",
                # log4j2.
                "spark.driver.extraJavaOptions": "-Dlog4j.configurationFile=file:/opt/app/log4j2.properties",
                "spark.executor.extraJavaOptions": "-Dlog4j.configurationFile=file:/opt/app/log4j2.properties",
                # Spark.
                "spark.jars.ivy": "/tmp",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.scheduler.mode": "FAIR",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.sources.ignoreDataLocality.enabled": "true",
                "spark.dynamicAllocation.enabled": "false",
                "spark.sql.shuffle.partitions": "64",
                # Event log (Spark History Server).
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": EVENT_LOG_DIR,
                "spark.eventLog.rolling.enabled": "true",
                "spark.eventLog.rolling.maxFileSize": "128m",
            },
        },
    }


with DAG(
    DAG_ID,
    description="Khử định danh một bảng theo phiếu PYC do DAK trigger",
    schedule=None,  # chỉ chạy khi DAK trigger (hoặc trigger tay để thử)
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    # PHẢI là False (mặc định). Native render đổi chuỗi trông như số thành số ("2" thành 2, "64" thành 64): env `value` và sparkConf
    # bắt buộc là chuỗi nên Kubernetes trả 422 "must be of type string". DAG này chỉ templating ra chuỗi nên không cần native.
    render_template_as_native_obj=False,
    max_active_runs=4,
    tags=["deid", "dak"],
) as dag:
    validate = PythonOperator(task_id="validate_conf", python_callable=validate_conf)

    # Spec là dict nên truyền qua `template_spec`. `application_file` chỉ nhận đường dẫn file hoặc chuỗi YAML: đưa dict vào đó thì
    # lỗi "AttributeError: 'dict' object has no attribute 'rstrip'" (provider cncf.kubernetes 7.x/8.x). Tên tham số còn lại
    # (kubernetes_conn_id, ...) tuỳ phiên bản provider: dùng đúng cách bạn đang gọi SparkKubernetesOperator trong DAG hiện có.
    run_job = SparkKubernetesOperator(
        task_id="pseudonymize_table",
        namespace=NAMESPACE,
        template_spec=build_spec(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        # Chờ job xong để dag run phản ánh đúng kết quả: DAK đọc state của dag run (success/failed) để cập nhật phiếu.
        # Nếu provider của bạn dùng sensor riêng (SparkKubernetesSensor), thêm vào ngay sau task này.
    )

    validate >> run_job
