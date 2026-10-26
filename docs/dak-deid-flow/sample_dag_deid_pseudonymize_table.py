"""DAG MẪU nhận dag run do DAK trigger để khử định danh một bảng (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 4.7, bước D11).

Spec SparkApplication ở đây giữ ĐỦ config và env của pseudonymize-table-app/k8s/pseudonymize-table.yaml (cùng nội dung với mẫu
dak-mock/k8s/deid/sparkapplication.yaml): image, hadoopConfigMap, Kerberos, Delta, event log, env của job và của pseudonym-udf. Chỉ
các giá trị theo từng phiếu lấy từ `conf`. Nếu môi trường của bạn khác (vd cụm HDFS `hkh`, warehouse `/prod4`, principal `k8s@...`
như DAG vailakehouse đang chạy) thì sửa các hằng ở khối "MÔI TRƯỜNG" bên dưới.

DAK gọi POST /api/v1/dags/<dagId>/dagRuns với:
  dag_run_id = deid-<năm>-<6 số>              (tên job, cũng là tên SparkApplication)
  conf = {requestCode, database, table, pseudoColumns, outputDb, outputTable, keySecretName}
`keySecretName` là TÊN Secret (cùng namespace với SparkApplication) có file pseudonym.hmac_v1 = "base64:<key bảng>". Key KHÔNG
nằm trong conf. DAG này phải nằm trong dak.deid.airflow.allowed-dag-ids của dak-mock.

TRIGGER TAY ĐỂ THỬ (không cần DAK): bấm Trigger DAG, bỏ trống conf. Khi đó dùng DEFAULTS bên dưới: khử định danh cột `name` của
demo_db.users_cdr_2 sang bảng demo_db.users_cdr_2_deid_manual, đọc key từ Secret `pseudonym-keys` có sẵn
(pseudonymize-table-app/k8s/pseudonym-keys-secret.yaml). Run do DAK tạo (dag_run_id bắt đầu bằng `deid-`) thì BẮT BUỘC có đủ conf,
không bao giờ rơi về DEFAULTS.

Mọi giá trị đều được kiểm lại ở task `validate_conf` vì DAG trigger tay được; sai thì DAG dừng trước khi tạo job.
"""
import copy
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
    "outputDb": "demo_db",
    "outputTable": "users_cdr_2_deid_manual",
    "keySecretName": "pseudonym-keys",
}

NAME = re.compile(r"[A-Za-z0-9_]{1,128}")
OUTPUT_NAME = re.compile(r"[A-Za-z0-9_]{1,200}")
PSEUDO_COLUMNS = re.compile(r"[A-Za-z0-9_]{1,128}:HASH(,[A-Za-z0-9_]{1,128}:HASH)*")
SECRET_NAME = re.compile(r"deid-key-\d{4}-\d{6}|pseudonym-keys")
REQUEST_CODE = re.compile(r"PYC-\d{4}-\d{6}")
DAK_RUN_ID = re.compile(r"deid-\d{4}-\d{6}")

CONF_RULES = {
    "requestCode": REQUEST_CODE,
    "database": NAME,
    "table": NAME,
    "pseudoColumns": PSEUDO_COLUMNS,
    "outputDb": OUTPUT_NAME,
    "outputTable": OUTPUT_NAME,
    "keySecretName": SECRET_NAME,
}


def validate_conf(**context):
    """Dừng DAG nếu conf thiếu hoặc có ký tự ngoài bộ cho phép (chống chèn vào env/volume của SparkApplication)."""
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    from_dak = dag_run.run_id.startswith("deid-")
    if from_dak and not DAK_RUN_ID.fullmatch(dag_run.run_id):
        raise AirflowFailException("dag_run_id phải dạng deid-<năm>-<6 số> (do DAK đặt)")
    for key, rule in CONF_RULES.items():
        if key not in conf and not from_dak:
            value = DEFAULTS[key]  # trigger tay: rơi về DEFAULTS
        else:
            value = conf.get(key)  # run của DAK: không có giá trị mặc định
        if not isinstance(value, str) or not rule.fullmatch(value):
            raise AirflowFailException(f"conf.{key} thiếu hoặc sai định dạng")


def conf_value(key):
    """Biểu thức Jinja lấy conf.<key>, rơi về DEFAULTS khi trigger tay (validate_conf đã chặn run của DAK thiếu key)."""
    return "{{ dag_run.conf.get('" + key + "', '" + DEFAULTS[key] + "') }}"


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
                    {"name": "OUTPUT_DB", "value": conf_value("outputDb")},
                    {"name": "OUTPUT_TABLE", "value": conf_value("outputTable")},
                    {"name": "OUTPUT_MODE", "value": "overwrite"},
                    # Đối chiếu nguồn/đích sau khi ghi (số dòng, null, số giá trị khác nhau, không sót giá trị gốc).
                    {"name": "VERIFY", "value": "true"},
                    {"name": "ENABLE_HIVE_SUPPORT", "value": "true"},
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
    render_template_as_native_obj=True,
    max_active_runs=4,
    tags=["deid", "dak"],
) as dag:
    validate = PythonOperator(task_id="validate_conf", python_callable=validate_conf)

    # Tên tham số (application_file / template_spec, kubernetes_conn_id, ...) tuỳ phiên bản provider cncf.kubernetes:
    # dùng đúng cách bạn đang gọi SparkKubernetesOperator trong DAG hiện có.
    run_job = SparkKubernetesOperator(
        task_id="pseudonymize_table",
        namespace=NAMESPACE,
        application_file=build_spec(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        # Chờ job xong để dag run phản ánh đúng kết quả: DAK đọc state của dag run (success/failed) để cập nhật phiếu.
        # Nếu provider của bạn dùng sensor riêng (SparkKubernetesSensor), thêm vào ngay sau task này.
    )

    validate >> run_job
