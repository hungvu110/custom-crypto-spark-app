"""DAG MẪU nhận dag run do DAK trigger để khử định danh một bảng (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md mục 4.7, bước D11).

Không chạy được nguyên trạng: copy vào thư mục dags của Airflow rồi thay `base_spec` bằng spec đang chạy tốt của bạn
(vd `test_spark_job_vailakehouse.py`: image, hadoopConfigMap, sparkConf Kerberos/Delta/eventLog...). Chỉ phần đánh dấu
"# DEID" là bắt buộc cho hợp đồng với DAK.

DAK gọi POST /api/v1/dags/<dagId>/dagRuns với:
  dag_run_id = deid-<năm>-<6 số>              (tên job, cũng là tên SparkApplication)
  conf = {requestCode, database, table, pseudoColumns, outputDb, outputTable, keySecretName}
`keySecretName` là TÊN Secret (cùng namespace với SparkApplication) có file pseudonym.hmac_v1 = "base64:<key bảng>". Key KHÔNG
nằm trong conf. DAG này phải nằm trong dak.deid.airflow.allowed-dag-ids của dak-mock.

Mọi giá trị conf đều được kiểm lại ở task `validate_conf` vì DAG cũng trigger tay được; sai thì DAG dừng trước khi tạo job.
"""
import copy
import re

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

DAG_ID = "deid_pseudonymize_table"  # DEID: phải khớp allowed-dag-ids của dak-mock
NAMESPACE = "vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy"  # DEID: namespace có Secret key (dak.deid.kubernetes.namespace)

NAME = re.compile(r"[A-Za-z0-9_]{1,128}")
OUTPUT_NAME = re.compile(r"[A-Za-z0-9_]{1,200}")
PSEUDO_COLUMNS = re.compile(r"[A-Za-z0-9_]{1,128}:HASH(,[A-Za-z0-9_]{1,128}:HASH)*")
SECRET_NAME = re.compile(r"deid-key-\d{4}-\d{6}")
REQUEST_CODE = re.compile(r"PYC-\d{4}-\d{6}")
RUN_ID = re.compile(r"deid-\d{4}-\d{6}")

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
    if not RUN_ID.fullmatch(dag_run.run_id):
        raise AirflowFailException("dag_run_id phải dạng deid-<năm>-<6 số> (do DAK đặt)")
    for key, rule in CONF_RULES.items():
        value = conf.get(key)
        if not isinstance(value, str) or not rule.fullmatch(value):
            raise AirflowFailException(f"conf.{key} thiếu hoặc sai định dạng")


# DEID: thay bằng spec đang chạy tốt của bạn. Ở đây chỉ giữ phần liên quan tới job khử định danh.
base_spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {"name": "{{ dag_run.run_id }}", "namespace": NAMESPACE},  # DEID: tên = dag_run_id
    "spec": {
        "type": "Scala",
        "mode": "cluster",
        "sparkVersion": "3.5.1",
        "image": "hub.vtcc.vn:8989/hungvt0110/pseudonymize-table-app:v0.1",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/app/pseudonymize-table-app.jar",
        "mainClass": "vai.lakehouse.pseudonymizetable.app.PseudonymizeTableApp",
        "restartPolicy": {"type": "Never"},
        "timeToLiveSeconds": 604800,
        "hadoopConfigMap": "teamtscauiy-hadoop-conf",
        "driver": {
            "cores": 1,
            "coreLimit": "1000m",
            "memory": "2048m",
            "serviceAccount": "spark-application-sa",
            "labels": {"version": "3.5.1", "dak.vai/request-code": "{{ dag_run.conf['requestCode'] }}"},
            "volumeMounts": [{"name": "pseudonym-keys", "mountPath": "/etc/pseudonym-keys", "readOnly": True}],
        },
        "executor": {
            "cores": 2,
            "coreLimit": "2000m",
            "memory": "4096m",
            "instances": 2,
            "serviceAccount": "spark-application-sa",
            "labels": {"version": "3.5.1"},
        },
        # DEID: Secret key của phiếu, do DAK tạo; job đọc file pseudonym.hmac_v1 trong thư mục mount.
        "volumes": [{"name": "pseudonym-keys", "secret": {"secretName": "{{ dag_run.conf['keySecretName'] }}"}}],
        "sparkConf": {
            "spark.jars": "local:///opt/app/key-prefix-lib-1.0-SNAPSHOT.jar,local:///opt/app/pseudonym-udf-1.0-SNAPSHOT.jar",
            # ... copy phần Kerberos/Delta/eventLog từ spec đang chạy của bạn ...
        },
    },
}


def build_spec():
    spec = copy.deepcopy(base_spec)
    driver_env = [  # DEID: tham số job lấy từ conf do DAK gửi
        {"name": "DB_NAME", "value": "{{ dag_run.conf['database'] }}"},
        {"name": "TABLE_NAME", "value": "{{ dag_run.conf['table'] }}"},
        {"name": "PSEUDO_COLUMNS", "value": "{{ dag_run.conf['pseudoColumns'] }}"},
        {"name": "PSEUDO_MODE", "value": "HASH"},
        {"name": "OUTPUT_DB", "value": "{{ dag_run.conf['outputDb'] }}"},
        {"name": "OUTPUT_TABLE", "value": "{{ dag_run.conf['outputTable'] }}"},
        {"name": "OUTPUT_MODE", "value": "overwrite"},
        {"name": "VERIFY", "value": "true"},
        {"name": "ENABLE_HIVE_SUPPORT", "value": "true"},
        {"name": "CRYPTO_PREFIX_SOURCE", "value": "file"},
        {"name": "CRYPTO_KEY_PREFIX_DIR", "value": "/etc/pseudonym-keys"},
        # {"name": "WAREHOUSE_PATH", "value": "hdfs://.../prod8/warehouse"},  # như spec đang chạy của bạn
    ]
    spec["spec"]["driver"]["env"] = driver_env
    return spec


with DAG(
    DAG_ID,
    description="Khử định danh một bảng theo phiếu PYC do DAK trigger",
    schedule=None,  # chỉ chạy khi DAK trigger
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
