"""DAG chuẩn hoá một lô file VTNET vào silver (docs/silver-ingest/PLAN.md), trigger bởi DAK khi phiếu chuẩn hoá được duyệt.

Luồng của job (docs/key-access/TRA-LOI-DAK-BY-PATH.md): hỏi DAK key theo đường dẫn (by-path) của `path`. DAK có key → file bị mã hoá cả
file, job giải mã (FILE_CIPHER); DAK trả rỗng → file không mã hoá, job đọc nguyên trạng. Sau đó mã hoá các cột `encryptColumns` bằng
cdr-crypto (key theo cột qua DAK by-column, onMissing=request) rồi ghi bảng. Cột chưa có key: DAK mở yêu cầu (PENDING), job CHỜ và gọi
lại theo exponential backoff (PENDING_WAIT) cho tới khi admin DAK duyệt, rồi chạy tiếp. Hết timeout mà chưa duyệt, hoặc yêu cầu bị từ
chối: job exit 5 (task failed), chưa ghi gì.

DAK trigger: POST /api/v1/dags/silver_ingest_vtnet/dagRuns, conf = {"path": "<tương đối theo INPUT_ROOT, vd ebs_3g_4g/dt=20261012/batch=1520>",
"batchId": "yyyyMMddHHmm", "table": "vtnet_ebs_3g_4g", "encryptColumns": ["msisdn", "imei"], "keyField": "start_time",
"outputMode": "overwrite-partitions|append" (tuỳ chọn, mặc định overwrite-partitions: chỉ thay partition của lô)}.
"""
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

import silver_ingest_common as common

DAG_ID = "silver_ingest_vtnet"

# Gốc landing của nguồn (GIẢ ĐỊNH, sửa theo cụm). INPUT_PATH = INPUT_ROOT + conf.path; đường dẫn gửi by-path chính là conf.path.
INPUT_ROOT = f"{common.HDFS}/prod8/landing/vtnet/"

# Chờ admin DAK duyệt yêu cầu key cột (by-column trả PENDING): lần đầu sau `initial` giây, mỗi lần sau nhân `multiplier`, tối đa
# `max_interval` giây một lần, bỏ cuộc sau `timeout` giây (job exit 5). Trong lúc chờ, driver và executor vẫn giữ tài nguyên.
PENDING_WAIT = {"timeout": 7200, "initial": 30, "max_interval": 600, "multiplier": 2}

# Cấu hình cố định theo nguồn. GIẢ ĐỊNH — sửa theo dữ liệu thật của VTNET (định dạng, schema, bảng đích). Cột cần mã hoá và keyField
# do người gửi phiếu chọn (conf).
SOURCE = {
    "name": "vtnet",
    "main_class": "vai.lakehouse.silveringest.app.VtnetSilverApp",
    "input_root": INPUT_ROOT,
    "conf_keys": ("path", "batchId", "table", "outputMode", "encryptColumns", "keyField"),
    "table_prefix": "vtnet_",
    "encrypts_columns": True,
    "env": {
        "INPUT_FORMAT": "csv",
        "INPUT_OPTION_header": "true",
        "OUTPUT_DB": "silver",
        "OUTPUT_FORMAT": "delta",
        # Thuật toán giải mã cả file, chỉ dùng khi by-path có key cho đường dẫn. GIẢ ĐỊNH cho tới khi xác nhận định dạng thật.
        "FILE_CIPHER": "aes-256-gcm",
        # Gốc mà đường dẫn by-path tương đối theo: job gửi đúng phần của INPUT_PATH sau gốc, tức conf.path.
        "KEY_PATH_ROOT": INPUT_ROOT,
        # onMissing của by-column: "request" — cột chưa có key thì DAK mở yêu cầu, job chờ (PENDING_WAIT) tới khi admin DAK duyệt
        # (docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md mục 3.2).
        "CDR_ON_MISSING": "request",
    },
    "driver_extra_env": [
        # Cột mã hoá bằng cdr-crypto (mỗi cột một key, dataset = OUTPUT_DB.OUTPUT_TABLE) và cột keyField giữ plaintext: từ phiếu.
        {"name": "CDR_ENCRYPT_COLUMNS", "value": "{{ ((dag_run.conf or {}).get('encryptColumns') or []) | join(',') }}"},
        {"name": "CDR_KEY_FIELD", "value": common.conf_value("keyField")},
        # ---- Nguồn key: DAK (by-path và by-column), client NỘI BỘ, chỉ driver cần ----
        {"name": "CRYPTO_PREFIX_SOURCE", "value": "dak"},
        # Đang trỏ tới dak-mock (HTTP, cùng namespace). Với DAK thật: https://<dak> và XOÁ DAK_ALLOW_INSECURE_HTTP.
        {"name": "DAK_ADDR", "value": "http://dak-mock:8085"},
        {"name": "DAK_ALLOW_INSECURE_HTTP", "value": "true"},
        {"name": "DAK_TOKEN_URL",
         "value": "https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/token"},
        # GIẢ ĐỊNH — client-id nội bộ thật (dak-mock: "kind: internal").
        {"name": "DAK_CLIENT_ID", "value": "<client-id-noi-bo>"},
        {"name": "DAK_CLIENT_SECRET", "valueFrom": {"secretKeyRef": {"name": "spark-dak-client", "key": "clientSecret"}}},
        # Client nội bộ BẮT BUỘC gửi tag=internal.
        {"name": "DAK_TAG", "value": "internal"},
        {"name": "DAK_CALLER", "value": "silver-ingest-vtnet"},
        {"name": "DAK_PENDING_WAIT_TIMEOUT_SECONDS", "value": str(PENDING_WAIT["timeout"])},
        {"name": "DAK_PENDING_WAIT_INITIAL_SECONDS", "value": str(PENDING_WAIT["initial"])},
        {"name": "DAK_PENDING_WAIT_MAX_INTERVAL_SECONDS", "value": str(PENDING_WAIT["max_interval"])},
        {"name": "DAK_PENDING_WAIT_MULTIPLIER", "value": str(PENDING_WAIT["multiplier"])},
    ],
}


def validate_conf(**context):
    """Task đầu: dừng DAG trước khi tạo job nếu conf sai."""
    try:
        common.check_conf(SOURCE, context["dag_run"].conf)
    except ValueError as error:
        raise AirflowFailException(str(error)) from None


with DAG(
    DAG_ID,
    description="Chuẩn hoá một lô file VTNET (mã hoá cả file) vào silver, mã hoá cột bằng cdr-crypto",
    schedule=None,  # chỉ chạy khi DAK trigger
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    # PHẢI là False: native render đổi "2" thành 2 và Kubernetes trả 422 (env value phải là chuỗi).
    render_template_as_native_obj=False,
    user_defined_macros={"k8s_name": common.k8s_name},
    max_active_runs=4,
    tags=["silver", "vtnet", "dak"],
) as dag:
    validate = PythonOperator(task_id="validate_conf", python_callable=validate_conf)
    run_job = SparkKubernetesOperator(
        task_id="ingest",
        namespace=common.NAMESPACE,
        template_spec=common.build_spec(SOURCE),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )
    validate >> run_job
