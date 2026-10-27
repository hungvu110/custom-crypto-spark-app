"""DAG chuẩn hoá một lô file VTT vào silver (docs/silver-ingest/PLAN.md), trigger bởi DAK khi phiếu chuẩn hoá được duyệt. File VTT đã
được mã hoá từng cột: job chỉ đọc rồi ghi bảng, không gọi DAK.

DAK trigger: POST /api/v1/dags/silver_ingest_vtt/dagRuns, conf = {"path": "<tương đối theo INPUT_ROOT, vd f001-mpre-call/date=20261024/hour=12>",
"batchId": "yyyyMMddHH", "table": "vtt_f001_mpre_call", "outputMode": "overwrite-partitions|append" (tuỳ chọn, mặc định overwrite-partitions)}. Test không cần Airflow:
    python3 -B -m unittest discover -s airflow/tests
"""
import os
import sys

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# Airflow chỉ thêm gốc thư mục DAG vào sys.path; khi file nằm trong thư mục con (vd. .../dags/dags/) thì phải thêm thư mục của file.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import silver_ingest_common as common  # noqa: E402

DAG_ID = "silver_ingest_vtt"

# Cấu hình cố định theo nguồn. GIẢ ĐỊNH — sửa theo dữ liệu thật của VTT (định dạng, schema, bảng đích, gốc thư mục landing).
SOURCE = {
    "name": "vtt",
    "main_class": "vai.lakehouse.silveringest.app.VttSilverApp",
    "input_root": f"{common.HDFS}/landing/vtt/lakehouse/vtt-core/",
    "conf_keys": ("path", "batchId", "table", "outputMode"),
    "table_prefix": "vtt_",
    "env": {
        "INPUT_FORMAT": "csv",
        "INPUT_OPTION_header": "true",
        "OUTPUT_DB": "silver",
        "OUTPUT_FORMAT": "delta",
    },
}


def validate_conf(**context):
    """Task đầu: dừng DAG trước khi tạo job nếu conf sai."""
    try:
        common.check_conf(SOURCE, context["dag_run"].conf)
    except ValueError as error:
        raise AirflowFailException(str(error)) from None


with DAG(
    DAG_ID,
    description="Chuẩn hoá một lô file VTT (đã mã hoá cột) vào silver",
    schedule=None,  # chỉ chạy khi DAK trigger
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    # PHẢI là False: native render đổi "2" thành 2 và Kubernetes trả 422 (env value phải là chuỗi).
    render_template_as_native_obj=False,
    user_defined_macros={"k8s_name": common.k8s_name},
    max_active_runs=4,
    tags=["silver", "vtt", "dak"],
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
