"""Two DAGs that look after the result tables of the de-identification (the spark-app image is the one of deid_pseudonymize_table).

1. `deid_purge_range` (triggered by DAK when a request expires): deletes the partitions (dt, hour) of the request from the result table.
   Each team has its OWN result table (ebs_teamA, ebs_teamB, ...), so a purge never touches another team's data.

   DAK calls POST /api/v1/dags/deid_purge_range/dagRuns with:
     dag_run_id = purge-<year>-<6 digits>[-r<n>]     (also the SparkApplication name; the -r<n> form is for a purge repeated on the same request)
     conf = {requestCode, env: {OUTPUT_DB, OUTPUT_TABLE, INPUT_FROM, INPUT_TO[, INPUT_HOURS]}}
   INPUT_FROM and INPUT_TO are ALWAYS required (an open range would delete the whole table). Every value is checked again by the task
   `validate_conf`; an invalid conf stops the DAG before any job is created. There is no default for a manual trigger either: a manual
   trigger must carry the same conf.

   What DAK can read from the dag run state (the job is idempotent, so calling again is safe):
     success -> the partitions are gone, OR the table does not exist (already deleted / never created): both mean "nothing left to delete";
     failed  -> the deletion did not complete (permission, infrastructure, partitioning refused): retry and alert.
   The job log tells the cases apart (`TABLE_NOT_FOUND ...`).

2. `deid_vacuum` (every day at 03:00): Delta only marks the files of a deleted/overwritten partition as removed; `VACUUM` is what deletes
   them from HDFS (after VACUUM_RETAIN_HOURS). Until it runs, the data of an expired request is still on HDFS and can be read by time
   travel. Tables: Airflow Variable `deid_vacuum_targets` (comma-separated <db> or <db>.<table>, default DEFAULT_VACUUM_TARGETS) and
   Variable `deid_vacuum_retain_hours` (default DEFAULT_VACUUM_RETAIN_HOURS).

The environment constants (namespace, image, Kerberos, HDFS) are the ones of sample_dag_deid_pseudonymize_table.py: change them there.
The tests do not need Airflow: python3 -B -m unittest discover -s airflow/tests
"""
import re

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

import sample_dag_deid_pseudonymize_table as deid

PURGE_DAG_ID = "deid_purge_range"  # DEID: DAK must be allowed to trigger it
VACUUM_DAG_ID = "deid_vacuum"

PURGE_RUN_ID = re.compile(r"purge-\d{4}-\d{6}(-r[0-9]{1,3})?")
PURGE_ENV = ("OUTPUT_DB", "OUTPUT_TABLE", "INPUT_FROM", "INPUT_TO", "INPUT_HOURS")  # rules: deid.JOB_ENV
PURGE_REQUIRED = ("OUTPUT_DB", "OUTPUT_TABLE", "INPUT_FROM", "INPUT_TO")

# Variable defaults of the vacuum DAG. The result tables are created in the OUTPUT_DB the requester typed: set the Variable to the
# database(s) that hold them.
DEFAULT_VACUUM_TARGETS = "demo_db"
DEFAULT_VACUUM_RETAIN_HOURS = "168"


def check_purge_conf(run_id, conf):
    """Raise ValueError when the conf of a purge run is invalid. Messages never repeat the received value (it may be user input)."""
    if run_id.startswith("purge-") and not PURGE_RUN_ID.fullmatch(run_id):
        raise ValueError("dag_run_id must have the form purge-<year>-<6 digits>[-r<n>] (set by DAK)")
    conf = conf or {}
    if set(conf) - {"requestCode", "env"}:
        raise ValueError("conf has a key that is not allowed")
    if not isinstance(conf.get("requestCode"), str) or not deid.REQUEST_CODE.fullmatch(conf["requestCode"]):
        raise ValueError("conf.requestCode is missing or malformed")
    env = conf.get("env")
    if not isinstance(env, dict):
        raise ValueError("conf.env must be an object")
    for name, value in env.items():
        if name not in PURGE_ENV:
            raise ValueError("conf.env has an env that is not allowed")
        if not isinstance(value, str) or not re.fullmatch(deid.JOB_ENV[name][1], value):
            raise ValueError(f"conf.env.{name} is malformed")
    for name in PURGE_REQUIRED:
        if name not in env:
            raise ValueError(f"conf.env.{name} is required")
    deid.check_range(env)


def validate_conf(**context):
    """First task of the purge DAG: stop before any job is created when the conf is invalid."""
    dag_run = context["dag_run"]
    try:
        check_purge_conf(dag_run.run_id, dag_run.conf)
    except ValueError as error:
        raise AirflowFailException(str(error)) from None


def conf_env(name):
    """Jinja expression for conf.env.<name>; INPUT_HOURS is the only one that may be absent (empty value = every hour)."""
    return "{{ ((dag_run.conf or {}).get('env') or {}).get('" + name + "', '') }}"


def spark_application(name, main_class, driver_env, executors, labels):
    """SparkApplication of a small maintenance job: same image, Kerberos/HDFS and Delta config as the de-identification job."""
    common_env = [{"name": "TZ", "value": "Asia/Ho_Chi_Minh"}, {"name": "MALLOC_ARENA_MAX", "value": "2"}]
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": name, "namespace": deid.NAMESPACE},
        "spec": {
            "type": "Scala",
            "mode": "cluster",
            "sparkVersion": "3.5.1",
            "image": deid.IMAGE,
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local:///opt/app/pseudonymize-table-app.jar",
            "mainClass": main_class,
            # Never restart by itself: read the log, then run it again on purpose (both jobs are safe to repeat).
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 604800,
            "hadoopConfigMap": deid.HADOOP_CONFIG_MAP,
            "driver": {
                "cores": 1,
                "coreLimit": "1000m",
                "memory": "2048m",
                "memoryOverhead": "512m",
                "labels": {"version": "3.5.1", **labels},
                "serviceAccount": deid.SERVICE_ACCOUNT,
                "env": common_env + driver_env + [{"name": "WAREHOUSE_PATH", "value": deid.WAREHOUSE_PATH},
                                                  {"name": "ENABLE_HIVE_SUPPORT", "value": "true"}],
            },
            "executor": {
                "instances": executors,
                "cores": 2,
                "coreLimit": "2000m",
                "memory": "4096m",
                "memoryOverhead": "1024m",
                "labels": {"version": "3.5.1", **labels},
                "serviceAccount": deid.SERVICE_ACCOUNT,
                "env": list(common_env),
            },
            # Fixed small executors: the job only touches the Delta log and (VACUUM) lists/deletes files.
            "sparkConf": {**deid.SHARED_SPARK_CONF, "spark.dynamicAllocation.enabled": "false", "spark.sql.shuffle.partitions": "8"},
        },
    }


def build_purge_spec():
    name = "{{ dag_run.run_id if dag_run.run_id.startswith('purge-') else 'purge-manual-' ~ (ts_nodash | lower) }}"
    env = [{"name": key, "value": conf_env(key)} for key in PURGE_ENV]
    env += [{"name": key, "value": value} for key, value in deid.SOURCE_PARTITION_ENV.items()]
    labels = {"app": "deid-purge-range", "dak.vai/request-code": "{{ (dag_run.conf or {}).get('requestCode', '') }}"}
    return spark_application(name, "vai.lakehouse.pseudonymizetable.app.PurgeRangeApp", env, executors=1, labels=labels)


def build_vacuum_spec():
    targets = "{{ var.value.get('deid_vacuum_targets', '" + DEFAULT_VACUUM_TARGETS + "') }}"
    retain = "{{ var.value.get('deid_vacuum_retain_hours', '" + DEFAULT_VACUUM_RETAIN_HOURS + "') }}"
    env = [{"name": "VACUUM_TARGETS", "value": targets}, {"name": "VACUUM_RETAIN_HOURS", "value": retain}]
    return spark_application("deid-vacuum-{{ ts_nodash | lower }}", "vai.lakehouse.pseudonymizetable.app.VacuumApp", env, executors=2,
                             labels={"app": "deid-vacuum"})


with DAG(
    PURGE_DAG_ID,
    description="Delete the partitions of an expired request from its result table (triggered by DAK)",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    render_template_as_native_obj=False,  # env values must stay strings
    max_active_runs=1,
    tags=["deid", "dak", "purge"],
) as purge_dag:
    validate = PythonOperator(task_id="validate_conf", python_callable=validate_conf)
    purge = SparkKubernetesOperator(
        task_id="purge_range",
        namespace=deid.NAMESPACE,
        template_spec=build_purge_spec(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        # Wait for the job: DAK reads the state of the dag run (success/failed).
    )
    validate >> purge

with DAG(
    VACUUM_DAG_ID,
    description="VACUUM the result tables so deleted/overwritten data really leaves HDFS",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    render_template_as_native_obj=False,
    max_active_runs=1,
    tags=["deid", "maintenance"],
) as vacuum_dag:
    SparkKubernetesOperator(
        task_id="vacuum_tables",
        namespace=deid.NAMESPACE,
        template_spec=build_vacuum_spec(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )
