"""SAMPLE DAG that receives a dag run triggered by DAK to de-identify one table (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md section 4.7, step D11).

The SparkApplication spec here keeps ALL the config and env of pseudonymize-table-app/k8s/pseudonymize-table.yaml (same content as the
template dak-mock/k8s/deid/sparkapplication.yaml): image, hadoopConfigMap, Kerberos, Delta, event log, job env and pseudonym-udf env. Only
the per-request values come from `conf`. If your environment differs (e.g. HDFS cluster `hkh`, warehouse `/prod4`, principal `k8s@...`
like the vailakehouse DAG that is running) adjust the constants in the "ENVIRONMENT" block below.

DAK calls POST /api/v1/dags/<dagId>/dagRuns with:
  dag_run_id = deid-<year>-<6 digits>              (job name, also the SparkApplication name)
  conf = {requestCode, database, table, pseudoColumns, keySecretName, env}
`env` is the FINAL env of the job (the env typed by the requester plus the default values filled in by DAK), made of OUTPUT_DB,
OUTPUT_TABLE, OUTPUT_MODE, OUTPUT_FORMAT, ENABLE_HIVE_SUPPORT and (when the requester enters them) the date-hour range INPUT_FROM,
INPUT_TO, INPUT_HOURS. Only the env listed in the JOB_ENV table below are accepted, and that table MUST match `dak.deid.job-env` of
dak-mock (a test guards against drift). `keySecretName` is the NAME of a Secret (same namespace as the SparkApplication) holding the file
pseudonym.hmac_v1 = "base64:<table key>". The key is NOT in conf. This DAG must be listed in dak.deid.airflow.allowed-dag-ids of dak-mock.

MANUAL TRIGGER FOR TESTING (no DAK needed): press Trigger DAG with an empty conf. DEFAULTS and MANUAL_OUTPUT below are then used: the column
`name` of demo_db.users_cdr_2 is de-identified into the table demo_db.users_cdr_2_deid_manual, reading the key from the existing Secret
`pseudonym-keys` (pseudonymize-table-app/k8s/pseudonym-keys-secret.yaml). Only part of the conf can be set, e.g. conf = {"env":
{"OUTPUT_MODE": "append"}}.
A run created by DAK (dag_run_id starting with `deid-`) MUST carry the full conf, including OUTPUT_DB and OUTPUT_TABLE in `env`, and never
falls back to the test values.

Every value is checked again in the `validate_conf` task (function `check_conf`) because the DAG can be triggered by hand; when a value is
invalid the DAG stops before any job is created.
The tests do not need Airflow: python3 -B -m unittest discover -s airflow/tests
"""
import copy
import datetime
import re

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

DAG_ID = "deid_pseudonymize_table"  # DEID: must match allowed-dag-ids of dak-mock

# ---------------------------------------------------------------- ENVIRONMENT (adjust to your cluster)
NAMESPACE = "vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy"  # DEID: namespace that holds the key Secret (dak.deid.kubernetes.namespace)
IMAGE = "hub.vtcc.vn:8989/hungvt0110/pseudonymize-table-app:v0.1"
HADOOP_CONFIG_MAP = "teamtscauiy-hadoop-conf"
SERVICE_ACCOUNT = "spark-application-sa"
HDFS = "hdfs://vailakehouse.datalakehla.viettel.com.vn:8020"
WAREHOUSE_PATH = f"{HDFS}/prod8/warehouse"
EVENT_LOG_DIR = f"{HDFS}/prod8/spark_history"
KERBEROS_PRINCIPAL = "presto@VAILAKEHOUSE.VIETTEL.COM"
KERBEROS_KEYTAB = "/etc/security/keytabs/presto.keytab"
KEY_MOUNT = "/etc/pseudonym-keys"

# ---------------------------------------------------------------- PER-REQUEST PARAMETERS
# Default values ONLY for a manual trigger; DAK always sends the full conf.
DEFAULTS = {
    "requestCode": "PYC-0000-000000",
    "database": "demo_db",
    "table": "users_cdr_2",
    "pseudoColumns": "name:HASH",
    "keySecretName": "pseudonym-keys",
}

# Job env that the requester may enter. MUST match dak.deid.job-env of dak-mock (the Java drift test reads exactly this block, so keep the
# format of every line: "NAME": (default or None, r"rule that matches the whole value")). Names outside this table are always rejected.
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

# Date-hour range that filters the source data (column date yyyyMMdd and hour HH of the table). No default: when it is not entered the
# driver gets an empty value (the job treats empty as not set). OUTPUT_MODE has no static default either: with a range it is
# overwrite-partitions, without one it is overwrite.
RANGE_ENV = ("INPUT_FROM", "INPUT_TO", "INPUT_HOURS")

# OUTPUT_DB and OUTPUT_TABLE have no default in JOB_ENV because DAK always sends them; these values are used ONLY for a manual trigger.
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
    """Check the conf of a dag run and raise ValueError when it is invalid. Messages never repeat the received value (it may be user input).

    A run created by DAK (dag_run_id starting with `deid-`) must carry the full conf and never falls back to the default values. For a
    manually triggered run, every missing key uses the test value; env that is set is still checked like the one from DAK."""
    conf = conf or {}
    from_dak = run_id.startswith("deid-")
    if from_dak and not DAK_RUN_ID.fullmatch(run_id):
        raise ValueError("dag_run_id must have the form deid-<year>-<6 digits> (set by DAK)")
    values = {}
    for key, rule in CONF_RULES.items():
        value = DEFAULTS[key] if (key not in conf and not from_dak) else conf.get(key)
        if not isinstance(value, str) or not rule.fullmatch(value):
            raise ValueError(f"conf.{key} is missing or malformed")
        values[key] = value
    env = conf.get("env")
    env = {} if env is None else env
    if not isinstance(env, dict):
        raise ValueError("conf.env must be an object")
    for name, value in env.items():
        if name not in JOB_ENV:
            raise ValueError("conf.env has an env that is not allowed")
        if not isinstance(value, str) or not re.fullmatch(JOB_ENV[name][1], value):
            raise ValueError(f"conf.env.{name} is malformed")
    if from_dak:
        for name in MANUAL_OUTPUT:
            if name not in env:
                raise ValueError(f"conf.env.{name} is missing (DAK always sends it)")
    check_range(env)
    effective = {name: env.get(name, MANUAL_OUTPUT.get(name)) for name in ("OUTPUT_DB", "OUTPUT_TABLE")}
    if effective["OUTPUT_DB"] == values["database"] and effective["OUTPUT_TABLE"] == values["table"]:
        raise ValueError("the result table must be different from the source table")


def check_range(env):
    """Rules between the date-hour env, the same rules as JobEnvPolicy of dak-mock and as the job: a real date, hour 00..23, FROM not after
    TO, INPUT_HOURS ascending, overwrite-partitions needs a range. The env already passed the format check in check_conf."""
    points = {}
    for name, missing_hour in (("INPUT_FROM", 0), ("INPUT_TO", 23)):
        value = env.get(name)
        if value is None:
            continue
        try:
            datetime.datetime.strptime(value[:8], "%Y%m%d")
        except ValueError:
            raise ValueError(f"conf.env.{name} is not a real date") from None
        if len(value) == 10 and int(value[8:]) > 23:
            raise ValueError(f"conf.env.{name}: the hour must be between 00 and 23")
        points[name] = int(value[:8]) * 100 + (int(value[8:]) if len(value) == 10 else missing_hour)
    if len(points) == 2 and points["INPUT_FROM"] > points["INPUT_TO"]:
        raise ValueError("conf.env.INPUT_TO must not be earlier than INPUT_FROM")
    hours = env.get("INPUT_HOURS")
    if hours is not None and "-" in hours:
        first, last = (int(part) for part in hours.split("-"))
        if first > last:
            raise ValueError("conf.env.INPUT_HOURS: the first hour must not be greater than the last hour")
    if env.get("OUTPUT_MODE") == "overwrite-partitions" and not any(name in env for name in RANGE_ENV):
        raise ValueError("conf.env.OUTPUT_MODE=overwrite-partitions needs INPUT_FROM, INPUT_TO or INPUT_HOURS")


def validate_conf(**context):
    """First task of the DAG: stop the DAG before any job is created when the conf is invalid (guards against injection into the env/volumes
    of the SparkApplication)."""
    dag_run = context["dag_run"]
    try:
        check_conf(dag_run.run_id, dag_run.conf)
    except ValueError as error:
        raise AirflowFailException(str(error)) from None


def conf_value(key):
    """Jinja expression that reads conf.<key>, falling back to DEFAULTS on a manual trigger (check_conf already rejects a DAK run with a missing key)."""
    return "{{ (dag_run.conf or {}).get('" + key + "', '" + DEFAULTS[key] + "') }}"


def env_value(name):
    """Jinja expression that reads conf.env.<name>; when it is missing the default of JOB_ENV is used (the test value for OUTPUT_DB and
    OUTPUT_TABLE; empty for the date-hour range). A missing OUTPUT_MODE is overwrite-partitions when there is a date-hour range, otherwise
    overwrite."""
    env = "((dag_run.conf or {}).get('env') or {})"
    if name == "OUTPUT_MODE":
        has_range = " or ".join(f"{env}.get('{key}')" for key in RANGE_ENV)
        return ("{{ " + env + ".get('OUTPUT_MODE') or ('overwrite-partitions' if (" + has_range + ") else 'overwrite') }}")
    default = JOB_ENV[name][0]
    if default is None:
        default = MANUAL_OUTPUT.get(name, "")
    return "{{ " + env + ".get('" + name + "', '" + default + "') }}"


def job_env_entries():
    """Every env of JOB_ENV always has exactly one line in driver.env: a fixed list, only the values change."""
    return [{"name": name, "value": env_value(name)} for name in JOB_ENV]


# SparkApplication name = the DAK dag_run_id; for a manual trigger a valid DNS-1123 name is built from ts_nodash.
APP_NAME = "{{ dag_run.run_id if dag_run.run_id.startswith('deid-') else 'deid-manual-' ~ (ts_nodash | lower) }}"


def build_spec():
    """De-identification SparkApplication: same content as pseudonymize-table.yaml, parameters taken from conf."""
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
            # The job writes the target table: on failure read the log and rerun on purpose, no automatic restart. One-week TTL to keep the log readable.
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 604800,
            # ConfigMap with core-site.xml + hdfs-site.xml (+ hive-site.xml) of the HDFS cluster; must be in the same namespace.
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
                    # ---- Job input (pseudonymize-table-app/README.md section 3), taken from the conf sent by DAK ----
                    {"name": "DB_NAME", "value": conf_value("database")},
                    {"name": "TABLE_NAME", "value": conf_value("table")},
                    {"name": "PSEUDO_COLUMNS", "value": conf_value("pseudoColumns")},  # e.g. name:HASH
                    {"name": "PSEUDO_MODE", "value": "HASH"},
                    # ---- Env typed by the requester (JOB_ENV): OUTPUT_DB, OUTPUT_TABLE, OUTPUT_MODE, OUTPUT_FORMAT, ENABLE_HIVE_SUPPORT, INPUT_FROM, INPUT_TO, INPUT_HOURS ----
                    *job_env_entries(),
                    # Compare source and target after writing (row count, nulls, distinct values, no original value left). Fixed.
                    {"name": "VERIFY", "value": "true"},
                    {"name": "WAREHOUSE_PATH", "value": WAREHOUSE_PATH},
                    # ---- Env of pseudonym-udf: the key is read from a file in the Secret of the request (only the driver needs it) ----
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
                # Executors do NOT need the key: it is read on the driver and sits in the closure of the UDF.
                "env": copy.deepcopy(common_env),
            },
            # DEID: key Secret of the request, created by DAK (file pseudonym.hmac_v1). Without the Secret the driver pod cannot start.
            "volumes": [{"name": "pseudonym-keys", "secret": {"secretName": conf_value("keySecretName")}}],
            "sparkConf": {
                # Pseudonymization libs: 2 separate jars in the image, a real UDF so the executors load them too.
                "spark.jars": "local:///opt/app/key-prefix-lib-1.0-SNAPSHOT.jar,local:///opt/app/pseudonym-udf-1.0-SNAPSHOT.jar",
                # Kerberos (HDFS storage zone). keytab/krb5.conf are paths on the Spark Operator pod.
                "spark.kerberos.access.hadoopFileSystems": HDFS,
                "spark.kerberos.principal": KERBEROS_PRINCIPAL,
                "spark.kerberos.keytab": KERBEROS_KEYTAB,
                "spark.kubernetes.kerberos.krb5.path": "/etc/krb5.conf",
                "spark.security.credentials.hadoopfs.enabled": "true",
                # HDFS security: keep wire encryption.
                "spark.hadoop.hadoop.security.authentication": "kerberos",
                "spark.hadoop.hadoop.security.authorization": "true",
                "spark.hadoop.fs.permissions.umask-mode": "002",
                # Delta Lake (already in the fat jar).
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                # MUST be false (patches the token block through ServiceLoader).
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
    description="De-identify one table for a PYC request triggered by DAK",
    schedule=None,  # runs only when DAK triggers it (or a manual trigger for testing)
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    # MUST be False (the default). Native rendering turns strings that look like numbers into numbers ("2" into 2, "64" into 64): env
    # `value` and sparkConf must be strings, so Kubernetes answers 422 "must be of type string". This DAG only templates into strings, so it
    # does not need native rendering.
    render_template_as_native_obj=False,
    max_active_runs=4,
    tags=["deid", "dak"],
) as dag:
    validate = PythonOperator(task_id="validate_conf", python_callable=validate_conf)

    # The spec is a dict, so it is passed through `template_spec`. `application_file` only accepts a file path or a YAML string: a dict there
    # fails with "AttributeError: 'dict' object has no attribute 'rstrip'" (provider cncf.kubernetes 7.x/8.x). The other parameter names
    # (kubernetes_conn_id, ...) depend on the provider version: use the same way you already call SparkKubernetesOperator in the existing DAG.
    run_job = SparkKubernetesOperator(
        task_id="pseudonymize_table",
        namespace=NAMESPACE,
        template_spec=build_spec(),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        # Wait for the job to finish so the dag run reflects the real result: DAK reads the state of the dag run (success/failed) to update the request.
        # If your provider uses a separate sensor (SparkKubernetesSensor), add it right after this task.
    )

    validate >> run_job
