"""SAMPLE DAG that receives a dag run triggered by DAK to de-identify one table (docs/dak-deid-flow/MOCK_IMPLEMENTATION_PLAN.md section 4.7, step D11).

The SparkApplication spec here keeps ALL the config and env of pseudonymize-table-app/k8s/pseudonymize-table.yaml (same content as the
template dak-mock/k8s/deid/sparkapplication.yaml): image, hadoopConfigMap, Kerberos, Delta, event log, job env and pseudonym-udf env. Only
the per-request values come from `conf`. If your environment differs (e.g. HDFS cluster `hkh`, warehouse `/prod4`, principal `k8s@...`
like the vailakehouse DAG that is running) adjust the constants in the "ENVIRONMENT" block below.

DAK calls POST /api/v1/dags/<dagId>/dagRuns with:
  dag_run_id = deid-<year>-<6 digits>[-r<n>]       (job name, also the SparkApplication name; -r2, -r3 for a re-run of the same request)
  conf = {requestCode, database, table, pseudoColumns, env}
`env` is the FINAL env of the job (the env typed by the requester plus the default values filled in by DAK), made of OUTPUT_DB,
OUTPUT_TABLE, OUTPUT_MODE, OUTPUT_FORMAT, ENABLE_HIVE_SUPPORT and (when the requester enters them) the date-hour range INPUT_FROM,
INPUT_TO, INPUT_HOURS. Only the env listed in the JOB_ENV table below (plus DAK_CLIENT_ID and DAK_CLIENT_SECRET, see CREDENTIAL_ENV) are accepted, and that table MUST match `dak.deid.job-env` of
dak-mock (a test guards against drift). The key is NOT in conf and NOT in a Kubernetes Secret (docs/key-access/YEU-CAU-TU-DAK-BO-K8S-SECRET.md):
the job calls GET /api/v1/keys/deid?database=&table=&requestCode= of DAK itself when it runs, with the access token of the client whose
DAK_CLIENT_ID/DAK_CLIENT_SECRET DAK sends in conf.env (Keycloak client_credentials), and keeps the key in memory only. The two values are
hidden on the Airflow UI (mask_credential registers them with the secrets masker) and on the Spark UI (spark.redaction.regex). A 403 (no APPROVED request, request expired, grant revoked) stops the job: extend the
request in DAK and run it again. conf.keySecretName is ignored if DAK still sends it. This DAG must be listed in
dak.deid.airflow.allowed-dag-ids of dak-mock.

MANUAL TRIGGER FOR TESTING (no DAK needed): press Trigger DAG with an empty conf. DEFAULTS and MANUAL_OUTPUT below are then used: the column
`name` of demo_db.users_cdr_2 is de-identified into the table demo_db.users_cdr_2_deid_manual. The key is fetched from DAK like for a DAK
run, so DAK must have an APPROVED request (DEFAULTS["requestCode"]) for that table, or conf.requestCode must be set. The client
credentials have no default, so give them: conf = {"env": {"DAK_CLIENT_ID": "...", "DAK_CLIENT_SECRET": "..."}} (plus any other env,
e.g. "OUTPUT_MODE": "append").
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
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from airflow import DAG

DAG_ID = "deid_pseudonymize_table"  # DEID: must match allowed-dag-ids of dak-mock

# ---------------------------------------------------------------- ENVIRONMENT (adjust to your cluster)
NAMESPACE = "vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy"  # DEID: namespace of the SparkApplication (dak.deid.kubernetes.namespace)
IMAGE = "hub.vtcc.vn:8989/hungvt0110/pseudonymize-table-app:v0.1"
HADOOP_CONFIG_MAP = "teamtscauiy-hadoop-conf"
SERVICE_ACCOUNT = "spark-application-sa"
HDFS = "hdfs://vailakehouse.datalakehla.viettel.com.vn:8020"
WAREHOUSE_PATH = f"{HDFS}/prod8/warehouse"
EVENT_LOG_DIR = f"{HDFS}/prod8/spark_history"
KERBEROS_PRINCIPAL = "presto@VAILAKEHOUSE.VIETTEL.COM"
KERBEROS_KEYTAB = "/etc/security/keytabs/presto.keytab"

# Key of the request, from DAK (GET /api/v1/keys/deid). The token is the one of the client whose client_id/client_secret DAK sends in
# conf.env for each run (CREDENTIAL_ENV below). Unlike by-column, keys/deid takes no tag, so DAK_TAG is not set.
# ASSUMPTION: adjust to the real environment, like DAK_* of silver_ingest_vtnet.py. Currently points to dak-mock-like HTTP in the cluster;
# with the real DAK: https://<dak> and REMOVE DAK_ALLOW_INSECURE_HTTP.
DAK_ADDR = "http://dak-api.lakehouse-database.svc.cluster.local:8080"
DAK_ALLOW_INSECURE_HTTP = "true"
DAK_TOKEN_URL = "https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/token"
DAK_CALLER = "pseudonymize-table"  # header X-Dak-Caller: the job name, only for the audit of DAK

# DAK itself is called over plain HTTP (internal Service), but the driver calls Keycloak (https) to get the token: it needs hostAliases and a
# truststore with the internal CA (ConfigMap keycloak-ca, key ca.pem, in the same namespace). Same configuration as
# silver_ingest_common.keycloak_trust().
KEYCLOAK_HOST = "sso-lakehouse.cyberspace.vn"
KEYCLOAK_IP = "10.221.148.42"
KEYCLOAK_CA_CONFIGMAP = "keycloak-ca"
TRUSTSTORE_DIR = "/opt/truststore"
TRUSTSTORE_FILE = f"{TRUSTSTORE_DIR}/truststore.p12"
TRUSTSTORE_PASSWORD = "changeit"  # temporary truststore in the pod's emptyDir (public CAs + the internal CA), not a secret
SPARK_UID = 185  # user of the apache/spark image

# Client credentials that DAK passes in conf.env on every run (they are NOT in JOB_ENV: that table is the env the requester may type and
# is guarded against dak-mock). Both are required, also for a manual trigger (there is no default). The value never appears in an error.
CREDENTIAL_ENV = {
    "DAK_CLIENT_ID": re.compile(r"[A-Za-z0-9._-]{1,128}"),
    "DAK_CLIENT_SECRET": re.compile(r"[A-Za-z0-9._~+/=-]{8,512}"),
}

SPARK_REDACTION_REGEX = "(?i)secret|password|token|access[.]?key|DAK_CLIENT"

# Spark resources. ONE DAG serves every table, so the executors scale between a small floor and a ceiling (dynamic allocation with
# shuffle tracking, no external shuffle service needed on Kubernetes): a small table keeps ~EXECUTORS_MIN, a big hour such as EBS
# (~150 GB raw per hour, processed one hour-partition at a time) grows up to EXECUTORS_MAX. Peak = EXECUTORS_MAX * (EXECUTOR_CORES cores,
# EXECUTOR_MEMORY + EXECUTOR_OVERHEAD) = 80 cores / ~200 GB: the namespace quota MUST allow it, otherwise lower EXECUTORS_MAX.
DRIVER_CORES = 2
DRIVER_MEMORY, DRIVER_OVERHEAD = "4096m", "1024m"
EXECUTOR_CORES = 4
EXECUTOR_MEMORY, EXECUTOR_OVERHEAD = "8192m", "2048m"
EXECUTORS_MIN, EXECUTORS_MAX = 2, 20
SHUFFLE_PARTITIONS = (
    400  # verify step (distinct/join); AQE coalesces it down for small tables
)

# ---------------------------------------------------------------- PER-REQUEST PARAMETERS
# Default values ONLY for a manual trigger; DAK always sends the full conf.
DEFAULTS = {
    "requestCode": "PYC-0000-000000",
    "database": "demo_db",
    "table": "users_cdr_2",
    "pseudoColumns": "name:HASH",
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

# Partition columns of the SOURCE (silver) table: silver partitions by `dt` and `hour`. Fixed here, not entered by the requester. The job
# follows the real partitioning of the source table (a day-partitioned dataset is filtered by date only), and the result table is
# partitioned the same way: one folder dt=<yyyyMMdd> with one folder hour=<HH> inside.
SOURCE_PARTITION_ENV = {"INPUT_DATE_KEY": "dt", "INPUT_HOUR_KEY": "hour"}

# OUTPUT_DB and OUTPUT_TABLE have no default in JOB_ENV because DAK always sends them; these values are used ONLY for a manual trigger.
MANUAL_OUTPUT = {"OUTPUT_DB": "demo_db", "OUTPUT_TABLE": "users_cdr_2_deid_manual"}

NAME = re.compile(r"[A-Za-z0-9_]{1,128}")
PSEUDO_COLUMNS = re.compile(r"[A-Za-z0-9_]{1,128}:HASH(,[A-Za-z0-9_]{1,128}:HASH)*")
# The request code is DAK's naming: REQ-<year>-<6 digits> (or PYC- for a request of the old kind), the form that the API keys/deid and the job
# (REQUEST_CODE) accept. It ends up in a label value and in a query string of DAK, so nothing else is let through.
REQUEST_CODE = re.compile(r"(REQ|PYC)-[0-9]{4}-[0-9]{6}")
# A re-run of the same request (extension after the zone was deleted) adds -r2, -r3, ... to the run id (and so to the SparkApplication name).
DAK_RUN_ID = re.compile(r"deid-\d{4}-\d{6}(-r[0-9]{1,3})?")

CONF_RULES = {
    "requestCode": REQUEST_CODE,
    "database": NAME,
    "table": NAME,
    "pseudoColumns": PSEUDO_COLUMNS,
}


def check_conf(run_id, conf):
    """Check the conf of a dag run and raise ValueError when it is invalid. Messages never repeat the received value (it may be user input).

    A run created by DAK (dag_run_id starting with `deid-`) must carry the full conf and never falls back to the default values. For a
    manually triggered run, every missing key uses the test value; env that is set is still checked like the one from DAK."""
    conf = conf or {}
    from_dak = run_id.startswith("deid-")
    if from_dak and not DAK_RUN_ID.fullmatch(run_id):
        raise ValueError("dag_run_id must have the form deid-<year>-<6 digits>[-r<n>] (set by DAK)")
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
        if name not in JOB_ENV and name not in CREDENTIAL_ENV:
            raise ValueError("conf.env has an env that is not allowed")
        rule = CREDENTIAL_ENV[name] if name in CREDENTIAL_ENV else re.compile(JOB_ENV[name][1])
        if not isinstance(value, str) or not rule.fullmatch(value):
            raise ValueError(f"conf.env.{name} is malformed")
    for name in CREDENTIAL_ENV:
        if name not in env:
            raise ValueError(f"conf.env.{name} is missing (DAK sends the client credentials)")
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
    for name in CREDENTIAL_ENV:  # hide them in the log of this task too
        mask_credential(((dag_run.conf or {}).get("env") or {}).get(name))
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


def mask_credential(value):
    """Register `value` with the Airflow secrets masker and return it unchanged. Used as a Jinja macro while the SparkApplication spec is
    rendered, so the value is already known to the masker when Airflow redacts the rendered template (UI tab Rendered Template, task logs).
    Airflow only masks by key name (e.g. `secret`), and here the env is {"name": "DAK_CLIENT_SECRET", "value": ...}, so key-based masking
    would not hide it."""
    if not value:
        return value
    try:
        from airflow.sdk.execution_time.secrets_masker import mask_secret  # Airflow 3
    except ImportError:
        try:
            from airflow.utils.log.secrets_masker import mask_secret  # Airflow 2
        except ImportError:
            return value  # no masker available (unit tests without Airflow)
    mask_secret(value)
    return value


def credential_value(name):
    """Jinja expression that reads the client credential conf.env.<name> (check_conf guarantees it is there) and masks it."""
    return "{{ mask_credential(((dag_run.conf or {}).get('env') or {}).get('" + name + "', '')) }}"


def job_env_entries():
    """Every env of JOB_ENV always has exactly one line in driver.env: a fixed list, only the values change."""
    return [{"name": name, "value": env_value(name)} for name in JOB_ENV]


def keycloak_trust():
    """Driver part that calls Keycloak: hostAliases, an initContainer that builds the truststore (JDK cacerts + internal CA), the mounts, the
    volumes and the JVM options. The Spark Operator webhook only injects into the pod the volumes that are mounted in the MAIN container
    (driver.volumeMounts), so keycloak-ca must be mounted in both places."""
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
        "initContainers": [
            {
                "name": "build-truststore",
                "image": IMAGE,
                "command": ["bash", "-ec"],
                "args": [build],
                "env": [{"name": "JAVA_TOOL_OPTIONS", "value": ""}],
                # The ResourceQuota of the namespace rejects containers without limits.
                "resources": {
                    "requests": {"cpu": "50m", "memory": "64Mi"},
                    "limits": {"cpu": "500m", "memory": "256Mi"},
                },
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"]},
                    "runAsNonRoot": True,
                    "runAsUser": SPARK_UID,
                },
                "volumeMounts": [
                    {"name": "keycloak-ca", "mountPath": "/ca", "readOnly": True},
                    {"name": "truststore", "mountPath": TRUSTSTORE_DIR},
                ],
            }
        ],
        "volumeMounts": [
            {"name": "truststore", "mountPath": TRUSTSTORE_DIR, "readOnly": True},
            {"name": "keycloak-ca", "mountPath": "/opt/keycloak-ca", "readOnly": True},
        ],
        "javaOptions": f"-Djavax.net.ssl.trustStore={TRUSTSTORE_FILE} -Djavax.net.ssl.trustStoreType=PKCS12 "
        f"-Djavax.net.ssl.trustStorePassword={TRUSTSTORE_PASSWORD}",
        "volumes": [
            {"name": "keycloak-ca", "configMap": {"name": KEYCLOAK_CA_CONFIGMAP}},
            {"name": "truststore", "emptyDir": {"sizeLimit": "16Mi"}},
        ],
    }


def dak_env():
    """Driver env of the key source: DAK (GET /api/v1/keys/deid). Only the driver needs it: the key travels to the executors inside the
    closure of the UDF. The client_id/client_secret are the ones DAK sends in conf.env for this run."""
    return [
        {"name": "CRYPTO_PREFIX_SOURCE", "value": "dak"},
        {"name": "DAK_ADDR", "value": DAK_ADDR},
        {"name": "DAK_ALLOW_INSECURE_HTTP", "value": DAK_ALLOW_INSECURE_HTTP},
        {"name": "DAK_TOKEN_URL", "value": DAK_TOKEN_URL},
        {"name": "DAK_CLIENT_ID", "value": credential_value("DAK_CLIENT_ID")},
        {"name": "DAK_CLIENT_SECRET", "value": credential_value("DAK_CLIENT_SECRET")},
        {"name": "DAK_CALLER", "value": DAK_CALLER},
        # The request whose key the job asks for: DAK checks it and the job compares it with the one in the response.
        {"name": "REQUEST_CODE", "value": conf_value("requestCode")},
    ]


# SparkApplication name = the DAK dag_run_id; for a manual trigger a valid DNS-1123 name is built from ts_nodash.
APP_NAME = "{{ dag_run.run_id if dag_run.run_id.startswith('deid-') else 'deid-manual-' ~ (ts_nodash | lower) }}"


# Spark config shared by every job of this image (de-identification, purge, vacuum): Kerberos/HDFS, Delta, log4j, AQE, event log.
# The jobs add their own `spark.jars`, executor sizing and shuffle partitions on top.
LOG4J = "-Dlog4j.configurationFile=file:/opt/app/log4j2.properties"
SHARED_SPARK_CONF = {
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
    "spark.driver.extraJavaOptions": LOG4J,
    "spark.executor.extraJavaOptions": LOG4J,
    # Spark.
    "spark.jars.ivy": "/tmp",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.scheduler.mode": "FAIR",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.sources.ignoreDataLocality.enabled": "true",
    # Event log (Spark History Server).
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": EVENT_LOG_DIR,
    "spark.eventLog.rolling.enabled": "true",
    "spark.eventLog.rolling.maxFileSize": "128m",
}


def build_spec():
    """De-identification SparkApplication: same content as pseudonymize-table.yaml, parameters taken from conf."""
    common_env = [
        {"name": "TZ", "value": "Asia/Ho_Chi_Minh"},
        {"name": "MALLOC_ARENA_MAX", "value": "2"},
    ]
    trust = keycloak_trust()
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
                "cores": DRIVER_CORES,
                "coreLimit": f"{DRIVER_CORES * 1000}m",
                "memory": DRIVER_MEMORY,
                "memoryOverhead": DRIVER_OVERHEAD,
                "labels": {
                    "version": "3.5.1",
                    "app": "pseudonymize-table",
                    "dak.vai/request-code": conf_value("requestCode"),
                },
                "serviceAccount": SERVICE_ACCOUNT,
                "env": common_env
                + [
                    # ---- Job input (pseudonymize-table-app/README.md section 3), taken from the conf sent by DAK ----
                    {"name": "DB_NAME", "value": conf_value("database")},
                    {"name": "TABLE_NAME", "value": conf_value("table")},
                    {
                        "name": "PSEUDO_COLUMNS",
                        "value": conf_value("pseudoColumns"),
                    },  # e.g. name:HASH
                    {"name": "PSEUDO_MODE", "value": "HASH"},
                    # ---- Env typed by the requester (JOB_ENV): OUTPUT_DB, OUTPUT_TABLE, OUTPUT_MODE, OUTPUT_FORMAT, ENABLE_HIVE_SUPPORT, INPUT_FROM, INPUT_TO, INPUT_HOURS ----
                    *job_env_entries(),
                    *[
                        {"name": name, "value": value}
                        for name, value in SOURCE_PARTITION_ENV.items()
                    ],
                    # Compare source and target after writing (row count, nulls, distinct values, no original value left). Fixed.
                    {"name": "VERIFY", "value": "true"},
                    {"name": "WAREHOUSE_PATH", "value": WAREHOUSE_PATH},
                    # ---- Env of pseudonym-udf: the key is fetched from DAK when the job runs (only the driver needs it) ----
                    *dak_env(),
                ],
                **{k: copy.deepcopy(trust[k]) for k in ("hostAliases", "initContainers", "volumeMounts")},
            },
            "executor": {
                "instances": EXECUTORS_MIN,  # starting number; dynamic allocation (sparkConf) moves it between MIN and MAX
                "cores": EXECUTOR_CORES,
                "coreLimit": f"{EXECUTOR_CORES * 1000}m",
                "memory": EXECUTOR_MEMORY,
                "memoryOverhead": EXECUTOR_OVERHEAD,
                "labels": {"version": "3.5.1", "app": "pseudonymize-table"},
                "serviceAccount": SERVICE_ACCOUNT,
                # Executors do NOT need the key: it is read on the driver and sits in the closure of the UDF.
                "env": copy.deepcopy(common_env),
            },
            # Truststore of the internal CA (the driver calls Keycloak for the token of the internal client).
            "volumes": copy.deepcopy(trust["volumes"]),
            "sparkConf": {
                **SHARED_SPARK_CONF,
                # The truststore is for the driver only (executors do not call Keycloak/DAK).
                "spark.driver.extraJavaOptions": f"{LOG4J} {trust['javaOptions']}",
                # Spark UI (Environment tab) and the event log show a property value as *********(redacted) when its NAME matches this
                # regex. The default is secret|password|token|access.key; DAK_CLIENT also hides DAK_CLIENT_ID (spark.kubernetes.driverEnv.*).
                "spark.redaction.regex": SPARK_REDACTION_REGEX,
                # Pseudonymization libs: 2 separate jars in the image, a real UDF so the executors load them too.
                "spark.jars": "local:///opt/app/key-prefix-lib-1.0-SNAPSHOT.jar,local:///opt/app/pseudonym-udf-1.0-SNAPSHOT.jar",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.shuffleTracking.enabled": "true",
                "spark.dynamicAllocation.minExecutors": str(EXECUTORS_MIN),
                "spark.dynamicAllocation.initialExecutors": str(EXECUTORS_MIN),
                "spark.dynamicAllocation.maxExecutors": str(EXECUTORS_MAX),
                "spark.dynamicAllocation.executorIdleTimeout": "120s",
                "spark.sql.shuffle.partitions": str(SHUFFLE_PARTITIONS),
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
    user_defined_macros={"mask_credential": mask_credential},  # used by credential_value(): hides client_id/secret on the Airflow UI
    # One run at a time: runs for different hours write into the SAME result table (one table per source), and concurrent writes to one
    # Delta table can conflict. Simple and safe; raise it only after the runs are split per result table.
    max_active_runs=1,
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
