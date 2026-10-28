"""Shared part of the silver ingest DAGs (silver_ingest_vtt.py, silver_ingest_vtnet.py). Design: docs/silver-ingest/PLAN.md.

Every DAG is RECURRING, one per dataset (silver_ingest_factory.py builds them): the request is approved ONCE for the whole dataset. DAK writes the
approved settings (table, columns, daily cron) into the Airflow Variable of the dataset and switches the DAG on; the DAG factory builds the DAG
with that cron as its `schedule`. Every run ingests the folder `<dataset>/<day folder>=<day>` (day folder: `dt` for VTNET, `date` for VTT) of
the day BEFORE the run (RUN_DATE) into the partition batch_id = <day>, so rerunning a day replaces that day only. conf is only used for a manual
run: {"date": "yyyyMMdd"} picks another day (backfill).

The fixed settings of a source (landing root, file format, schema, target database) are the SOURCE constant in the DAG file of that source.
The first task `validate_conf` checks the settings and the conf before the job is created.

This file lives in the same dags/ folder, so Airflow can import it with `import silver_ingest_common` (the dags folder is already on
sys.path). It contains no DAG.
"""

import copy
import datetime
import hashlib
import re

# ---------------------------------------------------------------- ENVIRONMENT (adjust to your cluster)
NAMESPACE = "vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy"
IMAGE = "hub.vtcc.vn:8989/hungvt0110/silver-ingest-app:v0.1"
HADOOP_CONFIG_MAP = "teamtscauiy-hadoop-conf"
SERVICE_ACCOUNT = "spark-application-sa"
HDFS = "hdfs://vailakehouse.datalakehla.viettel.com.vn:8020"
WAREHOUSE_PATH = f"{HDFS}/prod8/warehouse"
EVENT_LOG_DIR = f"{HDFS}/prod8/spark_history"
KERBEROS_PRINCIPAL = "presto@VAILAKEHOUSE.VIETTEL.COM"
KERBEROS_KEYTAB = "/etc/security/keytabs/presto.keytab"
# Internal Keycloak (DAK_TOKEN_URL, https): the driver must resolve the host name (hostAliases) and trust the internal CA (truststore). Same
# configuration as k8s/spark-application-cdr.yaml. The ConfigMap `keycloak-ca` (key `ca.pem`) must exist first, in the same namespace:
#   kubectl create configmap keycloak-ca --from-file=ca.pem=<internal CA file> -n <namespace>
KEYCLOAK_HOST = "sso-lakehouse.cyberspace.vn"
KEYCLOAK_IP = "10.221.148.42"
KEYCLOAK_CA_CONFIGMAP = "keycloak-ca"
TRUSTSTORE_DIR = "/opt/truststore"
TRUSTSTORE_FILE = f"{TRUSTSTORE_DIR}/truststore.p12"
TRUSTSTORE_PASSWORD = "changeit"  # temporary truststore in the pod's emptyDir, holds the public CAs + the internal CA, not a secret
SPARK_UID = 185  # user of the apache/spark image (USER 185 in silver-ingest-app/Dockerfile)
LOG4J = "-Dlog4j.configurationFile=file:/opt/app/log4j2.properties"
JARS = ("local:///opt/app/key-prefix-lib-1.0-SNAPSHOT.jar,local:///opt/app/cdr-crypto-udf-1.0-SNAPSHOT.jar,"
        "local:///opt/app/DataLakeSecurity_jv8.jar")

# Ingest modes of the Spark app (entry points of silver-ingest-app): the DAG of a dataset picks the one that fits its data.
#   passthrough:     read the files and write the table as they are, no DAK call.
#   decrypt-encrypt: DAK by-path key present -> decrypt the whole file (absent -> read as it is), then encrypt the requested columns
#                    with cdr-crypto (none requested -> columns are written as they are), then write the table.
MODES = {
    "passthrough": "vai.lakehouse.silveringest.app.PassthroughSilverApp",
    "decrypt-encrypt": "vai.lakehouse.silveringest.app.DecryptEncryptSilverApp",
}

# Time zone of the schedule (the daily run time on the request is local time) and of the day that a run ingests.
RUN_TIMEZONE = "Asia/Ho_Chi_Minh"
# The day a recurring run ingests, yyyyMMdd: conf.date for a manual run, otherwise the day at the START of the data interval. For the daily
# cron "0 3 * * *" the run of 03:00 on 28 October covers the interval that starts at 03:00 on 27 October, so it ingests 27 October. The
# interval start is converted to the local time zone first: Airflow's own `ds` is the UTC date, which is one day off for a run before 07:00.
RUN_DATE = (
    "{{ (dag_run.conf or {}).get('date') or data_interval_start.in_timezone('"
    + RUN_TIMEZONE
    + "').strftime('%Y%m%d') }}"
)

# overwrite-partitions (default): only the partition of the batch is replaced. append: write on top (rerunning the same batch writes
# duplicates). "overwrite" (replaces the WHOLE table, deleting every other batch) is not accepted from conf so DAK cannot delete data by mistake.
OUTPUT_MODES = ("overwrite-partitions", "append")
DEFAULT_OUTPUT_MODE = "overwrite-partitions"
# Name of the batch partition column in the target table (env BATCH_COLUMN of the job).
BATCH_COLUMN = "batch_id"
# Target table name chosen by the requester: lowercase letters, digits, "_"; must start with the prefix of the source
# (SOURCE["table_prefix"]) so a request cannot write into the table of another source.
TABLE = re.compile(r"[a-z0-9_]{1,128}")
COLUMN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
# Daily cron written by DAK: "<minute> <hour> * * *", nothing else (the value ends up as the `schedule` of a DAG).
DAILY_CRON = re.compile(r"(?:[0-5]?\d) (?:[01]?\d|2[0-3]) \* \* \*")
TICKET = re.compile(r"SLV-\d{4}-\d{6}")
RUN_DATE_VALUE = re.compile(r"\d{8}")
MAX_COLUMNS = 100


def check_columns(conf):
    """encryptColumns (list of distinct column names, may be empty or missing: the files are then only decrypted) and keyField (required
    when there are columns to encrypt, not in that list); only for sources that accept columns to encrypt.
    """

    columns = conf.get("encryptColumns")
    columns = [] if columns is None else columns
    if (
        not isinstance(columns, list)
        or len(columns) > MAX_COLUMNS
        or not all(isinstance(c, str) and COLUMN.fullmatch(c) for c in columns)
    ):
        raise ValueError(
            f"conf.encryptColumns must be a list of up to {MAX_COLUMNS} column names [A-Za-z_][A-Za-z0-9_]*"
        )
    lower = [c.lower() for c in columns]
    if len(set(lower)) != len(lower):
        raise ValueError(
            "conf.encryptColumns has a duplicated column (names are case-insensitive)"
        )
    key_field = conf.get("keyField")
    if columns and (
        not isinstance(key_field, str) or not COLUMN.fullmatch(key_field)
    ):
        raise ValueError(
            "conf.keyField is missing or malformed (required when encryptColumns is not empty)"
        )
    if key_field not in (None, "") and (
        not isinstance(key_field, str) or not COLUMN.fullmatch(key_field)
    ):
        raise ValueError("conf.keyField is malformed")
    if isinstance(key_field, str) and key_field.lower() in lower:
        raise ValueError(
            "conf.keyField must not be listed in encryptColumns (it stays plaintext as key material)"
        )


def check_schedule_config(source, config):
    """Check the settings that DAK wrote into the Airflow Variable of a recurring dataset; raise ValueError when they are invalid.

    `source["schedule_keys"]` lists the accepted keys (ticket, table, cron, outputMode and, for a decrypt-encrypt dataset, encryptColumns and
    keyField). Messages never repeat the value.
    """

    if not isinstance(config, dict):
        raise ValueError("schedule config must be an object")
    unknown = set(config) - set(source["schedule_keys"])
    if unknown:
        raise ValueError(
            f"schedule config has keys that are not accepted (only {', '.join(source['schedule_keys'])} are accepted)"
        )
    if not isinstance(config.get("ticket"), str) or not TICKET.fullmatch(config["ticket"]):
        raise ValueError("schedule config ticket is missing or malformed (SLV-<year>-<6 digits>)")
    table = config.get("table")
    if not isinstance(table, str) or not TABLE.fullmatch(table) or not table.startswith(source["table_prefix"]):
        raise ValueError(
            f"schedule config table is missing or malformed (lowercase letters, digits, '_', starting with '{source['table_prefix']}')"
        )
    if not isinstance(config.get("cron"), str) or not DAILY_CRON.fullmatch(config["cron"]):
        raise ValueError("schedule config cron is missing or malformed (a daily cron: '<minute> <hour> * * *')")
    if "outputMode" in config and config["outputMode"] not in OUTPUT_MODES:
        raise ValueError(f"schedule config outputMode must be one of {', '.join(OUTPUT_MODES)}")
    if source.get("encrypts_columns"):
        check_columns(config)


def check_conf(source, conf):
    """Run conf of a DAG: only an optional `date` (yyyyMMdd, a real date) to ingest another day by hand; raise ValueError when it is invalid.

    `source["conf_keys"]` lists the accepted keys. Messages never repeat the received value.
    """

    conf = conf or {}
    if not isinstance(conf, dict):
        raise ValueError("conf must be an object")
    unknown = set(conf) - set(source["conf_keys"])
    if unknown:
        raise ValueError(f"conf has keys that are not accepted (only {', '.join(source['conf_keys'])} are accepted)")
    if "date" in conf:
        value = conf["date"]
        try:
            if not isinstance(value, str) or not RUN_DATE_VALUE.fullmatch(value):
                raise ValueError
            datetime.datetime.strptime(value, "%Y%m%d")
        except ValueError:
            raise ValueError("conf.date must be a real date yyyyMMdd") from None


def k8s_name(prefix, run_id):
    """SparkApplication name that is a valid DNS-1123 name and unique per dag run (DAK may trigger several runs close together)."""

    return f"{prefix}-{hashlib.sha1(run_id.encode('utf-8')).hexdigest()[:12]}"


def keycloak_trust():
    """Driver part for sources that call Keycloak (source["calls_keycloak"]): hostAliases, an initContainer that builds the truststore (JDK
    cacerts + internal CA), the mounts, the volumes and the JVM options. The Spark Operator webhook only injects into the pod the volumes that
    are mounted in the MAIN container (driver.volumeMounts), so keycloak-ca must be mounted in both places.
    """

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
                # Like the build-truststore initContainer of dak-mock/k8s/dak-mock.yaml, except: it has runAsUser (needed to run) and does NOT
                # set readOnlyRootFilesystem.
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"]},
                    "runAsNonRoot": True,
                    "runAsUser": SPARK_UID,
                },
                "volumeMounts": [
                    {
                        "name": "keycloak-ca",
                        "mountPath": "/ca",
                        "readOnly": True,
                    },
                    {"name": "truststore", "mountPath": TRUSTSTORE_DIR},
                ],
            }
        ],
        "volumeMounts": [
            {
                "name": "truststore",
                "mountPath": TRUSTSTORE_DIR,
                "readOnly": True,
            },
            {
                "name": "keycloak-ca",
                "mountPath": "/opt/keycloak-ca",
                "readOnly": True,
            },
        ],
        "javaOptions": f"-Djavax.net.ssl.trustStore={TRUSTSTORE_FILE} -Djavax.net.ssl.trustStoreType=PKCS12 "
        f"-Djavax.net.ssl.trustStorePassword={TRUSTSTORE_PASSWORD}",
        "volumes": [
            {
                "name": "keycloak-ca",
                "configMap": {"name": KEYCLOAK_CA_CONFIGMAP},
            },
            {"name": "truststore", "emptyDir": {"sizeLimit": "16Mi"}},
        ],
    }


def build_spec(source):
    """SparkApplication that loads the files of one day of the dataset of `source`. Job env: silver-ingest-app/README.md.

    The table and the output mode are the settings approved on the request (source["fixed"], read from the Airflow Variable when the DAG is
    parsed); the path is `<dataset>/<day folder>=<RUN_DATE>` and the batch id is RUN_DATE.
    """

    fixed = source.get("fixed", {})

    common_env = [
        {"name": "TZ", "value": "Asia/Ho_Chi_Minh"},
        {"name": "MALLOC_ARENA_MAX", "value": "2"},
    ]
    job_env = [
        {"name": "APP_NAME", "value": f"SilverIngest-{source['name']}"},
        # input_root ends with "/", the relative path has no leading "/".
        {"name": "INPUT_PATH", "value": f"{source['input_root']}{source['dataset']}/{source['day_dir']}={RUN_DATE}"},
        {"name": "OUTPUT_MODE", "value": str(fixed.get("outputMode", DEFAULT_OUTPUT_MODE))},
        {"name": "BATCH_COLUMN", "value": BATCH_COLUMN},
        {"name": "BATCH_VALUE", "value": RUN_DATE},
        {"name": "OUTPUT_TABLE", "value": str(fixed.get("table", ""))},
        {"name": "WAREHOUSE_PATH", "value": WAREHOUSE_PATH},
        {"name": "ENABLE_HIVE_SUPPORT", "value": "true"},
    ] + [{"name": k, "value": v} for k, v in source["env"].items()]
    trust = keycloak_trust() if source.get("calls_keycloak") else None
    driver_trust = {k: copy.deepcopy(trust[k]) for k in ("hostAliases", "initContainers", "volumeMounts")} if trust else {}
    spec = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": "{{ k8s_name('silver-"
            + source["name"]
            + "', dag_run.run_id) }}",
            "namespace": NAMESPACE,
        },
        "spec": {
            "type": "Scala",
            "mode": "cluster",
            "sparkVersion": "3.5.1",
            "image": IMAGE,
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local:///opt/app/silver-ingest-app.jar",
            "mainClass": source["main_class"],
            # On failure, read the log and rerun on purpose (rerunning an append is a duplicate write); no automatic restart.
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 604800,
            "hadoopConfigMap": HADOOP_CONFIG_MAP,
            "driver": {
                "cores": 1,
                "coreLimit": "1000m",
                "memory": "2048m",
                "memoryOverhead": "512m",
                "labels": {
                    "version": "3.5.1",
                    "app": f"silver-ingest-{source['name']}",
                },
                "serviceAccount": SERVICE_ACCOUNT,
                "env": common_env
                + job_env
                + copy.deepcopy(source.get("driver_extra_env", [])),
                **driver_trust,
            },
            "executor": {
                "instances": 2,
                "cores": 2,
                "coreLimit": "2000m",
                "memory": "4096m",
                "memoryOverhead": "1024m",
                "labels": {
                    "version": "3.5.1",
                    "app": f"silver-ingest-{source['name']}",
                },
                "serviceAccount": SERVICE_ACCOUNT,
                # Executors do NOT need the DAK configuration: column keys and the file key are fetched on the driver and travel with the closure.
                "env": copy.deepcopy(common_env),
            },
            "sparkConf": {
                # cdr-crypto (the real UDF) + key-prefix-lib + the partner jar: the executors need them too.
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
                # MUST be false (patches the token block through ServiceLoader).
                "spark.driver.userClassPathFirst": "false",
                "spark.executor.userClassPathFirst": "false",
                # The truststore is for the driver only (executors do not call Keycloak/DAK).
                "spark.driver.extraJavaOptions": (
                    f"{LOG4J} {trust['javaOptions']}" if trust else LOG4J
                ),
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
