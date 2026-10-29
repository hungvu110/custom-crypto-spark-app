"""Shared part of the silver ingest DAGs (silver_ingest_vtt.py, silver_ingest_vtnet.py). Design: docs/silver-ingest/PLAN.md.

Every DAG is RECURRING, one per dataset (silver_ingest_factory.py builds them): the request is approved ONCE for the whole dataset. DAK writes the
approved settings (table, columns, cron) into the Airflow Variable of the dataset and switches the DAG on; the DAG factory builds the DAG
with that cron as its `schedule`. Every run ingests the folder `<dataset>/<day folder>=<day>` (day folder: `dt` for VTNET, `date` for VTT) of
the day BEFORE the run (run_date) into the rows dt = <day>, so rerunning a day replaces that day only. With an HOURLY cron
("<minute> * * * *") each run ingests the HOUR before instead: its dt and hour, and only those rows are replaced. conf is only used for a
manual run: {"date": "yyyyMMdd"} picks another day, and {"date": ..., "fromHour": "10", "toHour": "12"} only those hours of that day (the
rows replaced: only those hours of that day; see batch_hours). A range of days or hours is run by the DAG silver_ingest_backfill.py, which starts
one such run per day.

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
def run_date(conf, interval_start):
    """The day a run ingests, yyyyMMdd (see above); `conf` = dag_run.conf, `interval_start` = data_interval_start."""

    return (conf or {}).get("date") or interval_start.in_timezone(RUN_TIMEZONE).strftime("%Y%m%d")


# Hours of a day, "00".."23". A run ingests the whole day, or only the hours conf.fromHour..conf.toHour (both included) of conf.date.
# Where the hour is written differs per dataset: the DAG file of a source gives each dataset an `hour_glob`, the part of the path under the
# day folder with HOUR_TOKEN in place of the hour(s), e.g. VTT "hour=%H%" (hour=10), EBS "batch=%H%*" (batch=1000, batch=1010, ...), NIMS
# "batch=*/infra_cell_4g-%H%*" (the hour is in the file name). One hour gives "10", several hours give the Hadoop pattern "{10,11,12}".
HOUR_TOKEN = "%H%"
# Name of the hour column in the target table (env HOUR_COLUMN of the job).
HOUR_COLUMN = "hour"
HOUR_VALUE = re.compile(r"\d{1,2}")


def schedule_unit(cron):
    """What one scheduled run ingests: "hour" for the hourly cron "<minute> * * * *", "day" for the daily cron "<minute> <hour> * * *"."""

    return "hour" if cron.split(" ")[1] == "*" else "day"


def hours_of(conf, interval_start=None, unit="day"):
    """The hours of a run as ["10", "11", "12"], or [] for a whole day; `conf` has already passed check_conf.

    conf.fromHour/toHour pick the hours of a manual run. Without them, a run of an HOURLY schedule (unit "hour") ingests the hour at the
    START of its data interval (local time), and a run with conf.date only (a manual whole day) or of a daily schedule ingests the whole day.
    """

    conf = conf or {}
    if "fromHour" in conf:
        first = int(conf["fromHour"])
        last = int(conf.get("toHour", first))
        return [f"{hour:02d}" for hour in range(first, last + 1)]
    if unit == "hour" and "date" not in conf and interval_start is not None:
        return [interval_start.in_timezone(RUN_TIMEZONE).strftime("%H")]
    return []


def hour_pattern(hours):
    """"10" for one hour, "{10,11,12}" for several: the text that replaces HOUR_TOKEN."""

    return hours[0] if len(hours) == 1 else "{" + ",".join(hours) + "}"


def input_path(prefix, hour_glob, unit, conf, interval_start):
    """INPUT_PATH of a run. `prefix` = `<root><dataset>/<day folder>=`; the whole day folder, or only the folders/files of the hours of the
    run (see hours_of; `unit` = schedule_unit of the cron of the dataset)."""

    day = run_date(conf, interval_start)
    hours = hours_of(conf, interval_start, unit)
    if not hours:
        return f"{prefix}{day}"
    return f"{prefix}{day}/" + hour_glob.replace(HOUR_TOKEN, hour_pattern(hours))


def batch_hours(unit, conf, interval_start):
    """Value of BATCH_HOURS: the hours of the run that are replaced in the table ("10,11,12"), "" for a whole day.

    The batch (column `dt`) is always the DAY, whatever the run: a whole day, some hours of a day, a range of days all write rows with the same
    `dt` and `hour` columns, and a run replaces exactly the (dt, hour) rows it ingests, so reruns and overlapping runs never write twice.
    """

    return ",".join(hours_of(conf, interval_start, unit))


def hour_regex(hour_glob):
    """Regex (one capture group = the hour) that finds the hour in the path of a file, built from the `hour_glob` of a dataset.

    `%H%` becomes the two digits of the hour, `*` any text inside one path segment, the rest is taken literally; the match is anchored on the
    whole path segments, e.g. "hour=%H%" gives "/hour=(\\d{2})(?:/|$)" and "batch=*/infra_cell_4g-%H%*" gives "/batch=[^/]*/infra_cell_4g-(\\d{2})[^/]*(?:/|$)".
    """

    tokens = {HOUR_TOKEN: r"(\d{2})", "*": r"[^/]*"}
    segments = []
    for segment in hour_glob.split("/"):
        pieces = re.split("(" + re.escape(HOUR_TOKEN) + r"|\*)", segment)
        segments.append("".join(tokens.get(piece, re.escape(piece)) for piece in pieces))
    return "/" + "/".join(segments) + "(?:/|$)"


# overwrite-partitions (default): only the partition of the batch is replaced. append: write on top (rerunning the same batch writes
# duplicates). "overwrite" (replaces the WHOLE table, deleting every other batch) is not accepted from conf so DAK cannot delete data by mistake.
OUTPUT_MODES = ("overwrite-partitions", "append")
DEFAULT_OUTPUT_MODE = "overwrite-partitions"
# Name of the batch partition column in the target table (env BATCH_COLUMN of the job).
BATCH_COLUMN = "dt"
# Target table name chosen by the requester: lowercase letters, digits, "_"; must start with the prefix of the source
# (SOURCE["table_prefix"]) so a request cannot write into the table of another source.
TABLE = re.compile(r"[a-z0-9_]{1,128}")
COLUMN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
# Cron of the request, written by DAK as it is (the value ends up as the `schedule` of a DAG). Two forms, nothing else:
#   daily   "<minute> <hour> * * *"  each run ingests the DAY before;
#   hourly  "<minute> * * * *"       each run ingests the HOUR before (only for a dataset with an `hour_glob`).
SCHEDULE_CRON = re.compile(r"(?:[0-5]?\d) (?:[01]?\d|2[0-3]|\*) \* \* \*")
TICKET = re.compile(r"SLV-\d{4}-\d{6}")
RUN_DATE_VALUE = re.compile(r"\d{8}")
# Keys of the conf of a manual run of a dataset DAG (check_conf).
CONF_KEYS = ("date", "fromHour", "toHour")
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
    if not isinstance(config.get("cron"), str) or not SCHEDULE_CRON.fullmatch(config["cron"]):
        raise ValueError(
            "schedule config cron is missing or malformed ('<minute> <hour> * * *' daily or '<minute> * * * *' hourly)"
        )
    if schedule_unit(config["cron"]) == "hour" and not source.get("hour_glob"):
        raise ValueError("schedule config cron is hourly but this dataset has no hour layout (hour_glob)")
    if "outputMode" in config and config["outputMode"] not in OUTPUT_MODES:
        raise ValueError(f"schedule config outputMode must be one of {', '.join(OUTPUT_MODES)}")
    if source.get("encrypts_columns"):
        check_columns(config)


def check_date(value, name):
    """A real date yyyyMMdd (a string); raise ValueError (never repeating the value) otherwise."""

    try:
        if not isinstance(value, str) or not RUN_DATE_VALUE.fullmatch(value):
            raise ValueError
        datetime.datetime.strptime(value, "%Y%m%d")
    except ValueError:
        raise ValueError(f"{name} must be a real date yyyyMMdd") from None


def check_hours(conf):
    """conf.fromHour (and optionally conf.toHour, default fromHour): whole hours 0-23, as a number or text, fromHour <= toHour.

    toHour alone is not accepted. Returns (first, last) as numbers, or None when conf has no hours.
    """

    if "fromHour" not in conf:
        if "toHour" in conf:
            raise ValueError("conf.toHour needs conf.fromHour")
        return None
    bounds = []
    for name in ("fromHour", "toHour"):
        if name not in conf and name == "toHour":
            bounds.append(bounds[0])
            continue
        value = conf[name]
        if isinstance(value, bool) or not (
            isinstance(value, int) or (isinstance(value, str) and HOUR_VALUE.fullmatch(value))
        ):
            raise ValueError(f"conf.{name} must be an hour 0-23")
        if not 0 <= int(value) <= 23:
            raise ValueError(f"conf.{name} must be an hour 0-23")
        bounds.append(int(value))
    if bounds[0] > bounds[1]:
        raise ValueError("conf.fromHour must not be after conf.toHour")
    return bounds[0], bounds[1]


def check_conf(source, conf):
    """Run conf of a DAG, for a manual run: an optional `date` (yyyyMMdd, a real date) to ingest another day, and `fromHour`/`toHour` to
    ingest only some hours of that day (needs `date`, and a dataset with an `hour_glob`); raise ValueError when it is invalid.

    `source["conf_keys"]` lists the accepted keys. Messages never repeat the received value.
    """

    conf = conf or {}
    if not isinstance(conf, dict):
        raise ValueError("conf must be an object")
    unknown = set(conf) - set(source["conf_keys"])
    if unknown:
        raise ValueError(f"conf has keys that are not accepted (only {', '.join(source['conf_keys'])} are accepted)")
    if "date" in conf:
        check_date(conf["date"], "conf.date")
    if check_hours(conf) is not None:
        if "date" not in conf:
            raise ValueError("conf.fromHour needs conf.date (the day of the hours)")
        if not source.get("hour_glob"):
            raise ValueError("this dataset has no hour layout: only a whole day can be ingested")


# ---------------------------------------------------------------- BACKFILL (silver_ingest_backfill.py)
# Backfill conf: the DAG of one dataset, a range of days and, optionally, the same range of hours on each of those days.
BACKFILL_KEYS = ("dag_id", "fromDate", "toDate", "fromHour", "toHour")
BACKFILL_DAG_ID = re.compile(r"silver_ingest_(?:vtt|vtnet)_[a-z0-9_]{1,100}")
MAX_BACKFILL_DAYS = 31


def plan_backfill(conf):
    """(dag_id, [conf of one run, ...]) of a backfill: one run per day of fromDate..toDate (toDate defaults to fromDate), in date order.

    Each run conf is `{"date": "yyyyMMdd"}`, plus `fromHour`/`toHour` as "HH" when the backfill has hours. Raise ValueError when the
    backfill conf is invalid; messages never repeat the received value.
    """

    if not isinstance(conf, dict):
        raise ValueError("conf must be an object")
    unknown = set(conf) - set(BACKFILL_KEYS)
    if unknown:
        raise ValueError(f"conf has keys that are not accepted (only {', '.join(BACKFILL_KEYS)} are accepted)")
    dag_id = conf.get("dag_id")
    if not isinstance(dag_id, str) or not BACKFILL_DAG_ID.fullmatch(dag_id):
        raise ValueError("conf.dag_id must be the id of a silver ingest DAG (silver_ingest_<vtt|vtnet>_<dataset>)")
    if "fromDate" not in conf:
        raise ValueError("conf.fromDate is required")
    check_date(conf["fromDate"], "conf.fromDate")
    check_date(conf.get("toDate", conf["fromDate"]), "conf.toDate")
    first = datetime.datetime.strptime(conf["fromDate"], "%Y%m%d").date()
    last = datetime.datetime.strptime(conf.get("toDate", conf["fromDate"]), "%Y%m%d").date()
    if first > last:
        raise ValueError("conf.fromDate must not be after conf.toDate")
    days = (last - first).days + 1
    if days > MAX_BACKFILL_DAYS:
        raise ValueError(f"a backfill covers at most {MAX_BACKFILL_DAYS} days")
    hours = check_hours({k: conf[k] for k in ("fromHour", "toHour") if k in conf})
    confs = []
    for offset in range(days):
        run = {"date": (first + datetime.timedelta(days=offset)).strftime("%Y%m%d")}
        if hours:
            run["fromHour"], run["toHour"] = f"{hours[0]:02d}", f"{hours[1]:02d}"
        confs.append(run)
    return dag_id, confs


def backfill_run_id(backfill_run, run_conf):
    """Dag run id of one run of a backfill: readable (the day, the hours) and unique per backfill run, so the same range can be backfilled again."""

    label = run_conf["date"] + (f"_{run_conf['fromHour']}-{run_conf['toHour']}" if "fromHour" in run_conf else "")
    return f"backfill_{label}_{hashlib.sha1(backfill_run.encode('utf-8')).hexdigest()[:8]}"


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


# ---------------------------------------------------------------- DATASET SETTINGS (DATASETS in the DAG files)
# EVERYTHING that is specific to one dataset is written in its entry of DATASETS in the DAG file of its source (silver_ingest_vtt.py,
# silver_ingest_vtnet.py), and nowhere else. The entry is checked (check_dataset) when Airflow parses the DAG file: a typo is an import error,
# not a silent default. Keys:
#   mode               required  a key of MODES: passthrough, or decrypt-encrypt (DAK by-path / by-column keys).
#   hour_glob          optional  where the hour is written under the folder of a day, with HOUR_TOKEN for the hour(s). It lets a scheduled
#                                hourly run, a manual run or silver_ingest_backfill.py ingest only some hours; without it only whole days.
#   partition_by_hour  optional  true: the table is partitioned by (dt, hour) (big datasets); false (default): by dt only, `hour` is an
#                                ordinary column (small datasets, no 24 tiny partitions a day). Needs hour_glob.
#   key_path           optional  decrypt-encrypt only: the path asked from DAK by-path, one key for the whole dataset (default: the dataset).
#   resources          optional  Spark resources of the job of this dataset, any of the keys of RESOURCE_DEFAULTS (the rest keep the default).
DATASET_KEYS = ("mode", "hour_glob", "partition_by_hour", "key_path", "resources")

# Resources of the Spark job. These defaults are what every dataset used before `resources` existed; the sizes of a big dataset are set in
# its own entry of DATASETS. Rule of thumb for `shuffle_partitions`: the data of one run divided by 128-200 MB.
RESOURCE_DEFAULTS = {
    "driver_cores": 1,
    "driver_memory": "2048m",
    "driver_memory_overhead": "512m",
    "executors": 2,
    "executor_cores": 2,
    "executor_memory": "4096m",
    "executor_memory_overhead": "1024m",
    "shuffle_partitions": 64,
}
# Sanity limits (a typo such as 2000 executors is rejected). Counts and memory in MiB.
RESOURCE_COUNT_LIMITS = {
    "driver_cores": (1, 4),
    "executors": (1, 100),
    "executor_cores": (1, 16),
    "shuffle_partitions": (1, 20000),
}
RESOURCE_MEMORY_LIMITS_MB = (256, 65536)
MEMORY_VALUE = re.compile(r"([1-9]\d{0,5})([mg])")


def memory_mb(value):
    """MiB of a memory text such as "4096m" or "4g"; None when it is not one."""

    match = MEMORY_VALUE.fullmatch(value) if isinstance(value, str) else None
    if not match:
        return None
    return int(match.group(1)) * (1024 if match.group(2) == "g" else 1)


def check_resources(dataset, resources):
    """Raise ValueError when `resources` is not a dict of known keys with sane values (counts as integers, memory as "4096m" / "4g")."""

    if not isinstance(resources, dict):
        raise ValueError(f"{dataset}: resources must be an object")
    unknown = set(resources) - set(RESOURCE_DEFAULTS)
    if unknown:
        raise ValueError(f"{dataset}: resources has keys that are not accepted ({', '.join(sorted(unknown))}); accepted: {', '.join(RESOURCE_DEFAULTS)}")
    low, high = RESOURCE_MEMORY_LIMITS_MB
    for key, value in resources.items():
        if key in RESOURCE_COUNT_LIMITS:
            first, last = RESOURCE_COUNT_LIMITS[key]
            if isinstance(value, bool) or not isinstance(value, int) or not first <= value <= last:
                raise ValueError(f"{dataset}: resources.{key} must be a whole number from {first} to {last}")
        else:
            size = memory_mb(value)
            if size is None or not low <= size <= high:
                raise ValueError(f"{dataset}: resources.{key} must be a memory like '4096m' or '4g', from {low}m to {high // 1024}g")


def resources_of(settings):
    """The Spark resources of a dataset: its `resources` over RESOURCE_DEFAULTS."""

    return {**RESOURCE_DEFAULTS, **(settings.get("resources") or {})}


def check_dataset(dataset, settings):
    """Check the entry of one dataset in DATASETS; raise ValueError (naming the dataset) when it is wrong. See DATASET_KEYS."""

    if not isinstance(settings, dict):
        raise ValueError(f"{dataset}: the settings must be an object")
    unknown = set(settings) - set(DATASET_KEYS)
    if unknown:
        raise ValueError(f"{dataset}: unknown settings ({', '.join(sorted(unknown))}); accepted: {', '.join(DATASET_KEYS)}")
    if settings.get("mode") not in MODES:
        raise ValueError(f"{dataset}: mode must be one of {', '.join(MODES)}")
    glob = settings.get("hour_glob")
    if "hour_glob" in settings:
        # The glob is written inside a quoted Jinja expression (see build_spec) and becomes part of an HDFS path.
        if (
            not isinstance(glob, str)
            or HOUR_TOKEN not in glob
            or glob.startswith("/")
            or glob.endswith("/")
            or "//" in glob
            or any(bad in glob for bad in ("'", '"', "{", "}", "\\", " "))
        ):
            raise ValueError(f"{dataset}: hour_glob must be a relative path containing {HOUR_TOKEN} (no quotes, braces, spaces or backslashes)")
    if "partition_by_hour" in settings:
        if not isinstance(settings["partition_by_hour"], bool):
            raise ValueError(f"{dataset}: partition_by_hour must be true or false")
        if settings["partition_by_hour"] and not glob:
            raise ValueError(f"{dataset}: partition_by_hour needs hour_glob (the hour is read from the path)")
    if "key_path" in settings:
        if settings["mode"] != "decrypt-encrypt":
            raise ValueError(f"{dataset}: key_path is only for the decrypt-encrypt mode")
        if not isinstance(settings["key_path"], str) or not settings["key_path"].strip():
            raise ValueError(f"{dataset}: key_path must be a non-empty text")
    if "resources" in settings:
        check_resources(dataset, settings["resources"])


def dataset_fields(settings):
    """The fields of the SOURCE constant that come from the entry of a dataset in DATASETS (shared by the DAG files of every source)."""

    return {
        "hour_glob": settings.get("hour_glob", ""),
        "partition_by_hour": settings.get("partition_by_hour", False),
        "resources": resources_of(settings),
    }


def build_spec(source):
    """SparkApplication that loads the files of one day of the dataset of `source`. Job env: silver-ingest-app/README.md.

    The table and the output mode are the settings approved on the request (source["fixed"], read from the Airflow Variable when the DAG is
    parsed); the path is `<dataset>/<day folder>=<day>` (plus the hours of conf, see input_path) and the batch (column `dt`) is the day.
    """

    fixed = source.get("fixed", {})
    res = source.get("resources") or RESOURCE_DEFAULTS
    unit = schedule_unit(fixed["cron"]) if fixed.get("cron") else "day"

    common_env = [
        {"name": "TZ", "value": "Asia/Ho_Chi_Minh"},
        {"name": "MALLOC_ARENA_MAX", "value": "2"},
    ]
    job_env = [
        {"name": "APP_NAME", "value": f"SilverIngest-{source['name']}"},
        # input_root ends with "/", the relative path has no leading "/". The macros input_path, run_date and batch_hours (user_defined_macros of the DAG)
        # pick the whole day, or only the hours of conf.
        {
            "name": "INPUT_PATH",
            "value": "{{ input_path('"
            + f"{source['input_root']}{source['dataset']}/{source['day_dir']}="
            + "', '"
            + source.get("hour_glob", "")
            + "', '"
            + unit
            + "', dag_run.conf, data_interval_start) }}",
        },
        {"name": "OUTPUT_MODE", "value": str(fixed.get("outputMode", DEFAULT_OUTPUT_MODE))},
        {"name": "BATCH_COLUMN", "value": BATCH_COLUMN},
        {"name": "BATCH_VALUE", "value": "{{ run_date(dag_run.conf, data_interval_start) }}"},
        {"name": "OUTPUT_TABLE", "value": str(fixed.get("table", ""))},
        {"name": "WAREHOUSE_PATH", "value": WAREHOUSE_PATH},
        {"name": "ENABLE_HIVE_SUPPORT", "value": "true"},
    ] + [{"name": k, "value": v} for k, v in source["env"].items()]
    if source.get("hour_glob"):
        # The job adds the column `hour` from the path of every file; BATCH_HOURS = the hours of this run (empty: the whole day), so a run
        # replaces exactly the (dt, hour) rows that it ingests. Partitioning by hour only when the dataset asks for it (big datasets): a small
        # one stays partitioned by day and `hour` is an ordinary column.
        job_env += [
            {"name": "HOUR_PATH_REGEX", "value": hour_regex(source["hour_glob"])},
            {"name": "HOUR_COLUMN", "value": HOUR_COLUMN},
            {"name": "BATCH_HOURS", "value": "{{ batch_hours('" + unit + "', dag_run.conf, data_interval_start) }}"},
        ]
        if source.get("partition_by_hour"):
            job_env.append({"name": "OUTPUT_PARTITION_BY", "value": HOUR_COLUMN})
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
                "cores": res["driver_cores"],
                "coreLimit": f"{res['driver_cores'] * 1000}m",
                "memory": res["driver_memory"],
                "memoryOverhead": res["driver_memory_overhead"],
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
                "instances": res["executors"],
                "cores": res["executor_cores"],
                "coreLimit": f"{res['executor_cores'] * 1000}m",
                "memory": res["executor_memory"],
                "memoryOverhead": res["executor_memory_overhead"],
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
                "spark.sql.shuffle.partitions": str(res["shuffle_partitions"]),
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
