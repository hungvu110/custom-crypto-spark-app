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
# The library of VTNET (com.viettel.security.PassTranformer, a signed partner jar copied as it is into the image): only the jobs of a dataset
# whose provider_decrypt has cipher "vtnet" load it (the executors decrypt, so it goes into spark.jars).
VTNET_JAR = "local:///opt/app/viettelsecurity-2.1.jar"

# The constant key field of the global key (key_field_constant in DATASETS): the SAME in every table of every source, so one value (an isdn, an
# imsi) gives one ciphertext in the tables of VTT and of VTNET alike, and they join (f022-register.imsi = ebs_3g_4g.imsi).
JOIN_KEY_FIELD = {"name": "key_scope", "value": "vtt"}

# Ingest modes of the Spark app (entry points of silver-ingest-app): the DAG of a dataset picks the one that fits its data.
#   passthrough:     read the files and write the table as they are, no DAK call.
#   decrypt-encrypt: decrypt the data of the provider (DECRYPT_MODE: file = the whole file with the DAK by-path key, column = the columns that
#                    the provider encrypted, with the by-path prefix and cdr-crypto, none = nothing), group some columns (COLUMN_TRANSFORMS),
#                    then encrypt the requested columns with cdr-crypto (none requested -> written as they are), then write the table.
MODES = {
    "passthrough": "vai.lakehouse.silveringest.app.PassthroughSilverApp",
    "decrypt-encrypt": "vai.lakehouse.silveringest.app.DecryptEncryptSilverApp",
}

# Wait for the DAK admin to approve the column key request (by-column answers PENDING): first poll after `initial` seconds, each next one
# multiplied by `multiplier`, at most `max_interval` seconds apart, give up after `timeout` seconds (job exits with 5). While waiting, the
# driver and the executors keep their resources.
PENDING_WAIT = {"timeout": 7200, "initial": 30, "max_interval": 600, "multiplier": 2}


def dak_connection_env(caller):
    """Driver env of the key source: DAK (by-path, by-column and the global key), INTERNAL client; only the driver needs it. `caller` = the
    DAK_CALLER of the job (the DAG file of a source names it, e.g. silver-ingest-vtt-f014-mpre-tot-charge-d)."""

    return [
        {"name": "CRYPTO_PREFIX_SOURCE", "value": "dak"},
        # Currently points to dak-mock (HTTP, same namespace). With the real DAK: https://<dak> and REMOVE DAK_ALLOW_INSECURE_HTTP.
        {"name": "DAK_ADDR", "value": "http://dak-api.lakehouse-database.svc.cluster.local:8080"},
        {"name": "DAK_ALLOW_INSECURE_HTTP", "value": "true"},
        {
            "name": "DAK_TOKEN_URL",
            "value": "https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/token",
        },
        # ASSUMPTION - the real internal client id (dak-mock: "kind: internal").
        {"name": "DAK_CLIENT_ID", "value": "sa-vlp-datalake-test"},
        {
            "name": "DAK_CLIENT_SECRET",
            "valueFrom": {"secretKeyRef": {"name": "spark-dak-client", "key": "clientSecret"}},
        },
        # An internal client MUST send tag=internal.
        {"name": "DAK_TAG", "value": "internal"},
        {"name": "DAK_CALLER", "value": caller},
        {"name": "DAK_PENDING_WAIT_TIMEOUT_SECONDS", "value": str(PENDING_WAIT["timeout"])},
        {"name": "DAK_PENDING_WAIT_INITIAL_SECONDS", "value": str(PENDING_WAIT["initial"])},
        {"name": "DAK_PENDING_WAIT_MAX_INTERVAL_SECONDS", "value": str(PENDING_WAIT["max_interval"])},
        {"name": "DAK_PENDING_WAIT_MULTIPLIER", "value": str(PENDING_WAIT["multiplier"])},
    ]


# Identity of the audit log of cdr_encrypt (docs/cdr-crypto/CDR_CRYPTO_SQL_ENGINE_GUIDE.md section 2.2/2.4). tenant/workspace/team are those of
# the namespace vlp-<tenant>-<workspace>-<team> (the same tenant as DAK_TOKEN_URL). ASSUMPTION: adjust to the real namespace.
AUDIT_TENANT = "tenantw1xjixm"
AUDIT_WORKSPACE = "wsw7vtwvi"
AUDIT_TEAM = "teamtscauiy"


def audit_conf(engine_id):
    """spark.cdrcrypto.audit.* of the driver: one `CDR_AUDIT {json}` line per query that encrypts a column, written to the pod log (logger
    vai.lakehouse.cdrcrypto.audit in silver-ingest-app/src/main/resources/log4j2.properties). engineId = the job (it has no sql-engine id)."""

    return {
        "spark.cdrcrypto.audit.enabled": "true",
        "spark.cdrcrypto.audit.sinks": "log",
        "spark.cdrcrypto.audit.tenant": AUDIT_TENANT,
        "spark.cdrcrypto.audit.workspace": AUDIT_WORKSPACE,
        "spark.cdrcrypto.audit.team": AUDIT_TEAM,
        "spark.cdrcrypto.audit.engineId": engine_id,
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
    if is_bulk(conf):
        return []  # a bulk load loads whole days
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

    if is_bulk(conf):
        return f"{prefix}{BULK_DAY_TOKEN}"  # the job puts each day of the range in the place of the token
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


# ---------------------------------------------------------------- BULK LOAD
# A BULK LOAD is ONE run of the DAG of a dataset that loads many days (conf {"fromDate": ..., "toDate": ...}): the job (env BULK_FROM, BULK_TO)
# takes the days one after the other in ONE Spark session, each into the partition dt of that day. A day that is already loaded (a test run, an
# earlier load) is replaced, never written twice, and the days outside the range are not touched, so it does not matter what was run before.
# A day that fails stops the load there (the days before it are loaded; run again from that day). conf.missingDays "skip" skips a day without files.
BULK_DAY_TOKEN = "%DAY%"  # the place of the day in INPUT_PATH: the job replaces it
MAX_BULK_DAYS = 62  # the same limit as the job (silver-ingest-app BulkLoad.MaxDays)
MISSING_DAYS = ("fail", "skip")
BULK_KEYS = ("fromDate", "toDate", "missingDays")


def is_bulk(conf):
    """True when the run is a bulk load: conf.fromDate (check_conf makes sure that toDate and missingDays come with it)."""

    return "fromDate" in (conf or {})


def bulk_from(conf):
    """Value of BULK_FROM: the first day, "" when the run is not a bulk load."""

    return (conf or {}).get("fromDate", "")


def bulk_to(conf):
    """Value of BULK_TO: the last day (the first one when conf.toDate is missing), "" when the run is not a bulk load."""

    conf = conf or {}
    return conf.get("toDate", conf["fromDate"]) if is_bulk(conf) else ""


def bulk_missing_days(conf):
    """Value of BULK_MISSING_DAY: what the job does with a day that has no files ("fail" by default), "" when the run is not a bulk load."""

    conf = conf or {}
    return conf.get("missingDays", "fail") if is_bulk(conf) else ""


def batch_value(conf, interval_start):
    """Value of BATCH_VALUE: the day of the run, "" for a bulk load (the job takes the batch of each day from the day itself)."""

    return "" if is_bulk(conf) else run_date(conf, interval_start)


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
# Zone of a global key (the same rule as DAK): lowercase letters, digits, "_".
GLOBAL_ZONE = re.compile(r"[a-z0-9_]{1,64}")
RUN_DATE_VALUE = re.compile(r"\d{8}")
# Keys of the conf of a manual run of a dataset DAG (check_conf).
CONF_KEYS = ("date", "fromHour", "toHour", "fromDate", "toDate", "missingDays")
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


def check_global_key(config, needs_columns=True, required=False):
    """globalKey / globalZone (docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md §1): DAK writes them ONLY when the request uses a global key, so
    the pair is optional but all-or-nothing. globalKey is always the boolean true (DAK never sends false) and globalZone a zone name; the
    columns to encrypt must not be empty (DAK blocks that when the request is submitted, checked here too).

    `needs_columns=False`: the columns are fixed by the dataset (`encrypt_columns` in DATASETS), not chosen by the request, so the request does
    not carry them. `required=True`: the dataset ALWAYS encrypts with the global key of a zone, so the pair must be there."""

    if "globalKey" not in config and "globalZone" not in config:
        if required:
            raise ValueError("schedule config globalKey and globalZone are required (this dataset encrypts its columns with the global key of a zone)")
        return
    if config.get("globalKey") is not True:
        raise ValueError("schedule config globalKey must be true (omit it to use the per-column keys)")
    zone = config.get("globalZone")
    if not isinstance(zone, str) or not GLOBAL_ZONE.fullmatch(zone):
        raise ValueError("schedule config globalZone is missing or malformed (lowercase letters, digits, '_', up to 64)")
    if needs_columns and not config.get("encryptColumns"):
        raise ValueError("schedule config globalKey needs a non-empty encryptColumns (the columns that the global key encrypts)")


def global_zone(config):
    """The zone of the global key of an approved request (see check_global_key), or None for a request with the per-column keys."""

    return config.get("globalZone") if config.get("globalKey") is True else None


EMPTY_REQUEST_KEYS = ("encryptColumns", "keyField")


def check_schedule_config(source, config):
    """Check the settings that DAK wrote into the Airflow Variable of a recurring dataset; raise ValueError when they are invalid.

    `source["schedule_keys"]` lists the accepted keys (ticket, table, cron, outputMode and, for a decrypt-encrypt dataset, encryptColumns,
    keyField and, only when the request uses a global key, globalKey and globalZone). For a dataset with fixed columns (VTT) encryptColumns
    and keyField only record the request (check_fixed_columns). Messages never repeat a value of the request.
    """

    if not isinstance(config, dict):
        raise ValueError("schedule config must be an object")
    # DAK writes "encryptColumns": [] (and may write an empty keyField) on a request that encrypts nothing (e.g. nims, a passthrough): an
    # EMPTY value says "no column" and is accepted on any dataset; a non-empty one still needs a dataset that encrypts columns.
    unknown = {k for k, v in config.items() if not (k in EMPTY_REQUEST_KEYS and v in ([], "", None))} - set(source["schedule_keys"])
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
        check_global_key(config)
    elif source.get("requires_global_key"):
        check_fixed_columns(source, config)
        check_global_key(config, needs_columns=False, required=True)


def check_fixed_columns(source, config):
    """A dataset whose columns are fixed (`source["fixed_columns"]`, from `encrypt_columns` in DATASETS): DAK requires encryptColumns and
    keyField with a global key, so the request may carry them, but they only RECORD the request. They must name exactly the columns that
    the dataset encrypts (any order, case-insensitive), and keyField must be a column name that is not encrypted. The job still takes its
    columns and its key field from the dataset (the constant key field keeps one ciphertext per value in every table), never from them."""

    if "encryptColumns" not in config and "keyField" not in config:
        return
    if not config.get("encryptColumns"):
        raise ValueError("schedule config keyField needs encryptColumns (the columns that this dataset encrypts)")
    check_columns(config)
    expected = sorted(c.lower() for c in source["fixed_columns"])
    if sorted(c.lower() for c in config["encryptColumns"]) != expected:
        raise ValueError(f"schedule config encryptColumns must be exactly the columns that this dataset encrypts: {', '.join(expected)}")


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


def check_bulk(source, conf):
    """conf of a bulk load: fromDate (a real date yyyyMMdd), optional toDate (default fromDate, not before it, at most MAX_BULK_DAYS days in all)
    and missingDays ("fail" or "skip"). Not together with `date` or the hours (a bulk load loads whole days), and the request must write with
    overwrite-partitions so that a day that is already loaded is replaced. Messages never repeat the received value."""

    if "fromDate" not in conf:
        raise ValueError("conf.toDate and conf.missingDays need conf.fromDate (the first day of the bulk load)")
    for key in ("date", "fromHour", "toHour"):
        if key in conf:
            raise ValueError(f"a bulk load loads whole days: conf.{key} cannot be used with conf.fromDate")
    check_date(conf["fromDate"], "conf.fromDate")
    check_date(conf.get("toDate", conf["fromDate"]), "conf.toDate")
    first = datetime.datetime.strptime(conf["fromDate"], "%Y%m%d").date()
    last = datetime.datetime.strptime(conf.get("toDate", conf["fromDate"]), "%Y%m%d").date()
    if first > last:
        raise ValueError("conf.fromDate must not be after conf.toDate")
    if (last - first).days + 1 > MAX_BULK_DAYS:
        raise ValueError(f"a bulk load covers at most {MAX_BULK_DAYS} days: split it into several runs")
    if conf.get("missingDays", "fail") not in MISSING_DAYS:
        raise ValueError(f"conf.missingDays must be one of {', '.join(MISSING_DAYS)}")
    if (source.get("fixed") or {}).get("outputMode") == "append":
        raise ValueError("a bulk load needs the output mode overwrite-partitions: a day that is already loaded must be replaced, not appended")


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
    if any(key in conf for key in BULK_KEYS):
        check_bulk(source, conf)
        return
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


# ---------------------------------------------------------------- INPUT ON ANOTHER CLUSTER (input_cluster in DATASETS)
# The landing of a dataset may be on another HDFS cluster (VTNET EBS-4G: HKH) with its own KDC (same realm NAME as here, no trust between
# them): the job keeps its keytab of THIS cluster (writes the table in the metastore here, as before) and READS the other cluster with a
# delegation token of that cluster, kept in a Secret of THIS namespace (one per cluster, `token_secret`). The token is fetched BY HAND with
# `hdfs fetchdt` (docs/silver-ingest/VTNET-EBS-HKH.md) and renewed by hand before it ends (renew-interval of that cluster, Hadoop default 24 h);
# the DAG only mounts the Secret into the driver and the executors (INPUT_TOKEN_DIR): the executors load it through HADOOP_TOKEN_FILE_LOCATION,
# the driver through INPUT_TOKEN_FILE (its keytab login drops the token files). The job stops (exit 2) when the Secret holds no token of the
# cluster of INPUT_PATH.
INPUT_TOKEN_DIR = "/mnt/silver-input-token"
INPUT_TOKEN_KEY = "hadoop.token"
INPUT_CLUSTERS = {
    "hkh": {
        "hdfs": "hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020",
        "token_secret": "hkh-hdfs-token",
    },
}


def input_token_secret(source):
    """Name of the Secret (in NAMESPACE) with the delegation token of the cluster of the landing of `source`."""

    return INPUT_CLUSTERS[source["input_cluster"]]["token_secret"]


# ---------------------------------------------------------------- DATASET SETTINGS (DATASETS in the DAG files)
# EVERYTHING that is specific to one dataset is written in its entry of DATASETS in the DAG file of its source (silver_ingest_vtt.py,
# silver_ingest_vtnet.py), and nowhere else. The entry is checked (check_dataset) when Airflow parses the DAG file: a typo is an import error,
# not a silent default. Keys:
#   mode               required  a key of MODES: passthrough, or decrypt-encrypt (DAK by-path / by-column keys).
#   hour_glob          optional  where the hour is written under the folder of a day, with HOUR_TOKEN for the hour(s). It lets a scheduled
#                                hourly run, a manual run or silver_ingest_backfill.py ingest only some hours; without it only whole days.
#   hour_column        optional  the hour is NOT in the path but in this column of the data (the landing has only the day folder, every file holds
#                                all the hours of its day). The job writes the column `hour` ("HH", from 0..23 or 00..23, otherwise NA) next to it.
#                                Whole days only: no hourly cron, no fromHour/toHour (those need hour_glob). Not together with hour_glob.
#   partition_by_hour  optional  true: the table is partitioned by (dt, hour) (big datasets); false (default): by dt only, `hour` is an
#                                ordinary column (small datasets, no 24 tiny partitions a day). Needs hour_glob or hour_column.
#   key_path           optional  decrypt-encrypt only: the path asked from DAK by-path, one key for the whole dataset (default: the dataset).
#   resources          optional  Spark resources of the job of this dataset, any of the keys of RESOURCE_DEFAULTS (the rest keep the default).
# The settings below are for the decrypt-encrypt mode only and fix, per dataset, what the job does with the data of a provider that encrypts
# column by column (docs/silver-ingest/TRANSFORM-PHIEU-KHAI-THAC-VTT.md section 3). They become env of the job (see dataset_job_env):
#   decrypt_mode       optional  file (default: the whole file, with the by-path key), column (the columns of `provider_decrypt`, with the by-path
#                                key as the prefix) or none (nothing is decrypted, DAK by-path is never called).
#   provider_decrypt   column    {"columns": [...], "key_field": "..."}: the columns that the provider encrypted and the plaintext column of the
#                                provider that completes the key of each row (cdr-crypto, VTT). Required with decrypt_mode column, not allowed
#                                otherwise. {"columns": [...], "cipher": "vtnet"}: encrypted with the library of VTNET (PassTranformer), the
#                                by-path key is its inputKey and there is no key_field.
# This setting says WHERE the landing is:
#   input_cluster      optional  a key of INPUT_CLUSTERS: the landing is on that other cluster, read with a delegation token of it (see INPUT ON
#                                ANOTHER CLUSTER). The DAG file sets the landing root on that cluster.
#   encrypt_columns    optional  the columns that the job encrypts with cdr-crypto, fixed by the dataset (not by the request). Needs
#                                key_field_constant; the request then only carries globalKey/globalZone.
#   key_field_constant optional  {"name": "...", "value": "..."}: a CONSTANT column that the job adds and uses as the key field of the encryption,
#                                so the same value gives the same ciphertext in every row and every table (needed to join them after hashing).
#   transforms         optional  {"column": "AGE_BAND" | "HOUR_BAND" | "ISDN_NATIONAL"}: columns that the job groups or normalizes (after the
#                                decryption, before the encryption). ISDN_NATIONAL turns 84/+84/0 + 9 digits into the 9 digits of VTT, so the
#                                isdn of another source gives the same ciphertext as VTT: it may be an encrypt column (a grouping may not).
# This setting says WHEN a dataset runs:
#   periodic           optional  false: the DAG has NO schedule even with an approved request (no daily run); it only runs by hand (conf
#                                {"date": ...}, a bulk load {"fromDate": ..., "toDate": ...} or silver_ingest_backfill.py). For a table that is not
#                                loaded every day (f016, monthly).
# This setting says how a big day is WRITTEN:
#   write_by_hour      optional  true: the job writes the hours of a day one by one (one Delta commit per hour) and logs one line after each
#                                hour (rows, files, size, time), to follow a very big day while it runs (VTNET EBS). Needs hour_glob and
#                                partition_by_hour. While it runs the day is partly loaded; a failed hour says which hours to rerun.
# This setting says how big the written FILES are:
#   output_file_shuffle_mb  optional  MB of SHUFFLE data per written file (job env OUTPUT_FILE_SHUFFLE_MB, default DEFAULT_FILE_SHUFFLE_MB). Before
#                                writing, the job gathers the rows of each partition (REBALANCE) and AQE cuts them into pieces of about this size,
#                                one file each. Shuffle data is bigger than compressed parquet: for files of about 128 MB set
#                                128 x (Shuffle Write on the Spark UI / size of the parquet written), measured on one day.
# This setting says how the DUPLICATES are analysed while DEDUP=check is on (see DUPLICATE CHECK below):
#   dedup_profile_columns  optional  up to DEDUP_PROFILE_MAX columns (job env DEDUP_PROFILE_COLUMNS) whose top values among the duplicate
#                                rows are logged, next to the copies per group and the folders that hold them (same file, same folder, two
#                                folders: for EBS a pair of adjacent batch=HHMM folders means the source sent an overlap). Same read as the
#                                measure. Low-cardinality codes only (event type, result): never an encrypted or key column (their values
#                                would be written to the log, so the DAG and the job refuse them).
DATASET_KEYS = (
    "mode", "hour_glob", "hour_column", "partition_by_hour", "key_path", "resources",
    "decrypt_mode", "provider_decrypt", "encrypt_columns", "key_field_constant", "transforms",
    "periodic", "output_file_shuffle_mb", "input_cluster", "write_by_hour", "dedup_profile_columns",
)
DEDUP_PROFILE_MAX = 4  # the same limit as the job (DuplicateCheck.MaxProfileColumns)
PROVIDER_CIPHERS = ("cdr", "vtnet")
# DUPLICATE CHECK (job env DEDUP): "check" measures the duplicate rows and files of every run in the log (DEDUP_CHECK_RESULT), nothing is removed.
# It costs one more read of the landing, so it is switched on for a few days only: set the Airflow Variable below to "check" (or "off").
# The Variable is read when a run starts, for every silver ingest DAG: no deploy to switch it. A wrong value fails the job (exit 2).
DEDUP_VARIABLE = "silver_ingest_dedup"
DEFAULT_FILE_SHUFFLE_MB = 384  # 128 MB of parquet x 3 (assumed ratio until measured); the same default as the job (OutputFiles.DefaultShuffleMb)
FILE_SHUFFLE_MB_LIMITS = (16, 4096)  # the same limits as the job
HOUR_COLUMN_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")  # the same rule as a column name of the job
DECRYPT_MODES = ("file", "column", "none")
TRANSFORM_KINDS = ("AGE_BAND", "HOUR_BAND", "ISDN_NATIONAL")
# Normalizations: they change a value to a common format BEFORE it is encrypted, so they are allowed on an encrypt column (a grouping is not).
NORMALIZE_KINDS = ("ISDN_NATIONAL",)
KEY_FIELD_VALUE = re.compile(r"[A-Za-z0-9_.:-]{1,64}")  # the value of the constant key field is written in every row

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
    "min_executors": (1, 100),
    "max_executors": (1, 100),
}
# DYNAMIC ALLOCATION: with `max_executors` the executors grow from `executors` (the initial number) up to `max_executors` while tasks wait and
# shrink down to `min_executors` (default 1) when they are idle for DYNAMIC_IDLE_TIMEOUT. Without `max_executors` the number is fixed. There is no
# external shuffle service on Kubernetes, so shuffle tracking is on. Neither key is in RESOURCE_DEFAULTS: no dataset has them unless it says so.
DYNAMIC_KEYS = ("min_executors", "max_executors")
# Optional, per dataset: MB of input that one read task takes (spark.sql.files.maxPartitionBytes, Spark default 128). A table written without
# a shuffle (one file per read task) gets files of about this input size once compressed: raise it for files closer to 128 MB.
SPLIT_KEY = "input_split_mb"
# Optional, per dataset: TASK SLOTS of an executor (spark.executor.cores) above its CPUs (executor_cores = the CPU request and limit of the pod,
# what the quota counts). For a job that waits on its reads about half of the time (VTNET EBS reads HKH: CPU time / run time = 0.5), more tasks
# than CPUs let one task compute while another waits for the network. Each task gets less memory: raise executor_memory with it.
TASK_SLOTS_KEY = "task_slots"
TASK_SLOTS_LIMITS = (1, 32)
# Optional, per dataset: KB of the read buffer of Hadoop streams (spark.hadoop.io.file.buffer.size, Hadoop default 4 KB). The CSV line reader
# reads through it: a bigger buffer, fewer read calls on a big remote CSV.
READ_BUFFER_KEY = "read_buffer_kb"
READ_BUFFER_KB_LIMITS = (4, 16384)
SPLIT_MB_LIMITS = (16, 2048)
DYNAMIC_IDLE_TIMEOUT = "120s"
# DECOMMISSION: an executor that dynamic allocation removes first moves its shuffle (and cached) blocks to the other executors, then exits.
# Without it the blocks die with the executor: the next stage fails to fetch them (FetchFailed) and Spark reruns the map tasks of that executor
# (VTNET EBS, dedup check: executor 17 removed at the end of the map stage, 156 tasks and 47.6 GiB read again, about 3 minutes). It also covers
# the REBALANCE shuffle before a write (VTT tables).
DECOMMISSION_CONF = {
    "spark.decommission.enabled": "true",
    "spark.storage.decommission.enabled": "true",
    "spark.storage.decommission.shuffleBlocks.enabled": "true",
    "spark.storage.decommission.rddBlocks.enabled": "true",
}
# WIDE TABLES (VTT f003, f014, f015, f016 have 150 to 450 columns). Above spark.sql.codegen.maxFields (default 100) Spark turns whole-stage codegen
# off for the plan, which is slower; above spark.sql.debug.maxToStringFields (default 25) it cuts the plan and the schema in the log. One limit for
# both, well above the widest table. A generated method that grows too big still falls back to the interpreted path (spark.sql.codegen.fallback).
WIDE_TABLE_MAX_FIELDS = 1000
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
    accepted = tuple(RESOURCE_DEFAULTS) + DYNAMIC_KEYS + (SPLIT_KEY, TASK_SLOTS_KEY, READ_BUFFER_KEY)
    whole = {SPLIT_KEY: (SPLIT_MB_LIMITS, "a whole number of MB"), TASK_SLOTS_KEY: (TASK_SLOTS_LIMITS, "a whole number"),
             READ_BUFFER_KEY: (READ_BUFFER_KB_LIMITS, "a whole number of KB")}
    unknown = set(resources) - set(accepted)
    if unknown:
        raise ValueError(f"{dataset}: resources has keys that are not accepted ({', '.join(sorted(unknown))}); accepted: {', '.join(accepted)}")
    low, high = RESOURCE_MEMORY_LIMITS_MB
    for key, value in resources.items():
        if key in whole:
            (first, last), kind = whole[key]
            if isinstance(value, bool) or not isinstance(value, int) or not first <= value <= last:
                raise ValueError(f"{dataset}: resources.{key} must be {kind} from {first} to {last}")
        elif key in RESOURCE_COUNT_LIMITS:
            first, last = RESOURCE_COUNT_LIMITS[key]
            if isinstance(value, bool) or not isinstance(value, int) or not first <= value <= last:
                raise ValueError(f"{dataset}: resources.{key} must be a whole number from {first} to {last}")
        else:
            size = memory_mb(value)
            if size is None or not low <= size <= high:
                raise ValueError(f"{dataset}: resources.{key} must be a memory like '4096m' or '4g', from {low}m to {high // 1024}g")
    if resources.get(TASK_SLOTS_KEY, 0) and resources[TASK_SLOTS_KEY] < resources.get("executor_cores", RESOURCE_DEFAULTS["executor_cores"]):
        raise ValueError(f"{dataset}: resources.{TASK_SLOTS_KEY} must be >= executor_cores (it adds task slots, it never removes CPUs)")
    if "max_executors" not in resources:
        if "min_executors" in resources:
            raise ValueError(f"{dataset}: resources.min_executors needs resources.max_executors (dynamic allocation is on only with max_executors)")
        return
    lowest = resources.get("min_executors", 1)
    initial = resources.get("executors", RESOURCE_DEFAULTS["executors"])
    if not lowest <= initial <= resources["max_executors"]:
        raise ValueError(f"{dataset}: resources must satisfy min_executors <= executors <= max_executors (executors is the initial number)")


def dynamic_allocation_conf(res):
    """The spark.dynamicAllocation.* conf of a dataset: off, or on with the numbers of its `resources` (see DYNAMIC_KEYS)."""

    if not res.get("max_executors"):
        return {"spark.dynamicAllocation.enabled": "false"}
    return {
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.shuffleTracking.enabled": "true",
        "spark.dynamicAllocation.minExecutors": str(res.get("min_executors", 1)),
        "spark.dynamicAllocation.maxExecutors": str(res["max_executors"]),
        "spark.dynamicAllocation.initialExecutors": str(res["executors"]),
        "spark.dynamicAllocation.executorIdleTimeout": DYNAMIC_IDLE_TIMEOUT,
        **DECOMMISSION_CONF,
    }


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
    if "output_file_shuffle_mb" in settings:
        size = settings["output_file_shuffle_mb"]
        low, high = FILE_SHUFFLE_MB_LIMITS
        if isinstance(size, bool) or not isinstance(size, int) or not low <= size <= high:
            raise ValueError(f"{dataset}: output_file_shuffle_mb must be a whole number of MB from {low} to {high}")
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
    column = settings.get("hour_column")
    if "hour_column" in settings:
        if not isinstance(column, str) or not HOUR_COLUMN_NAME.fullmatch(column):
            raise ValueError(f"{dataset}: hour_column must be the name of a column (letters, digits and '_', not starting with a digit)")
        if glob:
            raise ValueError(f"{dataset}: hour_glob and hour_column are exclusive (the hour comes from the path or from a column)")
    if "partition_by_hour" in settings:
        if not isinstance(settings["partition_by_hour"], bool):
            raise ValueError(f"{dataset}: partition_by_hour must be true or false")
        if settings["partition_by_hour"] and not (glob or column):
            raise ValueError(f"{dataset}: partition_by_hour needs hour_glob or hour_column (where the hour comes from)")
    if "key_path" in settings:
        if settings["mode"] != "decrypt-encrypt":
            raise ValueError(f"{dataset}: key_path is only for the decrypt-encrypt mode")
        if not isinstance(settings["key_path"], str) or not settings["key_path"].strip():
            raise ValueError(f"{dataset}: key_path must be a non-empty text")
        if settings.get("decrypt_mode") == "none":
            raise ValueError(f"{dataset}: key_path is not used with decrypt_mode none (DAK by-path is never called)")
    if "resources" in settings:
        check_resources(dataset, settings["resources"])
    check_decrypt_settings(dataset, settings)
    if "periodic" in settings and not isinstance(settings["periodic"], bool):
        raise ValueError(f"{dataset}: periodic must be true or false")
    if "write_by_hour" in settings:
        if not isinstance(settings["write_by_hour"], bool):
            raise ValueError(f"{dataset}: write_by_hour must be true or false")
        if settings["write_by_hour"] and not (settings.get("hour_glob") and settings.get("partition_by_hour")):
            raise ValueError(f"{dataset}: write_by_hour needs hour_glob and partition_by_hour (each hour is a group of files and a partition)")
    if "input_cluster" in settings and settings["input_cluster"] not in INPUT_CLUSTERS:
        raise ValueError(f"{dataset}: input_cluster must be one of {', '.join(INPUT_CLUSTERS)}")
    if "dedup_profile_columns" in settings:
        check_dedup_profile(dataset, settings)


def check_dedup_profile(dataset, settings):
    """dedup_profile_columns: up to DEDUP_PROFILE_MAX column names, none of them sensitive (their values are written to the log)."""

    profile = check_column_names(dataset, "dedup_profile_columns", settings["dedup_profile_columns"])
    if len(profile) > DEDUP_PROFILE_MAX:
        raise ValueError(f"{dataset}: dedup_profile_columns takes at most {DEDUP_PROFILE_MAX} columns")
    provider = settings.get("provider_decrypt") or {}
    sensitive = {
        c.lower()
        for c in [*settings.get("encrypt_columns", []), *provider.get("columns", []), provider.get("key_field"),
                  (settings.get("key_field_constant") or {}).get("name")]
        if c
    }
    leaked = sorted(set(profile) & sensitive)
    if leaked:
        raise ValueError(f"{dataset}: dedup_profile_columns must not hold a sensitive column ({', '.join(leaked)}): its values would be logged")


def check_column_names(dataset, key, value):
    """A non-empty list of distinct column names (case-insensitive), for a setting of DATASETS; returns it lower-cased."""

    if (
        not isinstance(value, list)
        or not value
        or len(value) > MAX_COLUMNS
        or not all(isinstance(c, str) and COLUMN.fullmatch(c) for c in value)
    ):
        raise ValueError(f"{dataset}: {key} must be a non-empty list of up to {MAX_COLUMNS} column names [A-Za-z_][A-Za-z0-9_]*")
    lower = [c.lower() for c in value]
    if len(set(lower)) != len(lower):
        raise ValueError(f"{dataset}: {key} has a duplicated column (names are case-insensitive)")
    return lower


def check_decrypt_settings(dataset, settings):
    """decrypt_mode, provider_decrypt, encrypt_columns, key_field_constant and transforms of an entry of DATASETS (see DATASET_KEYS)."""

    own = ("decrypt_mode", "provider_decrypt", "encrypt_columns", "key_field_constant", "transforms")
    for key in own:
        if key in settings and settings["mode"] != "decrypt-encrypt":
            raise ValueError(f"{dataset}: {key} is only for the decrypt-encrypt mode")
    decrypt_mode = settings.get("decrypt_mode", "file")
    if decrypt_mode not in DECRYPT_MODES:
        raise ValueError(f"{dataset}: decrypt_mode must be one of {', '.join(DECRYPT_MODES)}")
    if decrypt_mode == "column" and "provider_decrypt" not in settings:
        raise ValueError(f"{dataset}: decrypt_mode column needs provider_decrypt (the columns that the provider encrypted)")
    if "provider_decrypt" in settings:
        if decrypt_mode != "column":
            raise ValueError(f"{dataset}: provider_decrypt is only for decrypt_mode column")
        provider = settings["provider_decrypt"]
        if not isinstance(provider, dict) or "columns" not in provider or not set(provider) <= {"columns", "key_field", "cipher"}:
            raise ValueError(f"{dataset}: provider_decrypt must be an object with columns, and key_field (cdr) or cipher vtnet")
        cipher = provider.get("cipher", "cdr")
        if cipher not in PROVIDER_CIPHERS:
            raise ValueError(f"{dataset}: provider_decrypt.cipher must be one of {', '.join(PROVIDER_CIPHERS)}")
        columns = check_column_names(dataset, "provider_decrypt.columns", provider["columns"])
        if cipher == "vtnet":
            if "key_field" in provider:
                raise ValueError(f"{dataset}: provider_decrypt.key_field is not used with cipher vtnet (its key has no per-row part)")
        else:
            key_field = provider.get("key_field")
            if not isinstance(key_field, str) or not COLUMN.fullmatch(key_field):
                raise ValueError(f"{dataset}: provider_decrypt.key_field must be the name of a column")
            if key_field.lower() in columns:
                raise ValueError(f"{dataset}: provider_decrypt.key_field must not be one of the columns (it stays plaintext as key material)")
    encrypt = []
    if "encrypt_columns" in settings:
        encrypt = check_column_names(dataset, "encrypt_columns", settings["encrypt_columns"])
    if "key_field_constant" in settings:
        constant = settings["key_field_constant"]
        if not isinstance(constant, dict) or set(constant) != {"name", "value"}:
            raise ValueError(f"{dataset}: key_field_constant must be an object with exactly name and value")
        name, value = constant["name"], constant["value"]
        if not isinstance(name, str) or not COLUMN.fullmatch(name):
            raise ValueError(f"{dataset}: key_field_constant.name must be the name of a column")
        if not isinstance(value, str) or not KEY_FIELD_VALUE.fullmatch(value):
            raise ValueError(f"{dataset}: key_field_constant.value must be 1 to 64 characters of [A-Za-z0-9_.:-]")
        if "encrypt_columns" not in settings:
            raise ValueError(f"{dataset}: key_field_constant needs encrypt_columns (the columns that it encrypts)")
        if name.lower() in encrypt:
            raise ValueError(f"{dataset}: key_field_constant.name must not be one of the encrypt_columns (it stays plaintext as key material)")
    elif "encrypt_columns" in settings:
        raise ValueError(f"{dataset}: encrypt_columns needs key_field_constant (the constant column that is the key field of the encryption)")
    if "transforms" in settings:
        transforms = settings["transforms"]
        if not isinstance(transforms, dict) or not transforms:
            raise ValueError(f"{dataset}: transforms must be a non-empty object {{column: kind}}")
        lower = [c.lower() for c in transforms if isinstance(c, str)]
        if len(set(lower)) != len(lower):
            raise ValueError(f"{dataset}: transforms has a duplicated column (names are case-insensitive)")
        for column, kind in transforms.items():
            if not isinstance(column, str) or not COLUMN.fullmatch(column):
                raise ValueError(f"{dataset}: transforms has a column name that is not valid")
            if kind not in TRANSFORM_KINDS:
                raise ValueError(f"{dataset}: transforms.{column} must be one of {', '.join(TRANSFORM_KINDS)}")
            if column.lower() in encrypt and kind not in NORMALIZE_KINDS:
                raise ValueError(f"{dataset}: transforms.{column} is an encrypt column (a grouped value cannot be encrypted)")


def dataset_job_env(dataset, settings):
    """Env of the job that comes from the decrypt/encrypt/group settings of an entry of DATASETS ({} for an entry that sets none of them, so
    the datasets that exist without them render exactly as before). KEY_PATH only for decrypt_mode column: one by-path key per dataset."""

    env = {}
    if "decrypt_mode" in settings:
        env["DECRYPT_MODE"] = settings["decrypt_mode"]
    if settings.get("decrypt_mode") == "column":
        env["KEY_PATH"] = settings.get("key_path", dataset)
    provider = settings.get("provider_decrypt")
    if provider:
        env["PROVIDER_DECRYPT_COLUMNS"] = ",".join(provider["columns"])
        if "cipher" in provider:
            env["PROVIDER_CIPHER"] = provider["cipher"]
        if "key_field" in provider:
            env["PROVIDER_KEY_FIELD"] = provider["key_field"]
    if settings.get("encrypt_columns"):
        env["CDR_ENCRYPT_COLUMNS"] = ",".join(settings["encrypt_columns"])
        env["CDR_KEY_FIELD"] = settings["key_field_constant"]["name"]
        env["CDR_KEY_FIELD_VALUE"] = settings["key_field_constant"]["value"]
    if settings.get("transforms"):
        env["COLUMN_TRANSFORMS"] = ",".join(f"{column}:{kind}" for column, kind in settings["transforms"].items())
    return env


def dataset_fields(settings):
    """The fields of the SOURCE constant that come from the entry of a dataset in DATASETS (shared by the DAG files of every source)."""

    return {
        "hour_glob": settings.get("hour_glob", ""),
        "hour_column": settings.get("hour_column", ""),
        "partition_by_hour": settings.get("partition_by_hour", False),
        "periodic": settings.get("periodic", True),
        "resources": resources_of(settings),
        "output_file_shuffle_mb": settings.get("output_file_shuffle_mb", DEFAULT_FILE_SHUFFLE_MB),
        **({"input_cluster": settings["input_cluster"]} if "input_cluster" in settings else {}),
        **({"write_by_hour": True} if settings.get("write_by_hour") else {}),
        **({"dedup_profile_columns": settings["dedup_profile_columns"]} if settings.get("dedup_profile_columns") else {}),
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
        {"name": "BATCH_VALUE", "value": "{{ batch_value(dag_run.conf, data_interval_start) }}"},
        # A bulk load (conf fromDate/toDate): the job loads every day of the range in this one run. Empty for any other run.
        {"name": "BULK_FROM", "value": "{{ bulk_from(dag_run.conf) }}"},
        {"name": "BULK_TO", "value": "{{ bulk_to(dag_run.conf) }}"},
        {"name": "BULK_MISSING_DAY", "value": "{{ bulk_missing_days(dag_run.conf) }}"},
        {"name": "OUTPUT_TABLE", "value": str(fixed.get("table", ""))},
        {"name": "WAREHOUSE_PATH", "value": WAREHOUSE_PATH},
        {"name": "ENABLE_HIVE_SUPPORT", "value": "true"},
        # Files of about the same size instead of one file per task and partition (see output_file_shuffle_mb in DATASET_KEYS).
        {"name": "OUTPUT_FILE_SHUFFLE_MB", "value": str(source.get("output_file_shuffle_mb", DEFAULT_FILE_SHUFFLE_MB))},
        {"name": "DEDUP", "value": "{{ var.value.get('" + DEDUP_VARIABLE + "', 'off') }}"},
    ] + (
        # Read by the job only while DEDUP=check (see dedup_profile_columns in DATASET_KEYS).
        [{"name": "DEDUP_PROFILE_COLUMNS", "value": ",".join(source["dedup_profile_columns"])}]
        if source.get("dedup_profile_columns") else []
    ) + [{"name": k, "value": v} for k, v in source["env"].items()]
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
        if source.get("write_by_hour"):
            # One Delta commit and one log line per hour (see write_by_hour in DATASET_KEYS).
            job_env.append({"name": "OUTPUT_WRITE_BY_HOUR", "value": "true"})
    elif source.get("hour_column"):
        # The hour is a column of the data (the landing has only the day folder): the job derives `hour` from it. No BATCH_HOURS: the files of a
        # day hold every hour, so a run always replaces the whole day (dt), whatever the hours of its rows.
        job_env += [
            {"name": "HOUR_FROM_COLUMN", "value": source["hour_column"]},
            {"name": "HOUR_COLUMN", "value": HOUR_COLUMN},
        ]
        if source.get("partition_by_hour"):
            job_env.append({"name": "OUTPUT_PARTITION_BY", "value": HOUR_COLUMN})
    token_conf = {}
    if source.get("input_cluster"):
        # The landing is on another cluster (see INPUT ON ANOTHER CLUSTER): the Secret of its token is mounted into the driver and the executors
        # by Spark itself (spark.kubernetes.*.secrets, no webhook needed). The driver loads it through INPUT_TOKEN_FILE (its keytab login drops the
        # token files), the executors through HADOOP_TOKEN_FILE_LOCATION.
        token_file = f"{INPUT_TOKEN_DIR}/{INPUT_TOKEN_KEY}"
        job_env.append({"name": "INPUT_TOKEN_FILE", "value": token_file})
        secret = input_token_secret(source)
        token_conf = {
            f"spark.kubernetes.driver.secrets.{secret}": INPUT_TOKEN_DIR,
            f"spark.kubernetes.executor.secrets.{secret}": INPUT_TOKEN_DIR,
            "spark.executorEnv.HADOOP_TOKEN_FILE_LOCATION": token_file,
        }
    jars = JARS + (f",{VTNET_JAR}" if source["env"].get("PROVIDER_CIPHER") == "vtnet" else "")
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
                # Task slots; the pod asks for executor_cores CPUs (coreRequest/coreLimit) whatever the slots (see TASK_SLOTS_KEY).
                "cores": res.get(TASK_SLOTS_KEY) or res["executor_cores"],
                **({"coreRequest": f"{res['executor_cores'] * 1000}m"} if res.get(TASK_SLOTS_KEY) else {}),
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
                # cdr-crypto (the real UDF) + key-prefix-lib + the partner jar(s): the executors need them too.
                "spark.jars": jars,
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
                # The job REBALANCEs the rows by partition before writing: AQE merges the small pieces and splits the big ones into files of
                # about the same size (both are Spark defaults, set here so that the file sizes do not depend on them).
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled": "true",
                "spark.sql.codegen.maxFields": str(WIDE_TABLE_MAX_FIELDS),
                "spark.sql.debug.maxToStringFields": str(WIDE_TABLE_MAX_FIELDS),
                **dynamic_allocation_conf(res),
                **({"spark.sql.files.maxPartitionBytes": str(res[SPLIT_KEY] * 1024 * 1024)} if res.get(SPLIT_KEY) else {}),
                **({"spark.hadoop.io.file.buffer.size": str(res[READ_BUFFER_KEY] * 1024)} if res.get(READ_BUFFER_KEY) else {}),
                "spark.sql.shuffle.partitions": str(res["shuffle_partitions"]),
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": EVENT_LOG_DIR,
                "spark.eventLog.rolling.enabled": "true",
                "spark.eventLog.rolling.maxFileSize": "128m",
                # Settings that are specific to one source (e.g. spark.cdrcrypto.audit.* of VTNET).
                **source.get("extra_spark_conf", {}),
                **token_conf,
            },
        },
    }
    if trust:
        spec["spec"]["volumes"] = copy.deepcopy(trust["volumes"])
    return spec
