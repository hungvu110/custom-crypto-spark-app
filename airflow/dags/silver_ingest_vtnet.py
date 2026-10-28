"""RECURRING DAGs that load the files of the VTNET datasets into the demo database (docs/silver-ingest/PLAN.md). There is ONE DAG PER DATASET
(DATASETS below), built by silver_ingest_factory.py, and each runs once a day at the time of its approved request. A dataset picks an ingest
mode (common.MODES).

  dataset    DAG                            mode             folder of a day
  ebs_3g_4g  silver_ingest_vtnet_ebs_3g_4g  passthrough      ebs_3g_4g/dt=YYYYMMDD/(batch=...)
  nims       silver_ingest_vtnet_nims       decrypt-encrypt  nims/dt=YYYYMMDD/(batch=...)

How a dataset gets its schedule (Airflow Variable written by DAK when the request is approved once): silver_ingest_factory.py.
A run ingests the folder of the day BEFORE the run (all its batch subfolders) into the partition batch_id = <day>; a manual run can take
another day: conf {"date": "yyyyMMdd"}.

passthrough: read the files and write the table as they are (no DAK call).
decrypt-encrypt (docs/key-access/TRA-LOI-DAK-BY-PATH.md): ask DAK for the key of the dataset (by-path, ONE key per dataset). If DAK has a key,
the whole file is encrypted and the job decrypts it (FILE_CIPHER); if DAK answers empty, the file is not encrypted and the job reads it as it
is. Then the `encryptColumns` of the request are encrypted with cdr-crypto (per-column key through DAK by-column, onMissing=request) and the
table is written; with no `encryptColumns` the files are only decrypted. When a column has no key yet, DAK opens a request (PENDING); the job
WAITS and polls again with exponential backoff (PENDING_WAIT) until the DAK admin approves. If the timeout expires before approval, or the
request is rejected, the job exits with code 5 (task failed) and nothing is written.
"""

import os
import sys

# Airflow only puts the root of the dags folder on sys.path; when this file sits in a subfolder (e.g. .../dags/dags/), the folder of the
# file itself has to be added.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import silver_ingest_common as common  # noqa: E402
import silver_ingest_factory as factory  # noqa: E402

# Landing root shared by every VTNET dataset (ASSUMPTION, adjust to the cluster). INPUT_PATH = INPUT_ROOT + <dataset>/dt=<day>.
INPUT_ROOT = f"{common.HDFS}/prod8/landing/vtnet/"

# Wait for the DAK admin to approve the column key request (by-column answers PENDING): first poll after `initial` seconds, each next one
# multiplied by `multiplier`, at most `max_interval` seconds apart, give up after `timeout` seconds (job exits with 5). While waiting, the
# driver and the executors keep their resources.
PENDING_WAIT = {"timeout": 7200, "initial": 30, "max_interval": 600, "multiplier": 2}

# The datasets of VTNET, one DAG each. `mode`: a key of common.MODES. `key_path` (decrypt-encrypt only): the path sent to DAK by-path, one key
# for the whole dataset (defaults to the dataset name). ASSUMPTION: adjust to the real data.
DATASETS = {
    "ebs_3g_4g": {"mode": "passthrough"},
    "nims": {"mode": "decrypt-encrypt"},
}

# Settings shared by every dataset. ASSUMPTION: adjust to the real VTNET data (format, schema, target database).
BASE_ENV = {
    "INPUT_FORMAT": "csv",
    "INPUT_OPTION_header": "true",
    # The folder of a day holds batch=... subfolders: read them all, and do not turn `batch=1520` into a partition column.
    "INPUT_OPTION_recursiveFileLookup": "true",
    "OUTPUT_DB": "demo_db",
    "OUTPUT_FORMAT": "delta",
}


def dak_env(slug, config):
    """Driver env of the key source: DAK (by-path and by-column), INTERNAL client, only the driver needs it."""
    return [
        # Columns encrypted with cdr-crypto (one key per column, dataset = OUTPUT_DB.OUTPUT_TABLE) and the plaintext keyField column: from the
        # approved request. An empty list means that no column is encrypted.
        {"name": "CDR_ENCRYPT_COLUMNS", "value": ",".join(config.get("encryptColumns") or [])},
        {"name": "CDR_KEY_FIELD", "value": config.get("keyField") or ""},
        {"name": "CRYPTO_PREFIX_SOURCE", "value": "dak"},
        # Currently points to dak-mock (HTTP, same namespace). With the real DAK: https://<dak> and REMOVE DAK_ALLOW_INSECURE_HTTP.
        {"name": "DAK_ADDR", "value": "http://dak-mock:8085"},
        {"name": "DAK_ALLOW_INSECURE_HTTP", "value": "true"},
        {
            "name": "DAK_TOKEN_URL",
            "value": "https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/token",
        },
        # ASSUMPTION - the real internal client id (dak-mock: "kind: internal").
        {"name": "DAK_CLIENT_ID", "value": "<client-id-noi-bo>"},
        {"name": "DAK_CLIENT_SECRET", "valueFrom": {"secretKeyRef": {"name": "spark-dak-client", "key": "clientSecret"}}},
        # An internal client MUST send tag=internal.
        {"name": "DAK_TAG", "value": "internal"},
        {"name": "DAK_CALLER", "value": f"silver-ingest-vtnet-{slug}"},
        {"name": "DAK_PENDING_WAIT_TIMEOUT_SECONDS", "value": str(PENDING_WAIT["timeout"])},
        {"name": "DAK_PENDING_WAIT_INITIAL_SECONDS", "value": str(PENDING_WAIT["initial"])},
        {"name": "DAK_PENDING_WAIT_MAX_INTERVAL_SECONDS", "value": str(PENDING_WAIT["max_interval"])},
        {"name": "DAK_PENDING_WAIT_MULTIPLIER", "value": str(PENDING_WAIT["multiplier"])},
    ]


def source_for(dataset, settings, config=None):
    """The SOURCE constant (see common.check_conf and common.build_spec) of the DAG of one dataset.

    `config` = the approved settings read from the Airflow Variable (already validated), or None when the dataset has none: then `fixed` is
    empty and the DAG has no schedule.
    """
    mode = settings["mode"]
    slug = dataset.replace("_", "-")  # SparkApplication names and label values do not accept "_"
    config = config or {}
    source = {
        "name": f"vtnet-{slug}",
        "dataset": dataset,
        "day_dir": "dt",
        "main_class": common.MODES[mode],
        "input_root": INPUT_ROOT,
        "conf_keys": ("date",),
        "schedule_keys": ("ticket", "table", "cron", "outputMode"),
        "table_prefix": "vtnet_",
        "env": dict(BASE_ENV),
        "fixed": config,
    }
    if mode == "decrypt-encrypt":
        source["schedule_keys"] += ("encryptColumns", "keyField")
        source["encrypts_columns"] = True
        # The driver calls Keycloak (https) to get a DAK token: it needs hostAliases and a truststore with the internal CA (ConfigMap keycloak-ca).
        source["calls_keycloak"] = True
        source["env"].update(
            {
                # Whole-file decryption algorithm, only used when by-path has a key. ASSUMPTION until the real format is confirmed.
                "FILE_CIPHER": "aes-256-gcm",
                # One by-path key for the whole dataset, not one per day or batch folder.
                "KEY_PATH": settings.get("key_path", dataset),
                # onMissing of by-column: "request" - when a column has no key yet, DAK opens a request and the job waits (PENDING_WAIT) until the
                # DAK admin approves (docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md section 3.2).
                "CDR_ON_MISSING": "request",
            }
        )
        source["driver_extra_env"] = dak_env(slug, config)
    return source


# Airflow finds the DAGs among the module globals: one per dataset.
SOURCES, CONFIG_PROBLEMS = factory.register(globals(), "vtnet", DATASETS, source_for)
