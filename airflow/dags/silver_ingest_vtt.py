"""RECURRING DAGs that load the files of the VTT tables into the demo database (docs/silver-ingest/PLAN.md and
docs/silver-ingest/TRANSFORM-PHIEU-KHAI-THAC-VTT.md). There is ONE DAG PER TABLE (DATASETS below), built by silver_ingest_factory.py, and each
runs once a day at the time of its approved request.

VTT writes parquet files and encrypts SOME COLUMNS itself (cdr-crypto: `keyInput = prefix || value of the key field of the row`). What a DAG does:

  decrypt_mode column  the columns that VTT encrypted are decrypted with the by-path prefix of DAK (the path is the name of the table) and
                       the key field of VTT, then grouped (age, hour), then encrypted AGAIN with ONE global key and a constant key field.
  decrypt_mode none    the table has nothing that VTT encrypted (f001 calling_number, f-customer cust_id): the columns are only encrypted
                       with the same global key and constant key field, so one number gives the same ciphertext in every table.
  passthrough          the reference tables d-*: nothing to protect, written as they are.

The step after the silver table (pseudonymize-table-app) hashes those ciphertexts, so the tables of one request can be joined.

  dataset (folder of the table)  columns VTT encrypted -> key field of VTT        encrypted with the global key          grouped
  f001-mpre-call                 (none)                                           calling_number                         hour_id
  f002-mpre-sms                  calling_number, called_number, called_number_cl -> sta_datetime     the same three columns                hour_id
  f003-mpre-data                 calling_number -> sta_datetime                   calling_number                         hour_id
  f014-mpre-tot-charge-d         isdn -> sta_date                                 isdn, customer_id                      customer_age
  f015-mpre-tot-charge-d-acc     isdn -> customer_id                              isdn, customer_id                      customer_age
  f016-mpre-tot-charge-m         isdn -> customer_id                              isdn, customer_id                      customer_age
  f022-register                  isdn -> sta_date                                 isdn                                   hour_id
  f-home-bts-daily-area          isdn -> station_code                             isdn                                   (none)
  f-customer                     (none)                                           cust_id                                customer_age
  d-*  (9 tables)                (none)                                           (none)                                 (none)

How a dataset gets its schedule (Airflow Variable written by DAK when the request is approved once): silver_ingest_factory.py.
A run ingests the folder of the day BEFORE the run (all its files) into the partitions dt = <day>, hour = <hour of each row, from the
column hour_id>; a manual run can take another day: conf {"date": "yyyyMMdd"}. The folder has no hour layout, so only whole days run
(a daily cron; no hourly cron, no fromHour/toHour). The tables that encrypt need `globalKey` and `globalZone` in their approved request: the
columns are fixed by the dataset below, the request only picks the zone of the global key.
"""

import os
import sys

# Airflow only puts the root of the dags folder on sys.path; when this file sits in a subfolder (e.g. .../dags/dags/), the folder of the
# file itself has to be added.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import silver_ingest_common as common  # noqa: E402
import silver_ingest_factory as factory  # noqa: E402

# Landing root of VTT (ASSUMPTION, adjust to the cluster). INPUT_PATH = INPUT_ROOT + <dataset>/date=<day>.
INPUT_ROOT = f"{common.HDFS}/landing/vtt/lakehouse/vtt-core/"

# The constant column that the job adds and uses as the key field of the encryption: the same value in every row of every table, so the same
# number gives the same ciphertext everywhere (the key field of VTT changes per row and per table, so it cannot be used for this).
KEY_SCOPE = {"name": "key_scope", "value": "vtt"}
HOUR_BAND = {"hour_id": "HOUR_BAND"}
AGE_BAND = {"customer_age": "AGE_BAND"}
# VTT writes ONE folder per day, date=YYYYMMDD, and every row carries its hour in the column `hour_id` (not encrypted), so the hour comes from
# `hour_column` and not from a path (`hour_glob`): the silver table gets the partitions (dt, hour).
HOUR_FROM_COLUMN = {"hour_column": "hour_id", "partition_by_hour": True}
# The FIRST LOAD is asked for, not automatic: trigger the DAG of a table with conf {"fromDate": "20261104", "toDate": "20261122"} (a BULK LOAD, one run
# that loads every day of the range, replacing the days that are already there) and from then on the DAG runs daily. docs/silver-ingest/BACKFILL.md.

# Spark RESOURCES, chosen from the size of ONE day on HDFS (parquet, 2026-11-10) and what the job does. The job is limited by reading, decompressing
# and writing parquet, NOT by the encryption (cdr-crypto measured at 0.74 microsecond per value, ~1.3 million values per second per core), so the
# cores follow the number of tasks (size / 128 MB) and the memory follows the width of the table (a task of a wide table keeps more column
# buffers). 4 GB of heap per core for the wide tables (150 to 450 columns), 3 GB for the others; a tiny table needs one small executor. Every data
# table scales with dynamic allocation (common.DYNAMIC_KEYS): it grows while tasks wait and gives the executors back when idle, also between the
# days of a bulk load; only the reference tables (d-*) keep one static executor. The numbers are an estimate, not a
# measurement of this cluster: run one day, look at the Spark UI (time per task, spill, GC) and adjust. Limits: the ResourceQuota of the namespace
# (unknown here): lower max_executors if it is smaller, the run is then only slower. Sizes per day: f003 33.1 G, f016 28.5 G (per batch), f015 23.7 G,
# f014 17.7 G, f-customer 12.1 G, f022 5.0 G, f001 3.7 G, f-home-bts 2.8 G, f002 1.4 G; the d-* tables 13 K to 482 M in all.
WIDE = {"executor_cores": 4, "executor_memory": "16384m", "executor_memory_overhead": "3072m", "driver_memory": "6144m",
        "driver_memory_overhead": "1536m"}
NARROW = {"executor_cores": 4, "executor_memory": "12288m", "executor_memory_overhead": "2048m", "driver_memory": "4096m",
          "driver_memory_overhead": "1024m"}
TINY = {"executors": 1, "executor_cores": 2, "executor_memory": "2048m", "executor_memory_overhead": "512m", "driver_memory": "1024m",
        "shuffle_partitions": 8}

# The tables of VTT, one DAG each. Everything that is specific to a table is in ITS entry below, nowhere else: the keys (mode, decrypt_mode,
# provider_decrypt, encrypt_columns, key_field_constant, transforms, hour_column, partition_by_hour, key_path, resources) are described, and checked
# when this file is parsed, in common.DATASET_KEYS. The name of an entry is the name of the landing folder of the table AND the path of its
# by-path key on DAK (decrypt_mode column): write it exactly like that on DAK, case and hyphens included.
# ASSUMPTION: only f001-mpre-call and f014-mpre-tot-charge-d are confirmed folder names; check the others with `hdfs dfs -ls <INPUT_ROOT>`.
# The `resources` of each table are explained above (WIDE, NARROW, TINY); the defaults of common.RESOURCE_DEFAULTS are for a small dataset.
DATASETS = {
    "f001-mpre-call": {
        "resources": {**NARROW, "executors": 2, "min_executors": 1, "max_executors": 4, "shuffle_partitions": 32},
        "mode": "decrypt-encrypt", "decrypt_mode": "none",
        "encrypt_columns": ["calling_number"], "key_field_constant": KEY_SCOPE, "transforms": HOUR_BAND, **HOUR_FROM_COLUMN,
    },
    "f002-mpre-sms": {
        "resources": {**NARROW, "executors": 1, "min_executors": 1, "max_executors": 3, "shuffle_partitions": 16},
        "mode": "decrypt-encrypt", "decrypt_mode": "column",
        "provider_decrypt": {"columns": ["calling_number", "called_number", "called_number_cl"], "key_field": "sta_datetime"},
        "encrypt_columns": ["calling_number", "called_number", "called_number_cl"], "key_field_constant": KEY_SCOPE,
        "transforms": HOUR_BAND, **HOUR_FROM_COLUMN,
    },
    "f003-mpre-data": {
        "resources": {**WIDE, "executors": 4, "min_executors": 2, "max_executors": 8, "shuffle_partitions": 256},
        "mode": "decrypt-encrypt", "decrypt_mode": "column",
        "provider_decrypt": {"columns": ["calling_number"], "key_field": "sta_datetime"},
        "encrypt_columns": ["calling_number"], "key_field_constant": KEY_SCOPE, "transforms": HOUR_BAND, **HOUR_FROM_COLUMN,
    },
    "f014-mpre-tot-charge-d": {
        "resources": {**WIDE, "executors": 3, "min_executors": 2, "max_executors": 6, "shuffle_partitions": 128},
        "mode": "decrypt-encrypt", "decrypt_mode": "column",
        "provider_decrypt": {"columns": ["isdn"], "key_field": "sta_date"},
        "encrypt_columns": ["isdn", "customer_id"], "key_field_constant": KEY_SCOPE, "transforms": AGE_BAND,
    },
    "f015-mpre-tot-charge-d-acc": {
        "resources": {**WIDE, "executors": 3, "min_executors": 2, "max_executors": 6, "shuffle_partitions": 192},
        "mode": "decrypt-encrypt", "decrypt_mode": "column",
        # customer_id is the key field of VTT AND a column that is encrypted: decryption runs first, so it is still plaintext when it is needed.
        "provider_decrypt": {"columns": ["isdn"], "key_field": "customer_id"},
        "encrypt_columns": ["isdn", "customer_id"], "key_field_constant": KEY_SCOPE, "transforms": AGE_BAND,
    },
    # f016 is a MONTHLY table: it has no daily schedule; it only runs by hand (conf {"date": ...}, a bulk load or silver_ingest_backfill.py), once its
    # folder layout is known.
    "f016-mpre-tot-charge-m": {
        "resources": {**WIDE, "executors": 3, "min_executors": 2, "max_executors": 6, "shuffle_partitions": 224},
        "mode": "decrypt-encrypt", "periodic": False, "decrypt_mode": "column",
        "provider_decrypt": {"columns": ["isdn"], "key_field": "customer_id"},
        "encrypt_columns": ["isdn", "customer_id"], "key_field_constant": KEY_SCOPE, "transforms": AGE_BAND,
    },
    "f022-register": {
        "resources": {**NARROW, "executors": 2, "min_executors": 1, "max_executors": 4, "shuffle_partitions": 48},
        "mode": "decrypt-encrypt", "decrypt_mode": "column",
        "provider_decrypt": {"columns": ["isdn"], "key_field": "sta_date"},
        "encrypt_columns": ["isdn"], "key_field_constant": KEY_SCOPE, "transforms": HOUR_BAND, **HOUR_FROM_COLUMN,
    },
    "f-home-bts-daily-area": {
        "resources": {**NARROW, "executors": 2, "min_executors": 1, "max_executors": 3, "shuffle_partitions": 24},
        "mode": "decrypt-encrypt", "decrypt_mode": "column",
        "provider_decrypt": {"columns": ["isdn"], "key_field": "station_code"},
        "encrypt_columns": ["isdn"], "key_field_constant": KEY_SCOPE,
    },
    "f-customer": {
        "resources": {**NARROW, "executors": 2, "min_executors": 1, "max_executors": 4, "shuffle_partitions": 96},
        "mode": "decrypt-encrypt", "decrypt_mode": "none",
        "encrypt_columns": ["cust_id"], "key_field_constant": KEY_SCOPE, "transforms": AGE_BAND,
    },
    # Reference tables: nothing to protect, written as they are.
    "d-cust-type": {"mode": "passthrough", "resources": TINY},
    "d-khdn": {"mode": "passthrough", "resources": TINY},
    "d-mpre-data-package": {"mode": "passthrough", "resources": TINY},
    "d-addon-data-package": {"mode": "passthrough", "resources": TINY},
    "d-device-ttvas": {"mode": "passthrough", "resources": TINY},
    "d-os-type": {"mode": "passthrough", "resources": TINY},
    "d-sub-type-gb": {"mode": "passthrough", "resources": TINY},
    "d-telecom-service": {"mode": "passthrough", "resources": TINY},
    "d-vas-direction": {"mode": "passthrough", "resources": TINY},
}

# Settings shared by every table. The landing files are parquet: they are read as they are (the columns that VTT encrypted are decrypted by the
# job afterwards), so no schema and no header. ASSUMPTION: adjust the target database.
BASE_ENV = {
    "INPUT_FORMAT": "parquet",
    # Read the whole folder of a day, including any subfolder, and do not turn a folder name such as `hour=12` into a partition column.
    "INPUT_OPTION_recursiveFileLookup": "true",
    "OUTPUT_DB": "silver",
    "OUTPUT_FORMAT": "delta",
}


def global_key_env(config):
    """GLOBAL_KEY_ZONE of the job: the zone of the global key that the approved request picked (every table of VTT uses the same zone, so that
    one number gives one ciphertext in every table). Nothing for a table that does not encrypt."""
    zone = common.global_zone(config)
    return [{"name": "GLOBAL_KEY_ZONE", "value": zone}] if zone else []


def source_for(dataset, settings, config=None):
    """The SOURCE constant (see common.check_conf and common.build_spec) of the DAG of one dataset.

    `config` = the approved settings read from the Airflow Variable (already validated), or None when the dataset has none: then `fixed` is
    empty and the DAG has no schedule.
    """
    mode = settings["mode"]
    slug = dataset.replace("_", "-")  # SparkApplication names and label values do not accept "_"
    config = config or {}
    source = {
        "name": f"vtt-{slug}",
        "dataset": dataset,
        "day_dir": "date",
        "main_class": common.MODES[mode],
        "input_root": INPUT_ROOT,
        "conf_keys": common.CONF_KEYS,
        **common.dataset_fields(settings),  # hour_glob, hour_column, partition_by_hour, resources
        "schedule_keys": ("ticket", "table", "cron", "outputMode"),
        "table_prefix": "vtt_",
        "env": dict(BASE_ENV),
        "fixed": config,
    }
    if mode == "decrypt-encrypt":
        if settings.get("encrypt_columns"):
            # The columns are fixed by the dataset; the request says which global key zone encrypts them. DAK requires encryptColumns and
            # keyField with a global key: they are accepted when they name exactly these columns, but the job never takes them from the request.
            source["schedule_keys"] += ("encryptColumns", "keyField", "globalKey", "globalZone")
            source["requires_global_key"] = True
            source["fixed_columns"] = tuple(settings["encrypt_columns"])
        # The driver calls Keycloak (https) to get a DAK token: it needs hostAliases and a truststore with the internal CA (ConfigMap keycloak-ca).
        source["calls_keycloak"] = True
        source["env"].update(common.dataset_job_env(dataset, settings))
        source["driver_extra_env"] = global_key_env(config) + common.dak_connection_env(f"silver-ingest-vtt-{slug}")
        source["extra_spark_conf"] = common.audit_conf(f"silver-ingest-vtt-{slug}")
    return source


# Airflow finds the DAGs among the module globals: one per dataset.
SOURCES, CONFIG_PROBLEMS = factory.register(globals(), "vtt", DATASETS, source_for)
