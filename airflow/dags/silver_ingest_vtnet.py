"""RECURRING DAGs that load the files of the VTNET datasets into the demo database (docs/silver-ingest/PLAN.md). There is ONE DAG PER DATASET
(DATASETS below), built by silver_ingest_factory.py, and each runs once a day at the time of its approved request. A dataset picks an ingest
mode (common.MODES).

  dataset    DAG                            mode             folder of a day
  ebs_3g_4g  silver_ingest_vtnet_ebs_3g_4g  decrypt-encrypt  ebs_3g_4g/dt=YYYYMMDD/(batch=...) ON HKH: imsi decrypted (VTNET library), encrypted
                                                             with the global key (joins f022-register); read with the token of HKH in the
                                                             Secret hkh-hdfs-token, fetched by hand (common: INPUT ON ANOTHER CLUSTER)
  nims       silver_ingest_vtnet_nims       passthrough      nims/dt=YYYYMMDD/batch=N ON HKH: every batch folder into the partition dt
  isdn_imsi_mapping                         decrypt-encrypt  isdn_imsi_mapping/dt=YYYYMMDD/batch=NN ON HKH: isdn and imsi decrypted (VTNET
             silver_ingest_vtnet_isdn_imsi_mapping           library), encrypted with the global key (joins the isdn of VTT and the imsi of
                                                             f022-register and ebs_3g_4g); every batch folder into the partition dt

How a dataset gets its schedule (Airflow Variable written by DAK when the request is approved once): silver_ingest_factory.py.
A run ingests the folder of the day BEFORE the run (all its batch subfolders) into the partitions dt = <day>; a manual run can take
another day: conf {"date": "yyyyMMdd"}.

passthrough: read the files and write the table as they are (no DAK call).
A request that picks a global key zone adds globalKey=true and globalZone to the Variable (docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md): the
job then gets GLOBAL_KEY_ZONE and encrypts ALL `encryptColumns` with the ONE global key of that zone (DAK /api/v1/keys/global) instead of a
by-column key per column, so datasets of the same zone can be joined; there is no PENDING wait for a global key.

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
# Landing root of the datasets that are on another cluster (input_cluster): INPUT_PATH = <root> + <dataset>/dt=<day>.
CLUSTER_INPUT_ROOTS = {"hkh": f"{common.INPUT_CLUSTERS['hkh']['hdfs']}/landing/"}

# Wait for the DAK admin to approve the column key request: the same for every source, see common.PENDING_WAIT.
PENDING_WAIT = common.PENDING_WAIT

# The datasets of VTNET, one DAG each. Everything that is specific to a dataset is in ITS entry below, nowhere else: the keys (mode, hour_glob,
# partition_by_hour, key_path, resources) are described, and checked when this file is parsed, in common.DATASET_KEYS. ASSUMPTION: adjust to
# the real data. The landing of VTNET is on HKH (input_cluster), one folder per day dt=YYYYMMDD holding batch=... folders of CSV files
# with a header, next to a manifest.json and a _SUCCESS (BASE_ENV reads only *.csv):
#   ebs_3g_4g          batch=HHmm folders every 20 minutes (batch=0000, 0020, 0040, ...): the hour is the start of the folder name, so
#                      "batch=%H%*", and the table is partitioned by (dt, hour). About 2.2 to 2.5 TB of CSV a day.
#   nims               ONE batch=1 folder a day, about 32 MB: every batch folder goes into the partition dt (no hour).
#   isdn_imsi_mapping  ONE batch=01 folder a day, one CSV file of about 4.8 GB: every batch folder goes into the partition dt (no hour).
#
# RESOURCES, from the size of one day (CSV, 2026-11-05/10). The namespace has 200 CPU and 400 GB, shared with VTT and the de-identification.
#   ebs_3g_4g          narrow (7 columns) but huge (about 29 billion rows a day of ~85 bytes). The VTNET library is static and not thread-safe,
#                      so the job decrypts under ONE lock per executor JVM: many executors of 2 cores (the lock is the limit, not the cores),
#                      2 GB per core is plenty for 7 columns. No shuffle (hour from the path, IngestConfig.rebalances): one file per read
#                      task, so a task reads 384 MB of CSV (input_split_mb) for files of about 100 to 150 MB. Peak 2 + 30 x 2 = 62 cores,
#                      5 + 30 x 4 = 125 GB. Estimate: 30 to 60 minutes a day; measure one day (Spark UI: task time, GC) and adjust.
#   isdn_imsi_mapping  4.8 GB, two columns decrypted under the same lock: about 38 read tasks, up to 6 executors of 2 cores. Peak 13 cores, 26 GB.
#   nims               32 MB: one small static executor, like the d-* tables of VTT. 3 cores, 4 GB.
EBS = {"driver_cores": 2, "driver_memory": "4096m", "driver_memory_overhead": "1024m", "executor_cores": 2, "executor_memory": "3072m",
       "executor_memory_overhead": "1024m", "executors": 8, "min_executors": 4, "max_executors": 30, "shuffle_partitions": 512,
       "input_split_mb": 384}
MAPPING = {"executor_cores": 2, "executor_memory": "3072m", "executor_memory_overhead": "1024m", "executors": 2, "min_executors": 1,
           "max_executors": 6, "shuffle_partitions": 48}
TINY = {"executors": 1, "executor_cores": 2, "executor_memory": "2048m", "executor_memory_overhead": "512m", "driver_memory": "1024m",
        "shuffle_partitions": 8}
DATASETS = {
    # EBS-4G: the landing is on HKH (input_cluster), CSV with a header (imsi,lac,sac,eci,event_time,event_id,event_result). VTNET encrypts only imsi,
    # with its own library (PassTranformer; the inputKey is the by-path key of DAK for the path "ebs_3g_4g"); the job decrypts it and encrypts it
    # again with the global key and the SAME constant key field as VTT, so an imsi gives the same ciphertext here and in f022-register (join).
    "ebs_3g_4g": {
        "mode": "decrypt-encrypt", "input_cluster": "hkh", "hour_glob": "batch=%H%*", "partition_by_hour": True,
        # 2.2 to 2.5 TB a day: written hour by hour, one log line per hour (HOUR HH DONE ...) to follow the run.
        "write_by_hour": True,
        "decrypt_mode": "column", "provider_decrypt": {"columns": ["imsi"], "cipher": "vtnet"},
        "encrypt_columns": ["imsi"], "key_field_constant": common.JOIN_KEY_FIELD, "resources": EBS,
    },
    # NIMS-4G (cell inventory): no column is flagged in the request (BM01), so it is written as it is.
    "nims": {"mode": "passthrough", "input_cluster": "hkh", "resources": TINY},
    # The isdn <-> imsi mapping: both columns are flagged. ASSUMPTION: VTNET encrypts both, like imsi of ebs_3g_4g (by-path key of DAK for the
    # path "isdn_imsi_mapping"); a plain value stops the job ("not a VTNET ciphertext"): then drop it from provider_decrypt. Encrypted again with
    # the global key and the constant key field of VTT, so the mapping joins the isdn of VTT and the imsi of f022-register and ebs_3g_4g.
    "isdn_imsi_mapping": {
        "mode": "decrypt-encrypt", "input_cluster": "hkh", "decrypt_mode": "column",
        "provider_decrypt": {"columns": ["isdn", "imsi"], "cipher": "vtnet"},
        "encrypt_columns": ["isdn", "imsi"], "key_field_constant": common.JOIN_KEY_FIELD, "resources": MAPPING,
        # VTNET writes the isdn as 84 + 9 digits, VTT as the 9 digits only: normalized after the decryption, before the encryption, so one
        # number gives one ciphertext (and one pseudonym) in this table and in every VTT table. imsi is the same in both (15 digits, 45204...).
        "transforms": {"isdn": "ISDN_NATIONAL"},
    },
    # Two datasets that share sensitive columns (msisdn, imsi) to test the GLOBAL key (docs/silver-ingest/KICH-BAN-TEST-KEY-GLOBAL.md): whole
    # days only, plain CSV files, DAK has no by-path key for them (the files are read as they are) and the requests choose the same zone.
    "sub_profile": {"mode": "decrypt-encrypt"},
    "sub_usage": {"mode": "decrypt-encrypt"},
}

# Settings shared by every dataset. ASSUMPTION: adjust to the real VTNET data (format, schema, target database).
BASE_ENV = {
    "INPUT_FORMAT": "csv",
    "INPUT_OPTION_header": "true",
    # The folder of a day holds batch=... subfolders: read them all, and do not turn `batch=1520` into a partition column.
    "INPUT_OPTION_recursiveFileLookup": "true",
    # Only the CSV files: a batch folder of VTNET also holds a manifest.json (and a _SUCCESS, hidden anyway), which is not data.
    "INPUT_OPTION_pathGlobFilter": "*.csv",
    "OUTPUT_DB": "silver",
    "OUTPUT_FORMAT": "delta",
}


def global_key_env(config):
    """GLOBAL_KEY_ZONE of the job when the approved request uses a global key: every column of encryptColumns is then encrypted with the
    global key of that zone (DAK /api/v1/keys/global, same client as by-path) instead of one by-column key per column. Nothing for a request
    without a global key, so its job runs exactly as before."""
    zone = common.global_zone(config)
    return [{"name": "GLOBAL_KEY_ZONE", "value": zone}] if zone else []


def dak_env(slug, config):
    """Driver env of the key source: DAK (by-path, by-column and the global key), INTERNAL client, only the driver needs it."""
    return global_key_env(config) + [
        # Columns encrypted with cdr-crypto (one key per column, dataset = OUTPUT_DB.OUTPUT_TABLE) and the plaintext keyField column: from the
        # approved request. An empty list means that no column is encrypted.
        {
            "name": "CDR_ENCRYPT_COLUMNS",
            "value": ",".join(config.get("encryptColumns") or []),
        },
        {"name": "CDR_KEY_FIELD", "value": config.get("keyField") or ""},
    ] + common.dak_connection_env(f"silver-ingest-vtnet-{slug}")


def audit_conf(slug):
    """spark.cdrcrypto.audit.* of the driver (see common.audit_conf): the engine is the job of this dataset."""
    return common.audit_conf(f"silver-ingest-vtnet-{slug}")


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
        "input_root": CLUSTER_INPUT_ROOTS[settings["input_cluster"]] if settings.get("input_cluster") else INPUT_ROOT,
        "conf_keys": common.CONF_KEYS,
        **common.dataset_fields(settings),  # hour_glob, partition_by_hour, resources
        "schedule_keys": ("ticket", "table", "cron", "outputMode"),
        "table_prefix": "vtnet_",
        "env": dict(BASE_ENV),
        "fixed": config,
    }
    if mode == "decrypt-encrypt" and settings.get("decrypt_mode") == "column":
        # The columns are fixed by the dataset and encrypted with the global key of the zone that the request picks (like VTT): DAK requires
        # encryptColumns and keyField with a global key, they are accepted when they name exactly these columns, never taken from the request.
        source["schedule_keys"] += ("encryptColumns", "keyField", "globalKey", "globalZone")
        source["requires_global_key"] = True
        source["fixed_columns"] = tuple(settings["encrypt_columns"])
        source["calls_keycloak"] = True
        source["env"].update(common.dataset_job_env(dataset, settings))
        source["driver_extra_env"] = global_key_env(config) + common.dak_connection_env(f"silver-ingest-vtnet-{slug}")
        source["extra_spark_conf"] = audit_conf(slug)
    elif mode == "decrypt-encrypt":
        # globalKey/globalZone: only present when the request uses a global key (docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md).
        source["schedule_keys"] += ("encryptColumns", "keyField", "globalKey", "globalZone")
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
        source["extra_spark_conf"] = audit_conf(slug)
    return source


# Airflow finds the DAGs among the module globals: one per dataset.
SOURCES, CONFIG_PROBLEMS = factory.register(globals(), "vtnet", DATASETS, source_for)
