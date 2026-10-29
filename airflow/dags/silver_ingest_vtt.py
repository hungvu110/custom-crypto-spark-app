"""RECURRING DAGs that load the files of the VTT datasets into the demo database (docs/silver-ingest/PLAN.md). There is ONE DAG PER DATASET
(DATASETS below), built by silver_ingest_factory.py, and each runs once a day at the time of its approved request. VTT files are already
encrypted column by column: the job only reads them and writes the table (passthrough mode), it never calls DAK.

  dataset         DAG                               mode         folder of a day
  f001-mpre-call  silver_ingest_vtt_f001_mpre_call  passthrough  f001-mpre-call/date=YYYYMMDD/(hour=...)

How a dataset gets its schedule (Airflow Variable written by DAK when the request is approved once): silver_ingest_factory.py.
A run ingests the folder of the day BEFORE the run (all its hour subfolders) into the partitions dt = <day>; a manual run can take
another day: conf {"date": "yyyyMMdd"}.
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

# The datasets (tables of the landing root) of VTT, one DAG each. Everything that is specific to a dataset is in ITS entry below, nowhere else: the
# keys (mode, hour_glob, partition_by_hour, key_path, resources) are described, and checked when this file is parsed, in common.DATASET_KEYS.
# ASSUMPTION: add the real tables. VTT writes one folder per hour, date=YYYYMMDD/hour=HH, so `hour_glob` is "hour=%H%" (`%H%` = the hour).
# A VTT day is about 165 GB: set `resources` for it (common.RESOURCE_DEFAULTS are small) once its run time has been measured.
DATASETS = {
    "f001-mpre-call": {"mode": "passthrough", "hour_glob": "hour=%H%", "partition_by_hour": True},
}

# Settings shared by every dataset. ASSUMPTION: adjust to the real VTT data (format, schema, target database).
BASE_ENV = {
    "INPUT_FORMAT": "csv",
    "INPUT_OPTION_header": "true",
    # The folder of a day holds hour=... subfolders: read them all, and do not turn `hour=12` into a partition column.
    "INPUT_OPTION_recursiveFileLookup": "true",
    "OUTPUT_DB": "demo_db",
    "OUTPUT_FORMAT": "delta",
}


def source_for(dataset, settings, config=None):
    """The SOURCE constant (see common.check_conf and common.build_spec) of the DAG of one dataset.

    `config` = the approved settings read from the Airflow Variable (already validated), or None when the dataset has none: then `fixed` is
    empty and the DAG has no schedule.
    """
    slug = dataset.replace("_", "-")  # SparkApplication names and label values do not accept "_"
    return {
        "name": f"vtt-{slug}",
        "dataset": dataset,
        "day_dir": "date",
        "main_class": common.MODES[settings["mode"]],
        "input_root": INPUT_ROOT,
        "conf_keys": common.CONF_KEYS,
        **common.dataset_fields(settings),  # hour_glob, partition_by_hour, resources
        "schedule_keys": ("ticket", "table", "cron", "outputMode"),
        "table_prefix": "vtt_",
        "env": dict(BASE_ENV),
        "fixed": config or {},
    }


# Airflow finds the DAGs among the module globals: one per dataset.
SOURCES, CONFIG_PROBLEMS = factory.register(globals(), "vtt", DATASETS, source_for)
