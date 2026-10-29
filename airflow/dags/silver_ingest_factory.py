"""DAG factory shared by silver_ingest_vtt.py and silver_ingest_vtnet.py: ONE RECURRING DAG PER DATASET, scheduled by the approved request.

HOW A DATASET GETS ITS SCHEDULE. A request is approved ONCE, for the whole dataset (its `path` is the dataset, not a day). On approval DAK
(1) writes the approved settings into the Airflow Variable named like the DAG, as JSON, through the Airflow REST API, and (2) switches the DAG
on. The DAG file calls `register` below: every time Airflow parses it, each dataset's Variable is read and the DAG is built with the cron
of the request as its `schedule` and the table, columns and output mode of the request as fixed settings. No Variable (or one that is not valid)
means a DAG without schedule that never runs. Stopping the request deletes the Variable and switches the DAG off. A change takes effect when
Airflow parses the DAG file again (about every 30 seconds).

  Variable `silver_ingest_vtnet_nims` = {"ticket": "SLV-2026-000001", "table": "vtnet_nims_infra_cell_4g", "cron": "0 3 * * *",
                                         "outputMode": "overwrite-partitions" (optional),
                                         "encryptColumns": ["msisdn"] (decrypt-encrypt only, may be empty), "keyField": "start_time"}

The DAG id and the Variable name are the same string: silver_ingest_<prefix>_<dataset with "-" turned into "_">.
What a run does is described in silver_ingest_common.py (run_date, input_path, batch_hours) and in the DAG files (modes, datasets).
"""

import json
import os
import sys

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# Airflow also parses this file on its own, and only puts the root of the dags folder on sys.path: when the file sits in a subfolder
# (e.g. .../dags/dags/), the folder of the file itself has to be added or the import below fails.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import silver_ingest_common as common  # noqa: E402


def dag_id_of(prefix, dataset):
    """DAG id, and name of the Airflow Variable, of a dataset: `silver_ingest_vtnet_nims`, `silver_ingest_vtt_f001_mpre_call`."""

    return f"silver_ingest_{prefix}_{dataset.replace('-', '_')}"


def read_config(dag_id, source):
    """(config, problem): the approved settings that DAK wrote into the Airflow Variable `dag_id`, or why they are not used.

    No Variable is not a problem: the dataset simply has no approved request. A Variable that is not valid is ignored (the DAG gets no
    schedule) and the reason is shown in the DAG description and fails its validate task. `source` is the SOURCE of the dataset without
    settings: it tells which keys and values are acceptable.
    """

    raw = Variable.get(dag_id, default_var=None)
    if raw is None:
        return None, None
    try:
        config = json.loads(raw)
    except ValueError:
        return None, "the Airflow Variable is not valid JSON"
    try:
        common.check_schedule_config(source, config)
    except ValueError as error:
        return None, str(error)
    return config, None


def make_validator(source, problem):
    def validate_conf(**context):
        """First task: stop the DAG before any job is created when there is no usable approved request or the conf is invalid."""

        if problem:
            raise AirflowFailException(f"the settings of this dataset in the Airflow Variable are not used: {problem}")
        if not source["fixed"]:
            raise AirflowFailException("no approved request is scheduled for this dataset")
        try:
            common.check_conf(source, context["dag_run"].conf)
        except ValueError as error:
            raise AirflowFailException(str(error)) from None

    return validate_conf


def build_dag(dag_id, source, prefix, mode, problem):
    config = source["fixed"]
    if config:
        doc = f"Approved request {config['ticket']}: table {config['table']}, cron `{config['cron']}` ({common.RUN_TIMEZONE}), each run ingests the {common.schedule_unit(config['cron'])} before."
    elif problem:
        doc = f"The settings in the Airflow Variable are not used ({problem}); this DAG has no schedule."
    else:
        doc = "No approved request for this dataset: this DAG has no schedule."
    with DAG(
        dag_id,
        description=f"Daily load of the files of one {prefix.upper()} dataset ({mode}) into the demo database",
        doc_md=doc,
        schedule=config["cron"] if config else None,  # the cron of the approved request
        start_date=pendulum.datetime(2026, 1, 1, tz=common.RUN_TIMEZONE),
        catchup=False,  # no backfill when a request is approved; silver_ingest_backfill.py runs a range of days or hours
        # MUST be False: native rendering turns "2" into 2 and Kubernetes answers 422 (an env value must be a string).
        render_template_as_native_obj=False,
        user_defined_macros={
            "k8s_name": common.k8s_name,
            "input_path": common.input_path,
            "run_date": common.run_date,
            "batch_hours": common.batch_hours,
        },
        max_active_runs=1,  # a day is ingested once at a time
        is_paused_upon_creation=False,
        tags=["silver", prefix, "dak", "daily", mode],
    ) as dag:
        validate = PythonOperator(task_id="validate_conf", python_callable=make_validator(source, problem))
        run_job = SparkKubernetesOperator(
            task_id="ingest",
            namespace=common.NAMESPACE,
            template_spec=common.build_spec(source),
            kubernetes_conn_id="kubernetes_default",
            do_xcom_push=False,
        )
        validate >> run_job
    return dag


def register(namespace, prefix, datasets, source_for):
    """Build the DAG of every dataset and put it among `namespace` (the globals() of the DAG file), where Airflow finds it.

    `datasets` = {dataset: {"mode": ...}}; `source_for(dataset, settings, config=None)` = the SOURCE constant of the dataset (see
    common.build_spec). Returns ({dag id: source}, {dag id: why the Variable is not used, or None}).
    """

    sources = {}
    problems = {}
    for dataset, settings in datasets.items():
        common.check_dataset(dataset, settings)  # a typo in DATASETS is an import error, not a silent default
        dag_id = dag_id_of(prefix, dataset)
        config, problem = read_config(dag_id, source_for(dataset, settings))
        sources[dag_id] = source_for(dataset, settings, config)
        problems[dag_id] = problem
        namespace[dag_id] = build_dag(dag_id, sources[dag_id], prefix, settings["mode"], problem)
    return sources, problems
