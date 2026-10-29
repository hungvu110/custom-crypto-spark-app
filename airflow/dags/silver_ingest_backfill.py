"""Backfill of the silver ingest DAGs: run the DAG of ONE dataset again for a RANGE OF DAYS, or for the same range of HOURS on each day.

Trigger this DAG by hand (Trigger DAG w/ config) with a conf like:

  {"dag_id": "silver_ingest_vtnet_ebs_3g_4g", "fromDate": "20261010", "toDate": "20261012"}                       the whole days 10..12
  {"dag_id": "silver_ingest_vtnet_ebs_3g_4g", "fromDate": "20261010", "fromHour": "10", "toHour": "12"}         hours 10..12 of the 10th
  {"dag_id": "silver_ingest_vtt_f001_mpre_call", "fromDate": "20261010", "toDate": "20261012", "fromHour": "01", "toHour": "03"}

toDate defaults to fromDate; fromHour/toHour are optional (both included; toHour defaults to fromHour; hours need a dataset with an
`hour_glob` in its DAG file). The task starts ONE dag run of the dataset DAG per day, each with the conf `{"date", "fromHour", "toHour"}` that
a manual run of that DAG takes, so every run is an ordinary run of the dataset (same table, columns, keys, output mode). The dataset DAG runs
one at a time (max_active_runs=1): the runs wait in the queue and go one after another. Whatever the run, its rows carry the columns `dt`
(the day) and `hour`, and a run replaces exactly the (dt, hour) rows it ingests (silver_ingest_common.batch_hours), so day runs, hour runs and
backfills agree and never write twice.

The dataset DAG must exist and not be paused (it is switched on when its request is approved).
"""

import os
import sys

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

from airflow import DAG

# Airflow only puts the root of the dags folder on sys.path; when this file sits in a subfolder (e.g. .../dags/dags/), the folder of the
# file itself has to be added.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import silver_ingest_common as common  # noqa: E402


def trigger_runs(**context):
    """Validate the backfill conf, then start one dag run of the dataset DAG per day (in date order)."""

    # Imported here: they are only needed when the task runs, not when Airflow parses this file.
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.models import DagModel

    run = context["dag_run"]
    try:
        dag_id, confs = common.plan_backfill(run.conf)
    except ValueError as error:
        raise AirflowFailException(str(error)) from None
    model = DagModel.get_dagmodel(dag_id)
    if model is None:
        raise AirflowFailException(f"the DAG {dag_id} does not exist (is the dataset approved and the DAG file deployed?)")
    if model.is_paused:
        raise AirflowFailException(f"the DAG {dag_id} is paused: its runs would never start")

    started = []
    for conf in confs:
        run_id = common.backfill_run_id(run.run_id, conf)
        try:
            # replace_microseconds=False: the runs are started within the same second, and their logical dates must differ.
            trigger_dag(dag_id=dag_id, run_id=run_id, conf=conf, replace_microseconds=False)
        except Exception as error:
            raise AirflowFailException(
                f"started {len(started)} of {len(confs)} runs ({', '.join(started) or 'none'}), then {type(error).__name__} on {run_id}"
            ) from error
        started.append(run_id)
    print(f"started {len(started)} runs of {dag_id}: {', '.join(started)}")


with DAG(
    "silver_ingest_backfill",
    description="Run a silver ingest DAG again for a range of days or hours (trigger by hand with a conf)",
    doc_md=__doc__,
    schedule=None,  # only by hand
    start_date=pendulum.datetime(2026, 1, 1, tz=common.RUN_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["silver", "backfill"],
) as dag:
    PythonOperator(task_id="trigger_runs", python_callable=trigger_runs)
