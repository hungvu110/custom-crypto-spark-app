"""Test cho dags/deid_maintenance.py (DAG xoá vùng `deid_purge_range` và DAG `deid_vacuum`), không cần cài Airflow.

Module `airflow` được giả lập như trong test_deid_dag.py; DAG khử định danh được nạp trước dưới đúng tên module mà deid_maintenance import.
"""
import importlib.util
import pathlib
import sys
import types
import unittest

import jinja2

DAGS = pathlib.Path(__file__).resolve().parents[1] / "dags"
STUBBED = ["airflow", "airflow.exceptions", "airflow.operators", "airflow.operators.python", "airflow.providers",
           "airflow.providers.cncf", "airflow.providers.cncf.kubernetes", "airflow.providers.cncf.kubernetes.operators",
           "airflow.providers.cncf.kubernetes.operators.spark_kubernetes", "pendulum"]
BASE = "sample_dag_deid_pseudonymize_table"


class _Recorder:
    instances = []

    def __init__(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs
        _Recorder.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __rshift__(self, other):
        return other


class _Fail(Exception):
    pass


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, DAGS / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load():
    for name in STUBBED:
        sys.modules[name] = types.ModuleType(name)
    sys.modules["airflow"].DAG = _Recorder
    sys.modules["airflow.exceptions"].AirflowFailException = _Fail
    sys.modules["airflow.operators.python"].PythonOperator = _Recorder
    sys.modules["airflow.providers.cncf.kubernetes.operators.spark_kubernetes"].SparkKubernetesOperator = _Recorder
    sys.modules["pendulum"].datetime = lambda *args, **kwargs: None
    try:
        _load(BASE, BASE + ".py")
        _Recorder.instances = []
        module = _load("deid_maintenance_under_test", "deid_maintenance.py")
        module.recorded = list(_Recorder.instances)
        return module
    finally:
        for name in STUBBED + [BASE]:
            sys.modules.pop(name, None)


m = load()
RUN_ID = "purge-2026-000007"
MANUAL = "manual__2026-10-30T03:00:00+00:00"


def conf(**env):
    final = {"OUTPUT_DB": "demo_db", "OUTPUT_TABLE": "ebs_teama", "INPUT_FROM": "20261012", "INPUT_TO": "20261014"}
    final.update(env)
    return {"requestCode": "PYC-2026-000007", "env": final}


class _DagRun:
    def __init__(self, run_id, conf):
        self.run_id, self.conf = run_id, conf


class _Var:
    def __init__(self, values):
        self.value = types.SimpleNamespace(get=lambda key, default=None: values.get(key, default))


def render(spec, run_id=RUN_ID, run_conf=None, variables=None):
    environment = jinja2.Environment()
    context = {"dag_run": _DagRun(run_id, run_conf if run_conf is not None else conf()), "ts_nodash": "20261030T030000",
               "var": _Var(variables or {})}

    def walk(node):
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(v) for v in node]
        if isinstance(node, str):
            return environment.from_string(node).render(**context)
        return node

    return walk(spec)


def env_of(spec):
    return {e["name"]: e["value"] for e in spec["spec"]["driver"]["env"]}


class PurgeConfTest(unittest.TestCase):
    def rejected(self, run_id=RUN_ID, run_conf=None):
        with self.assertRaises(ValueError):
            m.check_purge_conf(run_id, run_conf if run_conf is not None else conf())

    def test_a_complete_conf_is_accepted_with_or_without_hours_and_with_a_rerun_suffix(self):
        m.check_purge_conf(RUN_ID, conf())
        m.check_purge_conf(RUN_ID, conf(INPUT_HOURS="6-18"))
        m.check_purge_conf("purge-2026-000007-r2", conf(INPUT_FROM="2026101206", INPUT_TO="2026101406"))
        m.check_purge_conf(MANUAL, conf())

    def test_an_open_range_is_rejected_because_it_would_delete_the_whole_table(self):
        for missing in ("INPUT_FROM", "INPUT_TO"):
            c = conf()
            del c["env"][missing]
            self.rejected(run_conf=c)

    def test_the_output_target_is_always_required_there_is_no_default(self):
        for missing in ("OUTPUT_DB", "OUTPUT_TABLE"):
            c = conf()
            del c["env"][missing]
            self.rejected(run_conf=c)
        self.rejected(MANUAL, {"requestCode": "PYC-2026-000007", "env": {"INPUT_FROM": "20261012", "INPUT_TO": "20261012"}})
        self.rejected(MANUAL, {})

    def test_bad_values_are_rejected(self):
        self.rejected(run_conf=conf(OUTPUT_TABLE="a b;c"))
        self.rejected(run_conf=conf(INPUT_FROM="20261340"))
        self.rejected(run_conf=conf(INPUT_FROM="20261015", INPUT_TO="20261012"))
        self.rejected(run_conf=conf(INPUT_HOURS="18-6"))
        self.rejected(run_conf=conf(OUTPUT_MODE="overwrite"))  # an env outside the purge list
        self.rejected("purge-2026-000007-x", conf())
        self.rejected("purge-2026-7", conf())
        c = conf()
        c["requestCode"] = "PYC-1"
        self.rejected(run_conf=c)
        c = conf()
        c["pseudoColumns"] = "name:HASH"
        self.rejected(run_conf=c)


class SpecTest(unittest.TestCase):
    def purge_spec(self, run_id=RUN_ID, **env):
        return render(m.build_purge_spec(), run_id, conf(**env))

    def test_the_purge_job_gets_the_range_the_target_and_the_silver_partition_keys(self):
        spec = self.purge_spec(INPUT_HOURS="6-18")
        env = env_of(spec)

        self.assertEqual(spec["metadata"]["name"], RUN_ID)
        self.assertEqual(spec["spec"]["mainClass"], "vai.lakehouse.pseudonymizetable.app.PurgeRangeApp")
        self.assertEqual((env["OUTPUT_DB"], env["OUTPUT_TABLE"]), ("demo_db", "ebs_teama"))
        self.assertEqual((env["INPUT_FROM"], env["INPUT_TO"], env["INPUT_HOURS"]), ("20261012", "20261014", "6-18"))
        self.assertEqual((env["INPUT_DATE_KEY"], env["INPUT_HOUR_KEY"]), ("dt", "hour"))
        self.assertEqual(spec["spec"]["driver"]["labels"]["dak.vai/request-code"], "PYC-2026-000007")

    def test_an_absent_hours_is_an_empty_value_and_a_manual_run_gets_a_valid_name(self):
        spec = render(m.build_purge_spec(), MANUAL, conf())

        self.assertEqual(env_of(spec)["INPUT_HOURS"], "")
        self.assertEqual(spec["metadata"]["name"], "purge-manual-20261030t030000")

    def test_the_purge_does_not_mount_a_key_it_never_needs(self):
        spec = self.purge_spec()["spec"]

        self.assertNotIn("volumes", spec)
        self.assertNotIn("volumeMounts", spec["driver"])

    def test_every_value_in_both_specs_is_a_string_for_kubernetes(self):
        for spec in (self.purge_spec(), render(m.build_vacuum_spec())):
            for env in (spec["spec"]["driver"]["env"], spec["spec"]["executor"]["env"]):
                for item in env:
                    self.assertIsInstance(item["value"], str)
            for key, value in spec["spec"]["sparkConf"].items():
                self.assertIsInstance(value, str, key)

    def test_the_vacuum_reads_its_targets_from_the_variables_with_defaults(self):
        default = env_of(render(m.build_vacuum_spec()))
        custom = env_of(render(m.build_vacuum_spec(), variables={"deid_vacuum_targets": "a,b.c", "deid_vacuum_retain_hours": "336"}))

        self.assertEqual((default["VACUUM_TARGETS"], default["VACUUM_RETAIN_HOURS"]), ("demo_db", "168"))
        self.assertEqual((custom["VACUUM_TARGETS"], custom["VACUUM_RETAIN_HOURS"]), ("a,b.c", "336"))
        self.assertEqual(render(m.build_vacuum_spec())["spec"]["mainClass"], "vai.lakehouse.pseudonymizetable.app.VacuumApp")

    def test_the_dags_are_a_triggered_purge_and_a_daily_vacuum_one_run_at_a_time(self):
        dags = {r.args[0]: r.kwargs for r in m.recorded if r.args and r.args[0] in (m.PURGE_DAG_ID, m.VACUUM_DAG_ID)}

        self.assertIsNone(dags[m.PURGE_DAG_ID]["schedule"])
        self.assertEqual(dags[m.VACUUM_DAG_ID]["schedule"], "0 3 * * *")
        for kwargs in dags.values():
            self.assertEqual(kwargs["max_active_runs"], 1)
            self.assertIs(kwargs["render_template_as_native_obj"], False)
            self.assertIs(kwargs["catchup"], False)


if __name__ == "__main__":
    unittest.main()
