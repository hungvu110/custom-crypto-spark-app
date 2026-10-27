"""Test cho DAG khử định danh (dags/sample_dag_deid_pseudonymize_table.py) mà KHÔNG cần cài Airflow: module `airflow` được giả lập.

Chạy từ thư mục gốc repo (chỉ cần Python 3 và jinja2, không cần pytest):
    python3 -B -m unittest discover -s dak-mock/k8s/airflow/tests -v
(pytest cũng chạy được: pytest dak-mock/k8s/airflow/tests)

Kiểm hai việc: (1) `check_conf` chặn conf sai trước khi tạo job, (2) spec SparkApplication render ra đúng, mọi giá trị env đều là
CHUỖI (Kubernetes trả 422 nếu env value là số). Nguồn sự thật cho tên env và quy tắc giá trị là `JOB_ENV` trong DAG, khớp với
`dak.deid.job-env` của dak-mock (có test Java chống lệch).
"""
import importlib.util
import pathlib
import re
import sys
import types
import unittest

import jinja2

DAG_FILE = pathlib.Path(__file__).resolve().parents[1] / "dags" / "sample_dag_deid_pseudonymize_table.py"
STUBBED = ["airflow", "airflow.exceptions", "airflow.operators", "airflow.operators.python", "airflow.providers",
           "airflow.providers.cncf", "airflow.providers.cncf.kubernetes", "airflow.providers.cncf.kubernetes.operators",
           "airflow.providers.cncf.kubernetes.operators.spark_kubernetes", "pendulum"]


class _Recorder:
    """Thay cho DAG và các operator: nhớ tham số khởi tạo, dùng được trong `with` và toán tử `>>`."""
    instances = []

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        _Recorder.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __rshift__(self, other):
        return other


class _AirflowFailException(Exception):
    pass


def load_dag_module():
    for name in STUBBED:
        sys.modules[name] = types.ModuleType(name)
    sys.modules["airflow"].DAG = _Recorder
    sys.modules["airflow.exceptions"].AirflowFailException = _AirflowFailException
    sys.modules["airflow.operators.python"].PythonOperator = _Recorder
    sys.modules["airflow.providers.cncf.kubernetes.operators.spark_kubernetes"].SparkKubernetesOperator = _Recorder
    sys.modules["pendulum"].datetime = lambda *args, **kwargs: None
    try:
        _Recorder.instances = []
        spec = importlib.util.spec_from_file_location("sample_dag_under_test", DAG_FILE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.recorded = list(_Recorder.instances)
        return module
    finally:
        for name in STUBBED:
            sys.modules.pop(name, None)


dag = load_dag_module()

DAK_RUN_ID = "deid-2026-000007"
MANUAL_RUN_ID = "manual__2026-10-26T08:40:32+00:00"


def dak_conf(**env):
    """Conf đúng như dak-mock gửi: các khoá bảng, tên Secret và env cuối cùng (đã điền mặc định)."""
    final_env = {"OUTPUT_DB": "demo_db", "OUTPUT_TABLE": "users_cdr_2_pseudonymized", "OUTPUT_MODE": "overwrite",
                 "OUTPUT_FORMAT": "delta", "ENABLE_HIVE_SUPPORT": "true"}
    final_env.update(env)
    return {"requestCode": "PYC-2026-000007", "database": "demo_db", "table": "users_cdr_2", "pseudoColumns": "name:HASH",
            "keySecretName": "deid-key-2026-000007", "env": final_env}


class _DagRun:
    def __init__(self, run_id, conf):
        self.run_id = run_id
        self.conf = conf


def render(run_id, conf):
    """Render spec bằng Jinja thường, giống DAG đặt render_template_as_native_obj=False."""
    environment = jinja2.Environment()
    context = {"dag_run": _DagRun(run_id, conf), "ts_nodash": "20261026T084032"}

    def walk(node):
        if isinstance(node, dict):
            return {key: walk(value) for key, value in node.items()}
        if isinstance(node, list):
            return [walk(value) for value in node]
        if isinstance(node, str):
            return environment.from_string(node).render(**context)
        return node

    return walk(dag.build_spec())


def driver_env(spec):
    return {item["name"]: item["value"] for item in spec["spec"]["driver"]["env"]}


class JobEnvTableTest(unittest.TestCase):
    def test_every_rule_compiles_and_every_default_matches_its_rule(self):
        for name, (default, rule) in dag.JOB_ENV.items():
            self.assertRegex(name, r"^[A-Z][A-Z0-9_]*$")
            re.compile(rule)
            if default is not None:
                self.assertTrue(re.fullmatch(rule, default), name)

    def test_the_output_target_has_no_static_default_but_has_a_manual_one(self):
        self.assertIsNone(dag.JOB_ENV["OUTPUT_DB"][0])
        self.assertIsNone(dag.JOB_ENV["OUTPUT_TABLE"][0])
        for name in ("OUTPUT_DB", "OUTPUT_TABLE"):
            self.assertTrue(re.fullmatch(dag.JOB_ENV[name][1], dag.MANUAL_OUTPUT[name]))


class CheckConfTest(unittest.TestCase):
    def check(self, run_id, conf):
        return dag.check_conf(run_id, conf)

    def test_a_complete_dak_conf_is_accepted(self):
        self.check(DAK_RUN_ID, dak_conf())

    def test_a_dak_run_without_env_is_rejected_because_dak_always_sends_the_output_target(self):
        conf = dak_conf()
        del conf["env"]
        with self.assertRaises(ValueError):
            self.check(DAK_RUN_ID, conf)

    def test_a_dak_run_missing_one_output_env_is_rejected(self):
        for name in ("OUTPUT_DB", "OUTPUT_TABLE"):
            conf = dak_conf()
            del conf["env"][name]
            with self.assertRaises(ValueError, msg=name):
                self.check(DAK_RUN_ID, conf)

    def test_a_dak_run_may_omit_the_env_that_have_defaults(self):
        conf = dak_conf()
        for name in ("OUTPUT_MODE", "OUTPUT_FORMAT", "ENABLE_HIVE_SUPPORT"):
            del conf["env"][name]
        self.check(DAK_RUN_ID, conf)

    def test_env_that_is_not_an_object_is_rejected(self):
        for bad in ([], "x", 7, ["OUTPUT_DB"]):
            conf = dak_conf()
            conf["env"] = bad
            with self.assertRaises(ValueError, msg=repr(bad)):
                self.check(DAK_RUN_ID, conf)

    def test_an_env_name_outside_the_table_is_rejected_without_echoing_the_value(self):
        for name in ("DB_NAME", "TABLE_NAME", "VERIFY", "INPUT_PATH", "CRYPTO_KEY_PREFIX_DIR", "JAVA_TOOL_OPTIONS", "ANYTHING"):
            conf = dak_conf(**{name: "super-secret-value"})
            with self.assertRaises(ValueError, msg=name) as caught:
                self.check(DAK_RUN_ID, conf)
            self.assertNotIn("super-secret-value", str(caught.exception))

    def test_a_value_that_breaks_its_rule_or_is_not_a_string_is_rejected(self):
        bad_values = {"OUTPUT_MODE": ["replace", "OVERWRITE", "", 7, None, True],
                      "OUTPUT_FORMAT": ["parquet", "delta ", ["delta"]],
                      "ENABLE_HIVE_SUPPORT": ["yes", "True", 1],
                      "OUTPUT_DB": ["Demo_DB", "a-b", "", "x" * 129, "{{ 7*7 }}", "a\nb"],
                      "OUTPUT_TABLE": ["a b", "../x", "", "$(id)"]}
        for name, values in bad_values.items():
            for value in values:
                conf = dak_conf(**{name: value})
                with self.assertRaises(ValueError, msg=f"{name}={value!r}"):
                    self.check(DAK_RUN_ID, conf)

    def test_the_output_table_must_differ_from_the_source_table(self):
        with self.assertRaises(ValueError):
            self.check(DAK_RUN_ID, dak_conf(OUTPUT_DB="demo_db", OUTPUT_TABLE="users_cdr_2"))
        self.check(DAK_RUN_ID, dak_conf(OUTPUT_DB="other_db", OUTPUT_TABLE="users_cdr_2"))
        self.check(DAK_RUN_ID, dak_conf(OUTPUT_DB="demo_db", OUTPUT_TABLE="users_cdr_2_copy"))

    def test_the_old_output_keys_are_ignored_not_trusted(self):
        conf = dak_conf()
        conf["outputDb"] = "whatever"
        conf["outputTable"] = "whatever"
        self.check(DAK_RUN_ID, conf)
        self.assertEqual(driver_env(render(DAK_RUN_ID, conf))["OUTPUT_TABLE"], "users_cdr_2_pseudonymized")

    def test_table_conf_values_are_still_checked(self):
        for key, bad in (("database", "demo-db"), ("table", "../t"), ("pseudoColumns", "a:ENCRYPT"),
                         ("keySecretName", "other-secret"), ("requestCode", "PYC-1"), ("table", None)):
            conf = dak_conf()
            conf[key] = bad
            with self.assertRaises(ValueError, msg=f"{key}={bad!r}"):
                self.check(DAK_RUN_ID, conf)

    def test_a_dag_run_id_that_looks_like_a_dak_run_but_is_malformed_is_rejected(self):
        with self.assertRaises(ValueError):
            self.check("deid-26-1", dak_conf())

    def test_a_manual_run_with_empty_conf_falls_back_to_the_manual_defaults(self):
        self.check(MANUAL_RUN_ID, None)
        self.check(MANUAL_RUN_ID, {})

    def test_a_manual_run_may_set_part_of_the_env_but_it_is_still_checked(self):
        self.check(MANUAL_RUN_ID, {"env": {"OUTPUT_MODE": "append"}})
        with self.assertRaises(ValueError):
            self.check(MANUAL_RUN_ID, {"env": {"OUTPUT_MODE": "nope"}})
        with self.assertRaises(ValueError):
            self.check(MANUAL_RUN_ID, {"env": {"DB_NAME": "x"}})

    def test_the_manual_default_output_must_also_differ_from_the_source_table(self):
        with self.assertRaises(ValueError):
            self.check(MANUAL_RUN_ID, {"table": "users_cdr_2_deid_manual"})


class SpecTest(unittest.TestCase):
    def test_the_dag_does_not_use_native_rendering(self):
        dag_kwargs = [r.kwargs for r in dag.recorded if "render_template_as_native_obj" in r.kwargs]
        self.assertEqual(len(dag_kwargs), 1)
        self.assertIs(dag_kwargs[0]["render_template_as_native_obj"], False)

    def test_a_dak_run_puts_the_final_env_into_the_driver(self):
        env = driver_env(render(DAK_RUN_ID, dak_conf(OUTPUT_MODE="append", OUTPUT_FORMAT="iceberg",
                                                      ENABLE_HIVE_SUPPORT="false", OUTPUT_DB="deid_out",
                                                      OUTPUT_TABLE="users_pseudo")))

        self.assertEqual(env["OUTPUT_DB"], "deid_out")
        self.assertEqual(env["OUTPUT_TABLE"], "users_pseudo")
        self.assertEqual(env["OUTPUT_MODE"], "append")
        self.assertEqual(env["OUTPUT_FORMAT"], "iceberg")
        self.assertEqual(env["ENABLE_HIVE_SUPPORT"], "false")
        self.assertEqual(env["DB_NAME"], "demo_db")
        self.assertEqual(env["TABLE_NAME"], "users_cdr_2")
        self.assertEqual(env["PSEUDO_COLUMNS"], "name:HASH")

    def test_an_env_missing_from_conf_gets_its_default(self):
        conf = dak_conf()
        for name in ("OUTPUT_MODE", "OUTPUT_FORMAT", "ENABLE_HIVE_SUPPORT"):
            del conf["env"][name]

        env = driver_env(render(DAK_RUN_ID, conf))

        self.assertEqual((env["OUTPUT_MODE"], env["OUTPUT_FORMAT"], env["ENABLE_HIVE_SUPPORT"]), ("overwrite", "delta", "true"))

    def test_a_manual_run_with_empty_conf_renders_the_manual_defaults(self):
        spec = render(MANUAL_RUN_ID, None)
        env = driver_env(spec)

        self.assertEqual(spec["metadata"]["name"], "deid-manual-20261026t084032")
        self.assertEqual((env["DB_NAME"], env["TABLE_NAME"], env["PSEUDO_COLUMNS"]), ("demo_db", "users_cdr_2", "name:HASH"))
        self.assertEqual((env["OUTPUT_DB"], env["OUTPUT_TABLE"]), ("demo_db", "users_cdr_2_deid_manual"))
        self.assertEqual(spec["spec"]["volumes"][0]["secret"]["secretName"], "pseudonym-keys")

    def test_a_dak_run_is_named_after_the_dag_run_id_and_mounts_its_own_secret(self):
        spec = render(DAK_RUN_ID, dak_conf())

        self.assertEqual(spec["metadata"]["name"], DAK_RUN_ID)
        self.assertEqual(spec["spec"]["volumes"][0]["secret"]["secretName"], "deid-key-2026-000007")

    def test_every_job_env_appears_exactly_once_and_the_fixed_env_stay_fixed(self):
        spec = render(DAK_RUN_ID, dak_conf())
        names = [item["name"] for item in spec["spec"]["driver"]["env"]]
        env = driver_env(spec)

        self.assertEqual(len(names), len(set(names)), "duplicate env names in the driver")
        for name in dag.JOB_ENV:
            self.assertEqual(names.count(name), 1, name)
        self.assertEqual(env["PSEUDO_MODE"], "HASH")
        self.assertEqual(env["VERIFY"], "true")
        self.assertEqual(env["CRYPTO_PREFIX_SOURCE"], "file")
        self.assertEqual(env["CRYPTO_KEY_PREFIX_DIR"], "/etc/pseudonym-keys")

    def test_only_the_driver_gets_the_job_env(self):
        spec = render(DAK_RUN_ID, dak_conf())
        executor_names = {item["name"] for item in spec["spec"]["executor"]["env"]}

        self.assertFalse(executor_names & set(dag.JOB_ENV))

    def test_every_env_value_label_and_spark_conf_is_a_string(self):
        # Kubernetes trả 422 "must be of type string" nếu "2" hay "64" bị đổi thành số.
        spec = render(DAK_RUN_ID, dak_conf())["spec"]
        for role in ("driver", "executor"):
            for item in spec[role]["env"]:
                self.assertIsInstance(item["value"], str, item["name"])
            for key, value in spec[role]["labels"].items():
                self.assertIsInstance(value, str, key)
        for key, value in spec["sparkConf"].items():
            self.assertIsInstance(value, str, key)
        self.assertIsInstance(spec["driver"]["cores"], int)
        self.assertIsInstance(spec["executor"]["instances"], int)

    def test_a_template_in_a_value_would_not_be_evaluated_because_the_check_rejects_it(self):
        conf = dak_conf(OUTPUT_TABLE="{{ 7*7 }}")
        with self.assertRaises(ValueError):
            dag.check_conf(DAK_RUN_ID, conf)


if __name__ == "__main__":
    unittest.main()
