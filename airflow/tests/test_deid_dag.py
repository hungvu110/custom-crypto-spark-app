"""Test cho DAG khử định danh (dags/sample_dag_deid_pseudonymize_table.py) mà KHÔNG cần cài Airflow: module `airflow` được giả lập.

Chạy từ thư mục gốc repo (chỉ cần Python 3 và jinja2, không cần pytest):
    python3 -B -m unittest discover -s airflow/tests -v
(pytest cũng chạy được: pytest airflow/tests)

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
MANUAL_CREDS = {"DAK_CLIENT_ID": "client-abc", "DAK_CLIENT_SECRET": "s3cr3t-Value_123"}  # no default: a manual trigger must give them


def dak_conf(**env):
    """Conf đúng như dak-mock gửi: các khoá bảng và env cuối cùng (đã điền mặc định). Không còn keySecretName (key lấy từ DAK)."""
    final_env = {"OUTPUT_DB": "demo_db", "OUTPUT_TABLE": "users_cdr_2_pseudonymized", "OUTPUT_MODE": "overwrite",
                 "OUTPUT_FORMAT": "delta", "ENABLE_HIVE_SUPPORT": "true",
                 "DAK_CLIENT_ID": "client-abc", "DAK_CLIENT_SECRET": "s3cr3t-Value_123"}
    final_env.update(env)
    return {"requestCode": "PYC-2026-000007", "database": "demo_db", "table": "users_cdr_2", "pseudoColumns": "name:HASH",
            "env": final_env}


class _DagRun:
    def __init__(self, run_id, conf):
        self.run_id = run_id
        self.conf = conf


class _Masked:
    """Stand-in for the Airflow secrets masker: remembers every value that the DAG's macro was asked to hide."""

    def __init__(self):
        self.values = []

    def append_and_return(self, value):
        self.values.append(value)
        return value


MASKED = _Masked()


def render(run_id, conf):
    """Render spec bằng Jinja thường, giống DAG đặt render_template_as_native_obj=False."""
    environment = jinja2.Environment()
    context = {"dag_run": _DagRun(run_id, conf), "ts_nodash": "20261026T084032", "mask_credential": MASKED.append_and_return}

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
    return {item["name"]: item["value"] for item in spec["spec"]["driver"]["env"] if "value" in item}


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
                         ("requestCode", "PYC 1"), ("requestCode", "-PYC"), ("requestCode", "R" * 64), ("requestCode", ""),
                         ("requestCode", "PYC-1"), ("requestCode", "REQ-2026-00001"), ("requestCode", "REQ-2026-000011&x=1"),
                         ("requestCode", "../REQ-2026-000011"), ("table", None)):
            conf = dak_conf()
            conf[key] = bad
            with self.assertRaises(ValueError, msg=f"{key}={bad!r}"):
                self.check(DAK_RUN_ID, conf)

    def test_the_naming_of_the_real_dak_is_accepted(self):
        for code in ("REQ-2026-000011", "PYC-2026-000007"):
            conf = dak_conf()
            conf["requestCode"] = code
            self.check(DAK_RUN_ID, conf)
            spec = render(DAK_RUN_ID, conf)
            self.assertEqual(spec["spec"]["driver"]["labels"]["dak.vai/request-code"], code)
            self.assertEqual(driver_env(spec)["REQUEST_CODE"], code)

    def test_a_dag_run_id_that_looks_like_a_dak_run_but_is_malformed_is_rejected(self):
        with self.assertRaises(ValueError):
            self.check("deid-26-1", dak_conf())

    def test_a_manual_run_with_empty_conf_falls_back_to_the_manual_defaults(self):
        # only the client credentials are needed (they have no default); everything else falls back
        self.check(MANUAL_RUN_ID, {"env": dict(MANUAL_CREDS)})
        with self.assertRaises(ValueError):
            self.check(MANUAL_RUN_ID, None)
        with self.assertRaises(ValueError):
            self.check(MANUAL_RUN_ID, {})

    def test_a_manual_run_may_set_part_of_the_env_but_it_is_still_checked(self):
        self.check(MANUAL_RUN_ID, {"env": {**MANUAL_CREDS, "OUTPUT_MODE": "append"}})
        with self.assertRaises(ValueError):
            self.check(MANUAL_RUN_ID, {"env": {**MANUAL_CREDS, "OUTPUT_MODE": "nope"}})
        with self.assertRaises(ValueError):
            self.check(MANUAL_RUN_ID, {"env": {**MANUAL_CREDS, "DB_NAME": "x"}})

    def test_the_manual_default_output_must_also_differ_from_the_source_table(self):
        with self.assertRaises(ValueError):
            self.check(MANUAL_RUN_ID, {"table": "users_cdr_2_deid_manual"})


class DateHourRangeCheckTest(unittest.TestCase):
    """INPUT_FROM / INPUT_TO / INPUT_HOURS: cùng luật với JobEnvPolicy của dak-mock và với job pseudonymize-table-app."""

    def accepted(self, **env):
        dag.check_conf(DAK_RUN_ID, dak_conf(**env))

    def rejected(self, **env):
        with self.assertRaises(ValueError, msg=repr(env)) as caught:
            dag.check_conf(DAK_RUN_ID, dak_conf(**env))
        return str(caught.exception)

    def test_valid_ranges_are_accepted(self):
        self.accepted(INPUT_FROM="20261012")
        self.accepted(INPUT_FROM="20261012", INPUT_TO="20261014")
        self.accepted(INPUT_FROM="2026101218", INPUT_TO="2026101306")
        self.accepted(INPUT_FROM="20261012", INPUT_TO="20261012")
        self.accepted(INPUT_FROM="2026101223", INPUT_TO="20261012")  # 23 <= 23
        self.accepted(INPUT_HOURS="18")
        self.accepted(INPUT_HOURS="6-18")
        self.accepted(INPUT_HOURS="06-18")
        self.accepted(INPUT_FROM="20240229")  # leap day

    def test_bad_format_date_or_hour_is_rejected_without_echoing_the_value(self):
        for name, value in (("INPUT_FROM", "202610120030"), ("INPUT_FROM", "2026-10-12"), ("INPUT_FROM", "20261340"),
                            ("INPUT_TO", "20230229"), ("INPUT_FROM", "2026101224"), ("INPUT_TO", "2026101225"),
                            ("INPUT_HOURS", "24"), ("INPUT_HOURS", "6-"), ("INPUT_HOURS", "-6"), ("INPUT_HOURS", "1-2-3"),
                            ("INPUT_FROM", 20261012), ("INPUT_HOURS", ""), ("INPUT_FROM", None)):
            message = self.rejected(**{name: value})
            if isinstance(value, str) and value:
                self.assertNotIn(value, message)

    def test_from_after_to_is_rejected_and_a_missing_hour_means_00_for_from_and_23_for_to(self):
        self.rejected(INPUT_FROM="20261014", INPUT_TO="20261012")
        self.rejected(INPUT_FROM="2026101218", INPUT_TO="2026101217")
        self.rejected(INPUT_FROM="2026101201", INPUT_TO="2026101200")
        self.accepted(INPUT_FROM="20261012", INPUT_TO="2026101200")

    def test_hours_must_not_go_backwards(self):
        self.rejected(INPUT_HOURS="18-6")
        self.accepted(INPUT_HOURS="6-6")

    def test_overwrite_partitions_needs_a_range(self):
        self.rejected(OUTPUT_MODE="overwrite-partitions")
        self.accepted(OUTPUT_MODE="overwrite-partitions", INPUT_FROM="20261012")
        self.accepted(OUTPUT_MODE="overwrite-partitions", INPUT_TO="20261012")
        self.accepted(OUTPUT_MODE="overwrite-partitions", INPUT_HOURS="5")
        self.accepted(OUTPUT_MODE="overwrite", INPUT_FROM="20261012")
        self.accepted(OUTPUT_MODE="append", INPUT_HOURS="5")

    def test_the_other_input_env_are_still_rejected(self):
        for name in ("INPUT_PATH", "INPUT_DATE_KEY", "INPUT_HOUR_KEY", "INPUT_FORMAT", "INPUT_BAD_FILES"):
            self.rejected(**{name: "x"})

    def test_a_manual_run_may_use_a_range(self):
        dag.check_conf(MANUAL_RUN_ID, {"env": {**MANUAL_CREDS, "INPUT_FROM": "20261012", "INPUT_HOURS": "5-6"}})
        with self.assertRaises(ValueError):
            dag.check_conf(MANUAL_RUN_ID, {"env": {**MANUAL_CREDS, "OUTPUT_MODE": "overwrite-partitions"}})


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

    def test_the_source_partition_columns_are_the_silver_ones_and_cannot_be_overridden(self):
        env = driver_env(render(DAK_RUN_ID, dak_conf()))

        self.assertEqual((env["INPUT_DATE_KEY"], env["INPUT_HOUR_KEY"]), ("dt", "hour"))

    def test_a_rerun_of_the_same_request_may_add_an_r_suffix_to_the_run_id(self):
        for run_id in ("deid-2026-000007-r2", "deid-2026-000007-r10"):
            dag.check_conf(run_id, dak_conf())
            self.assertEqual(render(run_id, dak_conf())["metadata"]["name"], run_id)
        for bad in ("deid-2026-000007-r", "deid-2026-000007-x2", "deid-2026-000007-r2-r3", "deid-2026-000007-R2"):
            with self.assertRaises(ValueError):
                dag.check_conf(bad, dak_conf())

    def test_only_one_run_at_a_time_because_runs_share_the_result_table(self):
        dag_kwargs = [r.kwargs for r in dag.recorded if "max_active_runs" in r.kwargs]

        self.assertEqual(dag_kwargs[0]["max_active_runs"], 1)

    def test_an_env_missing_from_conf_gets_its_default(self):
        conf = dak_conf()
        for name in ("OUTPUT_MODE", "OUTPUT_FORMAT", "ENABLE_HIVE_SUPPORT"):
            del conf["env"][name]

        env = driver_env(render(DAK_RUN_ID, conf))

        self.assertEqual((env["OUTPUT_MODE"], env["OUTPUT_FORMAT"], env["ENABLE_HIVE_SUPPORT"]), ("overwrite", "delta", "true"))

    def test_a_range_goes_into_the_driver_and_an_absent_range_is_an_empty_value(self):
        # An empty value is "not set" for the job (it ignores blank env), so the env list stays fixed.
        with_range = driver_env(render(DAK_RUN_ID, dak_conf(INPUT_FROM="2026101218", INPUT_TO="20261013", INPUT_HOURS="6-18",
                                                            OUTPUT_MODE="overwrite-partitions")))
        without = driver_env(render(DAK_RUN_ID, dak_conf()))

        self.assertEqual((with_range["INPUT_FROM"], with_range["INPUT_TO"], with_range["INPUT_HOURS"]),
                         ("2026101218", "20261013", "6-18"))
        self.assertEqual(with_range["OUTPUT_MODE"], "overwrite-partitions")
        self.assertEqual((without["INPUT_FROM"], without["INPUT_TO"], without["INPUT_HOURS"]), ("", "", ""))

    def test_output_mode_defaults_to_overwrite_partitions_with_a_range_and_to_overwrite_without(self):
        def mode(**env):
            conf = dak_conf(**env)
            del conf["env"]["OUTPUT_MODE"]
            return driver_env(render(DAK_RUN_ID, conf))["OUTPUT_MODE"]

        self.assertEqual(mode(), "overwrite")
        self.assertEqual(mode(INPUT_FROM="20261012"), "overwrite-partitions")
        self.assertEqual(mode(INPUT_TO="20261012"), "overwrite-partitions")
        self.assertEqual(mode(INPUT_HOURS="5"), "overwrite-partitions")
        self.assertEqual(driver_env(render(MANUAL_RUN_ID, None))["OUTPUT_MODE"], "overwrite")
        self.assertEqual(driver_env(render(MANUAL_RUN_ID, {"env": {"INPUT_HOURS": "5"}}))["OUTPUT_MODE"], "overwrite-partitions")
        self.assertEqual(driver_env(render(MANUAL_RUN_ID, {"env": {"INPUT_HOURS": "5", "OUTPUT_MODE": "append"}}))["OUTPUT_MODE"],
                         "append")

    def test_a_manual_run_with_empty_conf_renders_the_manual_defaults(self):
        spec = render(MANUAL_RUN_ID, None)
        env = driver_env(spec)

        self.assertEqual(spec["metadata"]["name"], "deid-manual-20261026t084032")
        self.assertEqual((env["DB_NAME"], env["TABLE_NAME"], env["PSEUDO_COLUMNS"]), ("demo_db", "users_cdr_2", "name:HASH"))
        self.assertEqual((env["OUTPUT_DB"], env["OUTPUT_TABLE"]), ("demo_db", "users_cdr_2_deid_manual"))
        self.assertEqual(env["REQUEST_CODE"], dag.DEFAULTS["requestCode"])

    def test_a_dak_run_is_named_after_the_dag_run_id(self):
        spec = render(DAK_RUN_ID, dak_conf())

        self.assertEqual(spec["metadata"]["name"], DAK_RUN_ID)

    def test_every_job_env_appears_exactly_once_and_the_fixed_env_stay_fixed(self):
        spec = render(DAK_RUN_ID, dak_conf())
        names = [item["name"] for item in spec["spec"]["driver"]["env"]]
        env = driver_env(spec)

        self.assertEqual(len(names), len(set(names)), "duplicate env names in the driver")
        for name in dag.JOB_ENV:
            self.assertEqual(names.count(name), 1, name)
        self.assertEqual(env["PSEUDO_MODE"], "HASH")
        self.assertEqual(env["VERIFY"], "true")
        self.assertEqual(env["CRYPTO_PREFIX_SOURCE"], "dak")
        self.assertNotIn("CRYPTO_KEY_PREFIX_DIR", env)

    def test_the_key_comes_from_dak_with_the_credentials_in_conf_and_not_from_a_secret_volume(self):
        spec = render(DAK_RUN_ID, dak_conf())["spec"]
        env = driver_env(render(DAK_RUN_ID, dak_conf()))

        self.assertEqual(env["CRYPTO_PREFIX_SOURCE"], "dak")
        self.assertEqual(env["REQUEST_CODE"], "PYC-2026-000007")
        for name in ("DAK_ADDR", "DAK_TOKEN_URL", "DAK_CALLER"):
            self.assertTrue(env[name], name)
        self.assertNotIn("DAK_TAG", env)  # keys/deid takes no tag (unlike by-column)
        self.assertEqual(env["DAK_CLIENT_ID"], "client-abc")
        self.assertEqual(env["DAK_CLIENT_SECRET"], "s3cr3t-Value_123")
        self.assertFalse([i for i in spec["driver"]["env"] if "valueFrom" in i])
        # no Secret of keys: only the volumes of the truststore of the internal CA
        self.assertEqual({v["name"] for v in spec["volumes"]}, {"keycloak-ca", "truststore"})
        self.assertFalse([v for v in spec["volumes"] if "secret" in v])
        self.assertFalse([m for m in spec["driver"]["volumeMounts"] if m["mountPath"] == "/etc/pseudonym-keys"])

    def test_the_driver_trusts_the_internal_ca_of_keycloak_and_the_executors_do_not(self):
        spec = render(DAK_RUN_ID, dak_conf())["spec"]

        self.assertIn("trustStore", spec["sparkConf"]["spark.driver.extraJavaOptions"])
        self.assertIn("log4j", spec["sparkConf"]["spark.driver.extraJavaOptions"])
        self.assertNotIn("trustStore", spec["sparkConf"]["spark.executor.extraJavaOptions"])
        self.assertEqual(spec["driver"]["hostAliases"][0]["hostnames"], [dag.KEYCLOAK_HOST])
        self.assertEqual(spec["driver"]["initContainers"][0]["name"], "build-truststore")
        self.assertNotIn("DAK_CLIENT_SECRET", {i["name"] for i in spec["executor"]["env"]})

    def test_the_client_credentials_are_required_and_checked_and_never_echoed(self):
        for name in dag.CREDENTIAL_ENV:
            conf = dak_conf()
            del conf["env"][name]
            with self.assertRaises(ValueError):
                dag.check_conf(DAK_RUN_ID, conf)
            with self.assertRaises(ValueError):
                dag.check_conf(MANUAL_RUN_ID, conf)
        bad = dak_conf(DAK_CLIENT_SECRET="bad secret;rm -rf")
        with self.assertRaises(ValueError) as caught:
            dag.check_conf(DAK_RUN_ID, bad)
        self.assertNotIn("bad secret", str(caught.exception))

    def test_the_credentials_go_through_the_masking_macro_and_the_dag_registers_it(self):
        MASKED.values.clear()
        render(DAK_RUN_ID, dak_conf())

        self.assertEqual(sorted(MASKED.values), ["client-abc", "s3cr3t-Value_123"])
        self.assertIn("mask_credential", dag.recorded[0].kwargs["user_defined_macros"])
        self.assertIs(dag.recorded[0].kwargs["user_defined_macros"]["mask_credential"], dag.mask_credential)
        self.assertEqual(dag.mask_credential("abc"), "abc")  # returns the value unchanged (no Airflow here: no-op)

    def test_the_spark_ui_hides_the_client_credentials(self):
        import re as regex

        spec = render(DAK_RUN_ID, dak_conf())["spec"]
        pattern = spec["sparkConf"]["spark.redaction.regex"]

        for key in ("spark.kubernetes.driverEnv.DAK_CLIENT_SECRET", "spark.kubernetes.driverEnv.DAK_CLIENT_ID"):
            self.assertTrue(regex.search(pattern, key), key)
        self.assertIn("secret", pattern)  # the Spark default is kept

    def test_the_credentials_are_not_in_the_executors_nor_in_job_env(self):
        spec = render(DAK_RUN_ID, dak_conf())["spec"]

        self.assertFalse(set(dag.CREDENTIAL_ENV) & set(dag.JOB_ENV))
        self.assertFalse(set(dag.CREDENTIAL_ENV) & {i["name"] for i in spec["executor"]["env"]})

    def test_a_keySecretName_that_dak_still_sends_is_ignored(self):
        conf = dak_conf()
        conf["keySecretName"] = "deid-key-2026-000007"

        dag.check_conf(DAK_RUN_ID, conf)
        self.assertNotIn("deid-key-2026-000007", str(render(DAK_RUN_ID, conf)))

    def test_only_the_driver_gets_the_job_env(self):
        spec = render(DAK_RUN_ID, dak_conf())
        executor_names = {item["name"] for item in spec["spec"]["executor"]["env"]}

        self.assertFalse(executor_names & set(dag.JOB_ENV))

    def test_every_env_value_label_and_spark_conf_is_a_string(self):
        # Kubernetes trả 422 "must be of type string" nếu "2" hay "64" bị đổi thành số.
        spec = render(DAK_RUN_ID, dak_conf())["spec"]
        for role in ("driver", "executor"):
            for item in spec[role]["env"]:
                if "value" in item:
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
