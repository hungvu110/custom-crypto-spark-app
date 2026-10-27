"""Test cho hai DAG chuẩn hoá silver (dags/silver_ingest_vtt.py, dags/silver_ingest_vtnet.py) mà KHÔNG cần cài Airflow.

Chạy từ gốc repo (cần Python 3 và jinja2):
    python3 -B -m unittest discover -s airflow/tests -v
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


class _Recorder:
    instances = []

    def __init__(self, *args, **kwargs):
        self.args = args
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


def load(name):
    for module in STUBBED:
        sys.modules[module] = types.ModuleType(module)
    sys.modules["airflow"].DAG = _Recorder
    sys.modules["airflow.exceptions"].AirflowFailException = _AirflowFailException
    sys.modules["airflow.operators.python"].PythonOperator = _Recorder
    sys.modules["airflow.providers.cncf.kubernetes.operators.spark_kubernetes"].SparkKubernetesOperator = _Recorder
    sys.modules["pendulum"].datetime = lambda *args, **kwargs: None
    sys.path.insert(0, str(DAGS))
    try:
        _Recorder.instances = []
        spec = importlib.util.spec_from_file_location(f"{name}_under_test", DAGS / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.recorded = list(_Recorder.instances)
        return module
    finally:
        sys.path.remove(str(DAGS))
        for module in STUBBED:
            sys.modules.pop(module, None)


vtt = load("silver_ingest_vtt")
vtnet = load("silver_ingest_vtnet")
common = vtt.common

ROOT_VTT = vtt.SOURCE["input_root"]
ROOT_VTNET = vtnet.INPUT_ROOT


def vtt_conf(**extra):
    conf = {"path": "vtt_cdr/20261027", "batchDate": "20261027"}
    conf.update(extra)
    return conf


def vtnet_conf(**extra):
    conf = {"path": "vtnet_cdr/20261027", "batchDate": "20261027", "encryptColumns": ["msisdn", "imei"],
            "keyField": "start_time"}
    conf.update(extra)
    return conf


class _DagRun:
    def __init__(self, run_id, conf):
        self.run_id = run_id
        self.conf = conf


def render(dag_module, conf, run_id="slv-2026-000001"):
    """Render spec bằng Jinja thường (render_template_as_native_obj=False) với macro k8s_name như DAG khai."""
    environment = jinja2.Environment()
    context = {"dag_run": _DagRun(run_id, conf), "k8s_name": common.k8s_name}

    def walk(node):
        if isinstance(node, dict):
            return {key: walk(value) for key, value in node.items()}
        if isinstance(node, list):
            return [walk(value) for value in node]
        if isinstance(node, str):
            return environment.from_string(node).render(**context)
        return node

    return walk(common.build_spec(dag_module.SOURCE))


def driver_env(spec):
    return {item["name"]: item.get("value", item.get("valueFrom")) for item in spec["spec"]["driver"]["env"]}


def task(dag_module, task_id):
    return next(r for r in dag_module.recorded if r.kwargs.get("task_id") == task_id)


class CheckConfTest(unittest.TestCase):
    def test_a_valid_conf_is_accepted(self):
        common.check_conf(vtt.SOURCE, vtt_conf())
        common.check_conf(vtt.SOURCE, vtt_conf(path="vtt_cdr/20261027/part-0001.csv", outputMode="append"))
        common.check_conf(vtnet.SOURCE, vtnet_conf())
        common.check_conf(vtnet.SOURCE, vtnet_conf(encryptColumns=["MSISDN"], keyField="Start_Time"))

    def test_path_is_required_relative_and_canonical(self):
        for bad in (None, "", 7, "/vtt_cdr/x", "vtt_cdr/x/", "vtt_cdr//x", "hdfs://nn:8020/landing/vtt/a", "a/../b",
                    "a b", "{{ 7*7 }}", "a/" + "x" * 1100):
            conf = vtt_conf(path=bad) if bad is not None else {"batchDate": "20261027"}
            with self.assertRaises(ValueError, msg=repr(bad)):
                common.check_conf(vtt.SOURCE, conf)

    def test_output_mode_is_checked_and_whole_table_overwrite_is_not_accepted_from_conf(self):
        for mode in ("merge", "overwrite"):
            with self.assertRaises(ValueError, msg=mode):
                common.check_conf(vtt.SOURCE, vtt_conf(outputMode=mode))

    def test_batch_date_is_required_and_must_be_a_real_date(self):
        for bad in (None, "", "2026-10-27", "20261340", "20230229", 20261027, "202610271"):
            conf = vtt_conf()
            if bad is None:
                del conf["batchDate"]
            else:
                conf["batchDate"] = bad
            with self.assertRaises(ValueError, msg=repr(bad)):
                common.check_conf(vtt.SOURCE, conf)
        common.check_conf(vtt.SOURCE, vtt_conf(batchDate="20240229"))

    def test_vtnet_needs_valid_encrypt_columns_and_key_field_from_the_request(self):
        bad_confs = (
            {"encryptColumns": None}, {"encryptColumns": []}, {"encryptColumns": "msisdn,imei"},
            {"encryptColumns": ["msisdn", "bad-col"]}, {"encryptColumns": ["msisdn", "MSISDN"]},
            {"encryptColumns": ["c%d" % i for i in range(101)]}, {"keyField": None}, {"keyField": "bad col"},
            {"keyField": "MSISDN"})
        for extra in bad_confs:
            conf = vtnet_conf(**extra)
            for key, value in extra.items():
                if value is None:
                    del conf[key]
            with self.assertRaises(ValueError, msg=repr(extra)):
                common.check_conf(vtnet.SOURCE, conf)

    def test_unknown_keys_are_rejected_and_vtt_takes_no_columns(self):
        for module, conf in ((vtt, vtt_conf(encryptColumns=["a"])), (vtt, vtt_conf(inputPath="x")),
                             (vtnet, vtnet_conf(keyPath="x")), (vtnet, vtnet_conf(outputTable="x"))):
            with self.assertRaises(ValueError, msg=repr(conf)):
                common.check_conf(module.SOURCE, conf)

    def test_messages_do_not_echo_the_value(self):
        with self.assertRaises(ValueError) as caught:
            common.check_conf(vtt.SOURCE, vtt_conf(path="secret/value with space"))
        self.assertNotIn("secret/value", str(caught.exception))

    def test_the_validate_task_turns_errors_into_airflow_failures(self):
        with self.assertRaises(_AirflowFailException):
            vtt.validate_conf(dag_run=_DagRun("r", {}))
        vtnet.validate_conf(dag_run=_DagRun("r", vtnet_conf()))


class SpecTest(unittest.TestCase):
    def test_both_dags_use_plain_rendering_and_the_k8s_name_macro(self):
        for module in (vtt, vtnet):
            dag_kwargs = [r.kwargs for r in module.recorded if "render_template_as_native_obj" in r.kwargs][0]
            self.assertIs(dag_kwargs["render_template_as_native_obj"], False)
            self.assertIs(dag_kwargs["user_defined_macros"]["k8s_name"], common.k8s_name)
            self.assertEqual(module.recorded[0].args[0], module.DAG_ID)

    def test_each_dag_runs_its_own_main_class(self):
        self.assertEqual(render(vtt, vtt_conf())["spec"]["mainClass"], "vai.lakehouse.silveringest.app.VttSilverApp")
        self.assertEqual(render(vtnet, vtnet_conf())["spec"]["mainClass"], "vai.lakehouse.silveringest.app.VtnetSilverApp")

    def test_the_input_path_is_the_root_of_the_source_plus_the_relative_path(self):
        env = driver_env(render(vtt, vtt_conf()))
        self.assertEqual(env["INPUT_PATH"], ROOT_VTT + "vtt_cdr/20261027")
        self.assertTrue(ROOT_VTT.endswith("/") and ROOT_VTNET.endswith("/"))
        self.assertEqual(env["OUTPUT_MODE"], "overwrite-partitions")
        self.assertEqual((env["BATCH_COLUMN"], env["BATCH_VALUE"]), ("batch_date", "20261027"))
        self.assertEqual((env["OUTPUT_DB"], env["OUTPUT_TABLE"], env["INPUT_FORMAT"]), ("silver", "vtt_cdr", "csv"))
        self.assertEqual(driver_env(render(vtt, vtt_conf(outputMode="append")))["OUTPUT_MODE"], "append")

    def test_vtt_has_no_dak_or_vtnet_env(self):
        names = set(driver_env(render(vtt, vtt_conf())))
        self.assertFalse({n for n in names if n.startswith(("DAK_", "CDR_", "FILE_", "KEY_"))} | {"CRYPTO_PREFIX_SOURCE"} & names)

    def test_vtnet_takes_the_columns_from_the_request_and_asks_by_path_for_the_same_relative_path(self):
        env = driver_env(render(vtnet, vtnet_conf(encryptColumns=["msisdn", "imei", "cell_id"])))
        self.assertEqual(env["CDR_ENCRYPT_COLUMNS"], "msisdn,imei,cell_id")
        self.assertEqual(env["CDR_KEY_FIELD"], "start_time")
        self.assertEqual(env["CDR_ON_MISSING"], "request")
        # Chờ admin DAK duyệt yêu cầu key cột theo exponential backoff, có timeout.
        self.assertEqual(env["DAK_PENDING_WAIT_TIMEOUT_SECONDS"], str(vtnet.PENDING_WAIT["timeout"]))
        self.assertGreater(int(env["DAK_PENDING_WAIT_TIMEOUT_SECONDS"]), 0)
        self.assertLessEqual(int(env["DAK_PENDING_WAIT_INITIAL_SECONDS"]), int(env["DAK_PENDING_WAIT_MAX_INTERVAL_SECONDS"]))
        self.assertGreaterEqual(float(env["DAK_PENDING_WAIT_MULTIPLIER"]), 1.0)
        self.assertEqual(env["FILE_CIPHER"], "aes-256-gcm")
        # Job gửi by-path đúng phần của INPUT_PATH sau KEY_PATH_ROOT, tức conf.path.
        self.assertEqual(env["KEY_PATH_ROOT"], ROOT_VTNET)
        self.assertEqual(env["INPUT_PATH"], ROOT_VTNET + "vtnet_cdr/20261027")
        self.assertNotIn("KEY_PATH", env)

    def test_vtnet_driver_reads_the_dak_secret_by_reference_and_executors_get_no_dak_env(self):
        spec = render(vtnet, vtnet_conf())
        env = driver_env(spec)
        self.assertEqual(env["DAK_CLIENT_SECRET"], {"secretKeyRef": {"name": "spark-dak-client", "key": "clientSecret"}})
        self.assertEqual((env["CRYPTO_PREFIX_SOURCE"], env["DAK_TAG"]), ("dak", "internal"))
        executor = {item["name"] for item in spec["spec"]["executor"]["env"]}
        self.assertFalse({n for n in executor if n.startswith(("DAK_", "CDR_", "FILE_", "CRYPTO_", "KEY_"))})

    def test_the_application_name_is_valid_and_unique_per_run(self):
        a = render(vtnet, vtnet_conf(), run_id="slv-2026-000001")["metadata"]["name"]
        b = render(vtnet, vtnet_conf(), run_id="slv-2026-000002")["metadata"]["name"]
        self.assertNotEqual(a, b)
        for name in (a, b):
            self.assertRegex(name, r"^silver-vtnet-[0-9a-f]{12}$")

    def test_every_env_value_and_spark_conf_is_a_string_and_jars_include_cdr_crypto(self):
        for module, conf in ((vtt, vtt_conf()), (vtnet, vtnet_conf())):
            spec = render(module, conf)["spec"]
            for role in ("driver", "executor"):
                for item in spec[role]["env"]:
                    if "value" in item:
                        self.assertIsInstance(item["value"], str, item["name"])
            for key, value in spec["sparkConf"].items():
                self.assertIsInstance(value, str, key)
            self.assertIn("cdr-crypto-udf", spec["sparkConf"]["spark.jars"])
            self.assertIn("DataLakeSecurity_jv8.jar", spec["sparkConf"]["spark.jars"])

    def test_the_validate_task_runs_before_the_job(self):
        for module in (vtt, vtnet):
            self.assertEqual(task(module, "validate_conf").kwargs["python_callable"], module.validate_conf)
            self.assertEqual(task(module, "ingest").kwargs["namespace"], common.NAMESPACE)


if __name__ == "__main__":
    unittest.main()
