"""Tests for the silver ingest DAGs (dags/silver_ingest_vtt.py and dags/silver_ingest_vtnet.py: one RECURRING DAG per dataset, built by
dags/silver_ingest_factory.py) WITHOUT installing Airflow.

Run from the repository root (needs Python 3 and jinja2):
    python3 -B -m unittest discover -s airflow/tests -v
"""

import datetime
import importlib.util
import json
import pathlib
import sys
import types
import unittest
import unittest.mock

import jinja2

DAGS = pathlib.Path(__file__).resolve().parents[1] / "dags"
STUBBED = [
    "airflow",
    "airflow.exceptions",
    "airflow.models",
    "airflow.operators",
    "airflow.operators.python",
    "airflow.providers",
    "airflow.providers.cncf",
    "airflow.providers.cncf.kubernetes",
    "airflow.providers.cncf.kubernetes.operators",
    "airflow.providers.cncf.kubernetes.operators.spark_kubernetes",
    "pendulum",
]


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


def load(name, variables=None):
    """Import a DAG file with Airflow stubbed; `variables` = the Airflow Variables (name -> JSON text) that the file will find."""

    class _Variable:
        @staticmethod
        def get(key, default_var=None):
            return (variables or {}).get(key, default_var)

    for module in STUBBED:
        sys.modules[module] = types.ModuleType(module)
    sys.modules["airflow"].DAG = _Recorder
    sys.modules["airflow.exceptions"].AirflowFailException = _AirflowFailException
    sys.modules["airflow.models"].Variable = _Variable
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
        sys.modules.pop("silver_ingest_factory", None)  # it holds the stubs of this load: import it afresh next time


class _Dag:
    """One DAG of a loaded module: its id, its SOURCE constant and the objects recorded while it was built (DAG, then its two tasks)."""

    def __init__(self, dag_id, source, recorded):
        self.DAG_ID = dag_id
        self.SOURCE = source
        self.recorded = recorded
        self.kwargs = recorded[0].kwargs


def _dags_of(module):
    return {
        r.args[0]: _Dag(r.args[0], module.SOURCES[r.args[0]], module.recorded[i : i + 3])
        for i, r in enumerate(module.recorded)
        if r.args and r.args[0] in module.SOURCES
    }


NIMS_ID = "silver_ingest_vtnet_nims"
EBS_ID = "silver_ingest_vtnet_ebs_3g_4g"
VTT_ID = "silver_ingest_vtt_f001_mpre_call"

NIMS_CONFIG = {
    "ticket": "SLV-2026-000001",
    "table": "vtnet_nims_infra_cell_4g",
    "cron": "0 3 * * *",
    "encryptColumns": [],
}
NIMS_ENCRYPTING_CONFIG = {
    "ticket": "SLV-2026-000003",
    "table": "vtnet_nims_infra_cell_4g_enc",
    "cron": "15 4 * * *",
    "outputMode": "append",
    "encryptColumns": ["lng", "lat"],
    "keyField": "eci",
}
EBS_CONFIG = {"ticket": "SLV-2026-000002", "table": "vtnet_ebs_3g_4g", "cron": "30 2 * * *"}
VTT_CONFIG = {"ticket": "SLV-2026-000004", "table": "vtt_f001_mpre_call", "cron": "30 1 * * *"}


def variables(configs):
    """The Airflow Variables of a test: {name: config as a dict or as raw text} -> {name: JSON text}."""
    return {name: config if isinstance(config, str) else json.dumps(config) for name, config in configs.items()}


vtt_module = load("silver_ingest_vtt", variables({VTT_ID: VTT_CONFIG}))
vtnet_module = load("silver_ingest_vtnet", variables({NIMS_ID: NIMS_CONFIG, EBS_ID: EBS_CONFIG}))
encrypting_module = load("silver_ingest_vtnet", variables({NIMS_ID: NIMS_ENCRYPTING_CONFIG, EBS_ID: EBS_CONFIG}))
unscheduled_vtnet = load("silver_ingest_vtnet", {})
unscheduled_vtt = load("silver_ingest_vtt", {})
invalid_module = load("silver_ingest_vtnet", variables({NIMS_ID: "this is not json", EBS_ID: {"ticket": "bad"}}))
common = vtt_module.common

_VTT = _dags_of(vtt_module)
_VTNET = _dags_of(vtnet_module)
VTT = _VTT[VTT_ID]
EBS = _VTNET[EBS_ID]
NIMS = _VTNET[NIMS_ID]
NIMS_ENC = _dags_of(encrypting_module)[NIMS_ID]
ALL_DAGS = (VTT, EBS, NIMS)

ROOT_VTT = vtt_module.INPUT_ROOT
ROOT_VTNET = vtnet_module.INPUT_ROOT


class _Pendulum(datetime.datetime):
    """The `data_interval_start` of a run: a datetime that converts to a time zone like pendulum does."""

    def in_timezone(self, name):
        assert name == "Asia/Ho_Chi_Minh"
        return self.astimezone(datetime.timezone(datetime.timedelta(hours=7)))


class _DagRun:
    def __init__(self, run_id, conf):
        self.run_id = run_id
        self.conf = conf


# 03:00 on 28 October in Ho Chi Minh City is 20:00 on 27 October UTC: the run covers the interval that starts at 03:00 on 27 October.
RUN_INTERVAL_START = _Pendulum(2026, 10, 26, 20, 0, tzinfo=datetime.timezone.utc)


def render(dag, conf=None, run_id="scheduled__2026-10-27T03:00:00", interval_start=RUN_INTERVAL_START):
    """Render spec bằng Jinja thường (render_template_as_native_obj=False) với macro k8s_name và data_interval_start như Airflow."""
    environment = jinja2.Environment()
    context = {
        "dag_run": _DagRun(run_id, conf),
        "k8s_name": common.k8s_name,
        "input_path": common.input_path,
        "run_date": common.run_date,
        "batch_hours": common.batch_hours,
        "data_interval_start": interval_start,
    }

    def walk(node):
        if isinstance(node, dict):
            return {key: walk(value) for key, value in node.items()}
        if isinstance(node, list):
            return [walk(value) for value in node]
        if isinstance(node, str):
            return environment.from_string(node).render(**context)
        return node

    return walk(common.build_spec(dag.SOURCE))


def driver_env(spec):
    return {item["name"]: item.get("value", item.get("valueFrom")) for item in spec["spec"]["driver"]["env"]}


def task(dag, task_id):
    return next(r for r in dag.recorded if r.kwargs.get("task_id") == task_id)


class ScheduleConfigTest(unittest.TestCase):
    """The settings that DAK writes into the Airflow Variable of a dataset when the request is approved."""

    def test_valid_settings_are_accepted(self):
        common.check_schedule_config(VTT.SOURCE, dict(VTT_CONFIG))
        common.check_schedule_config(EBS.SOURCE, dict(EBS_CONFIG))
        common.check_schedule_config(NIMS.SOURCE, dict(NIMS_CONFIG))
        common.check_schedule_config(NIMS.SOURCE, dict(NIMS_ENCRYPTING_CONFIG))

    def test_nims_accepts_no_columns_to_encrypt_the_files_are_then_only_decrypted(self):
        for extra in ({}, {"encryptColumns": []}, {"encryptColumns": [], "keyField": ""}):
            config = {"ticket": "SLV-2026-000001", "table": "vtnet_nims_x", "cron": "0 3 * * *", **extra}
            common.check_schedule_config(NIMS.SOURCE, config)

    def test_the_cron_must_be_a_daily_or_an_hourly_cron_and_nothing_else(self):
        for good in ("0 3 * * *", "30 2 * * *", "5 23 * * *", "59 0 * * *", "0 0 * * *", "5 * * * *", "0 * * * *", "59 * * * *"):
            common.check_schedule_config(EBS.SOURCE, {**EBS_CONFIG, "cron": good})
        for bad in (
            None, "", 7, "*/5 * * * *", "* * * * *", "*/2 3 * * *", "0 3 * * 1", "0 3 1 * *", "0 */2 * * *", "60 3 * * *", "0 24 * * *",
            "0 3 * * * ", "@daily", "0 3 * * *; x",
        ):
            with self.assertRaises(ValueError, msg=repr(bad)):
                common.check_schedule_config(EBS.SOURCE, {**EBS_CONFIG, "cron": bad})

    def test_an_hourly_cron_needs_a_dataset_with_an_hour_layout(self):
        without_layout = {**EBS.SOURCE, "hour_glob": ""}
        common.check_schedule_config(without_layout, {**EBS_CONFIG, "cron": "30 2 * * *"})
        with self.assertRaises(ValueError):
            common.check_schedule_config(without_layout, {**EBS_CONFIG, "cron": "5 * * * *"})

    def test_ticket_table_and_output_mode_are_checked(self):
        for override in (
            {"ticket": "slv-2026-000001"},
            {"ticket": "SLV-26-1"},
            {"ticket": None},
            {"table": "vtt_x"},
            {"table": "VTNET_x"},
            {"table": "vtnet_" + "x" * 130},
            {"table": "vtnet_x y"},
            {"table": None},
            {"outputMode": "overwrite"},
            {"outputMode": "merge"},
        ):
            config = {**EBS_CONFIG, **override}
            for key, value in override.items():
                if value is None:
                    del config[key]
            with self.assertRaises(ValueError, msg=repr(override)):
                common.check_schedule_config(EBS.SOURCE, config)

    def test_a_table_must_carry_the_prefix_of_its_own_source(self):
        with self.assertRaises(ValueError):
            common.check_schedule_config(VTT.SOURCE, {**VTT_CONFIG, "table": "vtnet_x"})
        with self.assertRaises(ValueError):
            common.check_schedule_config(NIMS.SOURCE, {**NIMS_CONFIG, "table": "vtt_x"})

    def test_unknown_keys_are_rejected_and_a_passthrough_dataset_takes_no_columns(self):
        for source, config in (
            (EBS.SOURCE, {**EBS_CONFIG, "path": "x"}),
            (EBS.SOURCE, {**EBS_CONFIG, "batchId": "20261027"}),
            (EBS.SOURCE, {**EBS_CONFIG, "encryptColumns": ["imsi"]}),
            (EBS.SOURCE, {**EBS_CONFIG, "keyField": "event_time"}),
            (VTT.SOURCE, {**VTT_CONFIG, "encryptColumns": ["msisdn"]}),
            (VTT.SOURCE, {**VTT_CONFIG, "keyField": "start_time"}),
            (NIMS.SOURCE, {**NIMS_CONFIG, "keyPath": "x"}),
        ):
            with self.assertRaises(ValueError, msg=repr(config)):
                common.check_schedule_config(source, config)

    def test_a_decrypt_encrypt_dataset_needs_valid_columns_and_a_key_field_when_there_are_columns(self):
        for override in (
            {"encryptColumns": "lng,lat", "keyField": "eci"},
            {"encryptColumns": ["lng", "bad-col"], "keyField": "eci"},
            {"encryptColumns": ["lng", "LNG"], "keyField": "eci"},
            {"encryptColumns": ["c%d" % i for i in range(101)], "keyField": "eci"},
            {"encryptColumns": ["lng"]},
            {"encryptColumns": ["lng"], "keyField": ""},
            {"encryptColumns": ["lng"], "keyField": "bad col"},
            {"encryptColumns": ["lng"], "keyField": "LNG"},
        ):
            config = {"ticket": "SLV-2026-000001", "table": "vtnet_nims_x", "cron": "0 3 * * *", **override}
            with self.assertRaises(ValueError, msg=repr(override)):
                common.check_schedule_config(NIMS.SOURCE, config)

    def test_it_must_be_an_object_and_messages_do_not_echo_the_value(self):
        for bad in (None, [], "text", 7):
            with self.assertRaises(ValueError, msg=repr(bad)):
                common.check_schedule_config(EBS.SOURCE, bad)
        with self.assertRaises(ValueError) as caught:
            common.check_schedule_config(EBS.SOURCE, {**EBS_CONFIG, "table": "secret value"})
        self.assertNotIn("secret", str(caught.exception))


class RunConfTest(unittest.TestCase):
    """A scheduled run has no conf; a manual run may pick the day to ingest."""

    def test_no_conf_and_a_real_date_are_accepted(self):
        for dag in ALL_DAGS:
            for conf in (None, {}, {"date": "20261027"}, {"date": "20240229"}):
                common.check_conf(dag.SOURCE, conf)

    def test_the_date_must_be_a_real_yyyymmdd(self):
        for bad in ("", "2026-10-27", "2026102", "202610277", "20261340", "20260230", 20261027, None, "2026102a"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                common.check_conf(NIMS.SOURCE, {"date": bad})

    def test_everything_else_comes_from_the_approved_request_so_it_is_not_accepted_in_conf(self):
        for dag in ALL_DAGS:
            for conf in (
                {"path": "nims"},
                {"batchId": "20261027"},
                {"table": "vtnet_other"},
                {"encryptColumns": ["a"]},
                {"outputMode": "append"},
                {"date": "20261027", "table": "vtnet_x"},
            ):
                with self.assertRaises(ValueError, msg=repr(conf)):
                    common.check_conf(dag.SOURCE, conf)

    def test_the_validate_task_accepts_a_manual_date_and_turns_errors_into_airflow_failures(self):
        for dag in ALL_DAGS:
            validate = task(dag, "validate_conf").kwargs["python_callable"]
            validate(dag_run=_DagRun("r", {}))
            validate(dag_run=_DagRun("r", {"date": "20261001"}))
            with self.assertRaises(_AirflowFailException):
                validate(dag_run=_DagRun("r", {"date": "20261340"}))
            with self.assertRaises(_AirflowFailException):
                validate(dag_run=_DagRun("r", {"table": "vtnet_other"}))

    def test_messages_do_not_echo_the_value(self):
        with self.assertRaises(ValueError) as caught:
            common.check_conf(VTT.SOURCE, {"secret-key": "secret-value"})
        self.assertNotIn("secret", str(caught.exception).replace("(only date, fromHour, toHour are accepted)", ""))


class ScheduleTest(unittest.TestCase):
    def test_there_is_one_dag_per_dataset_and_each_takes_its_schedule_from_the_approved_request(self):
        self.assertEqual(set(_VTNET), {f"silver_ingest_vtnet_{name}" for name in vtnet_module.DATASETS})
        self.assertEqual(set(_VTT), {f"silver_ingest_vtt_{name.replace('-', '_')}" for name in vtt_module.DATASETS})
        for module, dags in ((vtnet_module, _VTNET), (vtt_module, _VTT)):
            for dag_id in dags:
                self.assertTrue(hasattr(module, dag_id), dag_id)  # Airflow finds the DAGs among the module globals
        self.assertEqual(NIMS.kwargs["schedule"], "0 3 * * *")
        self.assertEqual(EBS.kwargs["schedule"], "30 2 * * *")
        self.assertEqual(VTT.kwargs["schedule"], "30 1 * * *")
        self.assertEqual(NIMS_ENC.kwargs["schedule"], "15 4 * * *")
        self.assertIn("SLV-2026-000001", NIMS.kwargs["doc_md"])
        self.assertIn("SLV-2026-000004", VTT.kwargs["doc_md"])

    def test_a_dataset_without_an_approved_request_has_no_schedule_and_fails_if_it_is_run_by_hand(self):
        for dag in list(_dags_of(unscheduled_vtnet).values()) + list(_dags_of(unscheduled_vtt).values()):
            self.assertIsNone(dag.kwargs["schedule"], dag.DAG_ID)
            self.assertEqual(dag.SOURCE["fixed"], {})
            validate = task(dag, "validate_conf").kwargs["python_callable"]
            with self.assertRaises(_AirflowFailException) as caught:
                validate(dag_run=_DagRun("r", {}))
            self.assertIn("no approved request", str(caught.exception))

    def test_settings_that_are_not_valid_are_ignored_and_the_reason_is_shown(self):
        dags = _dags_of(invalid_module)
        nims = dags[NIMS_ID]
        ebs = dags[EBS_ID]
        for dag in (nims, ebs):
            self.assertIsNone(dag.kwargs["schedule"], dag.DAG_ID)
            self.assertEqual(dag.SOURCE["fixed"], {})
            with self.assertRaises(_AirflowFailException):
                task(dag, "validate_conf").kwargs["python_callable"](dag_run=_DagRun("r", {}))
        self.assertIn("not valid JSON", nims.kwargs["doc_md"])
        self.assertIn("ticket", ebs.kwargs["doc_md"])
        self.assertEqual(invalid_module.CONFIG_PROBLEMS[NIMS_ID], "the Airflow Variable is not valid JSON")

    def test_one_run_at_a_time_no_backfill_and_plain_rendering(self):
        for dag in ALL_DAGS:
            self.assertEqual(dag.kwargs["max_active_runs"], 1)
            self.assertIs(dag.kwargs["catchup"], False)
            self.assertIs(dag.kwargs["is_paused_upon_creation"], False)
            self.assertIs(dag.kwargs["render_template_as_native_obj"], False)
            self.assertIs(dag.kwargs["user_defined_macros"]["k8s_name"], common.k8s_name)
            self.assertEqual(dag.recorded[0].args[0], dag.DAG_ID)


class SpecTest(unittest.TestCase):
    def test_each_mode_runs_its_own_main_class(self):
        passthrough = "vai.lakehouse.silveringest.app.PassthroughSilverApp"
        self.assertEqual(render(VTT)["spec"]["mainClass"], passthrough)
        self.assertEqual(render(EBS)["spec"]["mainClass"], passthrough)
        self.assertEqual(render(NIMS)["spec"]["mainClass"], "vai.lakehouse.silveringest.app.DecryptEncryptSilverApp")

    def test_a_run_ingests_the_folder_of_its_day_into_the_partition_of_that_day(self):
        for dag, root, folder, table in (
            (NIMS, ROOT_VTNET, "nims/dt=20261027", "vtnet_nims_infra_cell_4g"),
            (EBS, ROOT_VTNET, "ebs_3g_4g/dt=20261027", "vtnet_ebs_3g_4g"),
            (VTT, ROOT_VTT, "f001-mpre-call/date=20261027", "vtt_f001_mpre_call"),
        ):
            env = driver_env(render(dag))
            self.assertEqual(env["INPUT_PATH"], root + folder, dag.DAG_ID)
            self.assertEqual((env["BATCH_COLUMN"], env["BATCH_VALUE"]), ("dt", "20261027"), dag.DAG_ID)
            self.assertEqual((env["OUTPUT_DB"], env["OUTPUT_TABLE"]), ("demo_db", table), dag.DAG_ID)
            self.assertEqual(env["OUTPUT_MODE"], "overwrite-partitions", dag.DAG_ID)
        self.assertTrue(ROOT_VTT.endswith("/") and ROOT_VTNET.endswith("/"))

    def test_the_day_is_the_local_day_at_the_start_of_the_data_interval_not_the_utc_date(self):
        # The interval that starts at 03:00 on 27 October (20:00 on 26 October UTC): airflow's `ds` would say 26 October, the data day is 27.
        self.assertEqual(driver_env(render(NIMS))["BATCH_VALUE"], "20261027")
        later = _Pendulum(2026, 10, 27, 20, 0, tzinfo=datetime.timezone.utc)  # 03:00 on 28 October local time
        self.assertEqual(driver_env(render(NIMS, interval_start=later))["BATCH_VALUE"], "20261028")

    def test_a_manual_run_can_ingest_another_day(self):
        for dag, folder in ((NIMS, "nims/dt=20261001"), (VTT, "f001-mpre-call/date=20261001")):
            env = driver_env(render(dag, {"date": "20261001"}, run_id="manual__2026-10-28"))
            self.assertTrue(env["INPUT_PATH"].endswith(folder), dag.DAG_ID)
            self.assertEqual(env["BATCH_VALUE"], "20261001")

    def test_the_table_columns_and_output_mode_are_the_ones_of_the_approved_request(self):
        env = driver_env(render(NIMS_ENC))
        self.assertEqual(env["OUTPUT_TABLE"], "vtnet_nims_infra_cell_4g_enc")
        self.assertEqual(env["OUTPUT_MODE"], "append")
        self.assertEqual(env["CDR_ENCRYPT_COLUMNS"], "lng,lat")
        self.assertEqual(env["CDR_KEY_FIELD"], "eci")
        self.assertEqual(env["CDR_ON_MISSING"], "request")

    def test_a_decrypt_encrypt_dataset_without_columns_renders_empty_column_env_so_the_job_only_decrypts(self):
        env = driver_env(render(NIMS))
        self.assertEqual((env["CDR_ENCRYPT_COLUMNS"], env["CDR_KEY_FIELD"]), ("", ""))
        self.assertEqual(env["CRYPTO_PREFIX_SOURCE"], "dak")  # DAK is still needed for by-path

    def test_passthrough_dags_have_no_dak_or_decrypt_env(self):
        for dag in (VTT, EBS):
            names = set(driver_env(render(dag)))
            self.assertFalse({n for n in names if n.startswith(("DAK_", "CDR_", "FILE_", "KEY_", "CRYPTO_"))}, dag.DAG_ID)

    def test_a_day_folder_is_read_recursively_without_turning_the_batch_folder_into_a_column(self):
        for dag in ALL_DAGS:
            self.assertEqual(driver_env(render(dag))["INPUT_OPTION_recursiveFileLookup"], "true", dag.DAG_ID)

    def test_the_key_of_the_dataset_is_asked_from_dak_with_the_name_of_the_dataset(self):
        env = driver_env(render(NIMS))
        self.assertEqual(env["FILE_CIPHER"], "aes-256-gcm")
        # One by-path key for the whole dataset: the same KEY_PATH for every day, no KEY_PATH_ROOT.
        self.assertEqual(env["KEY_PATH"], "nims")
        self.assertEqual(driver_env(render(NIMS, interval_start=_Pendulum(2026, 10, 27, 20, 0, tzinfo=datetime.timezone.utc)))["KEY_PATH"], "nims")
        self.assertNotIn("KEY_PATH_ROOT", env)
        # Chờ admin DAK duyệt yêu cầu key cột theo exponential backoff, có timeout.
        self.assertEqual(env["DAK_PENDING_WAIT_TIMEOUT_SECONDS"], str(vtnet_module.PENDING_WAIT["timeout"]))
        self.assertGreater(int(env["DAK_PENDING_WAIT_TIMEOUT_SECONDS"]), 0)
        self.assertLessEqual(int(env["DAK_PENDING_WAIT_INITIAL_SECONDS"]), int(env["DAK_PENDING_WAIT_MAX_INTERVAL_SECONDS"]))
        self.assertGreaterEqual(float(env["DAK_PENDING_WAIT_MULTIPLIER"]), 1.0)

    def test_nims_driver_reads_the_dak_secret_by_reference_and_executors_get_no_dak_env(self):
        spec = render(NIMS)
        env = driver_env(spec)
        self.assertEqual(env["DAK_CLIENT_SECRET"], {"secretKeyRef": {"name": "spark-dak-client", "key": "clientSecret"}})
        self.assertEqual((env["CRYPTO_PREFIX_SOURCE"], env["DAK_TAG"]), ("dak", "internal"))
        executor = {item["name"] for item in spec["spec"]["executor"]["env"]}
        self.assertFalse({n for n in executor if n.startswith(("DAK_", "CDR_", "FILE_", "CRYPTO_", "KEY_"))})

    def test_nims_driver_trusts_the_internal_ca_and_resolves_keycloak_like_the_cdr_manifest(self):
        # k8s/spark-application-cdr.yaml: hostAliases + truststore dựng bằng initContainer + mount ConfigMap keycloak-ca + trustStore của driver.
        spec = render(NIMS)["spec"]
        driver = spec["driver"]

        self.assertEqual(driver["hostAliases"], [{"ip": common.KEYCLOAK_IP, "hostnames": [common.KEYCLOAK_HOST]}])
        init = driver["initContainers"][0]
        self.assertEqual(init["name"], "build-truststore")
        self.assertEqual(init["image"], common.IMAGE)
        self.assertIn("limits", init["resources"])  # ResourceQuota của namespace từ chối container thiếu limits
        # Như initContainer build-truststore của dak-mock.yaml (không leo quyền, bỏ capability, không chạy root, có runAsUser) nhưng rootfs
        # KHÔNG chỉ đọc: cần runAsUser mới chạy được, và bỏ readOnlyRootFilesystem.
        self.assertEqual(
            init["securityContext"],
            {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}, "runAsNonRoot": True, "runAsUser": common.SPARK_UID},
        )
        # JAVA_TOOL_OPTIONS rỗng để keytool không bị cờ truststore (chưa có file) của môi trường kéo theo.
        self.assertEqual(init["env"], [{"name": "JAVA_TOOL_OPTIONS", "value": ""}])
        self.assertEqual({m["name"] for m in init["volumeMounts"]}, {"keycloak-ca", "truststore"})
        # Webhook của Spark Operator chỉ đưa vào pod các volume được mount ở container chính.
        self.assertEqual({m["name"] for m in driver["volumeMounts"]}, {"keycloak-ca", "truststore"})
        self.assertEqual({v["name"] for v in spec["volumes"]}, {"keycloak-ca", "truststore"})
        self.assertEqual(next(v for v in spec["volumes"] if v["name"] == "keycloak-ca")["configMap"]["name"], "keycloak-ca")
        options = spec["sparkConf"]["spark.driver.extraJavaOptions"]
        self.assertIn("-Djavax.net.ssl.trustStore=/opt/truststore/truststore.p12", options)
        self.assertIn("-Dlog4j.configurationFile=file:/opt/app/log4j2.properties", options)
        # Executor không gọi Keycloak/DAK: không cần truststore.
        self.assertNotIn("trustStore", spec["sparkConf"]["spark.executor.extraJavaOptions"])
        self.assertNotIn("hostAliases", spec["executor"])
        self.assertNotIn("initContainers", spec["executor"])

    def test_passthrough_dags_do_not_call_keycloak_so_they_have_no_hostalias_truststore_or_volumes(self):
        for dag in (VTT, EBS):
            spec = render(dag)["spec"]

            self.assertNotIn("hostAliases", spec["driver"], dag.DAG_ID)
            self.assertNotIn("initContainers", spec["driver"], dag.DAG_ID)
            self.assertNotIn("volumeMounts", spec["driver"], dag.DAG_ID)
            self.assertNotIn("volumes", spec, dag.DAG_ID)
            self.assertNotIn("trustStore", spec["sparkConf"]["spark.driver.extraJavaOptions"], dag.DAG_ID)

    def test_the_application_name_is_valid_and_unique_per_run(self):
        for dag, pattern in (
            (NIMS, r"^silver-vtnet-nims-[0-9a-f]{12}$"),
            (EBS, r"^silver-vtnet-ebs-3g-4g-[0-9a-f]{12}$"),
            (VTT, r"^silver-vtt-f001-mpre-call-[0-9a-f]{12}$"),
        ):
            a = render(dag, run_id="scheduled__2026-10-27T03:00:00")["metadata"]["name"]
            b = render(dag, run_id="scheduled__2026-10-28T03:00:00")["metadata"]["name"]
            self.assertNotEqual(a, b)
            for name in (a, b):
                self.assertRegex(name, pattern)
                self.assertLessEqual(len(name), 63)

    def test_every_env_value_and_spark_conf_is_a_string_and_jars_include_cdr_crypto(self):
        for dag in (VTT, EBS, NIMS, NIMS_ENC):
            spec = render(dag)["spec"]
            for role in ("driver", "executor"):
                for item in spec[role]["env"]:
                    if "value" in item:
                        self.assertIsInstance(item["value"], str, item["name"])
            for key, value in spec["sparkConf"].items():
                self.assertIsInstance(value, str, key)
            self.assertIn("cdr-crypto-udf", spec["sparkConf"]["spark.jars"])
            self.assertIn("DataLakeSecurity_jv8.jar", spec["sparkConf"]["spark.jars"])

    def test_the_validate_task_runs_before_the_job(self):
        for dag in ALL_DAGS:
            self.assertTrue(callable(task(dag, "validate_conf").kwargs["python_callable"]))
            self.assertEqual(task(dag, "ingest").kwargs["namespace"], common.NAMESPACE)


class HoursTest(unittest.TestCase):
    """A manual run can ingest only some hours of a day; each dataset writes the hour in its own way."""

    def path(self, dag, conf):
        return driver_env(render(dag, conf, run_id="manual__x"))["INPUT_PATH"]

    def test_each_dataset_selects_the_hour_the_way_its_data_is_laid_out(self):
        one = {"date": "20261013", "fromHour": "10"}
        many = {"date": "20261013", "fromHour": "10", "toHour": "12"}
        self.assertEqual(self.path(VTT, one), ROOT_VTT + "f001-mpre-call/date=20261013/hour=10")
        self.assertEqual(self.path(VTT, many), ROOT_VTT + "f001-mpre-call/date=20261013/hour={10,11,12}")
        self.assertEqual(self.path(EBS, one), ROOT_VTNET + "ebs_3g_4g/dt=20261013/batch=10*")
        self.assertEqual(self.path(EBS, many), ROOT_VTNET + "ebs_3g_4g/dt=20261013/batch={10,11,12}*")
        self.assertEqual(self.path(NIMS, one), ROOT_VTNET + "nims/dt=20261013/batch=*/infra_cell_4g-10*")
        self.assertEqual(self.path(NIMS, many), ROOT_VTNET + "nims/dt=20261013/batch=*/infra_cell_4g-{10,11,12}*")

    def test_hours_are_two_digits_and_may_be_given_as_numbers_or_text(self):
        for conf in ({"date": "20261013", "fromHour": 5, "toHour": 7}, {"date": "20261013", "fromHour": "5", "toHour": "07"}):
            self.assertTrue(self.path(VTT, conf).endswith("hour={05,06,07}"), conf)

    def test_every_run_writes_the_day_into_dt_and_says_which_hours_it_replaces(self):
        env = lambda dag, conf: driver_env(render(dag, conf, run_id="manual__x"))  # noqa: E731
        for conf, hours in (
            ({"date": "20261013"}, ""),
            ({"date": "20261013", "fromHour": "10"}, "10"),
            ({"date": "20261013", "fromHour": "10", "toHour": "12"}, "10,11,12"),
        ):
            for dag in ALL_DAGS:
                # The batch is the DAY whatever the run is: no other batch format exists, so runs by day and by hour cannot disagree.
                self.assertEqual((env(dag, conf)["BATCH_COLUMN"], env(dag, conf)["BATCH_VALUE"]), ("dt", "20261013"), (dag.DAG_ID, conf))
                self.assertEqual(env(dag, conf)["BATCH_HOURS"], hours, (dag.DAG_ID, conf))
        self.assertEqual(driver_env(render(EBS))["BATCH_HOURS"], "")  # a scheduled run replaces the whole day

    def test_the_hour_column_is_read_from_the_path_with_the_regex_of_the_dataset(self):
        import re

        for dag, paths in (
            (VTT, {"hdfs://nn/x/f001-mpre-call/date=20261013/hour=09/part-1.csv": "09"}),
            (EBS, {"hdfs://nn/x/ebs_3g_4g/dt=20261013/batch=1520/ebs_3g_4g-152000.csv": "15"}),
            (NIMS, {"hdfs://nn/x/nims/dt=20261013/batch=2/infra_cell_4g-130012.csv": "13"}),
        ):
            regex = driver_env(render(dag))["HOUR_PATH_REGEX"]
            self.assertEqual(re.compile(regex).groups, 1, dag.DAG_ID)
            for path, hour in paths.items():
                self.assertEqual(re.search(regex, path).group(1), hour, dag.DAG_ID)
            self.assertIsNone(re.search(regex, "hdfs://nn/x/other/dt=20261013/other/file.csv"), dag.DAG_ID)
            self.assertEqual(driver_env(render(dag))["HOUR_COLUMN"], "hour")

    def test_only_the_big_datasets_partition_the_table_by_hour_nims_stays_partitioned_by_day(self):
        self.assertEqual(driver_env(render(VTT))["OUTPUT_PARTITION_BY"], "hour")
        self.assertEqual(driver_env(render(EBS))["OUTPUT_PARTITION_BY"], "hour")
        self.assertNotIn("OUTPUT_PARTITION_BY", driver_env(render(NIMS)))

    def test_a_dataset_without_an_hour_layout_has_no_hour_settings(self):
        source = {**EBS.SOURCE, "hour_glob": ""}
        names = {item["name"] for item in common.build_spec(source)["spec"]["driver"]["env"]}
        self.assertFalse(names & {"HOUR_PATH_REGEX", "HOUR_COLUMN", "BATCH_HOURS", "OUTPUT_PARTITION_BY"})

    def test_a_scheduled_run_and_a_manual_day_run_still_ingest_the_whole_day(self):
        self.assertTrue(driver_env(render(EBS))["INPUT_PATH"].endswith("ebs_3g_4g/dt=20261027"))
        self.assertTrue(self.path(EBS, {"date": "20261013"}).endswith("ebs_3g_4g/dt=20261013"))

    def test_hours_need_a_day_a_sane_range_and_a_dataset_with_an_hour_layout(self):
        for dag in ALL_DAGS:
            common.check_conf(dag.SOURCE, {"date": "20261013", "fromHour": "00", "toHour": "23"})
            for bad in (
                {"fromHour": "10"},  # the day is missing
                {"date": "20261013", "toHour": "10"},  # toHour alone
                {"date": "20261013", "fromHour": "12", "toHour": "10"},
                {"date": "20261013", "fromHour": "24"},
                {"date": "20261013", "fromHour": -1},
                {"date": "20261013", "fromHour": "ab"},
                {"date": "20261013", "fromHour": "100"},
                {"date": "20261013", "fromHour": True},
                {"date": "20261013", "fromHour": "10", "toHour": None},
            ):
                with self.assertRaises(ValueError, msg=repr(bad)):
                    common.check_conf(dag.SOURCE, bad)
        without_layout = {**EBS.SOURCE, "hour_glob": ""}
        with self.assertRaises(ValueError):
            common.check_conf(without_layout, {"date": "20261013", "fromHour": "10"})
        common.check_conf(without_layout, {"date": "20261013"})

    def test_the_validate_task_rejects_hours_without_a_day(self):
        validate = task(EBS, "validate_conf").kwargs["python_callable"]
        validate(dag_run=_DagRun("r", {"date": "20261013", "fromHour": "10"}))
        with self.assertRaises(_AirflowFailException):
            validate(dag_run=_DagRun("r", {"fromHour": "10"}))

    def test_every_dataset_declares_an_hour_glob_with_the_hour_token(self):
        for dag in ALL_DAGS:
            self.assertIn(common.HOUR_TOKEN, dag.SOURCE["hour_glob"], dag.DAG_ID)
            self.assertNotIn("}}", dag.SOURCE["hour_glob"])  # it is written inside a Jinja expression


class DatasetSettingsTest(unittest.TestCase):
    """The entry of a dataset in DATASETS: one place for everything specific to it, checked when the DAG file is parsed."""

    GOOD = {"mode": "passthrough", "hour_glob": "hour=%H%", "partition_by_hour": True, "resources": {"executors": 10, "executor_memory": "8g"}}

    def test_the_entries_of_the_real_dag_files_are_valid(self):
        for module in (vtt_module, vtnet_module):
            for dataset, settings in module.DATASETS.items():
                common.check_dataset(dataset, settings)

    def test_a_full_entry_and_the_smallest_entry_are_valid(self):
        common.check_dataset("d", self.GOOD)
        common.check_dataset("d", {"mode": "passthrough"})
        common.check_dataset("d", {"mode": "decrypt-encrypt", "key_path": "nims", "hour_glob": "batch=*/f-%H%*"})

    def test_a_typo_or_a_wrong_value_is_rejected_and_names_the_dataset(self):
        for bad in (
            None,
            [],
            {},
            {"mode": "other"},
            {"mode": "passthrough", "hour_glb": "hour=%H%"},  # a typo must not silently fall back to a default
            {"mode": "passthrough", "hour_glob": "hour="},  # no hour token
            {"mode": "passthrough", "hour_glob": "/hour=%H%"},
            {"mode": "passthrough", "hour_glob": "hour=%H%/"},
            {"mode": "passthrough", "hour_glob": "a//hour=%H%"},
            {"mode": "passthrough", "hour_glob": "hour='%H%"},  # would break out of the Jinja string
            {"mode": "passthrough", "hour_glob": "hour={%H%}"},
            {"mode": "passthrough", "hour_glob": "hour= %H%"},
            {"mode": "passthrough", "hour_glob": 5},
            {"mode": "passthrough", "partition_by_hour": True},  # the hour is read from the path: needs hour_glob
            {"mode": "passthrough", "hour_glob": "hour=%H%", "partition_by_hour": "yes"},
            {"mode": "passthrough", "key_path": "nims"},  # decrypt-encrypt only
            {"mode": "decrypt-encrypt", "key_path": " "},
            {"mode": "decrypt-encrypt", "key_path": 7},
            {"mode": "passthrough", "resources": "big"},
        ):
            with self.assertRaises(ValueError, msg=repr(bad)) as caught:
                common.check_dataset("my-dataset", bad)
            self.assertIn("my-dataset", str(caught.exception))

    def test_resources_must_be_known_keys_with_sane_values(self):
        for good in ({}, {"executors": 1}, {"executors": 100}, {"executor_memory": "256m"}, {"driver_memory": "64g"}, {"shuffle_partitions": 20000}):
            common.check_resources("d", good)
        for bad in (
            {"executor": 4},  # typo of "executors"
            {"executors": 0},
            {"executors": 101},
            {"executors": "4"},
            {"executors": 4.0},
            {"executors": True},
            {"executor_cores": 17},
            {"driver_cores": 5},
            {"shuffle_partitions": 0},
            {"shuffle_partitions": 20001},
            {"executor_memory": "4096"},  # no unit
            {"executor_memory": "4G"},
            {"executor_memory": "0g"},
            {"executor_memory": "255m"},
            {"executor_memory": "65g"},
            {"executor_memory": 4096},
            {"driver_memory_overhead": "-1m"},
        ):
            with self.assertRaises(ValueError, msg=repr(bad)):
                common.check_resources("d", bad)

    def test_resources_of_overrides_only_the_keys_that_are_set(self):
        self.assertEqual(common.resources_of({"mode": "passthrough"}), common.RESOURCE_DEFAULTS)
        mine = common.resources_of({"resources": {"executors": 10}})
        self.assertEqual(mine["executors"], 10)
        self.assertEqual({k: v for k, v in mine.items() if k != "executors"}, {k: v for k, v in common.RESOURCE_DEFAULTS.items() if k != "executors"})

    def test_the_defaults_are_what_every_dataset_used_before_resources_existed(self):
        for dag in ALL_DAGS:
            spec = render(dag)["spec"]
            self.assertEqual(
                (spec["driver"]["cores"], spec["driver"]["coreLimit"], spec["driver"]["memory"], spec["driver"]["memoryOverhead"]),
                (1, "1000m", "2048m", "512m"),
            )
            self.assertEqual(
                (spec["executor"]["instances"], spec["executor"]["cores"], spec["executor"]["coreLimit"], spec["executor"]["memory"],
                 spec["executor"]["memoryOverhead"]),
                (2, 2, "2000m", "4096m", "1024m"),
            )
            self.assertEqual(spec["sparkConf"]["spark.sql.shuffle.partitions"], "64")

    def test_the_resources_of_a_dataset_reach_its_spark_application_and_only_its_own(self):
        big = {**VTT.SOURCE, "resources": common.resources_of({"resources": {
            "executors": 10, "executor_cores": 4, "executor_memory": "8g", "executor_memory_overhead": "2g",
            "driver_cores": 2, "driver_memory": "4g", "shuffle_partitions": 1200}})}
        spec = common.build_spec(big)["spec"]

        self.assertEqual((spec["executor"]["instances"], spec["executor"]["cores"], spec["executor"]["coreLimit"]), (10, 4, "4000m"))
        self.assertEqual((spec["executor"]["memory"], spec["executor"]["memoryOverhead"]), ("8g", "2g"))
        self.assertEqual((spec["driver"]["cores"], spec["driver"]["coreLimit"], spec["driver"]["memory"]), (2, "2000m", "4g"))
        self.assertEqual(spec["driver"]["memoryOverhead"], "512m")  # not set: the default
        self.assertEqual(spec["sparkConf"]["spark.sql.shuffle.partitions"], "1200")
        self.assertEqual(render(EBS)["spec"]["executor"]["instances"], 2)  # another dataset keeps the default

    def test_the_dag_file_checks_its_datasets_when_it_is_parsed(self):
        for bad in ({"mode": "other"}, {"mode": "passthrough", "resources": {"executor": 4}}):
            with self.assertRaises(ValueError):
                vtt_module.factory.register({}, "vtt", {"x": bad}, vtt_module.source_for)

    def test_the_source_of_every_dataset_carries_the_fields_of_its_entry(self):
        for dag in ALL_DAGS:
            self.assertEqual(dag.SOURCE["resources"], common.RESOURCE_DEFAULTS, dag.DAG_ID)
            self.assertIn("hour_glob", dag.SOURCE)
            self.assertIn("partition_by_hour", dag.SOURCE)


HOURLY_EBS_CONFIG = {**EBS_CONFIG, "cron": "5 * * * *"}
hourly_module = load("silver_ingest_vtnet", variables({NIMS_ID: NIMS_CONFIG, EBS_ID: HOURLY_EBS_CONFIG}))
HOURLY_EBS = _dags_of(hourly_module)[EBS_ID]


class HourlyScheduleTest(unittest.TestCase):
    """A request with the hourly cron "<minute> * * * *": each scheduled run ingests the HOUR before."""

    def test_the_dag_takes_the_hourly_cron_as_its_schedule(self):
        self.assertEqual(HOURLY_EBS.kwargs["schedule"], "5 * * * *")
        self.assertEqual(common.schedule_unit("5 * * * *"), "hour")
        self.assertEqual(common.schedule_unit("30 2 * * *"), "day")

    def test_a_scheduled_run_ingests_the_hour_at_the_start_of_its_data_interval(self):
        # The run of 04:05 covers the interval that starts at 03:05 on 27 October local time (20:05 on 26 October UTC): hour 03 of the 27th.
        start = _Pendulum(2026, 10, 26, 20, 5, tzinfo=datetime.timezone.utc)
        env = driver_env(render(HOURLY_EBS, interval_start=start))
        self.assertTrue(env["INPUT_PATH"].endswith("ebs_3g_4g/dt=20261027/batch=03*"))
        self.assertEqual((env["BATCH_COLUMN"], env["BATCH_VALUE"], env["BATCH_HOURS"]), ("dt", "20261027", "03"))

    def test_an_hour_after_midnight_belongs_to_the_local_day_not_the_utc_day(self):
        # 00:05 local on the 28th starts at 23:05 local on the 27th = 16:05 UTC on the 27th: dt 20261027, hour 23.
        start = _Pendulum(2026, 10, 27, 16, 5, tzinfo=datetime.timezone.utc)
        env = driver_env(render(HOURLY_EBS, interval_start=start))
        self.assertEqual((env["BATCH_VALUE"], env["BATCH_HOURS"]), ("20261027", "23"))

    def test_a_manual_run_with_a_day_or_with_hours_overrides_the_schedule(self):
        whole = driver_env(render(HOURLY_EBS, {"date": "20261013"}, run_id="manual__x"))
        self.assertEqual((whole["BATCH_VALUE"], whole["BATCH_HOURS"]), ("20261013", ""))
        self.assertTrue(whole["INPUT_PATH"].endswith("ebs_3g_4g/dt=20261013"))
        some = driver_env(render(HOURLY_EBS, {"date": "20261013", "fromHour": "10", "toHour": "11"}, run_id="manual__x"))
        self.assertEqual(some["BATCH_HOURS"], "10,11")
        self.assertTrue(some["INPUT_PATH"].endswith("ebs_3g_4g/dt=20261013/batch={10,11}*"))

    def test_a_daily_schedule_still_ingests_the_whole_day(self):
        env = driver_env(render(EBS))
        self.assertEqual(env["BATCH_HOURS"], "")
        self.assertTrue(env["INPUT_PATH"].endswith("ebs_3g_4g/dt=20261027"))

    def test_the_hourly_settings_of_a_dataset_with_an_hour_layout_are_accepted(self):
        # silver_ingest_vtnet declares an hour layout for every dataset, so the settings are valid there; the check is common.check_schedule_config.
        self.assertIsNone(hourly_module.CONFIG_PROBLEMS[EBS_ID])


class BackfillPlanTest(unittest.TestCase):
    """plan_backfill: the validated runs of a backfill."""

    DAG = "silver_ingest_vtnet_ebs_3g_4g"

    def test_a_range_of_days_is_one_run_per_day_in_date_order(self):
        dag_id, confs = common.plan_backfill({"dag_id": self.DAG, "fromDate": "20261030", "toDate": "20261102"})
        self.assertEqual(dag_id, self.DAG)
        self.assertEqual([c["date"] for c in confs], ["20261030", "20261031", "20261101", "20261102"])
        self.assertTrue(all(set(c) == {"date"} for c in confs))

    def test_toDate_defaults_to_the_day_and_hours_are_repeated_on_every_day(self):
        _, one = common.plan_backfill({"dag_id": self.DAG, "fromDate": "20261013", "fromHour": 5})
        self.assertEqual(one, [{"date": "20261013", "fromHour": "05", "toHour": "05"}])
        _, two = common.plan_backfill(
            {"dag_id": self.DAG, "fromDate": "20260228", "toDate": "20260301", "fromHour": "1", "toHour": "3"}
        )
        self.assertEqual(two, [{"date": d, "fromHour": "01", "toHour": "03"} for d in ("20260228", "20260301")])

    def test_every_planned_run_is_a_valid_conf_of_the_dataset_dag(self):
        _, confs = common.plan_backfill(
            {"dag_id": self.DAG, "fromDate": "20261010", "toDate": "20261012", "fromHour": "10", "toHour": "12"}
        )
        for conf in confs:
            common.check_conf(EBS.SOURCE, conf)

    def test_invalid_backfills_are_rejected_without_echoing_the_value(self):
        good = {"dag_id": self.DAG, "fromDate": "20261010"}
        for bad in (
            None,
            [],
            {},
            {**good, "dag_id": "other_dag"},
            {**good, "dag_id": "silver_ingest_backfill"},
            {**good, "dag_id": "silver_ingest_vtnet_x; rm"},
            {"dag_id": self.DAG},
            {**good, "fromDate": "2026-10-10"},
            {**good, "toDate": "20261340"},
            {**good, "toDate": "20261009"},  # before fromDate
            {**good, "toDate": "20261211"},  # more than 31 days
            {**good, "toHour": "10"},
            {**good, "fromHour": "25"},
            {**good, "fromHour": "12", "toHour": "10"},
            {**good, "secret-key": "secret-value"},
        ):
            with self.assertRaises(ValueError, msg=repr(bad)) as caught:
                common.plan_backfill(bad)
            self.assertNotIn("secret-value", str(caught.exception))
            self.assertNotIn("; rm", str(caught.exception))

    def test_a_backfill_covers_at_most_31_days(self):
        common.plan_backfill({"dag_id": self.DAG, "fromDate": "20261001", "toDate": "20261031"})
        with self.assertRaises(ValueError):
            common.plan_backfill({"dag_id": self.DAG, "fromDate": "20261001", "toDate": "20261101"})

    def test_run_ids_are_readable_unique_per_backfill_and_valid_dag_run_ids(self):
        whole = common.backfill_run_id("manual__a", {"date": "20261013"})
        hours = common.backfill_run_id("manual__a", {"date": "20261013", "fromHour": "10", "toHour": "12"})
        self.assertRegex(whole, r"^backfill_20261013_[0-9a-f]{8}$")
        self.assertRegex(hours, r"^backfill_20261013_10-12_[0-9a-f]{8}$")
        self.assertNotEqual(whole, common.backfill_run_id("manual__b", {"date": "20261013"}))  # the same range can be backfilled again


class _Model:
    def __init__(self, is_paused):
        self.is_paused = is_paused


def _fake_airflow(models, triggered, fail_on=None):
    """Modules that the backfill task imports when it runs: `models` = {dag id: DagModel}, `triggered` collects the started runs."""

    class _DagModel:
        @staticmethod
        def get_dagmodel(dag_id):
            return models.get(dag_id)

    def trigger_dag(dag_id, run_id, conf, replace_microseconds):
        if run_id == fail_on:
            raise RuntimeError("secret detail")
        assert replace_microseconds is False
        triggered.append((dag_id, run_id, conf))

    models_module = types.ModuleType("airflow.models")
    models_module.DagModel = _DagModel
    trigger_module = types.ModuleType("airflow.api.common.trigger_dag")
    trigger_module.trigger_dag = trigger_dag
    return {"airflow.models": models_module, "airflow.api.common.trigger_dag": trigger_module}


class BackfillDagTest(unittest.TestCase):
    DAG = "silver_ingest_vtnet_ebs_3g_4g"

    @classmethod
    def setUpClass(cls):
        cls.module = load("silver_ingest_backfill")
        cls.dag = cls.module.recorded[0]
        cls.callback = staticmethod(
            next(r for r in cls.module.recorded if r.kwargs.get("task_id") == "trigger_runs").kwargs["python_callable"]
        )

    def call(self, conf, models, fail_on=None):
        triggered = []
        with unittest.mock.patch.dict(sys.modules, _fake_airflow(models, triggered, fail_on)):
            self.callback(dag_run=_DagRun("manual__2026-10-28", conf))
        return triggered

    def test_the_dag_is_only_run_by_hand_one_backfill_at_a_time(self):
        self.assertEqual(self.dag.args[0], "silver_ingest_backfill")
        self.assertIsNone(self.dag.kwargs["schedule"])
        self.assertEqual(self.dag.kwargs["max_active_runs"], 1)
        self.assertFalse(self.dag.kwargs["catchup"])

    def test_it_starts_one_run_per_day_with_the_conf_of_a_manual_run(self):
        triggered = self.call(
            {"dag_id": self.DAG, "fromDate": "20261010", "toDate": "20261011", "fromHour": "10", "toHour": "12"},
            {self.DAG: _Model(False)},
        )
        self.assertEqual([t[0] for t in triggered], [self.DAG, self.DAG])
        self.assertEqual(
            [t[2] for t in triggered],
            [{"date": d, "fromHour": "10", "toHour": "12"} for d in ("20261010", "20261011")],
        )
        self.assertEqual(len({t[1] for t in triggered}), 2)

    def test_it_fails_before_starting_anything_when_the_conf_is_invalid_or_the_dag_is_missing_or_paused(self):
        for conf, models in (
            ({"dag_id": self.DAG}, {self.DAG: _Model(False)}),
            ({"dag_id": self.DAG, "fromDate": "20261010"}, {}),
            ({"dag_id": self.DAG, "fromDate": "20261010"}, {self.DAG: _Model(True)}),
        ):
            triggered = []
            with unittest.mock.patch.dict(sys.modules, _fake_airflow(models, triggered)):
                with self.assertRaises(_AirflowFailException, msg=repr(conf)):
                    self.callback(dag_run=_DagRun("manual__x", conf))
            self.assertEqual(triggered, [])

    def test_a_failure_in_the_middle_says_how_many_runs_were_started_and_hides_the_detail(self):
        conf = {"dag_id": self.DAG, "fromDate": "20261010", "toDate": "20261012"}
        second = common.backfill_run_id("manual__2026-10-28", {"date": "20261011"})
        triggered = []
        with unittest.mock.patch.dict(sys.modules, _fake_airflow({self.DAG: _Model(False)}, triggered, fail_on=second)):
            with self.assertRaises(_AirflowFailException) as caught:
                self.callback(dag_run=_DagRun("manual__2026-10-28", conf))
        self.assertIn("started 1 of 3", str(caught.exception))
        self.assertNotIn("secret detail", str(caught.exception))
        self.assertEqual(len(triggered), 1)


if __name__ == "__main__":
    unittest.main()
