"""Tests for the VTT datasets of silver_ingest_vtt.py (docs/silver-ingest/TRANSFORM-PHIEU-KHAI-THAC-VTT.md), WITHOUT installing Airflow.

VTT encrypts its files column by column: the job decrypts those columns with the by-path prefix of DAK and the keyField of VTT, groups the age
and the hour, and encrypts again with ONE global key and a constant key field. Run from the repository root (needs Python 3 and jinja2):
    python3 -B -m unittest discover -s airflow/tests -v
"""

import unittest

import datetime

from test_silver_dags import EBS as _VTNET_EBS
from test_silver_dags import _AirflowFailException, _dags_of, _Pendulum, driver_env, load, render, variables

GLOBAL = {"globalKey": True, "globalZone": "vtt"}

# dataset (the name of the landing folder) -> DAG id
F001, F002, F003 = "f001-mpre-call", "f002-mpre-sms", "f003-mpre-data"
F014, F015, F016 = "f014-mpre-tot-charge-d", "f015-mpre-tot-charge-d-acc", "f016-mpre-tot-charge-m"
F022, FHOME, FCUST = "f022-register", "f-home-bts-daily-area", "f-customer"
D_TABLES = (
    "d-cust-type", "d-khdn", "d-mpre-data-package", "d-addon-data-package", "d-device-ttvas", "d-os-type", "d-sub-type-gb",
    "d-telecom-service", "d-vas-direction",
)
COLUMN_DATASETS = (F002, F003, F014, F015, F016, F022, FHOME)
NONE_DATASETS = (F001, FCUST)
ALL_DATASETS = COLUMN_DATASETS + NONE_DATASETS + D_TABLES


def dag_id(dataset):
    return "silver_ingest_vtt_" + dataset.replace("-", "_")


def config(table, number, **extra):
    return {"ticket": f"SLV-2026-{number:06d}", "table": table, "cron": "30 1 * * *", **extra}


CONFIGS = {
    dag_id(F014): config("vtt_f014_mpre_tot_charge_d", 11, **GLOBAL),
    dag_id(F001): config("vtt_f001_mpre_call", 12, **GLOBAL),
    dag_id(F002): config("vtt_f002_mpre_sms", 13, **GLOBAL),
    dag_id(FCUST): config("vtt_f_customer", 14, **GLOBAL),
    dag_id(F016): config("vtt_f016_mpre_tot_charge_m", 16, **GLOBAL),
    dag_id("d-os-type"): config("vtt_d_os_type", 15),
}
vtt_module = load("silver_ingest_vtt", variables(CONFIGS))
common = vtt_module.common
DAGS = _dags_of(vtt_module)


def env_of(dataset, conf=None):
    return driver_env(render(DAGS[dag_id(dataset)], conf, run_id="manual__x" if conf else "scheduled__2026-10-27T03:00:00"))


class DatasetsTest(unittest.TestCase):
    def test_every_table_of_the_request_has_its_own_dag_and_nothing_else_is_added(self):
        self.assertEqual(set(vtt_module.DATASETS), set(ALL_DATASETS))
        self.assertEqual(len(ALL_DATASETS), 18)
        self.assertEqual({dag_id(d) for d in ALL_DATASETS}, set(vtt_module.SOURCES))

    def test_every_entry_is_valid(self):
        for dataset, settings in vtt_module.DATASETS.items():
            common.check_dataset(dataset, settings)

    def test_the_tables_that_vtt_encrypts_are_decrypted_by_column_with_the_key_field_of_vtt(self):
        expected = {
            F002: (["calling_number", "called_number", "called_number_cl"], "sta_datetime"),
            F003: (["calling_number"], "sta_datetime"),
            F014: (["isdn"], "sta_date"),
            F015: (["isdn"], "customer_id"),
            F016: (["isdn"], "customer_id"),
            F022: (["isdn"], "sta_date"),
            FHOME: (["isdn"], "station_code"),
        }
        self.assertEqual(set(expected), set(COLUMN_DATASETS))
        for dataset, (columns, key_field) in expected.items():
            settings = vtt_module.DATASETS[dataset]
            self.assertEqual(settings["mode"], "decrypt-encrypt", dataset)
            self.assertEqual(settings["decrypt_mode"], "column", dataset)
            self.assertEqual(settings["provider_decrypt"], {"columns": columns, "key_field": key_field}, dataset)

    def test_the_columns_that_vtt_does_not_encrypt_are_only_encrypted_never_decrypted(self):
        for dataset, columns in ((F001, ["calling_number"]), (FCUST, ["cust_id"])):
            settings = vtt_module.DATASETS[dataset]
            self.assertEqual(settings["decrypt_mode"], "none", dataset)
            self.assertNotIn("provider_decrypt", settings, dataset)
            self.assertEqual(settings["encrypt_columns"], columns, dataset)

    def test_the_columns_encrypted_with_the_global_key(self):
        expected = {
            F001: ["calling_number"],
            F002: ["calling_number", "called_number", "called_number_cl"],
            F003: ["calling_number"],
            F014: ["isdn", "customer_id"],
            F015: ["isdn", "customer_id"],
            F016: ["isdn", "customer_id"],
            F022: ["isdn"],
            FHOME: ["isdn"],
            FCUST: ["cust_id"],
        }
        for dataset, columns in expected.items():
            self.assertEqual(vtt_module.DATASETS[dataset]["encrypt_columns"], columns, dataset)

    def test_every_encrypting_table_uses_the_same_constant_key_field_so_that_one_number_gives_one_ciphertext(self):
        constants = {str(vtt_module.DATASETS[d]["key_field_constant"]) for d in COLUMN_DATASETS + NONE_DATASETS}
        self.assertEqual(len(constants), 1)
        self.assertEqual(vtt_module.DATASETS[F014]["key_field_constant"], {"name": "key_scope", "value": "vtt"})

    def test_age_and_hour_are_grouped_where_the_request_asks_for_it(self):
        expected = {
            F001: {"hour_id": "HOUR_BAND"},
            F002: {"hour_id": "HOUR_BAND"},
            F003: {"hour_id": "HOUR_BAND"},
            F022: {"hour_id": "HOUR_BAND"},
            F014: {"customer_age": "AGE_BAND"},
            F015: {"customer_age": "AGE_BAND"},
            F016: {"customer_age": "AGE_BAND"},
            FCUST: {"customer_age": "AGE_BAND"},
        }
        for dataset, transforms in expected.items():
            self.assertEqual(vtt_module.DATASETS[dataset]["transforms"], transforms, dataset)
        self.assertNotIn("transforms", vtt_module.DATASETS[FHOME])

    def test_the_hour_comes_from_hour_id_only_for_the_tables_that_have_it(self):
        for dataset in (F001, F002, F003, F022):
            settings = vtt_module.DATASETS[dataset]
            self.assertEqual((settings["hour_column"], settings["partition_by_hour"]), ("hour_id", True), dataset)
        for dataset in (F014, F015, F016, FHOME, FCUST) + D_TABLES:
            self.assertNotIn("hour_column", vtt_module.DATASETS[dataset], dataset)

    def test_the_reference_tables_are_written_as_they_are(self):
        for dataset in D_TABLES:
            self.assertEqual(vtt_module.DATASETS[dataset], {"mode": "passthrough", "resources": vtt_module.TINY}, dataset)


class SpecTest(unittest.TestCase):
    def test_a_column_decrypt_table_gets_the_decrypt_settings_the_key_path_and_the_global_key(self):
        env = env_of(F014)
        self.assertEqual(env["DECRYPT_MODE"], "column")
        self.assertEqual(env["KEY_PATH"], F014)  # one by-path key per table, the same for every day
        self.assertNotIn("KEY_PATH_ROOT", env)
        self.assertEqual((env["PROVIDER_DECRYPT_COLUMNS"], env["PROVIDER_KEY_FIELD"]), ("isdn", "sta_date"))
        self.assertEqual(env["CDR_ENCRYPT_COLUMNS"], "isdn,customer_id")
        self.assertEqual((env["CDR_KEY_FIELD"], env["CDR_KEY_FIELD_VALUE"]), ("key_scope", "vtt"))
        self.assertEqual(env["COLUMN_TRANSFORMS"], "customer_age:AGE_BAND")
        self.assertEqual(env["GLOBAL_KEY_ZONE"], "vtt")  # from the approved request
        self.assertEqual(env["CRYPTO_PREFIX_SOURCE"], "dak")
        self.assertNotIn("FILE_CIPHER", env)
        self.assertNotIn("DECRYPT_STAGING_PATH", env)

    def test_several_provider_columns_and_a_hour_group(self):
        env = env_of(F002)
        self.assertEqual(env["PROVIDER_DECRYPT_COLUMNS"], "calling_number,called_number,called_number_cl")
        self.assertEqual(env["PROVIDER_KEY_FIELD"], "sta_datetime")
        self.assertEqual(env["CDR_ENCRYPT_COLUMNS"], "calling_number,called_number,called_number_cl")
        self.assertEqual(env["COLUMN_TRANSFORMS"], "hour_id:HOUR_BAND")
        self.assertEqual((env["HOUR_FROM_COLUMN"], env["HOUR_COLUMN"], env["OUTPUT_PARTITION_BY"]), ("hour_id", "hour", "hour"))

    def test_a_table_that_vtt_does_not_encrypt_never_asks_dak_for_a_by_path_key(self):
        for dataset in (F001, FCUST):
            env = env_of(dataset)
            self.assertEqual(env["DECRYPT_MODE"], "none", dataset)
            for name in ("KEY_PATH", "KEY_PATH_ROOT", "PROVIDER_DECRYPT_COLUMNS", "PROVIDER_KEY_FIELD", "FILE_CIPHER"):
                self.assertNotIn(name, env, (dataset, name))
            self.assertEqual(env["GLOBAL_KEY_ZONE"], "vtt", dataset)
        self.assertEqual(env_of(F001)["CDR_ENCRYPT_COLUMNS"], "calling_number")
        self.assertEqual(env_of(FCUST)["CDR_ENCRYPT_COLUMNS"], "cust_id")
        self.assertNotIn("HOUR_FROM_COLUMN", env_of(FCUST))

    def test_a_reference_table_is_a_passthrough_without_dak_or_decrypt_settings(self):
        spec = render(DAGS[dag_id("d-os-type")])
        self.assertEqual(spec["spec"]["mainClass"], "vai.lakehouse.silveringest.app.PassthroughSilverApp")
        names = set(driver_env(spec))
        self.assertFalse({n for n in names if n.startswith(("DAK_", "CDR_", "FILE_", "KEY_", "CRYPTO_", "PROVIDER_", "COLUMN_", "DECRYPT_", "GLOBAL_"))})
        self.assertNotIn("hostAliases", spec["spec"]["driver"])
        self.assertFalse([k for k in spec["spec"]["sparkConf"] if k.startswith("spark.cdrcrypto.audit")])

    def test_the_decrypting_tables_run_the_decrypt_encrypt_app_trust_keycloak_and_audit_the_encryption(self):
        for dataset in (F014, F001, F002, FCUST):
            spec = render(DAGS[dag_id(dataset)])["spec"]
            self.assertEqual(spec["mainClass"], "vai.lakehouse.silveringest.app.DecryptEncryptSilverApp", dataset)
            self.assertIn("hostAliases", spec["driver"], dataset)
            self.assertEqual(spec["sparkConf"]["spark.cdrcrypto.audit.engineId"], "silver-ingest-vtt-" + dataset, dataset)
            self.assertEqual(driver_env(render(DAGS[dag_id(dataset)]))["DAK_CALLER"], "silver-ingest-vtt-" + dataset)

    def test_the_landing_files_are_parquet_and_are_read_as_they_are(self):
        for dataset in (F014, F001, "d-os-type"):
            env = env_of(dataset)
            self.assertEqual(env["INPUT_FORMAT"], "parquet", dataset)
            self.assertNotIn("INPUT_OPTION_header", env, dataset)
            self.assertNotIn("INPUT_SCHEMA", env, dataset)
            self.assertEqual(env["INPUT_OPTION_recursiveFileLookup"], "true", dataset)

    def test_a_run_ingests_the_day_folder_of_its_table_into_the_partition_dt(self):
        env = env_of(F014)
        self.assertEqual(env["INPUT_PATH"], vtt_module.INPUT_ROOT + "f014-mpre-tot-charge-d/date=20261027")
        self.assertEqual((env["BATCH_COLUMN"], env["BATCH_VALUE"], env["OUTPUT_TABLE"]), ("dt", "20261027", "vtt_f014_mpre_tot_charge_d"))

    def test_the_key_path_can_be_overridden_per_dataset(self):
        source = vtt_module.source_for(F014, {**vtt_module.DATASETS[F014], "key_path": "vtt-core"}, CONFIGS[dag_id(F014)])
        env = {item["name"]: item.get("value") for item in common.build_spec(source)["spec"]["driver"]["env"]}
        self.assertEqual(env["KEY_PATH"], "vtt-core")

    def test_every_env_value_is_a_string(self):
        for dataset in (F014, F001, F002, FCUST, "d-os-type"):
            spec = render(DAGS[dag_id(dataset)])["spec"]
            for role in ("driver", "executor"):
                for item in spec[role]["env"]:
                    if "value" in item:
                        self.assertIsInstance(item["value"], str, (dataset, item["name"]))

    def test_the_executors_get_no_dak_or_key_env(self):
        names = {item["name"] for item in render(DAGS[dag_id(F014)])["spec"]["executor"]["env"]}
        self.assertFalse({n for n in names if n.startswith(("DAK_", "CDR_", "KEY_", "CRYPTO_", "GLOBAL_", "PROVIDER_"))})


class ScheduleConfigTest(unittest.TestCase):
    def source(self, dataset):
        return DAGS[dag_id(dataset)].SOURCE

    def test_a_table_that_encrypts_needs_the_global_key_of_a_zone_in_its_approved_request(self):
        for dataset in COLUMN_DATASETS + NONE_DATASETS:
            source = vtt_module.source_for(dataset, vtt_module.DATASETS[dataset])
            common.check_schedule_config(source, config("vtt_x", 1, **GLOBAL))
            with self.assertRaises(ValueError, msg=dataset):
                common.check_schedule_config(source, config("vtt_x", 1))

    def test_the_global_key_keys_go_together_and_are_well_formed(self):
        source = self.source(F014)
        without_key = {k: v for k, v in config("vtt_x", 1, **GLOBAL).items() if k != "globalKey"}
        without_zone = {k: v for k, v in config("vtt_x", 1, **GLOBAL).items() if k != "globalZone"}
        for bad in (without_key, without_zone, config("vtt_x", 1, globalKey=False, globalZone="vtt"),
                    config("vtt_x", 1, globalKey=True, globalZone="Bad Zone"), config("vtt_x", 1, globalKey=True, globalZone="")):
            with self.assertRaises(ValueError, msg=repr(bad)):
                common.check_schedule_config(source, bad)

    def test_dak_requires_the_columns_with_a_global_key_so_a_request_may_carry_them(self):
        # f014 encrypts isdn and customer_id: the request may list them, in any order and case, with any keyField.
        source = self.source(F014)
        for extra in ({"encryptColumns": ["isdn", "customer_id"], "keyField": "sta_date"},
                      {"encryptColumns": ["CUSTOMER_ID", "isdn"], "keyField": "key_scope"}):
            common.check_schedule_config(source, config("vtt_x", 1, **GLOBAL, **extra))
        common.check_schedule_config(source, config("vtt_x", 1, **GLOBAL))  # still optional

    def test_the_columns_of_a_request_must_be_exactly_the_columns_that_the_dataset_encrypts(self):
        source = self.source(F014)
        for extra in ({"encryptColumns": ["isdn"], "keyField": "sta_date"},  # one missing
                      {"encryptColumns": ["isdn", "customer_id", "sta_date"], "keyField": "x"},  # one too many
                      {"encryptColumns": ["msisdn", "customer_id"], "keyField": "sta_date"}):  # another column
            with self.assertRaises(ValueError, msg=repr(extra)) as raised:
                common.check_schedule_config(source, config("vtt_x", 1, **GLOBAL, **extra))
            self.assertIn("customer_id, isdn", str(raised.exception))  # says which columns are expected

    def test_a_request_with_columns_needs_a_well_formed_key_field_that_is_not_encrypted(self):
        source = self.source(F014)
        columns = {"encryptColumns": ["isdn", "customer_id"]}
        for extra in (columns, {**columns, "keyField": ""}, {**columns, "keyField": "bad field"}, {**columns, "keyField": "isdn"},
                      {"keyField": "sta_date"}):
            with self.assertRaises(ValueError, msg=repr(extra)):
                common.check_schedule_config(source, config("vtt_x", 1, **GLOBAL, **extra))

    def test_the_columns_of_the_request_never_change_what_the_job_encrypts(self):
        with_columns = {**CONFIGS, dag_id(F014): config("vtt_f014_mpre_tot_charge_d", 11, **GLOBAL,
                                                         encryptColumns=["customer_id", "isdn"], keyField="sta_date")}
        module = load("silver_ingest_vtt", variables(with_columns))
        self.assertIsNone(module.CONFIG_PROBLEMS[dag_id(F014)])
        env = driver_env(render(_dags_of(module)[dag_id(F014)]))
        self.assertEqual(env["CDR_ENCRYPT_COLUMNS"], "isdn,customer_id")
        self.assertEqual((env["CDR_KEY_FIELD"], env["CDR_KEY_FIELD_VALUE"]), ("key_scope", "vtt"))

    def test_a_reference_table_takes_no_columns(self):
        with self.assertRaises(ValueError):
            common.check_schedule_config(self.source("d-os-type"), config("vtt_d_x", 1, encryptColumns=["a"], keyField="b"))

    def test_a_reference_table_takes_no_global_key(self):
        source = self.source("d-os-type")
        common.check_schedule_config(source, config("vtt_d_x", 1))
        with self.assertRaises(ValueError):
            common.check_schedule_config(source, config("vtt_d_x", 1, **GLOBAL))

    def test_the_table_of_a_request_must_carry_the_vtt_prefix(self):
        with self.assertRaises(ValueError):
            common.check_schedule_config(self.source(F014), config("vtnet_x", 1, **GLOBAL))

    def test_the_dag_of_an_approved_request_is_scheduled_and_the_others_have_no_schedule(self):
        self.assertEqual(DAGS[dag_id(F014)].kwargs["schedule"], "30 1 * * *")
        self.assertIsNone(vtt_module.CONFIG_PROBLEMS[dag_id(F014)])
        self.assertIsNone(DAGS[dag_id(F015)].kwargs["schedule"])

    def test_a_request_without_the_global_key_leaves_an_encrypting_dag_without_a_schedule_and_says_why(self):
        bad = load("silver_ingest_vtt", variables({dag_id(F014): config("vtt_f014_mpre_tot_charge_d", 11)}))
        self.assertIn("globalKey", bad.CONFIG_PROBLEMS[dag_id(F014)])


class DatasetEntryTest(unittest.TestCase):
    GOOD = {
        "mode": "decrypt-encrypt", "decrypt_mode": "column", "provider_decrypt": {"columns": ["isdn"], "key_field": "sta_date"},
        "encrypt_columns": ["isdn", "customer_id"], "key_field_constant": {"name": "key_scope", "value": "vtt"},
        "transforms": {"customer_age": "AGE_BAND"},
    }

    def test_a_full_entry_and_the_smaller_ones_are_valid(self):
        common.check_dataset("d", self.GOOD)
        common.check_dataset("d", {k: v for k, v in {**self.GOOD, "decrypt_mode": "none"}.items() if k != "provider_decrypt"})
        common.check_dataset("d", {"mode": "decrypt-encrypt", "decrypt_mode": "file"})
        common.check_dataset("d", {"mode": "decrypt-encrypt", "decrypt_mode": "none", "transforms": {"hour_id": "HOUR_BAND"}})

    def test_every_new_setting_is_checked_and_the_error_names_the_dataset(self):
        base = self.GOOD
        without = lambda *keys: {k: v for k, v in base.items() if k not in keys}  # noqa: E731
        for bad in (
            {**base, "decrypt_mode": "whole"},
            {**base, "decrypt_mode": None},
            without("provider_decrypt"),  # column needs the columns of VTT
            {**base, "decrypt_mode": "none"},  # provider_decrypt only with column
            {**base, "provider_decrypt": {"columns": [], "key_field": "sta_date"}},
            {**base, "provider_decrypt": {"columns": ["isdn"]}},
            {**base, "provider_decrypt": {"columns": ["isdn"], "key_field": "ISDN"}},  # the key field stays plaintext
            {**base, "provider_decrypt": {"columns": ["isdn", "ISDN"], "key_field": "sta_date"}},
            {**base, "provider_decrypt": {"columns": ["bad-name"], "key_field": "sta_date"}},
            {**base, "provider_decrypt": {"columns": ["isdn"], "key_field": "sta_date", "extra": 1}},
            {**base, "provider_decrypt": ["isdn"]},
            without("key_field_constant"),  # encrypted columns need the constant key field
            {**base, "key_field_constant": {"name": "isdn", "value": "vtt"}},  # not one of the encrypted columns
            {**base, "key_field_constant": {"name": "key_scope", "value": "a b"}},
            {**base, "key_field_constant": {"name": "key scope", "value": "vtt"}},
            {**base, "key_field_constant": {"name": "key_scope"}},
            {**base, "encrypt_columns": []},
            {**base, "encrypt_columns": ["isdn", "ISDN"]},
            {**base, "encrypt_columns": "isdn"},
            {**base, "transforms": {"customer_age": "MASK"}},
            {**base, "transforms": {"isdn": "AGE_BAND"}},  # an encrypted column cannot be grouped
            {**base, "transforms": {"bad-name": "AGE_BAND"}},
            {**base, "transforms": ["customer_age"]},
            {"mode": "passthrough", "decrypt_mode": "none"},
            {"mode": "passthrough", "encrypt_columns": ["isdn"], "key_field_constant": {"name": "k", "value": "v"}},
            {"mode": "passthrough", "transforms": {"customer_age": "AGE_BAND"}},
            {"mode": "decrypt-encrypt", "decrypt_mode": "none", "key_path": "x"},  # none never calls by-path
        ):
            with self.assertRaises(ValueError, msg=repr(bad)) as raised:
                common.check_dataset("my_dataset", bad)
            self.assertIn("my_dataset", str(raised.exception), repr(bad))

    def test_the_key_field_of_vtt_may_be_one_of_the_grouped_columns(self):
        # Decryption runs before the grouping, so the plaintext key field can be a column that is grouped afterwards.
        common.check_dataset("d", {**self.GOOD, "provider_decrypt": {"columns": ["isdn"], "key_field": "customer_age"}})


def local_day_start(year, month, day, hour=1, minute=30):
    """`data_interval_start` of a run: the cron time (local, UTC+7) of that day."""
    return _Pendulum(year, month, day, hour, minute, tzinfo=datetime.timezone(datetime.timedelta(hours=7)))


class PeriodicTest(unittest.TestCase):
    """f016 (a monthly table) has no schedule at all: it only runs by hand (conf date, or a bulk load)."""

    def test_f016_is_not_periodic_and_every_other_table_is(self):
        self.assertIs(vtt_module.DATASETS[F016]["periodic"], False)
        for dataset in ALL_DATASETS:
            if dataset != F016:
                self.assertNotIn("periodic", vtt_module.DATASETS[dataset], dataset)

    def test_f016_has_no_schedule_even_with_an_approved_request_but_runs_by_hand(self):
        dag = DAGS[dag_id(F016)]
        self.assertIsNone(vtt_module.CONFIG_PROBLEMS[dag_id(F016)])
        self.assertIsNone(dag.kwargs["schedule"])
        self.assertIs(dag.kwargs["catchup"], False)
        validate = dag.recorded[1].kwargs["python_callable"]
        self.assertIsNone(validate(dag_run=type("Run", (), {"run_id": "manual__x", "conf": {"date": "20261105"}})()))
        env = driver_env(render(dag, {"date": "20261105"}, run_id="manual__x"))
        self.assertTrue(env["INPUT_PATH"].endswith("f016-mpre-tot-charge-m/date=20261105"))
        self.assertEqual(env["OUTPUT_TABLE"], "vtt_f016_mpre_tot_charge_m")

    def test_the_other_tables_with_an_approved_request_are_scheduled_and_never_catch_up(self):
        kwargs = DAGS[dag_id(F014)].kwargs
        self.assertEqual(kwargs["schedule"], "30 1 * * *")
        self.assertIs(kwargs["catchup"], False)  # a first load is a bulk load that is asked for, not something that starts by itself
        self.assertEqual(kwargs["start_date"], ("pendulum.datetime", (2026, 1, 1), {"tz": "Asia/Ho_Chi_Minh"}))

    def test_the_periodic_flag_is_checked_and_reaches_the_source(self):
        common.check_dataset("d", {"mode": "passthrough", "periodic": False})
        for bad in ("no", 0, None):
            with self.assertRaises(ValueError, msg=repr(bad)) as raised:
                common.check_dataset("my_dataset", {"mode": "passthrough", "periodic": bad})
            self.assertIn("my_dataset", str(raised.exception))
        self.assertIs(DAGS[dag_id(F016)].SOURCE["periodic"], False)
        self.assertIs(DAGS[dag_id(F014)].SOURCE["periodic"], True)


class BulkLoadTest(unittest.TestCase):
    """A bulk load: ONE run of the DAG loads many days, whatever was loaded or tested before (docs/silver-ingest/BACKFILL.md). The run is asked for
    with conf {"fromDate": ..., "toDate": ...}; without them the run is the one day of the schedule or of conf.date."""

    RANGE = {"fromDate": "20261104", "toDate": "20261122"}

    def env(self, dataset, conf):
        return driver_env(render(DAGS[dag_id(dataset)], conf, run_id="manual__x"))

    def test_the_range_goes_to_the_job_and_the_path_has_the_place_of_the_day(self):
        env = self.env(F014, self.RANGE)
        self.assertEqual((env["BULK_FROM"], env["BULK_TO"], env["BULK_MISSING_DAY"]), ("20261104", "20261122", "fail"))
        self.assertEqual(env["INPUT_PATH"], vtt_module.INPUT_ROOT + "f014-mpre-tot-charge-d/date=%DAY%")
        self.assertEqual(env["BATCH_VALUE"], "")  # the job takes the batch of each day from the day itself
        self.assertEqual((env["BATCH_COLUMN"], env["OUTPUT_MODE"]), ("dt", "overwrite-partitions"))
        self.assertEqual(env["OUTPUT_TABLE"], "vtt_f014_mpre_tot_charge_d")

    def test_to_date_defaults_to_from_date_and_missing_days_can_be_skipped(self):
        env = self.env(F014, {"fromDate": "20261104"})
        self.assertEqual((env["BULK_FROM"], env["BULK_TO"]), ("20261104", "20261104"))
        self.assertEqual(self.env(F014, {**self.RANGE, "missingDays": "skip"})["BULK_MISSING_DAY"], "skip")

    def test_a_run_that_is_not_a_bulk_load_renders_exactly_as_before_with_empty_bulk_values(self):
        for conf in (None, {"date": "20261013"}):
            env = driver_env(render(DAGS[dag_id(F014)], conf, run_id="manual__x" if conf else "scheduled__2026-10-27T03:00:00"))
            self.assertEqual((env["BULK_FROM"], env["BULK_TO"], env["BULK_MISSING_DAY"]), ("", "", ""), conf)
            self.assertIn("date=2026", env["INPUT_PATH"])
            self.assertNotIn("%DAY%", env["INPUT_PATH"])
            self.assertNotEqual(env["BATCH_VALUE"], "")

    def test_every_dataset_can_be_bulk_loaded_including_f016_and_the_other_source(self):
        for dataset in ALL_DATASETS:
            if dag_id(dataset) in DAGS:
                self.assertTrue(self.env(dataset, self.RANGE)["INPUT_PATH"].endswith("date=%DAY%"), dataset)
        ebs = driver_env(render(_VTNET_EBS, self.RANGE, run_id="manual__x"))
        self.assertTrue(ebs["INPUT_PATH"].endswith("ebs_3g_4g/dt=%DAY%"))
        self.assertEqual((ebs["BATCH_HOURS"], ebs["BULK_FROM"]), ("", "20261104"))  # a day is loaded whole: no hours

    def test_valid_ranges_are_accepted(self):
        source = DAGS[dag_id(F014)].SOURCE
        for conf in (
            self.RANGE, {"fromDate": "20261104"}, {"fromDate": "20261104", "missingDays": "fail"},
            {"fromDate": "20261130", "toDate": "20261202"}, {"fromDate": "20261001", "toDate": "20261201"},  # exactly 62 days
        ):
            common.check_conf(source, conf)

    def test_an_invalid_range_is_refused_and_the_message_does_not_repeat_the_value(self):
        source = DAGS[dag_id(F014)].SOURCE
        secret = "20261304"
        for bad in (
            {"toDate": "20261122"},  # no fromDate
            {"missingDays": "skip"},
            {"fromDate": secret}, {"fromDate": "20261104", "toDate": secret}, {"fromDate": "2026-11-04"}, {"fromDate": 20261104},
            {"fromDate": "20261122", "toDate": "20261104"},  # backwards
            {"fromDate": "20261001", "toDate": "20261202"},  # 63 days
            {"fromDate": "20261104", "missingDays": "ignore"},
            {"fromDate": "20261104", "date": "20261105"},  # a bulk load or one day, not both
            {"fromDate": "20261104", "fromHour": "10"}, {"fromDate": "20261104", "toHour": "10"},  # a bulk load loads whole days
            {"fromDate": "20261104", "other": 1},
        ):
            with self.assertRaises(ValueError, msg=repr(bad)) as raised:
                common.check_conf(source, bad)
            self.assertNotIn(secret, str(raised.exception))

    def test_a_bulk_load_needs_overwrite_partitions_so_that_a_day_that_is_already_loaded_is_replaced(self):
        source = DAGS[dag_id(F014)].SOURCE
        appending = {**source, "fixed": {**source["fixed"], "outputMode": "append"}}
        with self.assertRaises(ValueError):
            common.check_conf(appending, self.RANGE)
        common.check_conf(appending, {"date": "20261104"})  # one day with append is still allowed, as before

    def test_the_validate_task_turns_a_bad_range_into_an_airflow_failure_before_any_job_is_created(self):
        validate = DAGS[dag_id(F014)].recorded[1].kwargs["python_callable"]
        run = lambda conf: type("Run", (), {"run_id": "manual__x", "conf": conf})()  # noqa: E731
        self.assertIsNone(validate(dag_run=run(self.RANGE)))
        with self.assertRaises(_AirflowFailException):
            validate(dag_run=run({"fromDate": "20261122", "toDate": "20261104"}))

    def test_hours_are_never_asked_of_a_bulk_load_even_on_an_hourly_schedule(self):
        start = local_day_start(2026, 11, 4)
        self.assertEqual(common.hours_of(self.RANGE, start, "hour"), [])
        self.assertEqual(common.batch_hours("hour", self.RANGE, start), "")

    def test_a_bulk_load_is_not_a_backfill_of_one_run_per_day(self):
        # silver_ingest_backfill still starts one run per day (max 31 days); a bulk load is one run of the dataset DAG (max 62 days).
        self.assertEqual(common.MAX_BACKFILL_DAYS, 31)
        self.assertEqual(common.MAX_BULK_DAYS, 62)


WIDE = {"executor_cores": 4, "executor_memory": "16384m", "executor_memory_overhead": "3072m", "driver_memory": "6144m",
        "driver_memory_overhead": "1536m"}
NARROW = {"executor_cores": 4, "executor_memory": "12288m", "executor_memory_overhead": "2048m", "driver_memory": "4096m",
          "driver_memory_overhead": "1024m"}
TINY = {"executors": 1, "executor_cores": 2, "executor_memory": "2048m", "executor_memory_overhead": "512m", "driver_memory": "1024m",
        "shuffle_partitions": 8}

# Sizes of one day on HDFS (parquet, 2026-11-10) -> tasks of 128 MB; docs/silver-ingest/TRANSFORM-PHIEU-KHAI-THAC-VTT.md section 10.
EXPECTED_RESOURCES = {
    F003: {**WIDE, "executors": 4, "min_executors": 2, "max_executors": 8, "shuffle_partitions": 256},  # 33.1 G
    F015: {**WIDE, "executors": 3, "min_executors": 2, "max_executors": 6, "shuffle_partitions": 192},  # 23.7 G
    F016: {**WIDE, "executors": 3, "min_executors": 2, "max_executors": 6, "shuffle_partitions": 224},  # 28.5 G per batch
    F014: {**WIDE, "executors": 3, "min_executors": 2, "max_executors": 6, "shuffle_partitions": 128},  # 17.7 G
    FCUST: {**NARROW, "executors": 2, "min_executors": 1, "max_executors": 4, "shuffle_partitions": 96},  # 12.1 G
    F022: {**NARROW, "executors": 2, "min_executors": 1, "max_executors": 4, "shuffle_partitions": 48},  # 5.0 G
    F001: {**NARROW, "executors": 2, "min_executors": 1, "max_executors": 4, "shuffle_partitions": 32},  # 3.7 G
    FHOME: {**NARROW, "executors": 2, "min_executors": 1, "max_executors": 3, "shuffle_partitions": 24},  # 2.8 G
    F002: {**NARROW, "executors": 1, "min_executors": 1, "max_executors": 3, "shuffle_partitions": 16},  # 1.4 G
    **{dataset: TINY for dataset in D_TABLES},  # 13 K to 482 M for the whole table
}


def spark_conf_of(dataset, dynamic_conf=None):
    source = vtt_module.source_for(dataset, vtt_module.DATASETS[dataset], None)
    return common.build_spec(source)["spec"]["sparkConf"], common.build_spec(source)["spec"]


class ResourcesTest(unittest.TestCase):
    """Spark resources of every table, from the size of its day on HDFS. The job is limited by reading, decompressing and writing parquet, not by
    the encryption (cdr-crypto: 0.74 microsecond per value, measured), so the cores follow the number of 128 MB tasks and the memory the width of
    the table."""

    def test_every_table_has_the_resources_chosen_from_the_size_of_its_day(self):
        self.assertEqual(set(EXPECTED_RESOURCES), set(ALL_DATASETS))
        for dataset, expected in EXPECTED_RESOURCES.items():
            self.assertEqual(vtt_module.DATASETS[dataset]["resources"], expected, dataset)

    def test_every_entry_with_resources_is_valid(self):
        for dataset, settings in vtt_module.DATASETS.items():
            common.check_dataset(dataset, settings)

    def test_every_data_table_scales_with_dynamic_allocation_only_the_reference_tables_stay_static(self):
        dynamic = {d for d, r in EXPECTED_RESOURCES.items() if "max_executors" in r}
        self.assertEqual(dynamic, {F003, F015, F016, F014, FCUST, F022, F001, FHOME, F002})
        for dataset in dynamic:
            r = vtt_module.DATASETS[dataset]["resources"]
            self.assertLessEqual(r["min_executors"], r["executors"], dataset)
            self.assertLessEqual(r["executors"], r["max_executors"], dataset)

    def test_a_dynamic_table_renders_dynamic_allocation_with_shuffle_tracking_and_no_external_shuffle_service(self):
        conf, spec = spark_conf_of(F003)
        self.assertEqual(conf["spark.dynamicAllocation.enabled"], "true")
        self.assertEqual(conf["spark.dynamicAllocation.shuffleTracking.enabled"], "true")  # kubernetes has no external shuffle service
        self.assertEqual((conf["spark.dynamicAllocation.minExecutors"], conf["spark.dynamicAllocation.maxExecutors"]), ("2", "8"))
        self.assertEqual(conf["spark.dynamicAllocation.initialExecutors"], "4")
        self.assertEqual(conf["spark.dynamicAllocation.executorIdleTimeout"], "120s")
        self.assertEqual(spec["executor"]["instances"], 4)
        self.assertEqual(spec["executor"]["cores"], 4)
        self.assertEqual(spec["executor"]["memory"], "16384m")
        self.assertEqual(spec["driver"]["memory"], "6144m")
        self.assertEqual(conf["spark.sql.shuffle.partitions"], "256")

    def test_a_static_table_keeps_dynamic_allocation_off_and_has_no_dynamic_keys(self):
        for dataset in D_TABLES:
            conf, spec = spark_conf_of(dataset)
            self.assertEqual(conf["spark.dynamicAllocation.enabled"], "false", dataset)
            self.assertFalse([k for k in conf if k.startswith("spark.dynamicAllocation.") and k != "spark.dynamicAllocation.enabled"], dataset)
        self.assertEqual(spark_conf_of("d-os-type")[1]["executor"]["instances"], 1)

    def test_a_small_data_table_also_scales_between_one_and_a_few_executors(self):
        conf, spec = spark_conf_of(F002)
        self.assertEqual(conf["spark.dynamicAllocation.enabled"], "true")
        self.assertEqual((conf["spark.dynamicAllocation.minExecutors"], conf["spark.dynamicAllocation.maxExecutors"]), ("1", "3"))
        self.assertEqual(spec["executor"]["memory"], "12288m")
        self.assertEqual(spec["driver"]["memory"], "4096m")

    def test_the_other_sources_are_unchanged_dynamic_allocation_stays_off(self):
        for dag in _dags_of(vtt_module).values():
            if "max_executors" not in dag.SOURCE["resources"]:
                self.assertEqual(common.build_spec(dag.SOURCE)["spec"]["sparkConf"]["spark.dynamicAllocation.enabled"], "false")

    def test_a_wide_table_keeps_whole_stage_codegen_and_its_full_plan_in_the_log(self):
        # f003, f014, f015 have 150 to 450 columns: above the default 100 fields Spark turns whole-stage codegen off, and above 25 it cuts
        # the plan in the log ("Truncated the string representation of a plan").
        for dataset in (F003, F001, "d-os-type"):
            conf, _ = spark_conf_of(dataset)
            self.assertEqual(conf["spark.sql.codegen.maxFields"], str(common.WIDE_TABLE_MAX_FIELDS), dataset)
            self.assertEqual(conf["spark.sql.debug.maxToStringFields"], str(common.WIDE_TABLE_MAX_FIELDS), dataset)
        self.assertGreaterEqual(common.WIDE_TABLE_MAX_FIELDS, 1000)

    def test_the_dynamic_keys_are_checked(self):
        base = {"executors": 3}
        common.check_resources("d", {**base, "min_executors": 1, "max_executors": 6})
        common.check_resources("d", {**base, "max_executors": 6})  # the minimum defaults to 1
        for bad in (
            {"min_executors": 1},  # a minimum without a maximum: dynamic allocation is on only with max_executors
            {**base, "min_executors": 4, "max_executors": 6},  # the initial executors are below the minimum
            {**base, "max_executors": 2},  # ... or above the maximum
            {**base, "min_executors": 5, "max_executors": 2},
            {**base, "max_executors": 0}, {**base, "max_executors": 101}, {**base, "max_executors": True}, {**base, "max_executors": "6"},
            {**base, "min_executors": 0, "max_executors": 6}, {**base, "min_executors": 1.5, "max_executors": 6},
        ):
            with self.assertRaises(ValueError, msg=repr(bad)) as raised:
                common.check_resources("my_dataset", bad)
            self.assertIn("my_dataset", str(raised.exception), repr(bad))

    def test_the_defaults_without_resources_are_what_every_dataset_used_before(self):
        self.assertEqual(common.RESOURCE_DEFAULTS["executors"], 2)
        self.assertNotIn("max_executors", common.RESOURCE_DEFAULTS)
        self.assertEqual(common.resources_of({}), common.RESOURCE_DEFAULTS)


class SharedDakEnvTest(unittest.TestCase):
    def test_the_dak_connection_env_is_shared_and_names_the_caller(self):
        env = {item["name"]: item for item in common.dak_connection_env("silver-ingest-x")}
        self.assertEqual(env["CRYPTO_PREFIX_SOURCE"]["value"], "dak")
        self.assertEqual(env["DAK_TAG"]["value"], "internal")
        self.assertEqual(env["DAK_CALLER"]["value"], "silver-ingest-x")
        self.assertEqual(env["DAK_CLIENT_SECRET"]["valueFrom"]["secretKeyRef"], {"name": "spark-dak-client", "key": "clientSecret"})
        for name in ("DAK_ADDR", "DAK_TOKEN_URL", "DAK_CLIENT_ID", "DAK_PENDING_WAIT_TIMEOUT_SECONDS", "DAK_PENDING_WAIT_MULTIPLIER"):
            self.assertIn(name, env)

    def test_the_audit_conf_names_the_engine(self):
        conf = common.audit_conf("silver-ingest-x")
        self.assertEqual(conf["spark.cdrcrypto.audit.engineId"], "silver-ingest-x")
        self.assertEqual(conf["spark.cdrcrypto.audit.enabled"], "true")


if __name__ == "__main__":
    unittest.main()
