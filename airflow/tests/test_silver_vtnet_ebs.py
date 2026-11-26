"""Tests for the EBS-4G dataset of silver_ingest_vtnet.py (landing on HKH, imsi encrypted by VTNET), WITHOUT installing Airflow.

The job runs here (HLA) with its keytab, reads the landing of HKH with a delegation token of HKH fetched by hand into the Secret hkh-hdfs-token
(docs/silver-ingest/VTNET-EBS-HKH.md), decrypts imsi with the library of VTNET and encrypts it again with the global key and the same constant key field as VTT (f022-register.imsi joins ebs_3g_4g.imsi).
Run from the repository root (needs Python 3 and jinja2):
    python3 -B -m unittest discover -s airflow/tests -v
"""

import unittest

from test_silver_dags import EBS, EBS_CONFIG, NIMS, VTT, common, driver_env, load, render

HKH = common.INPUT_CLUSTERS["hkh"]
SECRET = "hkh-hdfs-token"  # one Secret per cluster (INPUT_CLUSTERS), filled by hand
TOKEN_FILE = "/mnt/silver-input-token/hadoop.token"

vtt_module = load("silver_ingest_vtt")


class EbsJobTest(unittest.TestCase):
    """What the Spark job of EBS-4G does with the data."""

    def test_the_job_decrypts_imsi_with_the_vtnet_library_and_encrypts_it_with_the_global_key(self):
        env = driver_env(render(EBS))

        self.assertEqual(env["DECRYPT_MODE"], "column")
        self.assertEqual(env["PROVIDER_CIPHER"], "vtnet")
        self.assertEqual(env["PROVIDER_DECRYPT_COLUMNS"], "imsi")
        self.assertNotIn("PROVIDER_KEY_FIELD", env)  # the key of VTNET has no per-row part
        self.assertNotIn("FILE_CIPHER", env)  # not a whole-file decryption
        self.assertEqual(env["KEY_PATH"], "ebs_3g_4g")  # the by-path key of DAK holds the inputKey of VTNET
        self.assertEqual(env["CDR_ENCRYPT_COLUMNS"], "imsi")
        self.assertEqual(env["GLOBAL_KEY_ZONE"], "silver_vtt")

    def test_imsi_is_encrypted_with_the_same_constant_key_field_as_f022_so_the_two_tables_join(self):
        ebs = driver_env(render(EBS))
        f022 = vtt_module.DATASETS["f022-register"]

        self.assertIn("imsi", f022["encrypt_columns"])
        self.assertEqual((ebs["CDR_KEY_FIELD"], ebs["CDR_KEY_FIELD_VALUE"]), (f022["key_field_constant"]["name"], f022["key_field_constant"]["value"]))
        self.assertEqual(f022["key_field_constant"], common.JOIN_KEY_FIELD)

    def test_the_job_reads_the_landing_of_hkh_by_batch_folder_and_partitions_by_hour(self):
        env = driver_env(render(EBS, {"date": "20261120", "fromHour": "23"}, run_id="manual__x"))

        self.assertEqual(env["INPUT_PATH"], f"{HKH['hdfs']}/landing/ebs_3g_4g/dt=20261120/batch=23*")
        self.assertEqual(env["OUTPUT_PARTITION_BY"], "hour")
        self.assertEqual((env["INPUT_FORMAT"], env["INPUT_OPTION_header"]), ("csv", "true"))

    def test_only_the_vtnet_dataset_loads_the_vtnet_jar(self):
        self.assertIn(common.VTNET_JAR, render(EBS)["spec"]["sparkConf"]["spark.jars"].split(","))
        for dag in (VTT, NIMS):
            self.assertNotIn(common.VTNET_JAR, render(dag)["spec"]["sparkConf"]["spark.jars"], dag.DAG_ID)

    def test_the_request_must_pick_the_zone_of_the_global_key_and_name_exactly_imsi(self):
        common.check_schedule_config(EBS.SOURCE, EBS_CONFIG)
        common.check_schedule_config(EBS.SOURCE, {**EBS_CONFIG, "encryptColumns": ["IMSI"], "keyField": "key_scope"})
        for bad in (
            {k: v for k, v in EBS_CONFIG.items() if k not in ("globalKey", "globalZone")},
            {**EBS_CONFIG, "encryptColumns": ["imsi", "eci"], "keyField": "key_scope"},
        ):
            with self.assertRaises(ValueError, msg=repr(bad)):
                common.check_schedule_config(EBS.SOURCE, bad)


class InputTokenTest(unittest.TestCase):
    """The token of HKH: fetched by hand into the Secret of the cluster, mounted into the job; the DAG has no token task."""

    def test_the_token_secret_is_mounted_into_the_driver_and_the_executors(self):
        conf = render(EBS)["spec"]["sparkConf"]

        self.assertEqual(driver_env(render(EBS))["INPUT_TOKEN_FILE"], TOKEN_FILE)
        self.assertEqual(conf[f"spark.kubernetes.driver.secrets.{SECRET}"], "/mnt/silver-input-token")
        self.assertEqual(conf[f"spark.kubernetes.executor.secrets.{SECRET}"], "/mnt/silver-input-token")
        self.assertEqual(conf["spark.executorEnv.HADOOP_TOKEN_FILE_LOCATION"], TOKEN_FILE)

    def test_spark_does_not_ask_its_own_kdc_for_a_token_of_hkh(self):
        # Listing HKH here would make Spark ask the KDC of HLA for a token of HKH (another KDC with the same realm name): it would fail.
        conf = render(EBS)["spec"]["sparkConf"]
        self.assertEqual(conf["spark.kerberos.access.hadoopFileSystems"], common.HDFS)
        self.assertEqual(conf["spark.kerberos.principal"], common.KERBEROS_PRINCIPAL)  # the job keeps its keytab: writes the table here

    def test_the_dag_runs_validate_then_ingest_only(self):
        self.assertEqual([r.kwargs.get("task_id") for r in EBS.recorded[1:]], ["validate_conf", "ingest"])

    def test_the_datasets_of_this_cluster_have_no_token(self):
        for dag in (VTT, NIMS):
            spec = render(dag)["spec"]
            self.assertNotIn("INPUT_TOKEN_FILE", driver_env(render(dag)), dag.DAG_ID)
            self.assertFalse([k for k in spec["sparkConf"] if ".secrets." in k or k.startswith("spark.executorEnv.")], dag.DAG_ID)


class DatasetSettingsTest(unittest.TestCase):
    """input_cluster and provider_decrypt.cipher in DATASETS."""

    BASE = {"mode": "decrypt-encrypt", "decrypt_mode": "column", "encrypt_columns": ["imsi"], "key_field_constant": common.JOIN_KEY_FIELD}

    def test_valid_settings_are_accepted(self):
        common.check_dataset("x", {**self.BASE, "input_cluster": "hkh", "provider_decrypt": {"columns": ["imsi"], "cipher": "vtnet"}})
        common.check_dataset("x", {**self.BASE, "provider_decrypt": {"columns": ["isdn"], "key_field": "sta_date"}})
        common.check_dataset("x", {**self.BASE, "provider_decrypt": {"columns": ["isdn"], "key_field": "sta_date", "cipher": "cdr"}})

    def test_wrong_settings_are_rejected(self):
        for bad in (
            {**self.BASE, "input_cluster": "elsewhere", "provider_decrypt": {"columns": ["imsi"], "cipher": "vtnet"}},
            {**self.BASE, "provider_decrypt": {"columns": ["imsi"], "cipher": "vtnet", "key_field": "event_time"}},
            {**self.BASE, "provider_decrypt": {"columns": ["imsi"], "cipher": "aes"}},
            {**self.BASE, "provider_decrypt": {"columns": ["isdn"]}},  # cdr needs key_field
            {**self.BASE, "provider_decrypt": {"columns": ["isdn"], "key_field": "sta_date", "extra": 1}},
        ):
            with self.assertRaises(ValueError, msg=repr(bad)):
                common.check_dataset("x", bad)

    def test_the_job_env_names_the_cipher_only_when_it_is_set(self):
        vtnet = common.dataset_job_env("x", {**self.BASE, "provider_decrypt": {"columns": ["imsi"], "cipher": "vtnet"}})
        cdr = common.dataset_job_env("x", {**self.BASE, "provider_decrypt": {"columns": ["isdn"], "key_field": "sta_date"}})

        self.assertEqual(vtnet["PROVIDER_CIPHER"], "vtnet")
        self.assertNotIn("PROVIDER_KEY_FIELD", vtnet)
        self.assertNotIn("PROVIDER_CIPHER", cdr)  # VTT renders exactly as before
        self.assertEqual(cdr["PROVIDER_KEY_FIELD"], "sta_date")


if __name__ == "__main__":
    unittest.main()
