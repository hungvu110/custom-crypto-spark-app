package vai.lakehouse.stress

import org.scalatest.funsuite.AnyFunSuite

class StressConfigSpec extends AnyFunSuite {

  private val required = Map("RUN_ID" -> "run-01", "OUTPUT_PATH" -> "hdfs://nn:8020/prod6/stress_test/hdfs_load")

  private def ok(env: Map[String, String]): StressConfig =
    StressConfig.fromEnv(env).fold(errors => fail(errors.mkString("; ")), identity)

  private def errors(env: Map[String, String]): Seq[String] =
    StressConfig.fromEnv(env).fold(identity, c => fail(s"expected errors, got $c"))

  test("defaults to sustained 100 GiB/day in 5-minute batches for 24 hours") {
    val cfg = ok(required)

    assert(cfg.mode == Mode.Sustained)
    assert(cfg.batchIntervalSeconds == 300)
    assert(cfg.totalGib == 100.0)
    assert(cfg.durationMillis.contains(24L * 3600 * 1000))
    // 100 GiB * 300 / 86400 = 372 827 022 byte (~355.6 MiB) mỗi batch, 288 batch/ngày
    assert(cfg.bytesPerBatch == 372827022L)
    assert(cfg.runPath == "hdfs://nn:8020/prod6/stress_test/hdfs_load/run-01")
  }

  test("burst defaults to 100 GiB total in 5 GiB batches without a time limit") {
    val cfg = ok(required + ("MODE" -> "BURST"))

    assert(cfg.mode == Mode.Burst)
    assert(cfg.totalBytes == 100L * 1024 * 1024 * 1024)
    assert(cfg.bytesPerBatch == 5L * 1024 * 1024 * 1024)
    assert(cfg.durationMillis.isEmpty)
  }

  test("sustained total follows rate and duration unless TOTAL_GIB is set") {
    assert(ok(required ++ Map("STRESS_GIB_PER_DAY" -> "300", "DURATION_HOURS" -> "6")).totalGib == 75.0)
    assert(ok(required ++ Map("DURATION_HOURS" -> "6", "TOTAL_GIB" -> "10")).totalGib == 10.0)
  }

  test("reports every invalid value in one pass") {
    val errs = errors(Map(
      "MODE"                   -> "fast",
      "RUN_ID"                 -> "a/b",
      "OUTPUT_PATH"            -> "relative/path",
      "BATCH_INTERVAL_SECONDS" -> "5",
      "PAYLOAD_BYTES"          -> "abc",
      "COMPRESSION"            -> "brotli",
      "VERIFY_READ"            -> "yes"))

    Seq("MODE", "RUN_ID", "OUTPUT_PATH", "BATCH_INTERVAL_SECONDS", "PAYLOAD_BYTES", "COMPRESSION", "VERIFY_READ")
      .foreach(key => assert(errs.exists(_.startsWith(key)), s"missing error for $key in $errs"))
  }

  test("requires RUN_ID and OUTPUT_PATH") {
    val errs = errors(Map.empty)
    assert(errs.exists(_.startsWith("RUN_ID")))
    assert(errs.exists(_.startsWith("OUTPUT_PATH")))
  }

  test("requires TOTAL_GIB when a sustained run has no time limit") {
    assert(errors(required + ("DURATION_HOURS" -> "0")).exists(_.startsWith("TOTAL_GIB")))
    assert(ok(required ++ Map("DURATION_HOURS" -> "0", "TOTAL_GIB" -> "50")).durationMillis.isEmpty)
  }
}
