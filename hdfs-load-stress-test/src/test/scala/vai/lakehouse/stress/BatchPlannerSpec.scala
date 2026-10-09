package vai.lakehouse.stress

import org.scalatest.funsuite.AnyFunSuite

class BatchPlannerSpec extends AnyFunSuite {

  private val MiB = 1024L * 1024

  test("sizes a nominal 355.6 MiB batch into 3 files of at most 128 MiB") {
    val target = 372827022L
    assert(BatchPlanner.partitionsFor(target, 128 * MiB) == 3)
    assert(BatchPlanner.rowsFor(target, BatchPlanner.initialBytesPerRow(1024)) == 347787L)
  }

  test("always plans at least one row and one file") {
    assert(BatchPlanner.rowsFor(1L, 1072.0) == 1L)
    assert(BatchPlanner.partitionsFor(1L, 128 * MiB) == 1)
  }

  test("moves the bytes-per-row estimate halfway towards the measured value") {
    assert(BatchPlanner.updateEstimate(1000.0, actualBytes = 1200000L, rows = 1000L) == 1100.0)
    assert(BatchPlanner.updateEstimate(1000.0, actualBytes = 0L, rows = 1000L) == 1000.0)
    assert(BatchPlanner.updateEstimate(1000.0, actualBytes = 5L, rows = 0L) == 1000.0)
  }

  test("caps the last batch at what remains of the total") {
    assert(BatchPlanner.nextBatchBytes(100L, totalBytes = 250L, bytesDone = 200L) == 50L)
    assert(BatchPlanner.nextBatchBytes(100L, totalBytes = 250L, bytesDone = 260L) == 0L)
  }

  test("schedules sustained batches on a fixed grid from the run start") {
    assert(BatchPlanner.scheduledStart(1000L, 0, 300) == 1000L)
    assert(BatchPlanner.scheduledStart(1000L, 2, 300) == 601000L)
  }

  test("gives each batch its own id range") {
    assert(BatchPlanner.firstId(0) == 0L)
    assert(BatchPlanner.firstId(3) == 3L * BatchPlanner.IdsPerBatch)
  }

  test("adds the block token plugin once and keeps plugins declared in the manifest") {
    val plugin = HdfsLoadStressApp.BlockTokenPluginClass
    assert(HdfsLoadStressApp.mergedPlugins(None) == plugin)
    assert(HdfsLoadStressApp.mergedPlugins(Some("a.B, c.D")) == s"a.B,c.D,$plugin")
    assert(HdfsLoadStressApp.mergedPlugins(Some(plugin)) == plugin)
  }
}
