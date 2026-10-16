package vai.lakehouse.pseudonymizetable.job

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Regression test for a performance bug: with INPUT_BAD_FILES=skip the plan contains a nondeterministic tracking
 * expression, so a partition filter applied ABOVE it was not pushed down and every per-partition write job scanned all
 * partitions. The filter must be applied to the source before the transformations.
 */
class PartitionPruningSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder().appName("pruning-spec").master("local[1]")
    .config("spark.ui.enabled", "false").getOrCreate()

  override protected def afterAll(): Unit = spark.stop()

  private def filesScanned(df: DataFrame): Int =
    df.queryExecution.sparkPlan.collect { case s: FileSourceScanExec => s.selectedPartitions.flatMap(_.files).size }.sum

  "the per-partition plan" should "scan only the selected partition even with the skip tracker attached" in {
    val base = Files.createTempDirectory("pruning")
    Seq("0000", "0020", "0040").foreach { b =>
      val dir = Files.createDirectories(base.resolve("dt=20261014").resolve(s"batch=$b"))
      Files.write(dir.resolve("part-0.csv"), s"a$b\n".getBytes(UTF_8))
    }
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    val source = spark.read.format("csv").option("basePath", base.toString).schema("a STRING")
      .load(Seq("0000", "0020", "0040").map(b => s"$base/dt=20261014/batch=$b"): _*)
    val acc = FileTracker.newAccumulator(spark)
    val condition = PseudonymizeTableJob.partitionCondition(Seq("dt", "batch"), Seq("20261014", "0020"))

    // wrong: the filter above the nondeterministic tracker cannot be pushed down -> all 3 files scanned
    filesScanned(FileTracker.attach(source, "a", acc).where(condition)) shouldBe 3
    // right: the filter on the source prunes to the single partition directory
    filesScanned(FileTracker.attach(source.where(condition), "a", acc)) shouldBe 1
  }
}
