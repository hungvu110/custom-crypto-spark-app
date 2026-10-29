package vai.lakehouse.silveringest.job

import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SourceFilesSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val workDir = Files.createTempDirectory("source-files")
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().appName("SourceFilesSpec").master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    // A day folder of VTT (hour=HH) and of EBS (batch=HHmm), and a NIMS day with the time in the file name.
    Seq(
      "vtt/date=20261013/hour=09/part-1.csv", "vtt/date=20261013/hour=10/part-1.csv", "vtt/date=20261013/hour=11/part-1.csv",
      "vtt/date=20261013/hour=11/_SUCCESS", "vtt/date=20261013/hour=12/part-1.csv",
      "ebs/dt=20261013/batch=1000/a.csv", "ebs/dt=20261013/batch=1010/a.csv", "ebs/dt=20261013/batch=1100/a.csv",
      "ebs/dt=20261013/batch=1200/a.csv",
      "nims/dt=20261013/batch=1/infra_cell_4g-100012.csv", "nims/dt=20261013/batch=1/infra_cell_4g-130012.csv",
      "nims/dt=20261013/batch=2/infra_cell_4g-110500.csv").foreach(touch)
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def touch(relative: String): Path = {
    val path = workDir.resolve(relative)
    Files.createDirectories(path.getParent)
    Files.write(path, "x\n".getBytes("UTF-8"))
  }

  private def at(relative: String): String = workDir.resolve(relative).toString

  /** The listed files, relative to the work dir, so the assertions do not depend on the temp dir name. */
  private def listed(relative: String): Seq[String] = {
    val root = workDir.toString
    SourceFiles.list(spark, at(relative)).map(f => f.path.substring(f.path.indexOf(root) + root.length + 1))
  }

  "SourceFiles.list" should "list every file under a directory, skipping hidden files" in {
    listed("vtt/date=20261013") shouldBe Seq(
      "vtt/date=20261013/hour=09/part-1.csv", "vtt/date=20261013/hour=10/part-1.csv", "vtt/date=20261013/hour=11/part-1.csv",
      "vtt/date=20261013/hour=12/part-1.csv")
  }

  it should "list only the folders that a {a,b} pattern selects (a range of hours)" in {
    listed("vtt/date=20261013/hour={10,11}") shouldBe Seq("vtt/date=20261013/hour=10/part-1.csv", "vtt/date=20261013/hour=11/part-1.csv")
  }

  it should "select the batch folders of an hour when the hour is the start of the folder name" in {
    listed("ebs/dt=20261013/batch={10,11}*") shouldBe Seq(
      "ebs/dt=20261013/batch=1000/a.csv", "ebs/dt=20261013/batch=1010/a.csv", "ebs/dt=20261013/batch=1100/a.csv")
  }

  it should "select files by the hour in their name across batch folders" in {
    listed("nims/dt=20261013/batch=*/infra_cell_4g-{10,11}*") shouldBe Seq(
      "nims/dt=20261013/batch=1/infra_cell_4g-100012.csv", "nims/dt=20261013/batch=2/infra_cell_4g-110500.csv")
  }

  it should "list a single file once" in {
    listed("vtt/date=20261013/hour=10/part-1.csv") shouldBe Seq("vtt/date=20261013/hour=10/part-1.csv")
  }

  it should "fail when a path or a pattern matches nothing" in {
    val missing = the[IllegalArgumentException] thrownBy SourceFiles.list(spark, at("vtt/date=20250101"))
    missing.getMessage should include("INPUT_PATH does not exist")
    val noMatch = the[IllegalArgumentException] thrownBy SourceFiles.list(spark, at("vtt/date=20261013/hour={20,21}"))
    noMatch.getMessage should include("INPUT_PATH does not exist")
  }

  it should "fail when the folder holds no file" in {
    Files.createDirectories(workDir.resolve("vtt/date=20261014/hour=01"))
    val empty = the[IllegalArgumentException] thrownBy SourceFiles.list(spark, at("vtt/date=20261014"))
    empty.getMessage should include("No file under INPUT_PATH")
  }
}
