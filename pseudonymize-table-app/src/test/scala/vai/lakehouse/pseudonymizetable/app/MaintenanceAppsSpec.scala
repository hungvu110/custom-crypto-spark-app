package vai.lakehouse.pseudonymizetable.app

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.pseudonymizetable.config.{MaintenanceConfig, VacuumTarget}
import vai.lakehouse.pseudonymizetable.job.Maintenance

/** Xoá vùng khai thác của phiếu hết hạn và VACUUM, chạy app thật trên bảng Delta trong catalog của 1 session local. */
class MaintenanceAppsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val workDir = Files.createTempDirectory("maintenance")
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().appName("MaintenanceAppsSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", workDir.resolve("warehouse").toString)
      .config("spark.sql.extensions", SparkSessions.DeltaExtension)
      .config("spark.sql.catalog.spark_catalog", SparkSessions.DeltaCatalog)
      .getOrCreate()
    spark.sql("CREATE DATABASE deid")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def table(name: String, partitionedBy: String): Unit = {
    spark.sql(s"CREATE TABLE deid.$name (id BIGINT, isdn STRING, dt STRING, hour STRING) USING delta PARTITIONED BY ($partitionedBy)")
    spark.sql(
      s"""INSERT INTO deid.$name (id, isdn, dt, hour) VALUES
         |  (1, 'a', '20261012', '05'), (2, 'b', '20261012', '18'),
         |  (3, 'c', '20261013', '05'), (4, 'd', '20261013', '18'),
         |  (5, 'e', '20261014', '05')""".stripMargin)
  }

  private def ids(name: String): Set[Long] = spark.table(s"deid.$name").select("id").collect().map(_.getLong(0)).toSet

  private val keys = Seq("INPUT_DATE_KEY" -> "dt", "INPUT_HOUR_KEY" -> "hour")

  private def purge(name: String, extra: (String, String)*): Int =
    PurgeRangeApp.execute((Seq("OUTPUT_DB" -> "deid", "OUTPUT_TABLE" -> name, "ENABLE_HIVE_SUPPORT" -> "false") ++ keys ++ extra).toMap, Some(spark))

  "PurgeRangeApp" should "delete only the partitions of the range and keep the rest" in {
    table("team_a", "dt, hour")

    purge("team_a", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261013") shouldBe 0

    ids("team_a") shouldBe Set(5L)
  }

  it should "delete only some hours of each day with INPUT_HOURS" in {
    table("team_hours", "dt, hour")

    purge("team_hours", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261014", "INPUT_HOURS" -> "18") shouldBe 0

    ids("team_hours") shouldBe Set(1L, 3L, 5L)
  }

  it should "be idempotent: a second call finds nothing to delete and still succeeds" in {
    table("team_twice", "dt, hour")

    purge("team_twice", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012") shouldBe 0
    purge("team_twice", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012") shouldBe 0

    ids("team_twice") shouldBe Set(3L, 4L, 5L)
  }

  it should "succeed when the table does not exist (deleted before or never created)" in {
    purge("never_created", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012") shouldBe 0
  }

  it should "never touch another team's table" in {
    table("team_b1", "dt, hour")
    table("team_b2", "dt, hour")

    purge("team_b1", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261014") shouldBe 0

    ids("team_b1") shouldBe empty
    ids("team_b2") shouldBe Set(1L, 2L, 3L, 4L, 5L)
  }

  it should "delete whole days of a table partitioned by dt only" in {
    table("team_day", "dt")

    purge("team_day", "INPUT_FROM" -> "20261013", "INPUT_TO" -> "20261013") shouldBe 0

    ids("team_day") shouldBe Set(1L, 2L, 5L)
  }

  it should "refuse an hour constraint on a day-partitioned table and delete nothing" in {
    table("team_day_hours", "dt")

    purge("team_day_hours", "INPUT_FROM" -> "20261013", "INPUT_TO" -> "20261013", "INPUT_HOURS" -> "18") shouldBe MaintenanceExit.RuntimeError

    ids("team_day_hours") shouldBe Set(1L, 2L, 3L, 4L, 5L)
  }

  it should "refuse a range on a column that is not a partition and delete nothing" in {
    table("team_other", "hour")

    purge("team_other", "INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012") shouldBe MaintenanceExit.RuntimeError

    ids("team_other") shouldBe Set(1L, 2L, 3L, 4L, 5L)
  }

  it should "reject an open range (it would delete the whole table) as a config error" in {
    table("team_open", "dt, hour")

    purge("team_open", "INPUT_FROM" -> "20261012") shouldBe MaintenanceExit.ConfigError
    purge("team_open", "INPUT_TO" -> "20261012") shouldBe MaintenanceExit.ConfigError

    ids("team_open") should have size 5
  }

  it should "report every config error at once" in {
    MaintenanceConfig.purgeFromEnv(Map("OUTPUT_DB" -> "d;rop", "INPUT_FROM" -> "20261340")).left.toOption.get.mkString("|") should (
      include("OUTPUT_DB") and include("OUTPUT_TABLE: is required") and include("INPUT_TO: is required") and include("invalid date"))
  }

  "VacuumApp" should "vacuum the Delta tables of a database and skip tables that are not Delta" in {
    table("v1", "dt, hour")
    spark.sql("CREATE TABLE deid.plain (id BIGINT) USING parquet")

    val result = Maintenance.vacuum(spark, MaintenanceConfig.vacuumFromEnv(Map("VACUUM_TARGETS" -> "deid")).toOption.get)

    result.vacuumed should contain("deid.v1")
    result.skipped should contain("deid.plain")
    result.failed shouldBe empty
  }

  it should "exit 0 for a good target and 1 when a target cannot be vacuumed" in {
    val env = Map("ENABLE_HIVE_SUPPORT" -> "false")

    VacuumApp.execute(env + ("VACUUM_TARGETS" -> "deid.v1"), Some(spark)) shouldBe 0
    VacuumApp.execute(env + ("VACUUM_TARGETS" -> "no_such_db"), Some(spark)) shouldBe MaintenanceExit.RuntimeError
  }

  it should "accept a retention below 168 hours (the job turns the Delta check off) and reject nonsense" in {
    VacuumApp.execute(Map("VACUUM_TARGETS" -> "deid.v1", "VACUUM_RETAIN_HOURS" -> "24", "ENABLE_HIVE_SUPPORT" -> "false"), Some(spark)) shouldBe 0
    MaintenanceConfig.vacuumFromEnv(Map("VACUUM_TARGETS" -> "deid", "VACUUM_RETAIN_HOURS" -> "0")).isLeft shouldBe true
    MaintenanceConfig.vacuumFromEnv(Map("VACUUM_TARGETS" -> "de id")).left.toOption.get.mkString should include("must be <db> or <db>.<table>")
    MaintenanceConfig.vacuumFromEnv(Map.empty).left.toOption.get.mkString should include("VACUUM_TARGETS: is required")
    MaintenanceConfig.vacuumFromEnv(Map("VACUUM_TARGETS" -> "a,b.c")).toOption.get.targets shouldBe
      Seq(VacuumTarget("a", None), VacuumTarget("b", Some("c")))
  }
}
