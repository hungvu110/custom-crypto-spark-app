package vai.lakehouse.pseudonymizetable.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.pseudonymizetable.config.{JobConfig, TableInput}

class TableRangeSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder().appName("table-range-spec").master("local[1]")
    .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate()

  override protected def afterAll(): Unit = spark.stop()

  private val base = Map("DB_NAME" -> "crm", "TABLE_NAME" -> "subscriber", "PSEUDO_COLUMNS" -> "isdn")

  /** Rows `(isdn, date, hour)` for 3 days x hours 00, 05, 06, 12, 18, 23. */
  private lazy val data: DataFrame = {
    import spark.implicits._
    val rows = for {
      d <- Seq("20261012", "20261013", "20261014")
      h <- Seq("00", "05", "06", "12", "18", "23")
    } yield (s"$d$h", d, h)
    rows.toDF("isdn", "date", "hour")
  }

  private def select(env: (String, String)*): Seq[String] = {
    val range = JobConfig.fromEnv(base ++ env).toOption.get.input.asInstanceOf[TableInput].range.get
    TableRange(data, range, "crm.subscriber").select("isdn").collect().map(_.getString(0)).sorted.toSeq
  }

  "TableRange" should "keep whole days between FROM and TO (inclusive)" in {
    select("INPUT_FROM" -> "20261013", "INPUT_TO" -> "20261013") should have size 6
    select("INPUT_FROM" -> "20261013") should have size 12
    select("INPUT_TO" -> "20261012") should have size 6
    select("INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261014") should have size 18
  }

  it should "start a day at hour 00 and end it at hour 23 when FROM/TO have no hour" in {
    val rows = select("INPUT_FROM" -> "20261013", "INPUT_TO" -> "20261013")
    rows.head shouldBe "2026101300"
    rows.last shouldBe "2026101323"
  }

  it should "cut inside a day when FROM/TO carry an hour, across several days" in {
    select("INPUT_FROM" -> "2026101218", "INPUT_TO" -> "2026101305") shouldBe
      Seq("2026101218", "2026101223", "2026101300", "2026101305")
  }

  it should "apply INPUT_HOURS to every day, single hour or range" in {
    select("INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261013", "INPUT_HOURS" -> "18") shouldBe
      Seq("2026101218", "2026101318")
    select("INPUT_FROM" -> "20261012", "INPUT_TO" -> "20261012", "INPUT_HOURS" -> "5-12") shouldBe
      Seq("2026101205", "2026101206", "2026101212")
  }

  it should "combine INPUT_HOURS with an hour in FROM/TO" in {
    select("INPUT_FROM" -> "2026101206", "INPUT_TO" -> "2026101312", "INPUT_HOURS" -> "12-23") shouldBe
      Seq("2026101212", "2026101218", "2026101223", "2026101312")
  }

  it should "work with INPUT_HOURS alone (no date bounds)" in {
    select("INPUT_HOURS" -> "23") shouldBe Seq("2026101223", "2026101323", "2026101423")
  }

  it should "fail with a clear message when the table has no date/hour partition columns" in {
    val range = JobConfig.fromEnv(base + ("INPUT_FROM" -> "20261012")).toOption.get.input.asInstanceOf[TableInput].range.get
    val e = intercept[IllegalArgumentException](TableRange(data.drop("hour"), range, "crm.subscriber"))

    e.getMessage should include("missing: hour")
    e.getMessage should include("crm.subscriber")
  }
}
