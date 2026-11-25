package vai.lakehouse.silveringest.job

import java.nio.file.Files

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, countDistinct, input_file_name, lit, sha2}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.silveringest.app.SparkSessions
import vai.lakehouse.silveringest.config.{IngestConfig, Mode, OutputFiles}

/**
 * Số file ghi ra mỗi partition. Không gom lại thì mỗi task ghi một file cho MỖI giờ có trong dữ liệu của nó (VTT: mọi task có đủ 24 giờ),
 * nên số file mỗi giờ bằng số task đọc. Gom lại (`REBALANCE` + AQE) thì số file mỗi giờ theo kích thước dữ liệu của giờ đó.
 */
class TableWriterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val workDir = Files.createTempDirectory("table-writer")
  private var spark: SparkSession = _
  private val advisoryKey = "spark.sql.adaptive.advisoryPartitionSizeInBytes"
  private val inputTasks = 20

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().appName("TableWriterSpec").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", workDir.resolve("warehouse").toString)
      .config("spark.sql.extensions", SparkSessions.DeltaExtension)
      .config("spark.sql.catalog.spark_catalog", SparkSessions.DeltaCatalog)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def config(table: String, extra: (String, String)*): IngestConfig =
    IngestConfig.fromEnv(Mode.Passthrough, Map("INPUT_PATH" -> "file:///unused", "INPUT_FORMAT" -> "parquet", "OUTPUT_DB" -> "silver_test",
      "OUTPUT_TABLE" -> table, "BATCH_COLUMN" -> "dt", "BATCH_VALUE" -> "20261104", "OUTPUT_PARTITION_BY" -> "hour") ++ extra)
      .fold(e => fail(e.mkString(" | ")), identity)

  /** `rows` dòng chia đều cho `hours`, đọc bằng `inputTasks` task: mọi task có dòng của mọi giờ, như file landing của VTT. */
  private def data(rows: Long, hours: Seq[String]): DataFrame =
    spark.range(0, rows, 1, inputTasks).select(
      col("id"),
      // Chuỗi ngẫu nhiên (khó nén) để kích thước dữ liệu shuffle gần với dữ liệu thật.
      concat(sha2(col("id").cast("string"), 256), sha2(concat(col("id").cast("string"), lit("x")), 256)).as("payload"),
      (col("id") % hours.size).as("slot"))
      .selectExpr("id", "payload", s"element_at(array(${hours.map(h => s"'$h'").mkString(",")}), cast(slot AS INT) + 1) AS hour")

  private def filesPerHour(table: String): Map[String, Long] =
    spark.table(s"silver_test.$table").select(col("hour"), input_file_name().as("file")).groupBy("hour")
      .agg(countDistinct("file").as("files")).collect()
      .map(r => r.getString(0) -> r.getLong(1)).toMap

  "TableWriter.write" should "ghi MỘT file cho mỗi giờ nhỏ khi gom lại theo partition (mặc định)" in {
    val cfg = config("rebalanced")
    cfg.outputFiles.rebalance shouldBe true

    TableWriter.write(spark, cfg, data(30000, Seq("10", "11", "12")))

    filesPerHour("rebalanced") shouldBe Map("10" -> 1L, "11" -> 1L, "12" -> 1L)
  }

  it should "ghi một file cho mỗi task và mỗi giờ khi OUTPUT_REBALANCE=false, như trước" in {
    TableWriter.write(spark, config("per_task", "OUTPUT_REBALANCE" -> "false"), data(30000, Seq("10", "11", "12")))

    filesPerHour("per_task") shouldBe Map("10" -> inputTasks.toLong, "11" -> inputTasks.toLong, "12" -> inputTasks.toLong)
  }

  it should "chia một giờ lớn thành nhiều file theo kích thước dữ liệu shuffle mỗi file" in {
    // Khoảng 100k dòng x 140 byte = 14 MB cho một giờ, mỗi task đọc góp khoảng 0,7 MB. AQE chỉ tách theo ranh giới các phần của từng task
    // đọc, nên 4 MB mỗi file gộp được vài phần một: phải ra nhiều file, ít hơn hẳn 20 task. (Dữ liệu thật: mỗi task góp khoảng 5 MB cho
    // một giờ, rất nhỏ so với 384 MB mỗi file.)
    val cfg = config("split").copy(outputFiles = OutputFiles(rebalance = true, shuffleBytesPerFile = 4L * 1024 * 1024))

    TableWriter.write(spark, cfg, data(100000, Seq("10")))

    val files = filesPerHour("split")("10")
    files should be > 1L
    files should be < (inputTasks / 2).toLong
  }

  it should "vẫn thay đúng partition của lô khi bảng đã có (replaceWhere) và gom file như lần đầu" in {
    TableWriter.write(spark, config("rewrite"), data(3000, Seq("10", "11")))
    TableWriter.write(spark, config("rewrite"), data(6000, Seq("10", "11")))

    spark.table("silver_test.rewrite").count() shouldBe 6000L
    filesPerHour("rewrite") shouldBe Map("10" -> 1L, "11" -> 1L)
  }

  it should "trả lại kích thước partition của AQE sau khi ghi, kể cả khi ghi lỗi" in {
    spark.conf.set(advisoryKey, "12345")
    TableWriter.write(spark, config("restore"), data(1000, Seq("10")))
    spark.conf.get(advisoryKey) shouldBe "12345"

    // Bảng đã partition theo (dt, hour): ghi với partition khác bị từ chối.
    an[IllegalStateException] should be thrownBy TableWriter.write(spark, config("restore", "OUTPUT_PARTITION_BY" -> ""), data(10, Seq("10")))
    spark.conf.get(advisoryKey) shouldBe "12345"

    spark.conf.unset(advisoryKey)
    val before = spark.conf.getOption(advisoryKey)
    TableWriter.write(spark, config("restore"), data(1000, Seq("10")))
    spark.conf.getOption(advisoryKey) shouldBe before
  }

  it should "không để lại temp view sau khi ghi" in {
    TableWriter.write(spark, config("no_view"), data(1000, Seq("10")))

    spark.catalog.listTables().collect().filter(_.isTemporary).map(_.name) shouldBe empty
  }
}
