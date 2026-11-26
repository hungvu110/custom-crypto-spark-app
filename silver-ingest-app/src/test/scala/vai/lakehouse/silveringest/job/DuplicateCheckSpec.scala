package vai.lakehouse.silveringest.job

import java.nio.file.{Files, Path, StandardCopyOption}

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Đo dòng trùng và file trùng (DEDUP=check) trên file parquet thật. Bố trí:
 *   a: r1, r2, r2      b: bản sao byte-for-byte của a      c: r1      d: r3, r3      e: (null, x), (x, null)
 * Dòng: 11 (3 + 3 + 1 + 2 + 2). Nhóm trùng: r1 (a, b, c: 3 dòng), r2 (a, b: 4 dòng), r3 (chỉ trong d: 2 dòng). (null, x) và (x, null) KHÔNG trùng.
 * File: a và b trùng nhau.
 */
class DuplicateCheckSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val workDir = Files.createTempDirectory("duplicate-check")
  private var spark: SparkSession = _
  private val schema = StructType(Seq(StructField("isdn", StringType), StructField("cell", StringType)))

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().appName("DuplicateCheckSpec").master("local[2]")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  /** Một file parquet `name` trong `dir`, với các dòng cho trước theo đúng thứ tự. */
  private def parquet(dir: Path, name: String, rows: Seq[(String, String)]): Path = {
    val tmp = workDir.resolve(s"tmp_$name")
    spark.createDataFrame(spark.sparkContext.parallelize(rows.map { case (a, b) => Row(a, b) }, 1), schema)
      .coalesce(1).write.parquet(tmp.toString)
    val part = Files.list(tmp).iterator().asScala.find(_.getFileName.toString.endsWith(".parquet")).get
    Files.createDirectories(dir)
    Files.move(part, dir.resolve(s"$name.parquet"), StandardCopyOption.REPLACE_EXISTING)
  }

  private lazy val day: Path = {
    val dir = workDir.resolve("date=20261104")
    val a = parquet(dir, "a", Seq("r1" -> "c1", "r2" -> "c2", "r2" -> "c2"))
    Files.copy(a, dir.resolve("b.parquet"))
    parquet(dir, "c", Seq("r1" -> "c1"))
    parquet(dir, "d", Seq("r3" -> "c3", "r3" -> "c3"))
    parquet(dir, "e", Seq((null, "x"), ("x", null)))
    dir
  }

  private def files: Seq[SourceFile] = SourceFiles.list(spark, day.toUri.toString)

  private def source: DataFrame = spark.read.parquet(day.toUri.toString)

  private def measured = DuplicateCheck.measure(spark, files, source)

  "DuplicateCheck.measure" should "đếm dòng thừa, số nhóm trùng, và nhóm trùng chỉ nằm trong một file" in {
    val (_, report) = measured

    report.rows shouldBe 11L
    report.duplicateGroups shouldBe 3L // r1, r2, r3
    report.extraRows shouldBe 6L // r1: 2, r2: 3, r3: 1 (the rows that a dedup would remove)
    report.withinFileGroups shouldBe 1L // r3 (only in d)
    report.withinFileExtraRows shouldBe 1L
  }

  it should "không coi (null, x) và (x, null) là trùng (xxhash64 bỏ qua null)" in {
    val single = spark.read.parquet(day.resolve("e.parquet").toUri.toString)
    val (_, report) = DuplicateCheck.measure(spark, files.filter(_.path.endsWith("e.parquet")), single)

    report.rows shouldBe 2L
    report.duplicateGroups shouldBe 0L
  }

  it should "tìm file trùng nội dung trên executor, trong cùng lượt đọc (không đọc file trên driver)" in {
    val (report, _) = measured

    report.files shouldBe 5
    report.withRows shouldBe 5
    report.duplicateGroups shouldBe 1 // a and b
    report.duplicateFiles shouldBe 1 // one file too many
  }

  "DuplicateCheck.groupFiles" should "chỉ gộp file cùng số dòng và cùng tổng hash" in {
    val groups = DuplicateCheck.groupFiles(Seq(
      "x" -> ((3L, 10L, 20L)), "y" -> ((3L, 10L, 20L)), "z" -> ((4L, 10L, 20L)), "w" -> ((3L, 11L, 20L))))

    groups shouldBe Seq(Seq("x", "y"))
  }

  "DuplicateCheck.run" should "không bao giờ làm hỏng lần nạp: lỗi khi đo chỉ ghi cảnh báo" in {
    noException should be thrownBy DuplicateCheck.run(spark, files, spark.range(1).selectExpr("map('a', 1) AS m")) // a map cannot be hashed
  }
}
