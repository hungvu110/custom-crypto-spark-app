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

  it should "khi lấy mẫu: số dòng và file trùng vẫn chính xác" in {
    val (fileReport, rowReport) = DuplicateCheck.measure(spark, files, source, sample = 4)

    rowReport.rows shouldBe 11L
    rowReport.sample shouldBe 4
    rowReport.estimated shouldBe true
    fileReport.duplicateGroups shouldBe 1
    fileReport.duplicateFiles shouldBe 1
  }

  it should "khi lấy mẫu: ước lượng số dòng trùng gần đúng (mọi bản trùng của một dòng vào mẫu cùng nhau)" in {
    // 200 000 distinct ids; every 10th id appears twice -> 20 000 duplicate groups, 20 000 extra rows, all within one partition
    val ids = spark.range(0, 200000).selectExpr("CAST(id AS STRING) AS isdn", "'c' AS cell")
    val dup = ids.where("CAST(isdn AS BIGINT) % 10 = 0")
    val (_, report) = DuplicateCheck.measure(spark, Nil, ids.union(dup), sample = 8)

    report.rows shouldBe 220000L
    report.duplicateGroups.toDouble shouldBe 20000.0 +- 2000.0 // standard error about 380
    report.extraRows shouldBe report.duplicateGroups // two copies per group
    report.duplicateGroups % 8 shouldBe 0L // sampled count x 8
  }

  /**
   * Hai lô (thư mục) như EBS:  batch=1000/a: r1, r2, r2, r5   batch=1000/b: r3, r5   batch=1020/c: r1, r4
   * Nhóm trùng: r2 (trong một file), r5 (một thư mục, hai file), r1 (hai thư mục: batch=1000 + batch=1020). Mỗi nhóm 2 bản.
   */
  private lazy val batches: Path = {
    val root = workDir.resolve("dt=20261105")
    parquet(root.resolve("batch=1000"), "ba", Seq("r1" -> "c1", "r2" -> "c2", "r2" -> "c2", "r5" -> "c5"))
    parquet(root.resolve("batch=1000"), "bb", Seq("r3" -> "c3", "r5" -> "c5"))
    parquet(root.resolve("batch=1020"), "bc", Seq("r1" -> "c1", "r4" -> "c4"))
    root
  }

  private def profiled(sample: Int) = {
    val uri = batches.toUri.toString
    DuplicateCheck.measureAll(spark, SourceFiles.list(spark, uri),
      spark.read.option("recursiveFileLookup", "true").parquet(uri), sample, Seq("cell"))
  }

  "DuplicateCheck.measureAll" should "phân tích nhóm trùng: số bản, cùng file / cùng thư mục / khác thư mục, cặp thư mục, giá trị cột" in {
    val m = profiled(sample = 1)
    val p = m.profile.get

    m.rows.rows shouldBe 8L
    m.rows.extraRows shouldBe 3L
    p.copies shouldBe Seq(DuplicateShare("2", 3L, 3L))
    p.location.map(s => s.label -> s.extraRows).toMap shouldBe Map("one file" -> 1L, "one folder, other files" -> 1L, "other folders" -> 1L)
    p.folderPairs shouldBe Seq(DuplicateShare("batch=1000 + batch=1020", 1L, 1L))
    p.values.map(v => v.values -> ((v.extraRows, v.rows))).toMap shouldBe
      Map(Seq("c1") -> ((1L, 2L)), Seq("c2") -> ((1L, 2L)), Seq("c5") -> ((1L, 2L)))
  }

  it should "cho cùng số dòng và file trùng như khi không phân tích (cùng một lượt đọc)" in {
    val uri = batches.toUri.toString
    val plain = DuplicateCheck.measure(spark, SourceFiles.list(spark, uri), spark.read.option("recursiveFileLookup", "true").parquet(uri))

    (profiled(sample = 1).files, profiled(sample = 1).rows) shouldBe plain
  }

  it should "giữ HashAggregate khi phân tích (không SortAggregate: sắp xếp mọi dòng của 1,5 TB)" in {
    val plans = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(funcName: String, qe: org.apache.spark.sql.execution.QueryExecution, durationNs: Long): Unit =
        plans.add(qe.executedPlan.toString)
      override def onFailure(funcName: String, qe: org.apache.spark.sql.execution.QueryExecution, exception: Exception): Unit = ()
    }
    spark.listenerManager.register(listener)
    try {
      profiled(sample = 2)
      val deadline = System.currentTimeMillis() + 10000
      while (plans.isEmpty && System.currentTimeMillis() < deadline) Thread.sleep(50) // the listener bus is asynchronous
    } finally spark.listenerManager.unregister(listener)
    val all = plans.asScala.mkString("\n")

    all should include("HashAggregate")
    all should not include "SortAggregate"
  }

  it should "báo lỗi khi cột phân tích không có trong dữ liệu" in {
    val error = intercept[IllegalArgumentException](DuplicateCheck.measureAll(spark, files, source, 1, Seq("nope")))
    error.getMessage should include("DEDUP_PROFILE_COLUMNS: 'nope' is not a column of the data")
  }

  "DuplicateCheck.folderName" should "lấy thư mục ngay trước tên file" in {
    DuplicateCheck.folderName("hdfs://nn:8020/landing/ebs_3g_4g/dt=20261126/batch=1020/part-0.csv") shouldBe "batch=1020"
  }

  "DuplicateCheck.sampleFor" should "đo chính xác tới 64 GiB, lớn hơn thì luỹ thừa 2 nhỏ nhất để mỗi phần <= 64 GiB (tối đa 1024)" in {
    val gib = 1024L * 1024 * 1024
    DuplicateCheck.sampleFor(0L) shouldBe 1
    DuplicateCheck.sampleFor(64 * gib) shouldBe 1
    DuplicateCheck.sampleFor(64 * gib + 1) shouldBe 2
    DuplicateCheck.sampleFor(1500 * gib) shouldBe 32 // EBS, about 1.5 TB a day
    DuplicateCheck.sampleFor(Long.MaxValue) shouldBe 1024
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
