package vai.lakehouse.pseudonym

import java.nio.file.Files

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.scalatest.{BeforeAndAfterAll, Suite}

/** SparkSession `local[2]` có Delta (cho chế độ MAPPING), dựng 1 lần cho mỗi spec. */
trait SparkTestSession extends BeforeAndAfterAll { self: Suite =>

  protected val workDir = Files.createTempDirectory("pseudonym-udf-test")
  protected var spark: SparkSession = _

  /** Extension thêm của spec (vd PseudonymExtension). */
  protected def extraExtensions: Seq[SparkSessionExtensions => Unit] = Nil

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val builder = SparkSession.builder()
      .appName(getClass.getSimpleName).master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", workDir.resolve("warehouse").toString)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = extraExtensions.foldLeft(builder)(_ withExtensions _).getOrCreate()
  }

  override def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      super.afterAll()
    }
  }

  protected def tempPath(name: String): String = workDir.resolve(name).toString
}
