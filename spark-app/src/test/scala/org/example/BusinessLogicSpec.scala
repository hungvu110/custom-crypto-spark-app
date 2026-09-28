package org.example

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit test cho logic nghiệp vụ thuần (BusinessLogic), chạy bằng SparkSession
 * local[*] — không cần cluster Isilon thật. Phần I/O thật với HDFS/wire
 * encryption chỉ verify được trên môi trường có Kerberos + Isilon, xem
 * mục 14 của SPARK_HDFS_WIRE_ENCRYPTION_TASK.md.
 */
class BusinessLogicSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("BusinessLogicSpec")
      .master("local[2]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  "sampleDataFrame" should "sinh đúng schema và số dòng đã khai báo" in {
    val df = BusinessLogic.sampleDataFrame(spark)

    df.schema shouldBe BusinessLogic.schema
    df.count() shouldBe BusinessLogic.sampleRows.size
  }

  it should "chứa đúng dữ liệu của từng dòng mẫu" in {
    val df = BusinessLogic.sampleDataFrame(spark)

    val ids = df.collect().map(_.getAs[Long]("id")).sorted
    ids shouldBe Array(1L, 2L, 3L, 4L, 5L)
  }

  "quoteIdent" should "bọc identifier trong backtick" in {
    BusinessLogic.quoteIdent("demo_db") shouldBe "`demo_db`"
  }

  it should "escape backtick lồng bên trong identifier" in {
    BusinessLogic.quoteIdent("weird`name") shouldBe "`weird``name`"
  }

  "qualifiedTable" should "ghép db.table, mỗi phần đều được quote riêng" in {
    BusinessLogic.qualifiedTable("demo_db", "sample_table") shouldBe "`demo_db`.`sample_table`"
  }

  "buildCreateTableSql" should "sinh USING <type> cho delta" in {
    val sql = BusinessLogic.buildCreateTableSql("`demo_db`.`sample_table`", "delta")

    sql should include("CREATE TABLE IF NOT EXISTS `demo_db`.`sample_table`")
    sql should include("USING delta")
    sql should not include "STORED AS PARQUET"
  }

  it should "sinh USING <type> cho iceberg" in {
    val sql = BusinessLogic.buildCreateTableSql("`demo_db`.`sample_table`", "iceberg")

    sql should include("USING iceberg")
  }

  it should "sinh STORED AS PARQUET cho hive thay vì USING hive" in {
    val sql = BusinessLogic.buildCreateTableSql("`demo_db`.`sample_table`", "hive")

    sql should include("STORED AS PARQUET")
    sql should not include "USING hive"
  }
}
