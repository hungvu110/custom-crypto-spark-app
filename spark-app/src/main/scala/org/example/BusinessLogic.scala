package org.example

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Logic nghiệp vụ thuần (không đụng SparkSession.builder / HDFS I/O), tách
 * riêng để unit test bằng SparkSession local — không cần cluster Isilon thật.
 *
 * Flow nghiệp vụ: tạo DB -> tạo bảng -> insert dữ liệu mẫu -> query lại,
 * giống hệt vlp-spark-sample/MainApp.scala. SparkApp chịu trách nhiệm phần
 * Kerberos/wire-encryption/diagnostics đặc thù cho Isilon HKH và gọi các hàm
 * ở đây để thực thi phần nghiệp vụ.
 */
object BusinessLogic {

  val schema: StructType = StructType(Seq(
    StructField("id",         LongType,   nullable = false),
    StructField("name",       StringType, nullable = true),
    StructField("city",       StringType, nullable = true),
    StructField("created_at", StringType, nullable = true)
  ))

  def sampleRows: Seq[Row] = Seq(
    Row(1L, "Alice Nguyen",  "Ho Chi Minh City", "2025-01-01 08:00:00"),
    Row(2L, "Bob Tran",      "Hanoi",            "2025-01-02 09:30:00"),
    Row(3L, "Charlie Le",    "Da Nang",          "2025-01-03 10:15:00"),
    Row(4L, "Diana Pham",    "Can Tho",          "2025-01-04 11:45:00"),
    Row(5L, "Edward Hoang",  "Hai Phong",        "2025-01-05 14:00:00")
  )

  def sampleDataFrame(spark: SparkSession): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(sampleRows), schema)

  def quoteIdent(identifier: String): String = s"`${identifier.replace("`", "``")}`"

  def qualifiedTable(dbName: String, tableName: String): String =
    s"${quoteIdent(dbName)}.${quoteIdent(tableName)}"

  /**
   * Sinh DDL CREATE TABLE theo tableType: `delta`/`iceberg` -> `USING <type>`,
   * `hive` -> `STORED AS PARQUET`. Caller (SparkApp) đảm bảo tableType đã
   * được validate trước khi gọi hàm này.
   */
  def buildCreateTableSql(fullTableQuoted: String, tableType: String): String = {
    val storageClause = if (tableType == "hive") "STORED AS PARQUET" else s"USING $tableType"
    s"""CREATE TABLE IF NOT EXISTS $fullTableQuoted (
       |  id         BIGINT,
       |  name       STRING,
       |  city       STRING,
       |  created_at STRING
       |)
       |$storageClause""".stripMargin
  }
}
