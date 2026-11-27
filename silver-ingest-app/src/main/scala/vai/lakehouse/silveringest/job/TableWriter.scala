package vai.lakehouse.silveringest.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import vai.lakehouse.silveringest.config.{Batch, IngestConfig, OutputFormat}

/**
 * Ghi DataFrame vào bảng đích `OUTPUT_DB.OUTPUT_TABLE` (tạo database nếu chưa có), theo `OUTPUT_FORMAT`/`OUTPUT_MODE`.
 *
 * Có lô (`BATCH_VALUE`): thêm cột lô làm partition đầu tiên. `overwrite-partitions` chỉ thay partition của lô này: Delta dùng
 * `replaceWhere` (thay đúng các dòng của lô, mọi partition con), hive dùng dynamic partition overwrite (thay các partition có trong dữ
 * liệu lần này). Bảng đã có phải được partition đúng như cấu hình, không thì báo lỗi trước khi ghi. Trước khi ghi, dữ liệu được gom theo
 * partition để file có kích thước gần như nhau (`OutputFiles`) khi cần (`IngestConfig.rebalances`).
 */
/** Kết quả của một commit Delta: dòng, file và byte đã ghi. */
final case class CommitMetrics(rows: Long, files: Long, bytes: Long)

object TableWriter {

  /** Kích thước partition mà AQE nhắm tới khi tách và gộp partition của `REBALANCE`, đặt riêng cho lần ghi. */
  private[job] val AdvisoryPartitionSize = "spark.sql.adaptive.advisoryPartitionSizeInBytes"

  def write(spark: SparkSession, cfg: IngestConfig, data: DataFrame): Unit = {
    val withBatch = cfg.batch.fold(data) { b =>
      data.columns.find(_.equalsIgnoreCase(b.column)).foreach { c =>
        throw new IllegalArgumentException(s"BATCH_COLUMN '${b.column}' already exists in the data (column '$c'): choose another BATCH_COLUMN")
      }
      data.withColumn(b.column, lit(b.value))
    }
    if (cfg.rebalances) {
      withSessionConf(spark, AdvisoryPartitionSize, cfg.outputFiles.shuffleBytesPerFile.toString) {
        rebalanced(spark, cfg, withBatch)(df => writeTable(spark, cfg, df))
      }
    } else writeTable(spark, cfg, withBatch)
  }

  /**
   * Gom các dòng của cùng một partition về cùng các task trước khi ghi (`REBALANCE`): AQE tách partition lớn thành các phần khoảng
   * `advisoryPartitionSizeInBytes` và gộp các phần nhỏ, nên mỗi file có kích thước gần như nhau thay vì mỗi task một file mỗi partition.
   * Bảng không có partition thì chỉ chia lại theo kích thước. Cú pháp hint SQL vì `Dataset.hint("rebalance", cột)` bị từ chối khi cột đã
   * được resolve; temp view của lần ghi bị xoá sau khi ghi (bulk load ghi nhiều ngày trong cùng một session).
   */
  private def rebalanced[T](spark: SparkSession, cfg: IngestConfig, df: DataFrame)(body: DataFrame => T): T = {
    val view = s"silver_ingest_rebalance_${java.util.UUID.randomUUID().toString.replace("-", "")}"
    df.createOrReplaceTempView(view)
    try {
      val hint = if (cfg.partitionBy.isEmpty) "REBALANCE" else s"REBALANCE(${cfg.partitionBy.map(quote).mkString(", ")})"
      body(spark.sql(s"SELECT /*+ $hint */ * FROM ${quote(view)}"))
    } finally spark.catalog.dropTempView(view)
  }

  private def writeTable(spark: SparkSession, cfg: IngestConfig, df: DataFrame): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${quote(cfg.outputDb)}")
    val table = s"${quote(cfg.outputDb)}.${quote(cfg.outputTable)}"
    val exists = spark.catalog.tableExists(cfg.outputDb, cfg.outputTable)
    if (exists && cfg.batch.isDefined) requireSamePartitioning(spark, cfg, table)

    (cfg.outputMode, cfg.batch) match {
      case (IngestConfig.OverwritePartitions, Some(b)) if exists =>
        if (cfg.outputFormat == OutputFormat.Delta) {
          df.write.format("delta").mode("overwrite").option("replaceWhere", replaceCondition(cfg, b)).saveAsTable(table)
        } else insertOverwriteDynamic(spark, df, table)
      case _ =>
        // Bảng chưa có (kể cả overwrite-partitions lần đầu), hoặc overwrite / append.
        val mode = if (cfg.outputMode == IngestConfig.OverwritePartitions) "overwrite" else cfg.outputMode
        val writer = df.write.format(cfg.outputFormat.sparkFormat).mode(mode)
        (if (cfg.partitionBy.isEmpty) writer else writer.partitionBy(cfg.partitionBy: _*)).saveAsTable(table)
    }
  }

  /**
   * Điều kiện `replaceWhere`: các dòng của lô, và nếu chạy chỉ vài giờ (`BATCH_HOURS`) thì chỉ các giờ đó của lô. Giá trị lô đã qua kiểm
   * [A-Za-z0-9_.:-] và giờ là hai chữ số nên đặt thẳng vào điều kiện được. Dữ liệu ghi ra phải thoả điều kiện này, không thì Delta báo lỗi
   * (chặn việc ghi giờ ngoài khoảng đã chọn).
   */
  private def replaceCondition(cfg: IngestConfig, b: Batch): String = {
    val day = s"${quote(b.column)} = '${b.value}'"
    cfg.hours.filter(_.replace.nonEmpty).fold(day) { h =>
      s"$day AND ${quote(h.column)} IN (${h.replace.map(hour => s"'$hour'").mkString(", ")})"
    }
  }

  private def requireSamePartitioning(spark: SparkSession, cfg: IngestConfig, table: String): Unit = {
    val existing =
      if (cfg.outputFormat == OutputFormat.Delta) {
        spark.sql(s"DESCRIBE DETAIL $table").select("partitionColumns").head().getSeq[String](0)
      } else {
        spark.catalog.listColumns(cfg.outputDb, cfg.outputTable).collect().filter(_.isPartition).map(_.name).toSeq
      }
    if (existing.map(_.toLowerCase) != cfg.partitionBy.map(_.toLowerCase)) {
      throw new IllegalStateException(s"Target table ${cfg.target} already exists with partitioning [${existing.mkString(", ")}], " +
        s"not [${cfg.partitionBy.mkString(", ")}] needed for batch writes: use another OUTPUT_TABLE or recreate the table")
    }
  }

  private def insertOverwriteDynamic(spark: SparkSession, df: DataFrame, table: String): Unit =
    withSessionConf(spark, "spark.sql.sources.partitionOverwriteMode", "dynamic") {
      // insertInto khớp cột THEO VỊ TRÍ: xếp lại theo thứ tự cột của bảng đích.
      val columns = spark.table(table).columns.toSeq
      df.select(columns.map(c => col(quote(c))): _*).write.mode("overwrite").insertInto(table)
    }

  /** Đặt `key` = `value` trong session cho `body`, rồi trả lại giá trị cũ (hoặc bỏ đặt), kể cả khi `body` lỗi. */
  private def withSessionConf[T](spark: SparkSession, key: String, value: String)(body: => T): T = {
    val previous = spark.conf.getOption(key)
    spark.conf.set(key, value)
    try body
    finally previous match {
      case Some(v) => spark.conf.set(key, v)
      case None => spark.conf.unset(key)
    }
  }

  /** Số dòng, số file và số byte mà commit Delta MỚI NHẤT của bảng đích ghi ra (đọc từ log của Delta, không đọc dữ liệu). */
  def lastCommit(spark: SparkSession, cfg: IngestConfig): Option[CommitMetrics] =
    if (cfg.outputFormat != OutputFormat.Delta) None
    else scala.util.Try {
      val m = spark.sql(s"DESCRIBE HISTORY ${quote(cfg.outputDb)}.${quote(cfg.outputTable)} LIMIT 1").select("operationMetrics").head()
        .getMap[String, String](0)
      def at(k: String): Long = Option(m.get(k).orNull).flatMap(v => scala.util.Try(v.toLong).toOption).getOrElse(0L)
      CommitMetrics(at("numOutputRows"), at("numFiles"), at("numOutputBytes"))
    }.toOption

  private def quote(identifier: String): String = s"`${identifier.replace("`", "``")}`"
}
