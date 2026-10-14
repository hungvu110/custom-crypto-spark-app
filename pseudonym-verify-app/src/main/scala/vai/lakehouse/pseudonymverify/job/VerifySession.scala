package vai.lakehouse.pseudonymverify.job

import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.policy.{PseudonymPolicy, TablePolicy}
import vai.lakehouse.pseudonymverify.check.AttackDictionary
import vai.lakehouse.pseudonymverify.config.VerifyConfig
import vai.lakehouse.pseudonymverify.data.{SyntheticData, SyntheticTable}
import vai.lakehouse.pseudonymverify.key.KeyMaterial
import vai.lakehouse.pseudonymverify.logging.Log
import vai.lakehouse.pseudonymverify.report.ReportWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel


/** Những thứ dùng chung giữa các chế độ/mutation của 1 lần chạy app: dữ liệu tổng hợp, từ điển tấn công, pipeline. */
final class VerifySession(val spark: SparkSession, val cfg: VerifyConfig, val keys: KeyMaterial) {

  lazy val policies: Map[String, TablePolicy] = PseudonymPolicy.load(cfg.policyPath)

  lazy val tables: Seq[SyntheticTable] = SyntheticData.generate(spark, cfg)

  lazy val dictionary: DataFrame =
    SyntheticData.identifierDictionary(spark, cfg.nSubjects, cfg.partitions).persist(StorageLevel.MEMORY_AND_DISK)

  lazy val attackCandidates: DataFrame = AttackDictionary.candidates(dictionary).persist(StorageLevel.MEMORY_AND_DISK)

  lazy val pipeline = new Pipeline(spark, cfg, keys, tables, policies, dictionary, attackCandidates)

  def mappingPathFor(mode: PseudonymMode, path: String): Option[String] =
    if (mode == PseudonymMode.Mapping) Some(path) else None

  def rowCounts: Map[String, Any] = tables.map(t => t.name -> (t.raw.count(): Any)).toMap

  /** Ghi `${runPath}/<name>` (JSON) và in đường dẫn ra log. */
  def writeReport(name: String, report: Map[String, Any]): Unit = {
    val path = s"${cfg.runPath}/$name"
    ReportWriter.write(spark, path, ReportWriter.toJson(report))
    Log.kv("report", path)
  }
}
