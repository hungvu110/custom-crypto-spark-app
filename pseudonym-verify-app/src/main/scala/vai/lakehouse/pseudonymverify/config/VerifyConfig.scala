package vai.lakehouse.pseudonymverify.config

import vai.lakehouse.pseudonym.PseudonymMode

import java.net.URI
import java.util.Locale


import scala.collection.mutable.ArrayBuffer
import scala.util.Try

sealed abstract class RunMode(val name: String)

object RunMode {
  /** Nghiệm thu lib trên dữ liệu tổng hợp: chạy toàn bộ check U/P cho mọi chế độ trong PSEUDO_MODES. */
  case object Full extends RunMode("full")
  /** Kiểm chứng chính bộ check bằng các bản giả danh cố ý sai (mutation). */
  case object Selftest extends RunMode("selftest")
  /** Cổng chặn trên dữ liệu THẬT trước khi ghi: chỉ các check không cần đáp án. */
  case object Guard extends RunMode("guard")

  val All: Seq[RunMode] = Seq(Full, Selftest, Guard)

  def parse(raw: String): Option[RunMode] = All.find(_.name == raw.trim.toLowerCase(Locale.ROOT))
}

/**
 * Cấu hình của job, đọc từ biến môi trường của driver (`spec.driver.env` trong SparkApplication) — xem
 * docs/pseudonym-verify-app/PLAN.md mục 9.
 */
final case class VerifyConfig(
    appName: String,
    runMode: RunMode,
    pseudoModes: Seq[PseudonymMode],
    runId: String,
    outputPath: String,
    seed: String,
    nSubjects: Long,
    appEventsPerSubject: Int,
    btsConnPerSubject: Int,
    loyaltyTxnPerSubject: Int,
    partitions: Int,
    nullRate: Double,
    offerSample: Int,
    keySource: String,
    mappingTablePath: String,
    policyPath: String,
    writeOutput: Boolean,
    inputTable: Option[String],
    guardTable: Option[String],
    identifierSampleFraction: Double,
    previewRows: Int) {

  /** Thư mục của lần chạy: output/, _restricted/, report.json đều nằm dưới đây. */
  def runPath: String = s"${outputPath.stripSuffix("/")}/$runId"

  def outputDir: String = s"$runPath/output"

  def restrictedDir: String = s"$runPath/_restricted"

  def isEphemeralKeys: Boolean = keySource == VerifyConfig.EphemeralKeys

  def describe: Seq[(String, String)] = Seq(
    "RUN_MODE"                -> runMode.name,
    "PSEUDO_MODES"            -> pseudoModes.map(_.name).mkString(","),
    "RUN_ID"                  -> runId,
    "run path"                -> runPath,
    "SEED"                    -> seed,
    "N_SUBJECTS"              -> nSubjects.toString,
    "APP_EVENTS_PER_SUBJECT"  -> appEventsPerSubject.toString,
    "BTS_CONN_PER_SUBJECT"    -> btsConnPerSubject.toString,
    "LOYALTY_TXN_PER_SUBJECT" -> loyaltyTxnPerSubject.toString,
    "PARTITIONS"              -> partitions.toString,
    "NULL_RATE"               -> nullRate.toString,
    "OFFER_SAMPLE"            -> offerSample.toString,
    "KEY_SOURCE"              -> keySource,
    "MAPPING_TABLE_PATH"      -> (if (pseudoModes.contains(PseudonymMode.Mapping)) mappingTablePath else "(MAPPING tắt)"),
    "POLICY_PATH"             -> policyPath,
    "WRITE_OUTPUT"            -> writeOutput.toString,
    "PREVIEW_ROWS"            -> (if (previewRows > 0) previewRows.toString else "0 (tắt)"),
    "INPUT_TABLE"             -> inputTable.getOrElse("(chỉ cho guard)"),
    "GUARD_TABLE"             -> guardTable.getOrElse("(chỉ cho guard)"))
}

object VerifyConfig {

  val EphemeralKeys     = "ephemeral"
  val KeySources        = Seq(EphemeralKeys, "file", "vault", "dak")
  val DefaultModes: Seq[PseudonymMode] = Seq(PseudonymMode.Hash, PseudonymMode.Encrypt)
  val DefaultPolicyPath = "demo-policy.yaml"
  /** Giới hạn của hoán vị sinh `isdn` (8 chữ số sau đầu "849"). */
  val MaxSubjects       = 100000000L

  private val IdPattern = "^[A-Za-z0-9_-]{1,64}$"

  /** Gom MỌI lỗi cấu hình rồi trả về một lượt, để sửa manifest một lần là đủ. */
  def fromEnv(env: Map[String, String]): Either[Seq[String], VerifyConfig] = {
    val errors = ArrayBuffer.empty[String]

    def raw(key: String): Option[String] = env.get(key).map(_.trim).filter(_.nonEmpty)

    def parsed[T](key: String, default: T, rule: String)(parse: String => Option[T]): T =
      raw(key) match {
        case None => default
        case Some(value) => parse(value).getOrElse { errors += s"$key='$value': $rule"; default }
      }

    def long(key: String, default: Long, rule: String)(ok: Long => Boolean): Long =
      parsed(key, default, rule)(v => Try(v.toLong).toOption.filter(ok))

    def int(key: String, default: Int, rule: String)(ok: Int => Boolean): Int =
      parsed(key, default, rule)(v => Try(v.toInt).toOption.filter(ok))

    def rate(key: String, default: Double): Double =
      parsed(key, default, "số trong [0, 1]")(v => Try(v.toDouble).toOption.filter(d => d >= 0 && d <= 1))

    def id(key: String, default: Option[String]): String = raw(key).orElse(default) match {
      case Some(v) if v.matches(IdPattern) => v
      case Some(v) => errors += s"$key='$v': chỉ gồm [A-Za-z0-9_-], tối đa 64 ký tự"; ""
      case None    => errors += s"$key: bắt buộc"; ""
    }

    val runMode = parsed[RunMode]("RUN_MODE", RunMode.Full, s"phải là ${RunMode.All.map(_.name).mkString(" / ")}")(RunMode.parse)

    val pseudoModes = parsed("PSEUDO_MODES", DefaultModes, s"danh sách ${PseudonymMode.All.map(_.name).mkString(",")}") { v =>
      Try(v.split(",").map(_.trim).filter(_.nonEmpty).map(PseudonymMode.parse).toSeq.distinct).toOption.filter(_.nonEmpty)
    }

    val runId = id("RUN_ID", None)
    val seed  = id("SEED", Some("s1"))

    val outputPath = raw("OUTPUT_PATH") match {
      case Some(v) if isAbsoluteUri(v) => v
      case Some(v) => errors += s"OUTPUT_PATH='$v': phải là URI tuyệt đối có scheme, vd hdfs://<namenode>:8020/<path> hoặc file:///tmp/x"; ""
      case None    => errors += "OUTPUT_PATH: bắt buộc"; ""
    }
    val runPath = s"${outputPath.stripSuffix("/")}/$runId"

    val nSubjects = long("N_SUBJECTS", 1000000L, s"số nguyên 1..$MaxSubjects")(n => n >= 1 && n <= MaxSubjects)
    val appEvents = int("APP_EVENTS_PER_SUBJECT", 20, "số nguyên 1..1000")(n => n >= 1 && n <= 1000)
    val btsConn   = int("BTS_CONN_PER_SUBJECT", 10, "số nguyên 1..1000")(n => n >= 1 && n <= 1000)
    val loyalty   = int("LOYALTY_TXN_PER_SUBJECT", 3, "số nguyên 1..1000")(n => n >= 1 && n <= 1000)
    val parts     = int("PARTITIONS", 200, "số nguyên 1..100000")(n => n >= 1 && n <= 100000)
    val nullRate  = rate("NULL_RATE", 0.01)
    val offer     = int("OFFER_SAMPLE", 10000, "số nguyên >= 1")(_ >= 1)
    val keySource = parsed("KEY_SOURCE", EphemeralKeys, s"phải là ${KeySources.mkString(" / ")}")(v =>
      Some(v.toLowerCase(Locale.ROOT)).filter(KeySources.contains))

    val mappingPath = raw("MAPPING_TABLE_PATH").getOrElse(s"$runPath/_restricted/pseudonym_map")
    if (isUnder(mappingPath, s"$runPath/output")) {
      errors += s"MAPPING_TABLE_PATH='$mappingPath': bảng ánh xạ phải lưu RIÊNG, không nằm dưới thư mục output/"
    }

    val policyPath  = raw("POLICY_PATH").getOrElse(DefaultPolicyPath)
    val writeOutput = parsed("WRITE_OUTPUT", true, "phải là true hoặc false")(v =>
      v.toLowerCase(Locale.ROOT) match { case "true" => Some(true); case "false" => Some(false); case _ => None })
    val sampleFraction = parsed("IDENTIFIER_SAMPLE_FRACTION", 0.01, "số trong (0, 1]")(v =>
      Try(v.toDouble).toOption.filter(d => d > 0 && d <= 1))

    val previewRows = int("PREVIEW_ROWS", 10, "số nguyên 0..1000 (0 = tắt)")(n => n >= 0 && n <= 1000)

    val inputTable = raw("INPUT_TABLE")
    val guardTable = raw("GUARD_TABLE")
    if (runMode == RunMode.Guard) {
      if (inputTable.isEmpty) errors += "INPUT_TABLE: bắt buộc khi RUN_MODE=guard (đường dẫn parquet dữ liệu thật)"
      if (guardTable.isEmpty) errors += "GUARD_TABLE: bắt buộc khi RUN_MODE=guard (tên bảng trong policy)"
      if (keySource == EphemeralKeys) errors += "KEY_SOURCE: RUN_MODE=guard phải dùng khoá thật (file / vault / dak)"
    }

    if (errors.nonEmpty) Left(errors.toList)
    else Right(VerifyConfig(raw("APP_NAME").getOrElse("PseudonymVerify"), runMode, pseudoModes, runId, outputPath, seed,
      nSubjects, appEvents, btsConn, loyalty, parts, nullRate, offer, keySource, mappingPath, policyPath, writeOutput,
      inputTable, guardTable, sampleFraction, previewRows))
  }

  private def isAbsoluteUri(value: String): Boolean =
    Try(new URI(value)).toOption.exists(u => u.getScheme != null && Option(u.getPath).exists(_.startsWith("/")))

  private[pseudonymverify] def isUnder(path: String, dir: String): Boolean = {
    val p = path.stripSuffix("/")
    val d = dir.stripSuffix("/")
    p == d || p.startsWith(d + "/")
  }
}
