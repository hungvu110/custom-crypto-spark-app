package vai.lakehouse.pseudonym.verify

import java.nio.charset.StandardCharsets.UTF_8

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import vai.lakehouse.pseudonym.verify.checks.{CheckResult, Status}

import scala.collection.JavaConverters._

/** `report.json` + bảng tóm tắt trong log. Report KHÔNG chứa giá trị định danh gốc hay khoá (chỉ số liệu + khoá dòng). */
object ReportWriter {

  def checkJson(r: CheckResult): Map[String, Any] =
    Map("id" -> r.id, "level" -> r.level.name, "status" -> r.status.name, "description" -> r.description,
      "metric" -> r.metric, "observed" -> number(r.observed), "threshold" -> r.threshold, "samples" -> r.samples) ++
      (if (r.detail.nonEmpty) Map("detail" -> r.detail) else Map.empty) ++
      r.error.map(e => Map("error" -> e)).getOrElse(Map.empty)

  def logChecks(label: String, results: Seq[CheckResult]): Unit = results.foreach { r =>
    val line = f"$label%-14s ${r.id}%-4s ${r.status.name}%-7s ${r.metric}%-38s observed=${fmt(r.observed)}%-10s threshold=${r.threshold}"
    if (r.status == Status.Fail || r.status == Status.Error) Log.fail(line + r.error.map(e => s"  ($e)").getOrElse(""))
    else Log.info(line)
  }

  def toJson(value: Map[String, Any]): String =
    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(toJava(value))

  def write(spark: SparkSession, path: String, content: String): Unit = {
    val p = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val out = fs.create(p, true)
    try out.write(content.getBytes(UTF_8)) finally out.close()
  }

  private def number(d: Double): Any =
    if (d.isNaN || d.isInfinite) null else if (d == math.rint(d) && math.abs(d) < 1e15) d.toLong else d

  private def fmt(d: Double): String = number(d) match {
    case null => "n/a"
    case l: Long => l.toString
    case other => f"${other.asInstanceOf[Double]}%.4f"
  }

  private def toJava(value: Any): AnyRef = value match {
    case null => null
    case m: Map[_, _] => m.map { case (k, v) => k.toString -> toJava(v) }.asJava
    case s: Seq[_] => s.map(toJava).asJava
    case d: Double => number(d).asInstanceOf[AnyRef]
    case o: Option[_] => o.map(toJava).orNull
    case other => other.asInstanceOf[AnyRef]
  }
}
