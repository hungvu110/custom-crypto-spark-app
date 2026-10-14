package vai.lakehouse.pseudonym

import org.yaml.snakeyaml.Yaml
import vai.lakehouse.keyprefix.PrefixFiles

import scala.collection.JavaConverters._

/** 1 cột cần giả danh. `output` để đổi tên cột (vd `customer_id` -> `pseudonym_id` như ví dụ Viettel++). */
final case class ColumnRule(column: String, mode: PseudonymMode, output: Option[String] = None) {
  def outputName: String = output.getOrElse(column)
}

/** Các cột cần giả danh của 1 bảng. Cột không có trong policy giữ nguyên. */
final case class TablePolicy(table: String, columns: Seq[ColumnRule]) {
  def withMode(mode: PseudonymMode): TablePolicy = copy(columns = columns.map(_.copy(mode = mode)))
  def ruleFor(column: String): Option[ColumnRule] = columns.find(_.column == column)
}

/**
 * Parse policy YAML (không chứa bí mật, commit được vào Git):
 * {{{
 * tables:
 *   myviettel.app_event:
 *     isdn: { mode: HASH }
 *   loyalty.loyalty_txn:
 *     customer_id: { mode: HASH, output: pseudonym_id }
 * }}}
 */
object PseudonymPolicy {

  private val ColumnName = "^[A-Za-z_][A-Za-z0-9_]*$"

  def load(path: String): Map[String, TablePolicy] = parse(PrefixFiles.readContent(path, "Pseudonym policy file"))

  def parse(yamlContent: String): Map[String, TablePolicy] = {
    val tables = asMap(asMap(new Yaml().load[Any](yamlContent), "top level").get("tables"), "'tables'")
    val policies = tables.asScala.toSeq.map { case (table, raw) =>
      val columns = asMap(raw, s"table '$table'").asScala.toSeq.map { case (column, spec) =>
        rule(table, column, asMap(spec, s"column '$table.$column'"))
      }
      table -> validated(TablePolicy(table, columns))
    }
    requireSameModePerColumnName(policies.map(_._2))
    policies.toMap
  }

  private def rule(table: String, column: String, spec: java.util.Map[String, Any]): ColumnRule = {
    val where = s"'$table.$column'"
    val mode = Option(spec.get("mode")).map(m => PseudonymMode.parse(m.toString))
      .getOrElse(throw new IllegalArgumentException(s"Column $where: missing 'mode'"))
    ColumnRule(column, mode, Option(spec.get("output")).map(_.toString.trim))
  }

  /** Cột trùng tên, output trùng nhau hoặc tên không hợp lệ -> lỗi ngay khi nạp policy. */
  def validated(policy: TablePolicy): TablePolicy = {
    val names = policy.columns.map(_.column) ++ policy.columns.flatMap(_.output)
    names.filterNot(_.matches(ColumnName)).foreach { bad =>
      throw new IllegalArgumentException(s"Table '${policy.table}': invalid column name '$bad'")
    }
    val outputs = policy.columns.map(_.outputName)
    val duplicated = outputs.diff(outputs.distinct)
    require(duplicated.isEmpty, s"Table '${policy.table}': duplicated output column(s) ${duplicated.distinct.mkString(", ")}")
    policy
  }

  /** Cùng 1 cột định danh trên nhiều bảng phải cùng chế độ, nếu không định danh giả không join được với nhau. */
  private def requireSameModePerColumnName(policies: Seq[TablePolicy]): Unit =
    policies.flatMap(_.columns).groupBy(_.column).foreach { case (column, rules) =>
      val modes = rules.map(_.mode).distinct
      require(modes.size == 1,
        s"Column '$column' uses different modes across tables (${modes.map(_.name).mkString(", ")}): pseudonyms would not join")
    }

  private def asMap(raw: Any, where: String): java.util.Map[String, Any] = raw match {
    case m: java.util.Map[_, _] => m.asInstanceOf[java.util.Map[String, Any]]
    case null => throw new IllegalArgumentException(s"Pseudonym policy: $where is missing or empty")
    case _ => throw new IllegalArgumentException(s"Pseudonym policy: $where must be a YAML map")
  }
}
