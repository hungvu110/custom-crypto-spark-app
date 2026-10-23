package vai.lakehouse.keyprefix

/**
 * Một cột không dùng được key, theo `state` của DAK (`PENDING`, `MISSING`, `REJECTED`, `DISABLED`). Không chứa key.
 *
 * @param requestCode mã YC (`PENDING`, `REJECTED`)
 * @param reason      mã cố định của `MISSING` (`not_requested`, `yc_limit_reached`)
 */
final case class ColumnKeyIssue(column: String, state: String, requestCode: Option[String], reason: Option[String])

/** Có ít nhất một cột không có key dùng được. Thông báo liệt kê từng cột, không chứa key của các cột khác. */
final class ColumnKeysUnavailableException(val dataset: String, val issues: Seq[ColumnKeyIssue], requestIds: Seq[String])
  extends IllegalStateException(ColumnKeysUnavailableException.message(dataset, issues, requestIds))

object ColumnKeysUnavailableException {

  private def message(dataset: String, issues: Seq[ColumnKeyIssue], requestIds: Seq[String]): String =
    s"DAK has no usable key for ${issues.size} column(s) of '$dataset' (requestId=${requestIds.mkString(",")}): " +
      issues.map(describe).mkString("; ")

  private def describe(issue: ColumnKeyIssue): String = {
    val code = issue.requestCode.map(c => s"requestCode=$c").toSeq ++ issue.reason.map(r => s"reason=$r")
    val head = s"'${issue.column}' ${issue.state}${if (code.isEmpty) "" else code.mkString(" (", ", ", ")")}"
    val hint = (issue.state, issue.reason) match {
      case ("PENDING", _) => "DAK opened a key request; a DAK administrator must create the key, then retry"
      case ("MISSING", Some("yc_limit_reached")) =>
        "too many open key requests for this table; retry after a DAK administrator handles them"
      case ("MISSING", _) =>
        "no key for this column; when decrypting, check the table/column name or that the column was ever encrypted"
      case ("REJECTED", _) => "the key request was rejected by DAK; do not retry"
      case ("DISABLED", _) => "the key of this column is disabled in DAK"
      case _ => "not usable"
    }
    s"$head: $hint"
  }
}
