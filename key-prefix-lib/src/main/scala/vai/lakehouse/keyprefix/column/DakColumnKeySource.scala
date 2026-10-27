package vai.lakehouse.keyprefix

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.slf4j.LoggerFactory

/**
 * Lấy key theo CỘT qua `GET {addr}/api/v1/keys/by-column` (docs/key-access/DAK_BY_COLUMN_CLIENT_PLAN.md mục 1).
 *
 *  - Request: `database`, `table`, `columns` (chữ thường, gộp trùng, chia lô), `onMissing` theo [[KeyIntent]] (luôn gửi
 *    tường minh), `tag` nếu cấu hình (client nội bộ: `internal`), header `X-Dak-Caller` nếu cấu hình.
 *  - Response luôn `200`, mỗi cột một `state`. Chỉ trả key khi MỌI cột `READY`; có cột khác thì ném
 *    [[ColumnKeysUnavailableException]] liệt kê từng cột. Response sai hợp đồng (thiếu/thừa/lặp cột, lệch bảng, trạng thái lạ,
 *    `complete` mâu thuẫn, `READY` không có `keyPrefix`) là `IllegalStateException` riêng.
 *  - Mã khác `200`: như đường theo bảng (401 xin token mới, thử lại 1 lần; còn lại báo lỗi ngay), kèm gợi ý nguyên nhân.
 *
 * Thông báo lỗi không bao giờ chứa token, secret, keyPrefix hay body của DAK.
 */
class DakColumnKeySource(cfg: DakConfig, tokens: KeycloakTokenProvider, http: JsonHttp = new JsonHttp())
  extends ColumnKeySource {

  import DakColumnKeySource._

  private val logger = LoggerFactory.getLogger(classOf[DakColumnKeySource])

  private val client = new DakClient(cfg, tokens, http)

  private final case class Batch(keys: Seq[(String, String)], issues: Seq[ColumnKeyIssue], requestId: String)

  override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
    val ref = DakTableRef.parse(datasetName)
    val names = columns.map(DakColumnRef.parse).distinct
    if (names.isEmpty) throw new IllegalArgumentException(s"No column given for DAK key request of '${ref.qualified}'")
    val batches = ColumnBatches.split(names).map(batch => fetch(ref, batch, intent))
    val issues = batches.flatMap(_.issues)
    if (issues.nonEmpty) throw new ColumnKeysUnavailableException(ref.qualified, issues, batches.map(_.requestId).distinct)
    batches.flatMap(_.keys).toMap
  }

  private def fetch(ref: DakTableRef, columns: Seq[String], intent: KeyIntent): Batch = {
    val params = Seq("database" -> ref.database, "table" -> ref.table, "columns" -> columns.mkString(","),
      "onMissing" -> intent.onMissing) ++ cfg.tag.map("tag" -> _)
    val query = params.map { case (k, v) => s"$k=${encode(v)}" }.mkString("&")
    val result = client.get(s"$Path?$query", s"DAK column key request for '${ref.qualified}'")
    if (result.response.status != 200) throw new IllegalStateException(failureMessage(ref, result))
    parse(ref, columns, result)
  }

  private def parse(ref: DakTableRef, requested: Seq[String], result: DakClient.Result): Batch = {
    val body = result.response.body
    def invalid(why: String) = new IllegalStateException(
      s"DAK returned an invalid by-column response for '${ref.qualified}' ($why, requestId=${result.requestId})")

    if (body.path("database").asText("") != ref.database || body.path("table").asText("") != ref.table) {
      throw invalid("database/table do not match the request")
    }
    val columns = body.path("columns")
    if (!columns.isArray) throw invalid("no 'columns' array")
    val entries = columns.elements().asScala.toSeq.map(e => e.path("column").asText("") -> e)
    if (entries.map(_._1).distinct.size != entries.size) throw invalid("a column appears more than once")
    if (entries.map(_._1).toSet != requested.toSet) throw invalid("columns do not match the requested columns")
    val byName = entries.toMap

    val outcomes = requested.map(c => outcome(ref, c, byName(c), result, invalid))
    val issues = outcomes.collect { case Left(issue) => issue }
    if (issues.nonEmpty && body.path("complete").asBoolean(false)) throw invalid("complete=true but a column is not READY")
    Batch(outcomes.collect { case Right(key) => key }, issues, result.requestId)
  }

  private def outcome(ref: DakTableRef, column: String, entry: JsonNode, result: DakClient.Result,
                      invalid: String => IllegalStateException): Either[ColumnKeyIssue, (String, String)] =
    entry.path("state").asText("") match {
      case Ready =>
        val field = entry.path("keyPrefix")
        if (!field.isTextual || field.asText().isEmpty) {
          throw new IllegalStateException(s"DAK by-column response for '${ref.qualified}' has no non-empty string field " +
            s"'keyPrefix' for READY column '$column' (requestId=${result.requestId})")
        }
        logger.debug(s"DAK returned key for '${ref.qualified}.$column' " +
          s"(keyVersion=${entry.path("keyVersion").asText("?")}, requestId=${result.requestId})")
        Right(column -> field.asText())
      case state if NotReady.contains(state) =>
        if (state == "REJECTED") {
          logger.debug(s"DAK rejected the key request of '${ref.qualified}.$column': ${printable(entry.path("reasonText"))}")
        }
        Left(ColumnKeyIssue(column, state, safeCode(entry.path("requestCode")), safeCode(entry.path("reason"))))
      case state =>
        throw invalid(s"unknown state '${Option(state).filter(StateName.pattern.matcher(_).matches()).getOrElse("?")}' " +
          s"for column '$column'")
    }

  private def failureMessage(ref: DakTableRef, result: DakClient.Result): String = {
    val q = ref.qualified
    (result.response.status, result.errorCode) match {
      case (400, Some("invalid_tag")) =>
        s"DAK rejected the tag of the column key request for '$q' (${result.detail}). The internal DAK client must send " +
          "tag=internal and a team client must not: check the dak.tag setting."
      case (403, Some("access_denied")) =>
        s"DAK denied column keys for '$q' (${result.detail}). Possible causes: a team client without a valid grant on " +
          "every requested column, or a team client sending tag=internal. Ask the DAK admin to look up the requestId."
      case (401, _) =>
        s"DAK rejected the access token for '$q' (${result.detail}) even after requesting a new token. " +
          "Check that the client is registered and enabled in DAK."
      case (429, _) =>
        s"DAK rate limit reached for the column keys of '$q' (${result.detail}): retry later."
      case _ =>
        s"DAK column key request for '$q' failed (${result.detail})"
    }
  }
}

private object DakColumnKeySource {

  val Path = "/api/v1/keys/by-column"

  private val Ready = "READY"
  private val NotReady = Set("PENDING", "MISSING", "REJECTED", "DISABLED")
  private val StateName = "^[A-Z_]{1,32}$".r
  private val SafeCode = "^[A-Za-z0-9._-]{1,64}$".r
  private val MaxLoggedText = 200

  private def encode(s: String): String = URLEncoder.encode(s, StandardCharsets.UTF_8)

  /** Mã YC / mã `reason` chỉ được đưa vào thông báo nếu là định danh đơn giản. */
  private def safeCode(node: JsonNode): Option[String] =
    Option(node.asText(null)).filter(SafeCode.pattern.matcher(_).matches())

  // reasonText do người nhập: chỉ ghi DEBUG, bỏ ký tự điều khiển và cắt ngắn để không chèn được dòng log giả.
  private def printable(node: JsonNode): String =
    Option(node.asText(null)).map(_.take(MaxLoggedText).map(ch => if (Character.isISOControl(ch)) '?' else ch))
      .getOrElse("-")
}
