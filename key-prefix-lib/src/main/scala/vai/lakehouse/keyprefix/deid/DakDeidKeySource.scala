package vai.lakehouse.keyprefix

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.util.Try

import org.slf4j.LoggerFactory

/** Key khử định danh DAK giao cho job: `keyPrefix` là base64 chuẩn của đúng 32 byte, `keyVersion` là version ở Vault. */
final case class DeidKey(requestCode: String, keyPrefix: String, keyVersion: Int) {
  /** Không in `keyPrefix`. */
  override def toString: String = s"DeidKey(requestCode=$requestCode, keyPrefix=***, keyVersion=$keyVersion)"
}

/**
 * Lấy key khử định danh của một phiếu khai thác qua `GET {addr}/api/v1/keys/deid?database=&table=[&requestCode=]`
 * (docs/key-access/YEU-CAU-TU-DAK-BO-K8S-SECRET.md mục 2), thay cho K8s Secret `deid-key-*` mount vào `/etc/pseudonym-keys`.
 *
 *  - Token là token `client_credentials` của client Keycloak mà DAK đã xác nhận cho luồng khử định danh (client NỘI BỘ, cùng loại với
 *    luồng silver); request không mang `team`. Endpoint này không có tham số `tag` (DAK xác nhận) nên không bao giờ gửi `tag`, kể cả khi
 *    `DAK_TAG` được đặt trong môi trường.
 *  - Gửi `requestCode` ngay từ lần gọi đầu (job luôn có) nên không gặp `409`. `requestCode` trong response phải khớp,
 *    lệch là dừng job (không mã hoá bằng key của phiếu khác).
 *  - `401`: bỏ token đang cache, xin token mới, thử lại đúng 1 lần ([[DakClient]]). Mọi mã khác `200` báo lỗi ngay, không thử lại,
 *    KHÔNG cache key giữa các lượt chạy: mỗi lần gọi `fetch` là một lần GET (rào hạn phiếu phía DAK kiểm lúc GET).
 *  - `keyPrefix` sai định dạng (không phải base64 của đúng 32 byte) là lỗi.
 *
 * Thông báo lỗi không bao giờ chứa token, secret, keyPrefix hay body của DAK — chỉ mã lỗi và `requestId`.
 */
class DakDeidKeySource(cfg: DakConfig, tokens: KeycloakTokenProvider, http: JsonHttp = new JsonHttp()) {

  import DakDeidKeySource._

  private val logger = LoggerFactory.getLogger(classOf[DakDeidKeySource])

  private val client = new DakClient(cfg, tokens, http)

  def fetch(ref: DakTableRef, requestCode: Option[String]): DeidKey = {
    requestCode.foreach(DeidRequestCode.require)
    val params = Seq("database" -> ref.database, "table" -> ref.table) ++ requestCode.map("requestCode" -> _)
    val query = params.map { case (k, v) => s"$k=${URLEncoder.encode(v, StandardCharsets.UTF_8)}" }.mkString("&")
    logger.info(s"[DAK deid key] Sending request: GET ${cfg.addr.stripSuffix("/")}$Path?$query " +
      s"(auth=Bearer <token of client '${cfg.clientId}'>, caller=${cfg.caller.getOrElse("<none>")}, tag not sent)")
    val result = client.get(s"$Path?$query", s"DAK deid key request for '${ref.qualified}'")
    logger.info(s"[DAK deid key] Response: ${result.detail}; body fields (name (JSON type) = value): ${describe(result.response.body)}")
    if (result.response.status != 200) throw new IllegalStateException(failureMessage(ref, result))
    val key = parse(ref, requestCode, result)
    logger.info(s"DAK returned the deid key for '${ref.qualified}' (requestCode=${key.requestCode}, " +
      s"keyVersion=${key.keyVersion}, requestId=${result.requestId})")
    key
  }

  private def parse(ref: DakTableRef, requested: Option[String], result: DakClient.Result): DeidKey = {
    val body = result.response.body
    def invalid(why: String) = new IllegalStateException(
      s"DAK returned an invalid deid key response for '${ref.qualified}' ($why, requestId=${result.requestId})")

    val code = body.path("requestCode").asText("")
    if (code.isEmpty) throw invalid("no 'requestCode'")
    if (requested.exists(_ != code)) {
      throw invalid("'requestCode' is not the one that this job was given: refusing to encrypt with the key of another request")
    }
    if (body.path("database").asText(ref.database) != ref.database || body.path("table").asText(ref.table) != ref.table) {
      throw invalid("database/table do not match the request")
    }
    val prefix = body.path("keyPrefix")
    if (!prefix.isTextual || !isKey(prefix.asText())) {
      throw invalid("'keyPrefix' is not the base64 of exactly 32 bytes")
    }
    val version = body.path("keyVersion")
    val parsedVersion = positiveInt(version).getOrElse(
      throw invalid(s"'keyVersion' is not a positive integer, got JSON type ${version.getNodeType}"))
    DeidKey(code, prefix.asText(), parsedVersion)
  }

  /**
   * One line that shows the SHAPE of a DAK response so the DAK team can see what this job received: every top-level field with its
   * JSON type and, for harmless fields, its value (cut at 80 chars). `keyPrefix` and any field whose name looks like a secret are
   * never printed, only their type and length (and, for keyPrefix, whether it is the base64 of 32 bytes).
   */
  private[keyprefix] def describe(body: com.fasterxml.jackson.databind.JsonNode): String = {
    if (body == null || body.isMissingNode || body.isNull) return "<empty or not JSON>"
    if (!body.isObject) return s"<${body.getNodeType} (not an object)>"
    val fields = Seq.newBuilder[String]
    val iterator = body.fields()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (name, node) = (entry.getKey, entry.getValue)
      val shown =
        if (name == "keyPrefix") {
          val text = if (node.isTextual) node.asText() else ""
          s"<hidden, length=${text.length}, base64 of 32 bytes=${node.isTextual && isKey(text)}>"
        } else if (SensitiveName.findFirstIn(name).isDefined) s"<hidden, length=${node.asText("").length}>"
        else if (node.isValueNode) {
          val text = if (node.asText().length > 80) node.asText().take(80) + "..." else node.asText()
          if (node.isTextual) "\"" + text + "\"" else text
        } else s"<${node.size} item(s)>"
      fields += s"$name (${node.getNodeType}) = $shown"
    }
    fields.result().mkString("{", ", ", "}")
  }

  /** Số nguyên dương, chấp nhận cả số JSON (kể cả `3.0`) lẫn chuỗi toàn chữ số (`"3"`) vì DAK chưa cam kết kiểu của `keyVersion`. */
  private def positiveInt(node: com.fasterxml.jackson.databind.JsonNode): Option[Int] = {
    val value =
      if (node.isIntegralNumber || (node.isFloatingPointNumber && node.decimalValue().stripTrailingZeros().scale() <= 0)) {
        Some(node.decimalValue())
      } else if (node.isTextual && node.asText().matches("[0-9]{1,9}")) {
        Some(BigDecimal(node.asText()).bigDecimal)
      } else None
    value.filter(v => v.signum() > 0 && v.compareTo(java.math.BigDecimal.valueOf(Int.MaxValue)) <= 0).map(_.intValueExact())
  }

  private def failureMessage(ref: DakTableRef, result: DakClient.Result): String =
    result.response.status match {
      case 401 =>
        s"DAK rejected the team access token for '${ref.qualified}' (${result.detail}) even after requesting a new token. " +
          "Check DAK_CLIENT_ID/DAK_CLIENT_SECRET (the internal Keycloak client). Do not retry."
      case 403 =>
        s"DAK denied the deid key for '${ref.qualified}' (${result.detail}). Possible causes: no APPROVED request for this " +
          "table, the request expired or its grant was revoked, or the key is not created / is locked. Stop the job: extend " +
          "the request in DAK and run it again. Ask the DAK admin to look up the requestId."
      case 409 =>
        s"DAK found several APPROVED requests for '${ref.qualified}' (${result.detail}): send requestCode (REQUEST_CODE)."
      case 400 =>
        s"DAK rejected the database/table/requestCode of the deid key request for '${ref.qualified}' (${result.detail})."
      case _ =>
        s"DAK deid key request for '${ref.qualified}' failed (${result.detail})"
    }
}

object DakDeidKeySource {

  val Path = "/api/v1/keys/deid"

  /** Field names whose value is never logged (only type and length). */
  private val SensitiveName = "(?i)secret|password|token|key(?!Version)".r

  /** `keyPrefix` = base64 chuẩn của đúng 32 byte (44 ký tự, kết thúc `=`). */
  private[keyprefix] def isKey(value: String): Boolean =
    value.length == 44 && Try(Base64.getDecoder.decode(value)).toOption.exists(_.length == 32)
}

/** Mã phiếu khai thác gửi lên DAK: `REQ-<năm>-<6 số>` (hoặc `PYC-` với phiếu kiểu cũ). */
object DeidRequestCode {

  private val Pattern = "^(REQ|PYC)-[0-9]{4}-[0-9]{6}$".r

  def isValid(code: String): Boolean = Pattern.pattern.matcher(code).matches()

  def require(code: String): Unit =
    if (!isValid(code)) throw new IllegalArgumentException(
      s"requestCode must look like REQ-<year>-<6 digits> (or PYC-...), got '${code.take(60)}'")
}
