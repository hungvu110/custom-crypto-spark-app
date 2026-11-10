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
    val result = client.get(s"$Path?$query", s"DAK deid key request for '${ref.qualified}'")
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
    if (!version.isInt || version.asInt() < 1) throw invalid("'keyVersion' is not a positive integer")
    DeidKey(code, prefix.asText(), version.asInt())
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
