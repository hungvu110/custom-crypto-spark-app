package vai.lakehouse.keyprefix

import java.net.URI
import java.net.http.HttpRequest
import java.util.UUID

/**
 * Gửi GET tới DAK, dùng chung cho đường theo bảng (`DakPrefixSource`) và `by-column` (`DakColumnKeySource`): Bearer token
 * `client_credentials`, `X-Request-Id` sinh mới mỗi request, `X-Dak-Caller` nếu được cấu hình.
 *
 * 401: bỏ token đang cache, xin token mới, thử lại đúng 1 lần. Mọi mã khác trả nguyên cho bên gọi xử lý.
 */
private[keyprefix] final class DakClient(cfg: DakConfig, tokens: KeycloakTokenProvider, http: JsonHttp) {

  import DakClient._

  val base: String = cfg.addr.replaceAll("/+$", "")

  /** @param pathAndQuery đường dẫn tính từ `dak.addr`, đã mã hoá (vd `/api/v1/keys/demo_db/t`) */
  def get(pathAndQuery: String, what: String): Result = {
    val firstToken = tokens.token()
    val first = send(pathAndQuery, firstToken, what)
    if (first.response.status == 401) {
      tokens.invalidate(firstToken)
      send(pathAndQuery, tokens.token(), what)
    } else first
  }

  private def send(pathAndQuery: String, token: String, what: String): Result = {
    val sentRequestId = UUID.randomUUID().toString
    val builder = HttpRequest.newBuilder(URI.create(base + pathAndQuery))
      .timeout(JsonHttp.RequestTimeout)
      .header("Authorization", s"Bearer $token")
      .header("Accept", "application/json")
      .header("X-Request-Id", sentRequestId)
    cfg.caller.foreach(c => builder.header(CallerHeader, c))
    val response = http.execute(builder.GET().build(), what, base)
    Result(response, requestIdOf(response, sentRequestId))
  }
}

private[keyprefix] object DakClient {

  val CallerHeader = "X-Dak-Caller"

  /** Response và `requestId` để tra audit phía DAK. */
  final case class Result(response: JsonResponse, requestId: String) {
    def errorCode: Option[String] = JsonHttp.errorCode(response.body)

    /** `HTTP <mã>[ <error>], requestId=<id>` — không bao giờ chứa body. */
    def detail: String = s"HTTP ${response.status}${errorCode.map(c => s" $c").getOrElse("")}, requestId=$requestId"
  }

  private val RequestId = "^[A-Za-z0-9._-]{1,128}$".r

  // requestId của DAK (body, rồi header) nếu là định danh an toàn; không có thì dùng id mình đã gửi.
  private def requestIdOf(response: JsonResponse, sentRequestId: String): String = {
    val fromBody = Option(response.body.path("requestId").asText(null))
    (fromBody ++ response.header("X-Request-Id"))
      .find(id => RequestId.pattern.matcher(id).matches())
      .getOrElse(sentRequestId)
  }
}
