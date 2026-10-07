package vai.lakehouse.keyprefix

import java.net.URI
import java.net.http.HttpRequest
import java.util.{Locale, UUID}

import org.slf4j.LoggerFactory

/** Cấu hình nguồn `dak`. `toString` che secret để không lộ ra log khi in nguyên case class. */
final case class DakConfig(addr: String, tokenUrl: String, clientId: String, clientSecret: String) {
  override def toString: String =
    s"DakConfig(addr=$addr, tokenUrl=$tokenUrl, clientId=$clientId, clientSecret=***)"
}

/** Định danh bảng gửi lên DAK: `database.table`, luôn chữ thường. */
final case class DakTableRef(database: String, table: String) {
  def qualified: String = s"$database.$table"
}

object DakTableRef {

  // Tên Hive không chứa dấu chấm nên tách được đúng 2 phần; bộ ký tự an toàn để đặt thẳng vào path URL.
  private val Pattern = "^([A-Za-z0-9_]{1,128})\\.([A-Za-z0-9_]{1,128})$".r

  /**
   * Hive không phân biệt hoa thường, nên chuẩn hoá chữ thường: `Demo_DB.X` và `demo_db.x` phải ra cùng một key,
   * nếu không dữ liệu ghi bằng cách viết này sẽ không giải mã được bằng cách viết kia.
   */
  def parse(name: String): DakTableRef = Option(name).getOrElse("") match {
    case Pattern(database, table) => DakTableRef(database.toLowerCase(Locale.ROOT), table.toLowerCase(Locale.ROOT))
    case other => throw new IllegalArgumentException(
      s"With source=dak the table must be '<database>.<table>' (letters, digits and '_', up to 128 characters " +
        s"each), got '${other.take(300)}'")
  }
}

/**
 * Lấy keyPrefix qua DAK: xin access token `client_credentials` từ Keycloak (realm của tenant), rồi gọi
 * `GET {addr}/api/v1/keys/{database}/{table}`. DAK tự suy ra tenant/workspace/team từ token, nên request không
 * mang thông tin đó. Hợp đồng API: docs/DAK_API_SPEC.md.
 *
 *  - 401: bỏ token đang cache, xin token mới, thử lại đúng 1 lần.
 *  - 403 và mọi lỗi khác: báo lỗi ngay, không thử lại. 403 `access_denied` cố ý gộp nhiều nguyên nhân (không có
 *    quyền, quyền hết hạn, bảng không tồn tại) nên thông báo gợi ý các nguyên nhân và kèm `requestId` để tra audit.
 *
 * Thông báo lỗi không bao giờ chứa token, secret, prefix hay body của DAK — chỉ mã lỗi và `requestId`.
 */
class DakPrefixSource(cfg: DakConfig, tokens: KeycloakTokenProvider, http: JsonHttp = new JsonHttp())
  extends PrefixSource {

  private val logger = LoggerFactory.getLogger(classOf[DakPrefixSource])

  private val base = cfg.addr.replaceAll("/+$", "")

  private final case class Attempt(response: JsonResponse, sentRequestId: String)

  override def read(datasetName: String): String = {
    val ref = DakTableRef.parse(datasetName)
    val firstToken = tokens.token()
    val first = fetch(ref, firstToken)
    val attempt =
      if (first.response.status == 401) {
        tokens.invalidate(firstToken)
        fetch(ref, tokens.token())
      } else first
    prefixFrom(ref, attempt)
  }

  private def fetch(ref: DakTableRef, token: String): Attempt = {
    val requestId = UUID.randomUUID().toString
    val request = HttpRequest.newBuilder(URI.create(s"$base/api/v1/keys/${ref.database}/${ref.table}"))
      .timeout(JsonHttp.RequestTimeout)
      .header("Authorization", s"Bearer $token")
      .header("Accept", "application/json")
      .header("X-Request-Id", requestId)
      .GET()
      .build()
    Attempt(http.execute(request, s"DAK key request for '${ref.qualified}'", base), requestId)
  }

  private def prefixFrom(ref: DakTableRef, attempt: Attempt): String = {
    val response = attempt.response
    val requestId = requestIdOf(attempt)
    if (response.isSuccess) {
      val field = response.body.path("keyPrefix")
      if (!field.isTextual || field.asText().isEmpty) {
        throw new IllegalStateException(
          s"DAK response for '${ref.qualified}' has no non-empty string field 'keyPrefix' (requestId=$requestId)")
      }
      logger.debug(s"DAK returned key for '${ref.qualified}' " +
        s"(keyVersion=${response.body.path("keyVersion").asText("?")}, requestId=$requestId)")
      field.asText()
    } else {
      throw new IllegalStateException(failureMessage(ref, response, requestId))
    }
  }

  private def failureMessage(ref: DakTableRef, response: JsonResponse, requestId: String): String = {
    val code = JsonHttp.errorCode(response.body)
    val detail = s"HTTP ${response.status}${code.map(c => s" $c").getOrElse("")}, requestId=$requestId"
    response.status match {
      case 403 if code.contains("access_denied") =>
        s"DAK denied key for '${ref.qualified}' ($detail). Possible causes: no grant, grant expired, " +
          "or table not registered in this workspace. Ask the DAK admin to look up the requestId."
      case 401 =>
        s"DAK rejected the access token for '${ref.qualified}' ($detail) even after requesting a new token. " +
          "Check that the client is registered and enabled in DAK."
      case _ =>
        s"DAK key request for '${ref.qualified}' failed ($detail)"
    }
  }

  // requestId của DAK (body, rồi header) nếu là định danh an toàn; không có thì dùng id mình đã gửi.
  private def requestIdOf(attempt: Attempt): String = {
    val fromBody = Option(attempt.response.body.path("requestId").asText(null))
    (fromBody ++ attempt.response.header("X-Request-Id"))
      .find(id => DakPrefixSource.RequestId.pattern.matcher(id).matches())
      .getOrElse(attempt.sentRequestId)
  }
}

private object DakPrefixSource {
  private val RequestId = "^[A-Za-z0-9._-]{1,128}$".r
}
