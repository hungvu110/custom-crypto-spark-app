package vai.lakehouse.keyprefix

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.JsonNode
import org.slf4j.LoggerFactory

/**
 * Lấy key của một đường dẫn qua `GET {addr}/api/v1/keys/by-path` (docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md mục 2).
 *
 *  - Request: `path` tương đối theo gốc của nguồn, ở dạng chuẩn ([[PathKeySource.canonical]]), kiểm trước khi gọi: không rỗng, không
 *    `..`, không ký tự điều khiển, không có scheme (`hdfs://...`), tối đa 1024 ký tự; `tag` nếu cấu hình (client nội bộ: `internal`); header `X-Dak-Caller` nếu cấu hình.
 *  - `200` → [[PathKeyResult.Available]]; `path` trong thân phải đúng đường dẫn đã hỏi, `keyPrefix` là chuỗi khác rỗng.
 *  - `204` (rỗng: DAK không có key cho đường dẫn, không mở yêu cầu — đã thống nhất với DAK, docs/key-access/TRA-LOI-DAK-BY-PATH.md
 *    mục 5) → [[PathKeyResult.NotAvailable]]: file không mã hoá, không phải lỗi.
 *  - `403 key_disabled`: key của đường dẫn tồn tại nhưng đang bị khoá — LỖI, không phải "không có key" (file vẫn mã hoá).
 *  - `202` (DAK mở yêu cầu key — cách cũ, đã bỏ): LỖI. Nghĩa là DAK coi đường dẫn này cần key mà chưa có, tức file có thể đã mã hoá; đọc
 *    nguyên trạng sẽ ghi ciphertext vào bảng, nên dừng thay vì đoán.
 *  - `401`: xin token mới, thử lại một lần ([[DakClient]]). Mã khác (`400`, `403`, `429`, ...): `IllegalStateException` kèm requestId.
 *
 * Thông báo lỗi không bao giờ chứa token, secret, keyPrefix hay body của DAK.
 */
class DakPathKeySource(cfg: DakConfig, tokens: KeycloakTokenProvider, http: JsonHttp = new JsonHttp())
  extends PathKeySource {

  import DakPathKeySource._

  private val logger = LoggerFactory.getLogger(classOf[DakPathKeySource])

  private val client = new DakClient(cfg, tokens, http)

  override def readPath(path: String): PathKeyResult = {
    validate(path)
    val canonical = PathKeySource.canonical(path)
    val params = Seq("path" -> canonical) ++ cfg.tag.map("tag" -> _)
    val query = params.map { case (k, v) => s"$k=${encode(v)}" }.mkString("&")
    val result = client.get(s"$Path?$query", s"DAK path key request for '$canonical'")
    result.response.status match {
      case 200 => PathKeyResult.Available(key(canonical, result))
      case 204 =>
        logger.info(s"DAK has no key for '$canonical': the files are not encrypted (requestId=${result.requestId})")
        PathKeyResult.NotAvailable
      case 202 =>
        val code = safe(result.response.body.path("requestCode")).getOrElse("-")
        throw new IllegalStateException(s"DAK answered 202 for '$canonical' (requestCode=$code, ${result.detail}): it opened a key " +
          "request, so the files may be encrypted without a key yet. The agreed contract is 204 when a path has no key; refusing to " +
          "read possibly encrypted files as plaintext.")
      case _ => throw new IllegalStateException(failureMessage(canonical, result))
    }
  }

  private def key(path: String, result: DakClient.Result): PathKey = {
    val body = result.response.body
    def invalid(why: String) =
      new IllegalStateException(s"DAK returned an invalid by-path response for '$path' ($why, requestId=${result.requestId})")
    if (body.path("path").asText("") != path) throw invalid("path does not match the request")
    val field = body.path("keyPrefix")
    if (!field.isTextual || field.asText().isEmpty) throw invalid("no non-empty string field 'keyPrefix'")
    val version = body.path("keyVersion")
    val key = PathKey(path, field.asText(), if (version.canConvertToLong) Some(version.asLong()) else None)
    logger.debug(s"DAK returned key for '$path' (keyVersion=${key.keyVersion.getOrElse("-")}, requestId=${result.requestId})")
    key
  }

  private def failureMessage(path: String, result: DakClient.Result): String =
    (result.response.status, result.errorCode) match {
      case (400, Some("invalid_tag")) =>
        s"DAK rejected the tag of the path key request for '$path' (${result.detail}). The internal DAK client must send " +
          "tag=internal: check the dak.tag setting."
      case (400, _) =>
        s"DAK rejected the path '$path' (${result.detail}): empty, '..', control characters or too long."
      case (403, Some("key_disabled")) =>
        s"The key of the path '$path' is disabled in DAK (${result.detail}). The files are encrypted: do not read them as they are. " +
          "Ask a DAK administrator to enable the key."
      case (403, _) =>
        s"DAK denied the path key of '$path' (${result.detail}): this client type may not call by-path (internal client only)."
      case (401, _) =>
        s"DAK rejected the access token for '$path' (${result.detail}) even after requesting a new token. " +
          "Check that the client is registered and enabled in DAK."
      case (429, _) =>
        s"DAK rate limit reached for the path key of '$path' (${result.detail}): retry later."
      case _ =>
        s"DAK path key request for '$path' failed (${result.detail})"
    }
}

object DakPathKeySource {

  val Path = "/api/v1/keys/by-path"

  /** Giới hạn độ dài phía SA (DAK có giới hạn riêng, mục 2 của tài liệu). */
  val MaxPathLength = 1024

  private val SafeCode = "^[A-Za-z0-9._-]{1,64}$".r

  /** Kiểm đường dẫn trước khi gọi DAK, đúng các điều DAK trả `400`; lỗi không in lại đường dẫn có ký tự điều khiển. */
  def validate(path: String): Unit = {
    val p = Option(path).getOrElse("")
    require(PathKeySource.canonical(p).nonEmpty, "Key path must not be empty")
    require(!p.contains("://"), "Key path must be relative to the agreed root, without a scheme (hdfs://...)")
    require(p.length <= MaxPathLength, s"Key path must be at most $MaxPathLength characters")
    require(!p.exists(Character.isISOControl), "Key path must not contain control characters")
    require(!p.contains(".."), s"Key path must not contain '..': '$p'")
  }

  private def encode(s: String): String = URLEncoder.encode(s, StandardCharsets.UTF_8)

  private def safe(node: JsonNode): Option[String] =
    Option(node.asText(null)).filter(SafeCode.pattern.matcher(_).matches())
}
