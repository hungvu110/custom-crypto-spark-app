package vai.lakehouse.keyprefix

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.Locale

import org.slf4j.LoggerFactory

/**
 * Key global của một VÙNG dữ liệu (vd `khach_hang`): mọi dataset cùng vùng dùng cùng một key để query còn nối được với nhau.
 * `keyVersion` chỉ để ghi log (DAK chưa cam kết kiểu): `None` nếu DAK không gửi, gửi `null` hoặc gửi giá trị không phải số.
 */
final case class GlobalKey(zone: String, keyPrefix: String, keyVersion: Option[Long]) {
  override def toString: String = s"GlobalKey(zone=$zone, keyPrefix=***, keyVersion=${keyVersion.getOrElse("-")})"
}

/**
 * Lấy key global qua `GET {addr}/api/v1/keys/global?zone=<vùng>` (docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md mục 2).
 *
 *  - Token là token `client_credentials` của client mà DAK chấp nhận (client chính của team, hoặc client nội bộ đang dùng cho `by-path`/
 *    `by-column`). Endpoint này không có tham số `tag` nên không bao giờ gửi `tag`, kể cả khi `DAK_TAG` được đặt.
 *  - `200` → [[GlobalKey]]; `zone` trong thân phải đúng vùng đã hỏi, `keyPrefix` là chuỗi khác rỗng.
 *  - `401`: xin token mới, thử lại một lần ([[DakClient]]).
 *  - `403 access_denied`: không có phiếu silver hiệu lực nào cho vùng này (DAK trả CÙNG câu với vùng không tồn tại). `403 key_disabled`:
 *    key đang bị khoá. `429`: gọi quá nhanh. Mọi mã khác `200`: `IllegalStateException` kèm requestId, không thử lại.
 *
 * Thông báo lỗi không bao giờ chứa token, secret, keyPrefix hay body của DAK.
 */
class DakGlobalKeySource(cfg: DakConfig, tokens: KeycloakTokenProvider, http: JsonHttp = new JsonHttp()) {

  import DakGlobalKeySource._

  private val logger = LoggerFactory.getLogger(classOf[DakGlobalKeySource])

  private val client = new DakClient(cfg, tokens, http)

  def read(zone: String): GlobalKey = {
    validate(zone)
    val result = client.get(s"$Path?zone=${URLEncoder.encode(zone, StandardCharsets.UTF_8)}", s"DAK global key request for zone '$zone'")
    if (result.response.status != 200) throw new IllegalStateException(failureMessage(zone, result))
    val key = parse(zone, result)
    logger.info(s"DAK returned the global key for zone '$zone' (keyVersion=${key.keyVersion.getOrElse("-")}, requestId=${result.requestId})")
    key
  }

  private def parse(zone: String, result: DakClient.Result): GlobalKey = {
    val body = result.response.body
    def invalid(why: String) =
      new IllegalStateException(s"DAK returned an invalid global key response for zone '$zone' ($why, requestId=${result.requestId})")
    if (body.path("zone").asText("") != zone) throw invalid("zone does not match the request")
    val prefix = body.path("keyPrefix")
    if (!prefix.isTextual || prefix.asText().isEmpty) throw invalid("no non-empty string field 'keyPrefix'")
    val version = body.path("keyVersion")
    val parsed =
      if (version.isIntegralNumber) Some(version.asLong())
      else if (version.isTextual && version.asText().matches("[0-9]{1,18}")) Some(version.asText().toLong)
      else None
    GlobalKey(zone, prefix.asText(), parsed)
  }

  private def failureMessage(zone: String, result: DakClient.Result): String =
    (result.response.status, result.errorCode) match {
      case (403, Some("key_disabled")) =>
        s"The global key of zone '$zone' is disabled in DAK (${result.detail}). Ask a DAK administrator to enable the key."
      case (403, _) =>
        s"DAK denied the global key of zone '$zone' (${result.detail}): there is no effective silver request (approved or scheduled) " +
          "for this zone that this client may use, or the zone does not exist (DAK gives the same answer for both)."
      case (401, _) =>
        s"DAK rejected the access token for the global key of zone '$zone' (${result.detail}) even after requesting a new token. " +
          "Check that the client is registered and enabled in DAK."
      case (429, _) =>
        s"DAK rate limit reached for the global key of zone '$zone' (${result.detail}): retry later."
      case _ =>
        s"DAK global key request for zone '$zone' failed (${result.detail})"
    }
}

object DakGlobalKeySource {

  val Path = "/api/v1/keys/global"

  /** Vùng: `^[a-z0-9_]{1,64}$` (docs/key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md mục 1). */
  val ZonePattern = "^[a-z0-9_]{1,64}$"

  def isValidZone(zone: String): Boolean = Option(zone).exists(_.matches(ZonePattern))

  /** Kiểm vùng trước khi gọi DAK; lỗi không in lại giá trị. */
  def validate(zone: String): Unit =
    require(isValidZone(zone), s"Global key zone must match $ZonePattern")
}

/**
 * Cầu nối sang [[ColumnKeySource]]: mọi cột được hỏi dùng CÙNG key global của vùng, nên phần mã hoá cột (cdr-crypto) không phải đổi.
 * Key không bao giờ đổi sau khi tạo (DAK chặn xoay key) nên lấy một lần lúc cần rồi giữ trong bộ nhớ của driver; lỗi thì không cache.
 */
class GlobalColumnKeySource(source: DakGlobalKeySource, zone: String) extends ColumnKeySource {

  @volatile private var cached: Option[GlobalKey] = None

  override def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String] = {
    val key = cached.getOrElse {
      val fetched = source.read(zone)
      cached = Some(fetched)
      fetched
    }
    columns.map(c => c.toLowerCase(Locale.ROOT) -> key.keyPrefix).toMap
  }
}

/** Dựng [[DakGlobalKeySource]] từ cùng các key `dak.*` với `by-path`/`by-column` (`DAK_ADDR`, `DAK_TOKEN_URL`, `DAK_CLIENT_ID`, ...). */
object GlobalKeySourceFactory {

  /** Ném `IllegalArgumentException` (không lộ giá trị nhạy cảm) nếu cấu hình sai hoặc thiếu. */
  def create(cfg: ConfigSource): DakGlobalKeySource = {
    val source = cfg.get(PrefixSourceFactory.Keys.Source).map(_.toLowerCase(Locale.ROOT))
    require(source.contains(PrefixSourceFactory.DakSource),
      s"Global keys are only available from DAK: set ${cfg.describe(PrefixSourceFactory.Keys.Source)}=${PrefixSourceFactory.DakSource} " +
        s"(got: ${source.getOrElse("<not set>")})")
    val dak = DakSettings.config(cfg)
    new DakGlobalKeySource(dak, DakSettings.tokens(cfg, dak))
  }
}
