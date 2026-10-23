package vai.lakehouse.keyprefix

import org.slf4j.LoggerFactory

/**
 * Lấy keyPrefix cấp BẢNG qua DAK: xin access token `client_credentials` từ Keycloak (realm của tenant), rồi gọi
 * `GET {addr}/api/v1/keys/{database}/{table}`. DAK tự suy ra tenant/workspace/team từ token, nên request không
 * mang thông tin đó. Hợp đồng API: docs/key-access/DAK_API_SPEC.md. Key theo CỘT: [[DakColumnKeySource]].
 *
 *  - 401: bỏ token đang cache, xin token mới, thử lại đúng 1 lần ([[DakClient]]).
 *  - 403 và mọi lỗi khác: báo lỗi ngay, không thử lại. 403 `access_denied` cố ý gộp nhiều nguyên nhân (không có
 *    quyền, quyền hết hạn, bảng không tồn tại) nên thông báo gợi ý các nguyên nhân và kèm `requestId` để tra audit.
 *
 * Thông báo lỗi không bao giờ chứa token, secret, prefix hay body của DAK — chỉ mã lỗi và `requestId`.
 */
class DakPrefixSource(cfg: DakConfig, tokens: KeycloakTokenProvider, http: JsonHttp = new JsonHttp())
  extends PrefixSource {

  private val logger = LoggerFactory.getLogger(classOf[DakPrefixSource])

  private val client = new DakClient(cfg, tokens, http)

  override def read(datasetName: String): String = {
    val ref = DakTableRef.parse(datasetName)
    val result = client.get(s"/api/v1/keys/${ref.database}/${ref.table}", s"DAK key request for '${ref.qualified}'")
    prefixFrom(ref, result)
  }

  private def prefixFrom(ref: DakTableRef, result: DakClient.Result): String = {
    val response = result.response
    if (response.isSuccess) {
      val field = response.body.path("keyPrefix")
      if (!field.isTextual || field.asText().isEmpty) {
        throw new IllegalStateException(
          s"DAK response for '${ref.qualified}' has no non-empty string field 'keyPrefix' (requestId=${result.requestId})")
      }
      logger.debug(s"DAK returned key for '${ref.qualified}' " +
        s"(keyVersion=${response.body.path("keyVersion").asText("?")}, requestId=${result.requestId})")
      field.asText()
    } else {
      throw new IllegalStateException(failureMessage(ref, result))
    }
  }

  private def failureMessage(ref: DakTableRef, result: DakClient.Result): String =
    result.response.status match {
      case 403 if result.errorCode.contains("access_denied") =>
        s"DAK denied key for '${ref.qualified}' (${result.detail}). Possible causes: no grant, grant expired, " +
          "or table not registered in this workspace. Ask the DAK admin to look up the requestId."
      case 401 =>
        s"DAK rejected the access token for '${ref.qualified}' (${result.detail}) even after requesting a new token. " +
          "Check that the client is registered and enabled in DAK."
      case _ =>
        s"DAK key request for '${ref.qualified}' failed (${result.detail})"
    }
}
