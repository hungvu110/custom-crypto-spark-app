package vai.lakehouse.keyprefix

import java.net.{URI, URLEncoder}
import java.net.http.HttpRequest
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

/**
 * Lấy access token `client_credentials` từ token endpoint của một realm Keycloak và cache tới gần hết hạn
 * (`expires_in - skew`, `skew = min(30 giây, expires_in / 2)`). Không dùng refresh token: Keycloak không cấp
 * refresh token cho `client_credentials`, hết hạn thì xin token mới.
 *
 * Thread-safe: nhiều query analyze song song trên Thrift Server cùng cache miss chỉ tạo 1 request tới Keycloak.
 * Thông báo lỗi không bao giờ chứa secret hay token.
 *
 * @param settingsHint tên vật lý của các cấu hình credential, đưa vào thông báo lỗi để chỉ đúng chỗ cần sửa
 * @param nanoTime     đồng hồ, tiêm vào để test không phải ngủ
 */
final class KeycloakTokenProvider(
  tokenUrl: String,
  clientId: String,
  clientSecret: String,
  settingsHint: String,
  nanoTime: () => Long = () => System.nanoTime(),
  http: JsonHttp = new JsonHttp()
) {

  private final case class CachedToken(value: String, refreshAt: Long)

  @volatile private var cached: CachedToken = _

  def token(): String = {
    val current = cached
    if (isFresh(current)) current.value
    else synchronized {
      val again = cached
      if (isFresh(again)) again.value
      else {
        val fresh = fetch()
        cached = fresh
        fresh.value
      }
    }
  }

  /** Bỏ token khỏi cache — chỉ khi đó đúng là token vừa bị từ chối, để không xoá token mới thread khác vừa lấy. */
  def invalidate(rejected: String): Unit = synchronized {
    val current = cached
    if (current != null && current.value == rejected) cached = null
  }

  private def isFresh(t: CachedToken): Boolean = t != null && nanoTime() - t.refreshAt < 0

  private def fetch(): CachedToken = {
    val requestedAt = nanoTime()
    val form = Seq("grant_type" -> "client_credentials", "client_id" -> clientId, "client_secret" -> clientSecret)
      .map { case (k, v) => s"${encode(k)}=${encode(v)}" }
      .mkString("&")
    val request = HttpRequest.newBuilder(URI.create(tokenUrl))
      .timeout(JsonHttp.RequestTimeout)
      .header("Content-Type", "application/x-www-form-urlencoded")
      .header("Accept", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(form))
      .build()
    val response = http.execute(request, "Keycloak token request", tokenUrl)
    if (!response.isSuccess) {
      val code = JsonHttp.errorCode(response.body).map(c => s" ($c)").getOrElse("")
      throw new IllegalStateException(
        s"Keycloak token request failed: HTTP ${response.status}$code. Check $settingsHint")
    }
    val token = response.body.path("access_token").asText("")
    if (token.isEmpty) throw new IllegalStateException("Keycloak token response has no access_token")
    val expiresIn = response.body.path("expires_in").asLong(0L)
    if (expiresIn <= 0) throw new IllegalStateException("Keycloak token response has no positive expires_in")

    val lifetime = TimeUnit.SECONDS.toNanos(expiresIn)
    val skew = math.min(TimeUnit.SECONDS.toNanos(30), lifetime / 2)
    CachedToken(token, requestedAt + lifetime - skew)
  }

  private def encode(s: String): String = URLEncoder.encode(s, StandardCharsets.UTF_8)

  override def toString: String = s"KeycloakTokenProvider(tokenUrl=$tokenUrl, clientId=$clientId)"
}

object KeycloakTokenProvider {

  // Khoá gồm cả secret: 2 namespace cấu hình cùng client nhưng lệch secret thì không che lỗi của nhau.
  private final case class Key(tokenUrl: String, clientId: String, clientSecret: String)

  private val providers = new ConcurrentHashMap[Key, KeycloakTokenProvider]()

  /**
   * Provider dùng chung trong tiến trình: `spark.columncrypto.*` và `spark.cdrcrypto.*` cấu hình cùng một client
   * chỉ giữ một token. `tokenUrl` chứa realm nên hai realm trùng `client_id` không bao giờ dùng chung token.
   */
  def shared(tokenUrl: String, clientId: String, clientSecret: String, settingsHint: String): KeycloakTokenProvider =
    providers.computeIfAbsent(Key(tokenUrl, clientId, clientSecret),
      _ => new KeycloakTokenProvider(tokenUrl, clientId, clientSecret, settingsHint))
}
