package vai.lakehouse.columncrypto.prefix

import java.io.IOException
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Duration

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.slf4j.LoggerFactory
import vai.lakehouse.columncrypto.ColumnCryptoConfig

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Cấu hình truy cập Vault. keyPrefix của bảng T nằm ở KV v2: `<kvMount>/<kvBasePath>/T`, field `keyField`.
 * Hai cách xác thực:
 *  - `token = None`  : Kubernetes auth. Không chứa bí mật, JWT được đọc từ `jwtPath` lúc gọi.
 *  - `token = Some(t)`: dùng thẳng token Vault tĩnh `t` (vd token mà ExternalSecrets ClusterSecretStore đang dùng);
 *                       `role`/`authMount`/`jwtPath` bị bỏ qua.
 * `toString` che token để không lộ ra log khi in nguyên case class.
 */
case class VaultConfig(
  addr: String,
  role: String,
  authMount: String,
  kvMount: String,
  kvBasePath: String,
  keyField: String,
  jwtPath: String,
  token: Option[String] = None
) {
  override def toString: String =
    s"VaultConfig(addr=$addr, role=$role, authMount=$authMount, kvMount=$kvMount, kvBasePath=$kvBasePath, " +
      s"keyField=$keyField, jwtPath=$jwtPath, token=${if (token.isDefined) "***" else "<none>"})"
}

/**
 * Lấy keyPrefix từ Vault (KV v2). Chỉ dùng JDK HttpClient + Jackson có sẵn của Spark.
 *  - Kubernetes auth (mặc định): đăng nhập bằng JWT của service account, đọc secret, rồi thu hồi
 *    token vừa cấp (revoke-self).
 *  - Token tĩnh (`VaultConfig.token`): dùng luôn token đó để đọc; TUYỆT ĐỐI không login và không
 *    revoke, vì token này là credential dài hạn có thể đang được dịch vụ khác dùng chung.
 * Thông báo lỗi không bao giờ chứa JWT, token hay giá trị prefix.
 */
class VaultPrefixSource(cfg: VaultConfig) extends PrefixSource {

  private val logger = LoggerFactory.getLogger(classOf[VaultPrefixSource])

  private val ConnectTimeout = Duration.ofSeconds(5)
  private val RequestTimeout = Duration.ofSeconds(10)

  private val mapper = new ObjectMapper()
  private val client = HttpClient.newBuilder().connectTimeout(ConnectTimeout).build()

  private def trimSlashes(s: String): String = s.replaceAll("^/+|/+$", "")

  private val base = cfg.addr.replaceAll("/+$", "")

  override def read(datasetName: String): String = {
    ColumnCryptoConfig.requireValidDatasetName(datasetName)
    cfg.token match {
      case Some(staticToken) => fetchPrefix(staticToken, datasetName)
      case None =>
        val token = login()
        try fetchPrefix(token, datasetName)
        finally revoke(token)
    }
  }

  private def readJwt(): String =
    try new String(Files.readAllBytes(Paths.get(cfg.jwtPath)), StandardCharsets.UTF_8).trim
    catch {
      case e: IOException =>
        throw new IllegalStateException(s"Cannot read service account token at '${cfg.jwtPath}'", e)
    }

  private def login(): String = {
    val body = mapper.createObjectNode().put("role", cfg.role).put("jwt", readJwt()).toString
    val request = HttpRequest.newBuilder(URI.create(s"$base/v1/auth/${trimSlashes(cfg.authMount)}/login"))
      .timeout(RequestTimeout)
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val token = send(request, "login").path("auth").path("client_token").asText("")
    if (token.isEmpty) throw new IllegalStateException("Vault login response has no client_token")
    token
  }

  private def fetchPrefix(token: String, datasetName: String): String = {
    val secretPath = s"${trimSlashes(cfg.kvMount)}/${trimSlashes(cfg.kvBasePath)}/$datasetName"
    val request = HttpRequest.newBuilder(URI.create(s"$base/v1/${trimSlashes(cfg.kvMount)}/data/" +
        s"${trimSlashes(cfg.kvBasePath)}/$datasetName"))
      .timeout(RequestTimeout)
      .header("X-Vault-Token", token)
      .GET()
      .build()
    val data = send(request, s"read of secret '$secretPath'").path("data").path("data")
    val field = data.path(cfg.keyField)
    if (!field.isTextual) {
      val available = data.fieldNames().asScala.mkString(", ")
      throw new IllegalStateException(
        s"Vault secret '$secretPath' has no string field '${cfg.keyField}' (available fields: [$available])")
    }
    val prefix = field.asText()
    if (prefix.isEmpty) {
      throw new IllegalStateException(s"keyPrefix for dataset '$datasetName' in Vault secret '$secretPath' is empty")
    }
    prefix
  }

  // Best-effort: token có TTL nên nếu thu hồi lỗi thì vẫn tự hết hạn; chỉ cảnh báo, không làm job fail.
  private def revoke(token: String): Unit =
    try {
      val request = HttpRequest.newBuilder(URI.create(s"$base/v1/auth/token/revoke-self"))
        .timeout(RequestTimeout)
        .header("X-Vault-Token", token)
        .POST(HttpRequest.BodyPublishers.noBody())
        .build()
      send(request, "token revoke")
    } catch {
      case NonFatal(e) => logger.warn(s"Could not revoke Vault token (it will expire by TTL): ${e.getMessage}")
    }

  private def send(request: HttpRequest, what: String): JsonNode = {
    val response =
      try client.send(request, HttpResponse.BodyHandlers.ofString())
      catch {
        case e: IOException =>
          throw new IllegalStateException(s"Vault $what failed: cannot reach ${cfg.addr} (${e.getClass.getSimpleName})", e)
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new IllegalStateException(s"Vault $what interrupted", e)
      }
    if (response.statusCode() / 100 != 2) {
      throw new IllegalStateException(s"Vault $what failed: HTTP ${response.statusCode()}")
    }
    val body = response.body()
    if (body == null || body.isEmpty) mapper.createObjectNode() else mapper.readTree(body)
  }
}
