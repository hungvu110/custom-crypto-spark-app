package vai.lakehouse.keyprefix

import java.io.IOException
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.util.Try

/** Response đã đọc xong: mã HTTP, body đã parse (object rỗng nếu body rỗng hoặc không phải JSON), và header. */
private[keyprefix] final case class JsonResponse(status: Int, body: JsonNode, response: HttpResponse[String]) {
  def isSuccess: Boolean = status / 100 == 2

  def header(name: String): Option[String] = {
    val value = response.headers().firstValue(name)
    if (value.isPresent) Some(value.get()) else None
  }
}

/**
 * Gửi HTTP và parse JSON cho các nguồn keyPrefix (Vault, Keycloak, DAK). Chỉ dùng JDK `HttpClient` + Jackson
 * có sẵn của Spark. Lỗi mạng thành `IllegalStateException` chỉ nêu việc đang làm và địa chỉ đích — không bao
 * giờ đưa token, secret hay body vào thông báo.
 */
private[keyprefix] final class JsonHttp(connectTimeout: Duration = JsonHttp.ConnectTimeout) {

  val mapper = new ObjectMapper()

  private val client = HttpClient.newBuilder().connectTimeout(connectTimeout).build()

  /** Gửi request, trả về mọi mã HTTP. `what` mô tả việc đang làm, `target` là địa chỉ đích (cho thông báo lỗi). */
  def execute(request: HttpRequest, what: String, target: String): JsonResponse = {
    val response =
      try client.send(request, HttpResponse.BodyHandlers.ofString())
      catch {
        case e: IOException =>
          throw new IllegalStateException(s"$what failed: cannot reach $target (${e.getClass.getSimpleName})", e)
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new IllegalStateException(s"$what interrupted", e)
      }
    JsonResponse(response.statusCode(), parse(response.body()), response)
  }

  /** Như `execute` nhưng ném lỗi nếu mã HTTP không phải 2xx, trả về body JSON. */
  def send(request: HttpRequest, what: String, target: String): JsonNode = {
    val response = execute(request, what, target)
    if (!response.isSuccess) throw new IllegalStateException(s"$what failed: HTTP ${response.status}")
    response.body
  }

  // Body không phải JSON (vd trang lỗi HTML của ingress) coi như object rỗng: người gọi chỉ cần mã HTTP.
  private def parse(body: String): JsonNode =
    if (body == null || body.trim.isEmpty) mapper.createObjectNode()
    else Try(mapper.readTree(body)).toOption.filter(_ != null).getOrElse(mapper.createObjectNode())
}

private[keyprefix] object JsonHttp {
  val ConnectTimeout: Duration = Duration.ofSeconds(5)
  val RequestTimeout: Duration = Duration.ofSeconds(10)

  /** Mã lỗi OAuth/DAK chỉ được đưa vào thông báo nếu là định danh đơn giản, tránh chèn nội dung lạ vào log. */
  private val ErrorCode = "^[a-z][a-z0-9_]{0,63}$".r

  def errorCode(body: JsonNode): Option[String] =
    Option(body.path("error").asText(null)).filter(c => ErrorCode.pattern.matcher(c).matches())
}
