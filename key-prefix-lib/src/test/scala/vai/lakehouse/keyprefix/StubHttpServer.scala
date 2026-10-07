package vai.lakehouse.keyprefix

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList, Executors}
import java.util.concurrent.atomic.AtomicInteger

import com.sun.net.httpserver.{HttpExchange, HttpServer}

import scala.collection.JavaConverters._

/**
 * Server HTTP giả trên 127.0.0.1 (cổng ngẫu nhiên) đóng vai Keycloak/DAK trong test: trả response theo
 * (method, path) và ghi lại mọi request. Một route có thể khai nhiều response — trả lần lượt, response
 * cuối được lặp lại — để giả lập "401 lần đầu, 200 lần sau".
 */
final class StubHttpServer extends AutoCloseable {

  import StubHttpServer._

  private val routes = new ConcurrentHashMap[(String, String), Route]()
  private val recorded = new CopyOnWriteArrayList[Recorded]()
  private val pool = Executors.newFixedThreadPool(16)
  private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)

  server.setExecutor(pool)
  server.createContext("/", (ex: HttpExchange) => handle(ex))
  server.start()

  def url: String = s"http://127.0.0.1:${server.getAddress.getPort}"

  /** Khai các response cho (method, path); `delayMillis` để giả lập server chậm (test đồng thời). */
  def stub(method: String, path: String, responses: Response*): Unit =
    routes.put((method, path), Route(responses.toIndexedSeq, new AtomicInteger()))

  def requests: List[Recorded] = recorded.asScala.toList

  def requestsTo(path: String): List[Recorded] = requests.filter(_.path == path)

  override def close(): Unit = {
    server.stop(0)
    pool.shutdownNow()
  }

  private def handle(ex: HttpExchange): Unit = {
    val in = ex.getRequestBody
    val buf = new ByteArrayOutputStream()
    val chunk = new Array[Byte](1024)
    Iterator.continually(in.read(chunk)).takeWhile(_ != -1).foreach(n => buf.write(chunk, 0, n))
    val headers = ex.getRequestHeaders.asScala.map { case (k, v) => k.toLowerCase -> v.asScala.headOption.getOrElse("") }.toMap
    val method = ex.getRequestMethod
    val path = ex.getRequestURI.getPath
    recorded.add(Recorded(method, path, headers, new String(buf.toByteArray, StandardCharsets.UTF_8)))

    val response = Option(routes.get((method, path))).map(_.next()).getOrElse(Response(404, """{"error":"not_found"}"""))
    if (response.delayMillis > 0) Thread.sleep(response.delayMillis)
    response.headers.foreach { case (k, v) => ex.getResponseHeaders.add(k, v) }
    val bytes = response.body.getBytes(StandardCharsets.UTF_8)
    ex.sendResponseHeaders(response.status, if (bytes.isEmpty) -1L else bytes.length.toLong)
    if (bytes.nonEmpty) ex.getResponseBody.write(bytes)
    ex.close()
  }
}

object StubHttpServer {

  final case class Response(status: Int, body: String, headers: Map[String, String] = Map.empty, delayMillis: Long = 0)

  final case class Recorded(method: String, path: String, headers: Map[String, String], body: String) {
    /** Body dạng application/x-www-form-urlencoded đã giải mã. */
    def form: Map[String, String] =
      body.split("&").filter(_.nonEmpty).map { pair =>
        val kv = pair.split("=", 2)
        val value = if (kv.length > 1) kv(1) else ""
        java.net.URLDecoder.decode(kv(0), "UTF-8") -> java.net.URLDecoder.decode(value, "UTF-8")
      }.toMap
  }

  private final case class Route(responses: IndexedSeq[Response], calls: AtomicInteger) {
    def next(): Response = responses(math.min(calls.getAndIncrement(), responses.size - 1))
  }

  def tokenResponse(token: String, expiresIn: Long = 300): Response =
    Response(200, s"""{"access_token":"$token","expires_in":$expiresIn,"token_type":"Bearer"}""")
}
