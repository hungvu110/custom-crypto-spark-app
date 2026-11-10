package vai.lakehouse.keyprefix

import java.util.Base64

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.StubHttpServer.{Response, tokenResponse}

class DakDeidKeySourceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val TokenPath = "/realms/tenant-a/protocol/openid-connect/token"
  private val KeyPath   = "/api/v1/keys/deid"
  private val Code      = "REQ-2026-000011"
  private val Key       = Base64.getEncoder.encodeToString(Array.tabulate[Byte](32)(i => (i + 1).toByte))
  private val Ref       = DakTableRef("demo_db", "users_cdr_2")

  private var server: StubHttpServer = _

  override def beforeEach(): Unit = server = new StubHttpServer

  override def afterEach(): Unit = server.close()

  private def cfg(tag: Option[String] = None) = DakConfig(addr = server.url, tokenUrl = server.url + TokenPath, clientId = "internal",
    clientSecret = "s3cret", tag = tag, caller = Some("pseudonymize-table"))

  private def source(tag: Option[String] = None) = {
    val c = cfg(tag)
    new DakDeidKeySource(c, new KeycloakTokenProvider(c.tokenUrl, c.clientId, c.clientSecret, "TEST_hint"))
  }

  private def ok(code: String = Code, key: String = Key, version: String = "3") =
    Response(200, s"""{"requestCode":"$code","database":"demo_db","table":"users_cdr_2","keyPrefix":"$key","keyVersion":$version,"extra":1}""",
      Map("X-Request-Id" -> "req-200"))

  private def error(status: Int, code: String) =
    Response(status, s"""{"error":"$code","message":"m","requestId":"req-$status"}""")

  "fetch" should "GET /api/v1/keys/deid with database, table, requestCode and the token of the internal client, and never a tag" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, ok())

    val key = source().fetch(Ref, Some(Code))

    key shouldBe DeidKey(Code, Key, 3)
    val request = server.requestsTo(KeyPath).head
    request.query should (include("database=demo_db") and include("table=users_cdr_2") and include(s"requestCode=$Code"))
    request.query should not include "tag="
    request.headers("authorization") shouldBe "Bearer tok-1"
    request.headers("x-dak-caller") shouldBe "pseudonymize-table"
  }

  it should "not send tag even when DAK_TAG is configured: keys/deid has no such parameter" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, ok())

    source(Some("internal")).fetch(Ref, Some(Code))

    server.requestsTo(KeyPath).head.query should not include "tag"
  }

  it should "omit requestCode when none is given" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, ok())

    source().fetch(Ref, None)

    server.requestsTo(KeyPath).head.query should not include "requestCode"
  }

  it should "call DAK on every fetch and never cache the key between runs" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, ok())
    val s = source()

    s.fetch(Ref, Some(Code))
    s.fetch(Ref, Some(Code))

    server.requestsTo(KeyPath) should have size 2
  }

  it should "request a new token once on 401 and retry" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"), tokenResponse("tok-2"))
    server.stub("GET", KeyPath, error(401, "unauthorized"), ok())

    source().fetch(Ref, Some(Code)).keyVersion shouldBe 3

    server.requestsTo(KeyPath).map(_.headers("authorization")) shouldBe List("Bearer tok-1", "Bearer tok-2")
  }

  it should "stop on 403 without retrying and without leaking the key or the secret" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, error(403, "access_denied"))

    val e = the[IllegalStateException] thrownBy source().fetch(Ref, Some(Code))

    e.getMessage should (include("denied") and include("requestId=req-403") and include("expired"))
    e.getMessage should not include "s3cret"
    server.requestsTo(KeyPath) should have size 1
  }

  it should "report 409, 400 and 500 with the status and the requestId" in {
    Seq(409 -> "REQUEST_CODE", 400 -> "rejected", 500 -> "failed").foreach { case (status, text) =>
      val local = new StubHttpServer
      try {
        local.stub("POST", TokenPath, tokenResponse("tok-1"))
        local.stub("GET", KeyPath, error(status, "x"))
        val c = cfg().copy(addr = local.url, tokenUrl = local.url + TokenPath)
        val s = new DakDeidKeySource(c, new KeycloakTokenProvider(c.tokenUrl, c.clientId, c.clientSecret, "TEST_hint"))
        val e = the[IllegalStateException] thrownBy s.fetch(Ref, Some(Code))
        e.getMessage should (include(s"HTTP $status") and include(text) and include(s"requestId=req-$status"))
      } finally local.close()
    }
  }

  it should "refuse a response of another request" in {
    server.stub("POST", TokenPath, tokenResponse("tok-1"))
    server.stub("GET", KeyPath, ok(code = "REQ-2026-000099"))

    val e = the[IllegalStateException] thrownBy source().fetch(Ref, Some(Code))

    e.getMessage should include("another request")
    e.getMessage should not include Key
  }

  it should "refuse a keyPrefix that is not the base64 of 32 bytes, and a bad keyVersion" in {
    val short = Base64.getEncoder.encodeToString(Array.fill[Byte](16)(7))
    Seq(ok(key = short), ok(key = "base64:" + Key), ok(version = "0"), ok(version = "\"3\"")).foreach { response =>
      val local = new StubHttpServer
      try {
        local.stub("POST", TokenPath, tokenResponse("tok-1"))
        local.stub("GET", KeyPath, response)
        val c = cfg().copy(addr = local.url, tokenUrl = local.url + TokenPath)
        val s = new DakDeidKeySource(c, new KeycloakTokenProvider(c.tokenUrl, c.clientId, c.clientSecret, "TEST_hint"))
        val e = the[IllegalStateException] thrownBy s.fetch(Ref, Some(Code))
        e.getMessage should include("invalid deid key response")
        e.getMessage should not include Key
      } finally local.close()
    }
  }

  it should "reject a malformed requestCode before calling DAK" in {
    an[IllegalArgumentException] should be thrownBy source().fetch(Ref, Some("../x"))
    server.requests shouldBe empty
  }

  "DeidKey.toString" should "not print the key" in {
    DeidKey(Code, Key, 3).toString should not include Key
  }

  "DeidKeySourceFactory" should "build from DAK_* settings and recognise the dak source" in {
    val env = Map("CRYPTO_PREFIX_SOURCE" -> "dak", "DAK_ADDR" -> "https://dak", "DAK_TOKEN_URL" -> "https://sso/token",
      "DAK_CLIENT_ID" -> "c", "DAK_CLIENT_SECRET" -> "s")
    val config = new EnvConfigSource(env)

    DeidKeySourceFactory.isDak(config) shouldBe true
    DeidKeySourceFactory.create(config) should not be null
    DeidKeySourceFactory.isDak(new EnvConfigSource(Map("CRYPTO_PREFIX_SOURCE" -> "file"))) shouldBe false
    an[IllegalArgumentException] should be thrownBy DeidKeySourceFactory.create(new EnvConfigSource(env - "DAK_CLIENT_SECRET"))
  }
}
