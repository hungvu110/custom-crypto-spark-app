package vai.lakehouse.keyprefix

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.PrefixSourceFactory.Keys
import vai.lakehouse.keyprefix.StubHttpServer.{Response, tokenResponse}

class ColumnKeySourceFactorySpec extends AnyFlatSpec with Matchers {

  /** ConfigSource từ Map, tên vật lý = "TEST_<key>" để kiểm tra thông báo lỗi dùng đúng tên vật lý. */
  private def cfg(values: (String, String)*): ConfigSource = {
    val m = values.toMap
    new ConfigSource {
      override def get(key: String): Option[String] = m.get(key).map(_.trim).filter(_.nonEmpty)
      override def describe(key: String): String = s"TEST_$key"
    }
  }

  private val dakSettings = Seq(
    Keys.Source          -> "dak",
    Keys.DakAddr         -> "https://dak.example",
    Keys.DakTokenUrl     -> "https://kc.example/realms/t/protocol/openid-connect/token",
    Keys.DakClientId     -> "dak.internal",
    Keys.DakClientSecret -> "sec")

  "create" should "dựng nguồn DAK có cache theo cột khi source=dak" in {
    ColumnKeySourceFactory.create(cfg(dakSettings: _*)) shouldBe a[CachedColumnKeySource]
  }

  it should "bọc lớp chờ DAK duyệt khi dak.pendingWait.timeoutSeconds > 0" in {
    ColumnKeySourceFactory.create(cfg((dakSettings :+ (Keys.DakPendingTimeoutSeconds -> "3600")): _*)) shouldBe
      a[WaitingColumnKeySource]
  }

  it should "không bọc cache khi cacheTtlSeconds = 0" in {
    ColumnKeySourceFactory.create(cfg((dakSettings :+ (Keys.CacheTtlSeconds -> "0")): _*)) shouldBe a[DakColumnKeySource]
  }

  it should "từ chối mọi nguồn khác dak, kể cả khi không đặt source" in {
    Seq(None, Some("file"), Some("vault"), Some("file,vault")).foreach { source =>
      val settings = dakSettings.filterNot(_._1 == Keys.Source) ++ source.map(Keys.Source -> _)

      val ex = intercept[IllegalArgumentException](ColumnKeySourceFactory.create(cfg(settings: _*)))

      ex.getMessage should include("TEST_source")
      ex.getMessage should include("dak")
    }
  }

  it should "báo thiếu đúng tên cấu hình bắt buộc" in {
    val ex = intercept[IllegalArgumentException](
      ColumnKeySourceFactory.create(cfg(dakSettings.filterNot(_._1 == Keys.DakClientId): _*)))

    ex.getMessage should include("TEST_dak.clientId")
  }

  it should "bắt buộc https:// trừ khi bật allowInsecureHttp" in {
    val http = dakSettings.filterNot(_._1 == Keys.DakAddr) :+ (Keys.DakAddr -> "http://dak-mock:8085")

    intercept[IllegalArgumentException](ColumnKeySourceFactory.create(cfg(http: _*))).getMessage should include("https://")
    ColumnKeySourceFactory.create(cfg((http :+ (Keys.DakAllowInsecureHttp -> "true")): _*)) shouldBe a[CachedColumnKeySource]
  }

  it should "từ chối dak.caller và dak.tag sai định dạng, không in lại giá trị" in {
    val badCaller = intercept[IllegalArgumentException](
      ColumnKeySourceFactory.create(cfg((dakSettings :+ (Keys.DakCaller -> "bad caller!<x>")): _*)))
    badCaller.getMessage should include("TEST_dak.caller")
    badCaller.getMessage should not include "<x>"

    val badTag = intercept[IllegalArgumentException](
      ColumnKeySourceFactory.create(cfg((dakSettings :+ (Keys.DakTag -> "in ternal")): _*)))
    badTag.getMessage should include("TEST_dak.tag")
  }

  it should "gửi tag và X-Dak-Caller theo cấu hình tới DAK" in {
    val server = new StubHttpServer
    try {
      server.stub("POST", "/token", tokenResponse("tok-1"))
      server.stub("GET", "/api/v1/keys/by-column", Response(200,
        """{"database":"demo_db","table":"t","complete":true,"columns":[{"column":"a","state":"READY","keyPrefix":"k"}]}"""))
      val source = ColumnKeySourceFactory.create(cfg(
        Keys.Source -> "dak", Keys.DakAddr -> server.url, Keys.DakTokenUrl -> (server.url + "/token"),
        Keys.DakClientId -> "factory.client", Keys.DakClientSecret -> "sec", Keys.DakAllowInsecureHttp -> "true",
        Keys.DakTag -> "internal", Keys.DakCaller -> "job-1"))

      source.readColumns("demo_db.t", Seq("a"), KeyIntent.Decrypt) shouldBe Map("a" -> "k")

      val req = server.requestsTo("/api/v1/keys/by-column").head
      req.params("tag") shouldBe "internal"
      req.headers("x-dak-caller") shouldBe "job-1"
    } finally server.close()
  }

  "EnvConfigSource" should "ánh xạ dak.tag và dak.caller sang DAK_TAG, DAK_CALLER" in {
    val env = new EnvConfigSource(Map("DAK_TAG" -> " internal ", "DAK_CALLER" -> "job-1"))

    env.get(Keys.DakTag) shouldBe Some("internal")
    env.get(Keys.DakCaller) shouldBe Some("job-1")
    env.describe(Keys.DakTag) shouldBe "DAK_TAG"
  }
}
