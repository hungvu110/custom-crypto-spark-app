package vai.lakehouse.keyprefix

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrefixSourceFactorySpec extends AnyFlatSpec with Matchers {

  import PrefixSourceFactory.Keys

  /** ConfigSource từ Map, tên vật lý = "TEST_<key>" để kiểm tra thông báo lỗi dùng đúng tên vật lý. */
  private def cfg(values: (String, String)*): ConfigSource = {
    val m = values.toMap
    new ConfigSource {
      override def get(key: String): Option[String] = m.get(key).map(_.trim).filter(_.nonEmpty)
      override def describe(key: String): String = s"TEST_$key"
    }
  }

  private def prefixDir(files: (String, String)*): String = {
    val dir = Files.createTempDirectory("factory-prefix")
    dir.toFile.deleteOnExit()
    files.foreach { case (n, c) =>
      Files.write(dir.resolve(n), c.getBytes(StandardCharsets.UTF_8)).toFile.deleteOnExit()
    }
    dir.toString
  }

  private val vaultSettings = Seq(
    Keys.VaultAddr -> "http://127.0.0.1:1", Keys.VaultRole -> "r", Keys.VaultKvBasePath -> "base")

  "create" should "mặc định dùng nguồn file với TTL cache" in {
    val dir = prefixDir("t1" -> "file_prefix\n")

    val source = PrefixSourceFactory.create(cfg(Keys.FileDir -> dir))

    source shouldBe a[CachedPrefixSource]
    source.read("t1") shouldBe "file_prefix"
  }

  it should "không bọc cache khi cacheTtlSeconds = 0" in {
    val source = PrefixSourceFactory.create(cfg(Keys.FileDir -> prefixDir(), Keys.CacheTtlSeconds -> "0"))

    source shouldBe a[FilePrefixSource]
  }

  it should "dựng nguồn vault khi source=vault và đủ cấu hình bắt buộc" in {
    val source = PrefixSourceFactory.create(cfg((vaultSettings :+ (Keys.Source -> "vault")): _*))

    source should not be a[FilePrefixSource]
  }

  it should "fallback file -> vault khi source=file,vault" in {
    val dir = prefixDir("t1" -> "from_file")
    val source = PrefixSourceFactory.create(cfg((vaultSettings ++ Seq(
      Keys.Source -> " File , Vault ", Keys.FileDir -> dir, Keys.CacheTtlSeconds -> "0")): _*))

    source shouldBe a[ChainedPrefixSource]
    source.read("t1") shouldBe "from_file"
    // t2 không có file và Vault không kết nối được: lỗi phải nêu cả hai nguồn.
    val ex = intercept[IllegalStateException](source.read("t2"))
    ex.getMessage should include("file:")
    ex.getMessage should include("vault:")
  }

  it should "dùng token tĩnh khi authMethod=token, không cần role" in {
    // Vault giả không tồn tại nên đọc sẽ lỗi kết nối; chỉ kiểm tra dựng được nguồn mà không đòi vault.role.
    val source = PrefixSourceFactory.create(cfg(
      Keys.Source -> "vault", Keys.VaultAddr -> "http://127.0.0.1:1", Keys.VaultKvBasePath -> "base",
      Keys.VaultAuthMethod -> "token", Keys.VaultToken -> "tok", Keys.CacheTtlSeconds -> "0"))

    source shouldBe a[VaultPrefixSource]
  }

  it should "đòi vault.token khi authMethod=token" in {
    val ex = intercept[IllegalArgumentException](PrefixSourceFactory.create(cfg(
      Keys.Source -> "vault", Keys.VaultAddr -> "http://127.0.0.1:1", Keys.VaultKvBasePath -> "base",
      Keys.VaultAuthMethod -> "token")))

    ex.getMessage should include("TEST_vault.token")
  }

  it should "từ chối authMethod không hỗ trợ" in {
    val ex = intercept[IllegalArgumentException](PrefixSourceFactory.create(cfg(
      (vaultSettings ++ Seq(Keys.Source -> "vault", Keys.VaultAuthMethod -> "ldap")): _*)))

    ex.getMessage should include("TEST_vault.authMethod")
  }

  it should "báo tên vật lý của setting còn thiếu khi dùng vault" in {
    val ex = intercept[IllegalArgumentException](
      PrefixSourceFactory.create(cfg(Keys.Source -> "vault", Keys.VaultRole -> "r", Keys.VaultKvBasePath -> "b")))

    ex.getMessage should include("TEST_vault.addr")
  }

  it should "từ chối nguồn không hỗ trợ" in {
    val ex = intercept[IllegalArgumentException](PrefixSourceFactory.create(cfg(Keys.Source -> "kms")))

    ex.getMessage should include("TEST_source")
    ex.getMessage should include("kms")
  }

  it should "từ chối TTL không hợp lệ" in {
    Seq("abc", "-1").foreach { bad =>
      val ex = intercept[IllegalArgumentException](
        PrefixSourceFactory.create(cfg(Keys.FileDir -> prefixDir(), Keys.CacheTtlSeconds -> bad)))
      ex.getMessage should include("TEST_cacheTtlSeconds")
    }
  }

  private val dakSettings = Seq(
    Keys.DakAddr -> "https://dak.example", Keys.DakTokenUrl -> "https://kc.example/realms/t/protocol/openid-connect/token",
    Keys.DakClientId -> "dak.w1.team-a", Keys.DakClientSecret -> "SECRET-VALUE-42")

  "create với source=dak" should "dựng DakPrefixSource bọc cache" in {
    PrefixSourceFactory.create(cfg((dakSettings :+ (Keys.Source -> "dak")): _*)) shouldBe a[CachedPrefixSource]
    PrefixSourceFactory.create(cfg((dakSettings ++ Seq(Keys.Source -> " DAK ", Keys.CacheTtlSeconds -> "0")): _*)) shouldBe
      a[DakPrefixSource]
  }

  it should "cho phép vault.* cùng có mặt (fallback thủ công chỉ cần đổi source)" in {
    val withVault = dakSettings ++ vaultSettings :+ (Keys.VaultToken -> "tok")

    PrefixSourceFactory.create(cfg((withVault ++ Seq(Keys.Source -> "dak", Keys.CacheTtlSeconds -> "0")): _*)) shouldBe
      a[DakPrefixSource]
    PrefixSourceFactory.create(cfg((withVault ++ Seq(Keys.Source -> "vault", Keys.CacheTtlSeconds -> "0")): _*)) shouldBe
      a[VaultPrefixSource]
  }

  it should "báo tên vật lý của từng setting bắt buộc còn thiếu" in {
    Seq(Keys.DakAddr, Keys.DakTokenUrl, Keys.DakClientId, Keys.DakClientSecret).foreach { missing =>
      val settings = dakSettings.filterNot(_._1 == missing) :+ (Keys.Source -> "dak")
      val ex = intercept[IllegalArgumentException](PrefixSourceFactory.create(cfg(settings: _*)))
      ex.getMessage should include(s"TEST_$missing")
    }
  }

  it should "từ chối http:// trừ khi bật dak.allowInsecureHttp, không lộ secret" in {
    Seq(Keys.DakAddr -> "http://dak.example", Keys.DakTokenUrl -> "http://kc.example/token").foreach { insecure =>
      val settings = dakSettings.filterNot(_._1 == insecure._1) ++ Seq(insecure, Keys.Source -> "dak")

      val ex = intercept[IllegalArgumentException](PrefixSourceFactory.create(cfg(settings: _*)))
      ex.getMessage should include(s"TEST_${insecure._1}")
      ex.getMessage should include("https")
      ex.getMessage should not include "SECRET-VALUE-42"

      PrefixSourceFactory.create(cfg((settings :+ (Keys.DakAllowInsecureHttp -> "true")): _*)) shouldBe
        a[CachedPrefixSource]
    }
  }

  it should "từ chối URL không hợp lệ" in {
    val settings = dakSettings.filterNot(_._1 == Keys.DakAddr) ++ Seq(Keys.DakAddr -> "dak.example", Keys.Source -> "dak")

    val ex = intercept[IllegalArgumentException](PrefixSourceFactory.create(cfg(settings: _*)))
    ex.getMessage should include("TEST_dak.addr")
  }

  it should "không cho ghép dak với nguồn khác vì fallback sẽ bỏ qua phân quyền" in {
    Seq("dak,vault", "file,dak", "vault , dak").foreach { sources =>
      val ex = intercept[IllegalArgumentException](
        PrefixSourceFactory.create(cfg((dakSettings ++ vaultSettings :+ (Keys.Source -> sources)): _*)))
      ex.getMessage should include("TEST_source")
      ex.getMessage should include("dak")
      ex.getMessage should include("bypass")
    }
  }

  it should "lấy được keyPrefix qua Keycloak + DAK giả" in {
    val server = new StubHttpServer
    try {
      server.stub("POST", "/token", StubHttpServer.tokenResponse("tok-1"))
      server.stub("GET", "/api/v1/keys/demo_db/users_cdr", StubHttpServer.Response(200, """{"keyPrefix":"p1"}"""))
      val source = PrefixSourceFactory.create(cfg(
        Keys.Source -> "dak", Keys.DakAddr -> server.url, Keys.DakTokenUrl -> s"${server.url}/token",
        Keys.DakClientId -> "c", Keys.DakClientSecret -> "s", Keys.DakAllowInsecureHttp -> "true"))

      source.read("demo_db.users_cdr") shouldBe "p1"
      source.read("demo_db.users_cdr") shouldBe "p1" // lần 2 lấy từ cache prefix
      server.requestsTo("/api/v1/keys/demo_db/users_cdr") should have size 1
    } finally server.close()
  }

  "SparkConfSource" should "đọc key logic từ spark.columncrypto.* và bỏ qua giá trị rỗng" in {
    val conf = new SparkConf(false)
      .set("spark.columncrypto.source", " vault ")
      .set("spark.columncrypto.vault.addr", "  ")
    val source = new SparkConfSource(conf, "spark.columncrypto.")

    source.get(Keys.Source) shouldBe Some("vault")
    source.get(Keys.VaultAddr) shouldBe None
    source.get(Keys.VaultRole) shouldBe None
    source.describe(Keys.VaultAddr) shouldBe "spark.columncrypto.vault.addr"
  }
}
