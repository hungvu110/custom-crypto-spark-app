package org.example

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.columncrypto.prefix.{FilePrefixSource, PrefixSourceFactory}
import vai.lakehouse.columncrypto.prefix.PrefixSourceFactory.Keys

class EnvConfigSourceSpec extends AnyFlatSpec with Matchers {

  "EnvConfigSource" should "ánh xạ key logic sang đúng tên biến môi trường của bản cũ" in {
    val env = new EnvConfigSource(Map(
      "CRYPTO_PREFIX_SOURCE"  -> " vault ",
      "CRYPTO_KEY_PREFIX_DIR" -> "/etc/key-prefix",
      "VAULT_ADDR"            -> "https://vault:8200",
      "VAULT_AUTH_METHOD"     -> "token",
      "VAULT_TOKEN"           -> "tok-123",
      "VAULT_ROLE"            -> "spark",
      "VAULT_AUTH_MOUNT"      -> "k8s",
      "VAULT_KV_MOUNT"        -> "secret",
      "VAULT_KV_PATH"         -> "hla/key-prefix",
      "VAULT_KEY_FIELD"       -> "prefix",
      "VAULT_JWT_PATH"        -> "/tmp/jwt"))

    env.get(Keys.Source) shouldBe Some("vault")
    env.get(Keys.FileDir) shouldBe Some("/etc/key-prefix")
    env.get(Keys.VaultAddr) shouldBe Some("https://vault:8200")
    env.get(Keys.VaultAuthMethod) shouldBe Some("token")
    env.get(Keys.VaultToken) shouldBe Some("tok-123")
    env.get(Keys.VaultRole) shouldBe Some("spark")
    env.get(Keys.VaultAuthMount) shouldBe Some("k8s")
    env.get(Keys.VaultKvMount) shouldBe Some("secret")
    env.get(Keys.VaultKvBasePath) shouldBe Some("hla/key-prefix")
    env.get(Keys.VaultKeyField) shouldBe Some("prefix")
    env.get(Keys.VaultJwtPath) shouldBe Some("/tmp/jwt")
  }

  it should "coi biến rỗng hoặc chỉ có khoảng trắng như chưa đặt" in {
    new EnvConfigSource(Map("VAULT_ADDR" -> "  ")).get(Keys.VaultAddr) shouldBe None
  }

  "PrefixSourceFactory với EnvConfigSource" should "mặc định đọc file từ conf/key-prefix như bản cũ" in {
    val source = PrefixSourceFactory.create(new EnvConfigSource(Map.empty))

    source.read("sample_table") should not be empty
  }

  it should "dùng nguồn file khi CRYPTO_PREFIX_SOURCE=file" in {
    val source = PrefixSourceFactory.create(new EnvConfigSource(
      Map("CRYPTO_PREFIX_SOURCE" -> "file", "CRYPTO_CACHE_TTL_SECONDS" -> "0")))

    source shouldBe a[FilePrefixSource]
  }

  it should "báo thiếu đúng tên biến môi trường khi CRYPTO_PREFIX_SOURCE=vault" in {
    val ex = intercept[IllegalArgumentException](
      PrefixSourceFactory.create(new EnvConfigSource(Map("CRYPTO_PREFIX_SOURCE" -> "vault"))))

    ex.getMessage should include("VAULT_ADDR")
  }

  it should "từ chối giá trị CRYPTO_PREFIX_SOURCE không hợp lệ và nêu đúng tên biến" in {
    val ex = intercept[IllegalArgumentException](
      PrefixSourceFactory.create(new EnvConfigSource(Map("CRYPTO_PREFIX_SOURCE" -> "kms"))))

    ex.getMessage should include("CRYPTO_PREFIX_SOURCE")
  }
}
