package vai.lakehouse.keyprefix

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import vai.lakehouse.keyprefix.PrefixSourceFactory.Keys

class PathKeySourceFactorySpec extends AnyFlatSpec with Matchers {

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

  "create" should "dựng nguồn DAK khi source=dak" in {
    PathKeySourceFactory.create(cfg(dakSettings: _*)) shouldBe a[DakPathKeySource]
  }

  it should "từ chối nguồn khác dak, nêu tên cấu hình" in {
    val ex = intercept[IllegalArgumentException](
      PathKeySourceFactory.create(cfg(dakSettings.filterNot(_._1 == Keys.Source) :+ (Keys.Source -> "vault"): _*)))

    ex.getMessage should include("TEST_source")
  }

  it should "báo thiếu cấu hình bắt buộc theo tên vật lý, không in secret" in {
    val ex = intercept[IllegalArgumentException](
      PathKeySourceFactory.create(cfg(dakSettings.filterNot(_._1 == Keys.DakAddr): _*)))

    ex.getMessage should include("TEST_dak.addr")
    ex.getMessage should not include "sec"
  }
}
