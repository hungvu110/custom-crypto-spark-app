package vai.lakehouse.silveringest.security

import java.io.{DataOutputStream, FileOutputStream}
import java.nio.file.{Files, Path}

import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Token đọc INPUT_PATH ở cluster khác (lấy bằng tay, `hdfs fetchdt`): đọc file, kiểm đúng cluster, thêm vào user hiện tại. Token là giả. */
class InputTokenSpec extends AnyFlatSpec with Matchers {

  private val now = 1795193804000L
  private val hour = 3600000L
  private val inputPath = "hdfs://localhost:8020/landing/ebs_3g_4g/dt=%DAY%"
  private lazy val service = InputToken.expectedService(inputPath).fold(e => fail(e), identity)

  /** Token HDFS như NameNode cấp: identifier có lúc cấp và hạn tối đa. */
  private def hdfsToken(service: String, issued: Long, maxDate: Long, owner: String = "reader"): Token[TokenIdentifier] = {
    val id = new DelegationTokenIdentifier(new Text(owner), new Text(owner), new Text(owner))
    id.setIssueDate(issued)
    id.setMaxDate(maxDate)
    new Token[TokenIdentifier](id.getBytes, "password".getBytes, InputToken.HdfsKind, new Text(service))
  }

  private def creds(tokens: Token[TokenIdentifier]*): Credentials = {
    val c = new Credentials()
    tokens.foreach(t => c.addToken(t.getService, t))
    c
  }

  /** File như `hdfs fetchdt` ghi (định dạng token storage của Hadoop). */
  private def file(c: Credentials): Path = {
    val f = Files.createTempFile("hkh", ".token")
    val out = new DataOutputStream(new FileOutputStream(f.toFile))
    try c.writeTokenStorageToStream(out) finally out.close()
    f
  }

  private val fresh = hdfsToken(service, issued = now - hour, maxDate = now + 167 * hour)

  "InputToken.expectedService" should "là service mà DFSClient dùng, cho cả INPUT_PATH có %DAY% của bulk load" in {
    service should endWith(":8020")
    InputToken.expectedService("file:///tmp/x").isLeft shouldBe true
  }

  "InputToken.check" should "nhận token HDFS đúng cluster, cấp gần đây, không cảnh báo" in {
    InputToken.check(creds(fresh), inputPath, now).map(_._2) shouldBe Right(Nil)
  }

  it should "từ chối khi không có token cho cluster của INPUT_PATH, nêu service trong file và cách thêm alias" in {
    val result = InputToken.check(creds(hdfsToken("10.0.0.1:8020", now, now + hour)), inputPath, now)
    result.left.get should include(service)
    result.left.get should include("10.0.0.1:8020")
    result.left.get should include("dtutil edit")
  }

  it should "từ chối token đã quá hạn tối đa" in {
    InputToken.check(creds(hdfsToken(service, now - 200 * hour, now - 1)), inputPath, now).left.get should include("max lifetime")
  }

  it should "cảnh báo token cấp đã lâu (có thể đã hết hạn nếu chưa renew)" in {
    val old = InputToken.check(creds(hdfsToken(service, now - 21 * hour, now + 100 * hour)), inputPath, now)
    old.right.get._2.head should include("21 h ago")
    old.right.get._2.head should include("fetchdt --renew")
  }

  "InputToken.install" should "không làm gì khi không có INPUT_TOKEN_FILE" in {
    InputToken.install(Map.empty, inputPath, now) shouldBe Right(None)
  }

  it should "báo lỗi khi file không có hoặc không phải file token" in {
    InputToken.install(Map(InputToken.FileEnv -> "/nowhere/hadoop.token"), inputPath, now).left.get should include("no file")
    val junk = Files.createTempFile("junk", ".token")
    Files.write(junk, "not a token file".getBytes)
    InputToken.install(Map(InputToken.FileEnv -> junk.toString), inputPath, now).left.get should include("not a Hadoop token file")
  }

  it should "đọc file token và thêm token vào user hiện tại" in {
    val installed = InputToken.install(Map(InputToken.FileEnv -> file(creds(fresh)).toString), inputPath, now)

    installed.map(_.map(_.tokens.map(_.service))) shouldBe Right(Some(Seq(service)))
    UserGroupInformation.getCurrentUser.getCredentials.getToken(new Text(service)).getIdentifier shouldBe fresh.getIdentifier
  }

  "InputToken.describe" should "nêu owner, lúc cấp và hạn tối đa, không chứa mật khẩu" in {
    val line = InputToken.describe(InputToken.infos(creds(fresh)).head, now)
    line should include(service)
    line should include("owner reader")
    line should include("60 min ago")
    line should not include "password"
  }
}
