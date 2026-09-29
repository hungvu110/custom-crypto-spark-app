package vai.lakehouse.keyprefix

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrefixFilesSpec extends AnyFlatSpec with Matchers {

  private val DefaultPrefixDir = "conf/key-prefix"

  private def prefixDirWith(files: (String, String)*): Path = {
    val dir = Files.createTempDirectory("key-prefix-test")
    dir.toFile.deleteOnExit()
    files.foreach { case (name, content) =>
      val f = Files.write(dir.resolve(name), content.getBytes(StandardCharsets.UTF_8))
      f.toFile.deleteOnExit()
    }
    dir
  }

  "readPrefix" should "đọc keyPrefix từ file tên bảng trong thư mục" in {
    val dir = prefixDirWith("sample_table" -> "sub_rel")
    PrefixFiles.readPrefix(dir.toString, "sample_table") shouldBe "sub_rel"
  }

  it should "bỏ ký tự xuống dòng ở cuối (Secret tạo bằng --from-file thường có)" in {
    val dir = prefixDirWith("sample_table" -> "sub_rel\n", "other_table" -> "tot_charge\r\n")
    PrefixFiles.readPrefix(dir.toString, "sample_table") shouldBe "sub_rel"
    PrefixFiles.readPrefix(dir.toString, "other_table") shouldBe "tot_charge"
  }

  it should "chọn đúng file theo từng bảng" in {
    val dir = prefixDirWith("sample_table" -> "p1", "other_table" -> "p2")
    PrefixFiles.readPrefix(dir.toString, "other_table") shouldBe "p2"
  }

  it should "ném lỗi rõ ràng nếu keyPrefix rỗng" in {
    val dir = prefixDirWith("sample_table" -> "\n")
    val ex = intercept[IllegalArgumentException](PrefixFiles.readPrefix(dir.toString, "sample_table"))
    ex.getMessage should include("sample_table")
    ex.getMessage should include("empty")
  }

  it should "ném lỗi rõ ràng nếu không có file cho bảng đó" in {
    val dir = prefixDirWith("other_table" -> "p2")
    val ex = intercept[IllegalArgumentException](PrefixFiles.readPrefix(dir.toString, "sample_table"))
    ex.getMessage should include("not found")
    ex.getMessage should include("sample_table")
    ex.getMessage should not include "p2"
  }

  it should "từ chối tên bảng có thể thoát khỏi thư mục (path traversal)" in {
    val dir = prefixDirWith("sample_table" -> "p1")
    Seq("../sample_table", "a/b", "..", ".hidden", "", "sample table").foreach { bad =>
      intercept[IllegalArgumentException](PrefixFiles.readPrefix(dir.toString, bad))
    }
  }

  it should "đọc được từ classpath resource mặc định" in {
    PrefixFiles.readPrefix(DefaultPrefixDir, "sample_table") shouldBe "sample_table_dev_prefix"
  }
}
