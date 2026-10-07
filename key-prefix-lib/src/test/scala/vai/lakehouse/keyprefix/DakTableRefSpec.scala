package vai.lakehouse.keyprefix

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DakTableRefSpec extends AnyFlatSpec with Matchers {

  "parse" should "tách database.table" in {
    DakTableRef.parse("demo_db.users_cdr") shouldBe DakTableRef("demo_db", "users_cdr")
  }

  it should "chuẩn hoá về chữ thường để cùng một bảng luôn ra cùng một key" in {
    DakTableRef.parse("Demo_DB.Users_CDR") shouldBe DakTableRef("demo_db", "users_cdr")
    DakTableRef.parse("Demo_DB.Users_CDR").qualified shouldBe "demo_db.users_cdr"
  }

  it should "nhận đúng giới hạn 128 ký tự mỗi phần" in {
    val part = "a" * 128
    DakTableRef.parse(s"$part.$part") shouldBe DakTableRef(part, part)
  }

  it should "từ chối mọi dạng khác 'database.table', kèm hướng dẫn cú pháp" in {
    Seq("users_cdr", "a.b.c", ".users", "demo_db.", "", "demo-db.x", "demo_db.x y", "../x", "a/b.c",
      s"${"a" * 129}.t", s"db.${"t" * 129}").foreach { bad =>
      val ex = intercept[IllegalArgumentException](DakTableRef.parse(bad))
      ex.getMessage should include("'<database>.<table>'")
    }
  }
}
