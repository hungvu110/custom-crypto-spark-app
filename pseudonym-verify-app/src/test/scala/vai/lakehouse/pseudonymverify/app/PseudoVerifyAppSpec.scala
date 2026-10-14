package vai.lakehouse.pseudonymverify.app

import java.nio.file.Files

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `execute` là toàn bộ logic của `main` (trừ `sys.exit`), nên kiểm được exit code mà không thoát JVM test. */
class PseudoVerifyAppSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // App không hardcode master (spark-submit cấp) -> test cấp qua system property như `-Dspark.master=...`.
  private val sparkProps = Map("spark.master" -> "local[2]", "spark.ui.enabled" -> "false",
    "spark.sql.shuffle.partitions" -> "4", "spark.sql.warehouse.dir" -> Files.createTempDirectory("pv-wh").toString)

  override def beforeAll(): Unit = sparkProps.foreach { case (k, v) => System.setProperty(k, v) }

  override def afterAll(): Unit = sparkProps.keys.foreach(System.clearProperty)

  "PseudoVerifyApp.execute" should "trả exit code 2 khi cấu hình sai (không dựng Spark, không ném exception)" in {
    PseudoVerifyApp.execute(Map("RUN_MODE" -> "fast")) shouldBe 2
  }

  it should "trả exit code 0 khi chạy full trên dữ liệu nhỏ và kết quả PASS" in {
    val out = Files.createTempDirectory("pv-app").toUri.toString
    val env = Map("RUN_ID" -> "app-spec", "OUTPUT_PATH" -> out, "N_SUBJECTS" -> "200", "APP_EVENTS_PER_SUBJECT" -> "3",
      "BTS_CONN_PER_SUBJECT" -> "2", "LOYALTY_TXN_PER_SUBJECT" -> "2", "PARTITIONS" -> "2", "OFFER_SAMPLE" -> "20",
      "PSEUDO_MODES" -> "HASH", "PREVIEW_ROWS" -> "0")

    PseudoVerifyApp.execute(env) shouldBe 0
  }
}
