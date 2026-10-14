package vai.lakehouse.pseudonymverify.app

import org.apache.spark.sql.SparkSession
import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonymverify.validate.{PseudonymValidator, SelfTest, ValidationReport}

import scala.util.Try

/**
 * CLI mỏng quanh [[PseudonymValidator]] — chạy bằng `spark-submit` (hoặc spark-shell) sau pseudonymize-table-app.
 *
 * Hai lệnh:
 *  - `SOURCE_TABLE TARGET_TABLE COLUMNS [MODE]` — so bảng nguồn với bảng đích (COLUMNS cách nhau dấu phẩy, MODE mặc định HASH).
 *  - `selftest [MODE] [ROWS]` — tự sinh dữ liệu có đáp án, giả danh bằng khoá tạm rồi kiểm (HASH hoặc ENCRYPT).
 * Exit code: 0 = PASS, 1 = FAIL, 2 = sai tham số / lỗi khi chạy.
 */
object PseudonymValidateApp {

  val Usage: String =
    "Usage: PseudonymValidateApp <source_db.table> <target_db.table> <col1,col2,...> [HASH|ENCRYPT|MAPPING]\n" +
      "       PseudonymValidateApp selftest [HASH|ENCRYPT] [rows]"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("PseudonymValidate").getOrCreate()
    try sys.exit(execute(spark, args)) finally spark.stop()
  }

  /** Không `sys.exit`, gọi được từ test. */
  def execute(spark: SparkSession, args: Array[String]): Int =
    if (args.headOption.contains("selftest")) selftest(spark, args.drop(1)) else compare(spark, args)

  private def selftest(spark: SparkSession, args: Array[String]): Int = {
    val mode = Try(PseudonymMode.parse(args.headOption.getOrElse("HASH")))
    val rows = Try(args.lift(1).map(_.toInt).getOrElse(2000))
    if (args.length > 2 || mode.isFailure || mode.get == PseudonymMode.Mapping || rows.isFailure || rows.get <= 0) {
      System.err.println(Usage)
      2
    } else report(SelfTest.run(spark, mode.get, rows.get, math.max(1, rows.get / 10)))
  }

  private def report(r: ValidationReport): Int = {
    println(r.render)
    if (r.passed) 0 else 1
  }

  private def compare(spark: SparkSession, args: Array[String]): Int = {
    val columns = if (args.length >= 3) args(2).split(",").map(_.trim).filter(_.nonEmpty).toSeq else Nil
    val mode = Try(PseudonymMode.parse(if (args.length == 4) args(3) else "HASH"))
    if (args.length < 3 || args.length > 4 || columns.isEmpty || mode.isFailure) {
      System.err.println(Usage)
      2
    } else {
      try {
        report(PseudonymValidator.validate(spark.table(args(0)), spark.table(args(1)), columns, mode.get))
      } catch {
        case e: Exception =>
          System.err.println(s"VALIDATE_FAILED: ${e.getClass.getSimpleName}: ${e.getMessage}")
          2
      }
    }
  }
}
