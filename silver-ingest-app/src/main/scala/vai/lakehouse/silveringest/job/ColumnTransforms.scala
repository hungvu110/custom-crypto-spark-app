package vai.lakehouse.silveringest.job

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{coalesce, col, lit, regexp_extract, regexp_replace, when}
import org.apache.spark.sql.types.StringType
import vai.lakehouse.silveringest.config.{ColumnTransform, TransformKind}

/**
 * Biến đổi cột, chạy SAU giải mã và TRƯỚC mã hoá, từng dòng một (không đếm, không đọc lại bảng); tên và vị trí cột giữ nguyên.
 *  - Gom nhóm theo yêu cầu của team DS (docs/silver-ingest/TRANSFORM-PHIEU-KHAI-THAC-VTT.md mục 3.4): giá trị gốc thay bằng nhãn khung (tiếng
 *    Anh), kiểu cột thành chuỗi; giá trị không hợp lệ (null, rỗng, chữ, số âm, ngoài khoảng) thành `Unknown`.
 *  - Chuẩn hoá số thuê bao (`ISDN_NATIONAL`): về 9 chữ số như VTT, để `isdn` của VTNET ra cùng bản mã (và cùng định danh giả) với VTT.
 */
object ColumnTransforms {

  val Unknown = "Unknown"

  // en-dash (U+2013) như trong phiếu của DS.
  private val Dash = "–"

  /** Tuổi: tối đa 3 chữ số (số dài hơn là tuổi vô lý, không cần ép kiểu). */
  private val AgeDigits = "^[0-9]{1,3}$"
  private val MaxAge = 120

  /** Giờ 0..23, một hoặc hai chữ số (`5` và `05` như nhau). */
  private val HourDigits = "^([01]?[0-9]|2[0-3])$"

  /** Bỏ mọi khoảng trắng hai đầu (`trim` của Spark chỉ bỏ dấu cách, không bỏ tab). */
  private def stripped(column: Column): Column = regexp_replace(column.cast(StringType), "^\\s+|\\s+$", "")

  /** `Under 18`, `18–24`, `25–34`, `35–44`, `45–54`, `55–64`, `65+`; ngoài ra (kể cả tuổi trên 120) `Unknown`. */
  def ageBand(column: Column): Column = {
    val text = stripped(column)
    val age = text.cast("int")
    when(!coalesce(text.rlike(AgeDigits), lit(false)), lit(Unknown))
      .when(age < 18, lit("Under 18"))
      .when(age <= 24, lit(s"18${Dash}24"))
      .when(age <= 34, lit(s"25${Dash}34"))
      .when(age <= 44, lit(s"35${Dash}44"))
      .when(age <= 54, lit(s"45${Dash}54"))
      .when(age <= 64, lit(s"55${Dash}64"))
      .when(age <= MaxAge, lit("65+"))
      .otherwise(lit(Unknown))
  }

  /** `0–5h`, `6–8h`, `9–11h`, `12–13h`, `14–16h`, `17–19h`, `20–23h`; ngoài ra `Unknown`. */
  def hourBand(column: Column): Column = {
    val text = stripped(column)
    val hour = text.cast("int")
    when(!coalesce(text.rlike(HourDigits), lit(false)), lit(Unknown))
      .when(hour <= 5, lit(s"0${Dash}5h"))
      .when(hour <= 8, lit(s"6${Dash}8h"))
      .when(hour <= 11, lit(s"9${Dash}11h"))
      .when(hour <= 13, lit(s"12${Dash}13h"))
      .when(hour <= 16, lit(s"14${Dash}16h"))
      .when(hour <= 19, lit(s"17${Dash}19h"))
      .otherwise(lit(s"20${Dash}23h"))
  }

  /** `84`/`+84`/`0` + 9 chữ số thành 9 chữ số cuối (định dạng `isdn` của VTT); giá trị khác (và NULL) giữ nguyên. */
  def isdnNational(column: Column): Column = {
    val text = stripped(column)
    when(coalesce(text.rlike(IsdnWithPrefix), lit(false)), regexp_extract(text, "([0-9]{9})$", 1)).otherwise(column.cast(StringType))
  }

  /** Số thuê bao có mã nước (`84`, `+84`) hoặc số 0 đầu, theo sau là đúng 9 chữ số. */
  private val IsdnWithPrefix = "^(\\+?84|0)[0-9]{9}$"

  /** Áp lần lượt các transform; cột tìm theo tên không phân biệt hoa thường, thiếu cột là lỗi nêu tên cột và các cột có trong dữ liệu. */
  def apply(df: DataFrame, transforms: Seq[ColumnTransform]): DataFrame =
    transforms.foldLeft(df) { (acc, transform) =>
      val name = acc.columns.find(_.equalsIgnoreCase(transform.column)).getOrElse {
        throw new IllegalArgumentException(
          s"COLUMN_TRANSFORMS: column '${transform.column}' is not a column of the data (columns: ${acc.columns.mkString(", ")})")
      }
      val column = col(s"`${name.replace("`", "``")}`")
      val banded = transform.kind match {
        case TransformKind.AgeBand => ageBand(column)
        case TransformKind.HourBand => hourBand(column)
        case TransformKind.IsdnNational => isdnNational(column)
      }
      acc.withColumn(name, banded)
    }
}
