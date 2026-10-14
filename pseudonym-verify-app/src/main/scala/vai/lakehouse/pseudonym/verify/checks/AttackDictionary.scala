package vai.lakehouse.pseudonym.verify.checks

import java.nio.charset.StandardCharsets.UTF_8

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
 * Ứng viên của kẻ tấn công KHÔNG có khoá (check P4): với mỗi định danh trong từ điển — giả lập việc liệt kê toàn bộ
 * không gian SĐT/IMEI/mã KH — tính mọi cách băm "phổ biến" (md5/sha1/sha256/sha512, `sha256("salt"‖v)`, HMAC với khoá
 * đoán) ở các dạng hex đầy đủ, hex 32 ký tự, base64, base64url. Output của lib khớp bất kỳ ứng viên nào = suy ngược được.
 */
private[verify] object AttackDictionary {

  /** Khoá đoán: rỗng (HMAC đệm khoá bằng 0 nên khoá 1 byte 0x00 ≡ khoá rỗng), và vài chuỗi hay gặp. */
  val GuessedHmacKeys: Seq[Array[Byte]] =
    Array[Byte](0) +: Seq("secret", "salt", "changeit", "pseudonym").map(_.getBytes(UTF_8))

  def candidates(dictionary: DataFrame): DataFrame = {
    val v = col("value")
    val digests: Seq[Column] = Seq(md5(v), sha1(v), sha2(v, 256), sha2(v, 512), sha2(concat(lit("salt"), v), 256)) ++
      GuessedHmacKeys.map(k => hmacHex(k)(v))
    dictionary.select(explode(array(digests.flatMap(forms): _*)).as("candidate")).distinct()
  }

  private def forms(digestHex: Column): Seq[Column] = Seq(
    digestHex,
    substring(digestHex, 1, 32),
    base64(unhex(digestHex)),
    base64Url(unhex(digestHex)),
    base64Url(unhex(substring(digestHex, 1, 32))))

  private def base64Url(bin: Column): Column = regexp_replace(translate(base64(bin), "+/", "-_"), "=", "")

  private def hmacHex(key: Array[Byte]) = udf { (value: String) =>
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key, "HmacSHA256"))
    mac.doFinal(value.getBytes(UTF_8)).map(b => f"${b & 0xff}%02x").mkString
  }
}
