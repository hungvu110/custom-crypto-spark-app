package vai.lakehouse.columncrypto

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.types.StringType

/**
 * NGUỒN DUY NHẤT của công thức mã hoá cột. DataFrame API (`ColumnCrypto`) và SQL function
 * (`column_encrypt`/`column_decrypt`) đều dựng biểu thức qua đây nên hai đường luôn khớp nhau,
 * và dữ liệu ghi bằng đường này giải mã được bằng đường kia.
 *
 * AES-256-GCM qua `aes_encrypt`/`aes_decrypt` built-in của Spark (từ 3.3):
 *   key = unhex(sha2(keyPrefix || keyValue, 256))    -- SHA-256 luôn ra đúng 32 byte
 *   encrypt = base64(aes_encrypt(cast(v as string), key, 'GCM'))
 *   decrypt = decode(aes_decrypt(unbase64(v), key, 'GCM'), 'UTF-8')
 * IV do `aes_encrypt` tự sinh ngẫu nhiên và gắn vào đầu ciphertext.
 *
 * Biểu thức chỉ gồm hàm built-in nên executor không cần jar của lib. Hàm built-in được dựng qua
 * FunctionRegistry (đúng builder mà SQL parser dùng) để không lệ thuộc chữ ký constructor từng class.
 */
object CryptoExpressions {

  private val Mode    = "GCM"
  private val Charset = "UTF-8"
  private val ShaBits = 256

  private def call(name: String, args: Expression*): Expression =
    FunctionRegistry.builtin.lookupFunction(FunctionIdentifier(name), args)

  private def keyExpr(keyPrefix: String, keyValue: Expression): Expression =
    call("unhex", call("sha2", call("concat", Literal(keyPrefix), Cast(keyValue, StringType)), Literal(ShaBits)))

  def encrypt(value: Expression, keyValue: Expression, keyPrefix: String): Expression =
    call("base64", call("aes_encrypt", Cast(value, StringType), keyExpr(keyPrefix, keyValue), Literal(Mode)))

  def decrypt(value: Expression, keyValue: Expression, keyPrefix: String): Expression =
    call("decode",
      call("aes_decrypt", call("unbase64", value), keyExpr(keyPrefix, keyValue), Literal(Mode)),
      Literal(Charset))
}
