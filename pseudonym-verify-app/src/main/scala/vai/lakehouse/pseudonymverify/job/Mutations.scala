package vai.lakehouse.pseudonymverify.job

import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonym.core.HmacPseudonymizer
import vai.lakehouse.pseudonym.policy.TablePolicy
import vai.lakehouse.pseudonym.spark.{MappingTable, Pseudonymizer}
import vai.lakehouse.pseudonymverify.key.KeyMaterial
import vai.lakehouse.pseudonymverify.logging.Log

import java.nio.charset.StandardCharsets.UTF_8
import java.security.SecureRandom
import java.util.Base64

import javax.crypto.Cipher
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * Bản giả danh CỐ Ý SAI để kiểm chứng chính bộ check (docs/pseudonymization/VERIFY_APP_PLAN.md mục 6). Bộ check chỉ đáng tin khi
 * mọi mutation bị bắt: verdict ≠ PASS và các check trong `expected` nằm trong danh sách fail.
 */
final case class Mutation(id: String, mode: PseudonymMode, description: String, expected: Seq[String],
                          strategy: KeyMaterial => Strategy)

object Mutations {

  val DebugKeyConf = "spark.pseudonym.debug.hmacKey"

  val all: Seq[Mutation] = Seq(
    Mutation("M1", PseudonymMode.Hash, "Không làm gì (trả nguyên giá trị)", Seq("U3", "P2"),
      _ => perColumn("M1")(identity)),

    Mutation("M2", PseudonymMode.Hash, "sha2(value) không khoá, giữ đúng định dạng k1_ + 32 hex", Seq("P4"),
      _ => perColumn("M2")(v => concat(lit("k1_"), substring(sha2(v, 256), 1, 32)))),

    Mutation("M3", PseudonymMode.Hash, "Định danh giả ngẫu nhiên (UUID) mỗi dòng", Seq("U4", "U6", "U8"),
      _ => perColumn("M3")(_ => concat(lit("k1_"), regexp_replace(expr("uuid()"), "-", "")))),

    Mutation("M4", PseudonymMode.Hash, "Chỉ dùng 2 byte HMAC (lặp lại cho đủ 32 hex)", Seq("U5"), keys => {
      val h = new HmacPseudonymizer(keys.provider.hmacKey)
      val short = udf((v: String) => "k1_" + h.pseudonymize(v).substring(3, 7) * 8)
      perColumn("M4")(short(_))
    }),

    Mutation("M5", PseudonymMode.Hash, "Đưa khoá vào Spark conf và log", Seq("P7"), keys => {
      val secret = keys.secretStrings(PseudonymMode.Hash).head
      Strategy("M5", new Pseudonymizer(keys.provider).pseudonymize,
        prepare = s => { s.conf.set(DebugKeyConf, secret); Log.info(s"debug: hmac key = $secret") },
        cleanup = s => s.conf.unset(DebugKeyConf))
    }),

    Mutation("M6", PseudonymMode.Hash, "Khoá sinh lại mỗi lần chạy", Seq("U8"),
      _ => Strategy("M6", (df, policy) => new Pseudonymizer(KeyMaterial.random()).pseudonymize(df, policy))),

    Mutation("M7", PseudonymMode.Hash, "Thêm pseudonym_id nhưng quên xoá cột customer_id gốc", Seq("P1", "P2"), keys => {
      val h = new HmacPseudonymizer(keys.provider.hmacKey)
      val hash = udf((v: String) => h.pseudonymize(v))
      perColumn("M7", keepOriginal = true)(hash(_))
    }),

    Mutation("M8", PseudonymMode.Encrypt, "AES-GCM với IV ngẫu nhiên (như aes_encrypt mặc định của Spark)", Seq("U4", "U6"),
      keys => {
        val key = keys.provider.aesSivKey.exportBytes().take(32)
        val gcm = udf((v: String) => "e1_" + gcmRandomIv(key, v))
        perColumn("M8")(gcm(_))
      }),

    Mutation("M9", PseudonymMode.Mapping, "Không ghi bảng ánh xạ — token sinh lại mỗi lần chạy", Seq("U8"),
      _ => Strategy("M9", unpersistedMapping)),

    Mutation("M10", PseudonymMode.Mapping, "Token = sha256(raw_value) thay vì ngẫu nhiên", Seq("P4"),
      _ => perColumn("M10")(v => concat(lit(MappingTable.TokenPrefix), substring(sha2(v, 256), 1, 32))))
  )

  /**
   * Áp `f` lên từng cột trong policy theo đúng ngữ nghĩa của lib (thay tại chỗ, đổi tên theo `output`, null -> null);
   * `keepOriginal` giữ lại cột gốc khi đổi tên (lỗi của M7).
   */
  private def perColumn(name: String, keepOriginal: Boolean = false)(f: Column => Column): Strategy =
    Strategy(name, (df, policy) => df.select(df.columns.toSeq.flatMap { c =>
      val source = df.col(s"`$c`")
      policy.ruleFor(c) match {
        case Some(rule) =>
          val v = source.cast(StringType)
          val pseudo = when(v.isNull, lit(null).cast(StringType)).otherwise(f(v)).as(rule.outputName)
          if (keepOriginal && rule.outputName != c) Seq(source, pseudo) else Seq(pseudo)
        case None => Seq(source)
      }
    }: _*))

  private val random = new SecureRandom()

  private def gcmRandomIv(key: Array[Byte], value: String): String = {
    val iv = new Array[Byte](12)
    random.nextBytes(iv)
    val cipher = Cipher.getInstance("AES/GCM/NoPadding")
    cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, iv))
    Base64.getUrlEncoder.withoutPadding().encodeToString(iv ++ cipher.doFinal(value.getBytes(UTF_8)))
  }

  /** Token sinh mới cho mỗi lần gọi, không lưu bảng ánh xạ (chốt bằng localCheckpoint để trong 1 lần chạy vẫn nhất quán). */
  private def unpersistedMapping(df: DataFrame, policy: TablePolicy): DataFrame =
    policy.columns.foldLeft(df) { (acc, rule) =>
      val tokens = acc.select(acc.col(s"`${rule.column}`").cast(StringType).as("__raw")).where(col("__raw").isNotNull)
        .distinct().withColumn("__token", MappingTable.newTokenUdf()).localCheckpoint()
      val joined = acc.withColumn("__raw", acc.col(s"`${rule.column}`").cast(StringType)).join(tokens, Seq("__raw"), "left")
      joined.select(acc.columns.map(c => if (c == rule.column) col("__token").as(rule.outputName) else joined.col(s"`$c`")): _*)
    }
}
