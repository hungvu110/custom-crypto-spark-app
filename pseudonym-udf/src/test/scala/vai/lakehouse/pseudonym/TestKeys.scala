package vai.lakehouse.pseudonym

import vai.lakehouse.pseudonym.core.{AesSivPseudonymizer, HmacPseudonymizer, KeyProvider, PseudonymKey}

/** Khoá cố định cho test: HMAC = byte 0x00..0x1f, AES-SIV = byte 0x00..0x3f (khớp golden vector tính bằng openssl). */
object TestKeys {

  def hexOfRange(n: Int): String = (0 until n).map(i => f"$i%02x").mkString

  val HmacEncoded: String   = PseudonymKey.HexPrefix + hexOfRange(HmacPseudonymizer.KeyBytes)
  val AesSivEncoded: String = PseudonymKey.HexPrefix + hexOfRange(AesSivPseudonymizer.KeyBytes)

  val hmac: PseudonymKey   = PseudonymKey.parse("pseudonym.hmac_v1", 1, HmacEncoded, HmacPseudonymizer.KeyBytes)
  val aesSiv: PseudonymKey = PseudonymKey.parse("pseudonym.aes_siv_v1", 1, AesSivEncoded, AesSivPseudonymizer.KeyBytes)

  val provider: KeyProvider = fixed(hmac, aesSiv)

  /** Khoá ngẫu nhiên khác hẳn khoá cố định — mô phỏng "sai khoá". */
  def randomProvider(): KeyProvider = fixed(
    PseudonymKey.parse("pseudonym.hmac_v1", 1, PseudonymKey.randomEncoded(HmacPseudonymizer.KeyBytes), HmacPseudonymizer.KeyBytes),
    PseudonymKey.parse("pseudonym.aes_siv_v1", 1, PseudonymKey.randomEncoded(AesSivPseudonymizer.KeyBytes), AesSivPseudonymizer.KeyBytes))

  def fixed(h: PseudonymKey, a: PseudonymKey): KeyProvider = new KeyProvider {
    override def hmacKey: PseudonymKey   = h
    override def aesSivKey: PseudonymKey = a
  }
}
