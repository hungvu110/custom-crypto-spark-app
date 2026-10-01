# Plan: UDF bọc thư viện mã hoá CDR của đối tác (Data Lake / `TransformDL`)

Kế hoạch triển khai đầy đủ cho việc tích hợp thuật toán mã hoá CDR do đối tác cung cấp
(`DataLakeSecurity_jv8.jar`, class `com.viettel.datalake.security.TransformDL`) vào sql-engine
và `spark-app`, dưới dạng **UDF** — bắt buộc phải đi hướng này vì phải *tuân theo đúng* thuật
toán của đối tác (yêu cầu từ đối tác), không được tái tạo lại bằng biểu thức built-in như
`column_encrypt`/`column_decrypt` hiện có.

Tài liệu liên quan: [COLUMN_CRYPTO_ARCHITECTURE.md](COLUMN_CRYPTO_ARCHITECTURE.md) (kiến trúc
built-in hiện tại và lý thuyết chung về UDF), [COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md](COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md)
(cấu hình Vault/token đang dùng, sẽ được **tái sử dụng** ở đây), [VDL - Mã hóa CDR.docx](VDL%20-%20Mã%20hóa%20CDR.docx)
(đặc tả gốc từ đối tác).

## 0. Sự thật đã xác nhận — căn cứ để làm plan này

Đã decompile `TransformDL.class` và chạy thử thật với test vector đối tác gửi (xem
`tools/datalake-security-test/EncryptDecryptTest.java`, PASS). Không có gì trong mục này là
suy đoán.

| Thành phần | Giá trị / hành vi thật |
|---|---|
| Class | `com.viettel.datalake.security.TransformDL` — chỉ có `setInputKey`, `decrypt(String)`, `decrypt(String,String)`. **Không có `encrypt`.** |
| Thuật toán | AES-128, `Cipher.getInstance("AES")` không chỉ định mode/padding → mặc định JDK là **AES/ECB/PKCS5Padding**. Không có IV (ECB không dùng IV) → **mã hoá tất định** (cùng plaintext + cùng key luôn ra đúng cùng ciphertext). |
| Sinh key | Key 16 byte = key mặc định hard-code trong class, **cộng (mod 256)** từng ký tự của `keyInput` vào các vị trí 0..15 theo kiểu tuần hoàn (Vigenère-style). **Không phải SHA-256** như `CryptoExpressions` hiện tại. |
| `keyInput` | = `prefix ‖ giá_trị_field` — **ghép chuỗi thô, không dấu phân cách, không băm**. `prefix` cố định theo loại CDR (đối tác cấp), `field` là giá trị 1 cột khác **trên cùng dòng CDR** (vd `start_datetime`, `province_code`) — đúng khái niệm `keyField` mà `column_encrypt`/`column_decrypt` hiện tại đang dùng. |
| Test vector đã xác nhận | `ciphertext=MmQtOaOWlG1rRkt49Gn6Bw==`, `prefix=231x#@Vie3`, `field=20260928123456`, `keyInput=231x#@Vie320260928123456`, `plaintext=978356099` |
| Charset | `getBytes()`/`new String()` dùng **default charset của JVM**, không tường minh UTF-8 — rủi ro nếu dữ liệu có ký tự ngoài ASCII |

## 1. Cấu trúc module

Thêm **module Maven thứ 3**, song song `column-crypto-lib` và `spark-app`, phụ thuộc
`column-crypto-lib` để tái dùng hạ tầng lấy `keyPrefix` (Vault token/Kubernetes auth) đã có sẵn:

```
sample-spark-application-privacy-parent (pom.xml gốc)
├── column-crypto-lib/      # biểu thức built-in — GIỮ NGUYÊN, không đụng vào
├── spark-app/               # app mẫu — GIỮ NGUYÊN
└── cdr-crypto-udf/      # MODULE MỚI
    └── src/main/scala/vai/lakehouse/columncrypto/cdr/
        ├── CdrCipherCore.scala     # gọi decrypt() thật + tự viết encrypt(), không phụ thuộc Spark
        └── CdrCryptoExtension.scala # đăng ký cdr_encrypt/cdr_decrypt qua SparkSessionExtensions
```

Vì sao tách module riêng thay vì thêm vào `column-crypto-lib`:
- `column-crypto-lib` là jar **thuần**, không phụ thuộc gì ngoài Spark và `key-prefix-lib` (`provided`). Module
  mới phải phụ thuộc `DataLakeSecurity_jv8.jar` — một jar không nằm trên Maven Central, không
  do mình kiểm soát vòng đời. Tách riêng để lỗi/thay đổi phía đối tác không ảnh hưởng
  `column_encrypt`/`column_decrypt` đang chạy ổn định.
- `DataLakeSecurity_jv8.jar` bị `.gitignore` (không commit vào repo — là tài sản của đối tác).
  Nếu gộp chung module, `mvn clean package` ở gốc sẽ **fail với bất kỳ ai chưa cài jar này** —
  xem mục 2 cách xử lý bằng Maven profile riêng.

### 1.1. Đăng ký `DataLakeSecurity_jv8.jar` vào local Maven repo

```bash
mvn install:install-file \
  -Dfile=DataLakeSecurity_jv8.jar \
  -DgroupId=com.viettel.datalake -DartifactId=datalake-security -Dversion=jv8 \
  -Dpackaging=jar
```

`pom.xml` của `cdr-crypto-udf` khai phụ thuộc này ở scope `provided` (không đóng gói jar của
đối tác vào jar của mình — họ cấp cho mình dạng file riêng, việc phân phối lại vào hạ tầng
(image/HDFS) là quyết định vận hành cần xin phép/thoả thuận riêng, không phải việc của Maven
build):

```xml
<dependency>
  <groupId>com.viettel.datalake</groupId>
  <artifactId>datalake-security</artifactId>
  <version>jv8</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>vai.lakehouse</groupId>
  <artifactId>column-crypto-lib</artifactId>
  <version>${project.version}</version>
</dependency>
```

### 1.2. Maven profile để build chính không phụ thuộc jar đối tác

`mvn clean package` ở gốc **không được fail** với người chưa cài `DataLakeSecurity_jv8.jar`.
Thêm profile không kích hoạt mặc định trong `pom.xml` gốc:

```xml
<profiles>
  <profile>
    <id>cdr-crypto</id>
    <modules>
      <module>cdr-crypto-udf</module>
    </modules>
  </profile>
</profiles>
```

Build bình thường: `mvn clean package` — không đụng `cdr-crypto-udf`.
Build kèm module đối tác (sau khi đã `install:install-file` ở mục 1.1):
`mvn -Pcdr-crypto -pl cdr-crypto-udf -am clean package`.

## 2. `CdrCipherCore` — lõi crypto, không phụ thuộc Spark

Tách riêng khỏi Spark extension để test độc lập bằng JVM thuần (giống
`tools/datalake-security-test/EncryptDecryptTest.java` đã làm), và để có thể tái dùng nếu sau
này cần gọi từ DataFrame API.

```scala
package vai.lakehouse.columncrypto.cdr

import com.viettel.datalake.security.TransformDL
import java.lang.reflect.{Field, Method}
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

/**
 * Bọc TransformDL (jar đối tác, KHÔNG commit vào repo — xem .gitignore) thành 2 chiều:
 *   - decrypt: gọi THẲNG method public thật của đối tác, không viết lại logic.
 *   - encrypt: đối tác không cung cấp — tự viết, TÁI DÙNG chính xác phần sinh key của họ qua
 *     reflection (method private "a", field "key"/"algorithm") thay vì gõ lại thuật toán cộng
 *     dồn bằng tay, để loại trừ rủi ro chép sai. Chỉ tự viết phần đảo chiều Cipher
 *     (ENCRYPT_MODE thay DECRYPT_MODE). Đã xác nhận đúng byte-for-byte với test vector đối tác.
 *
 * keyInput = prefix + fieldValue, ghép thẳng — xem mục 0 của
 * docs/PARTNER_CDR_CRYPTO_UDF_PLAN.md. Không tự thêm dấu phân cách nào.
 */
object CdrCipherCore {

  def decrypt(ciphertextB64: String, keyInput: String): String =
    try new TransformDL().decrypt(ciphertextB64, keyInput)
    catch { case e: Exception => throw new IllegalStateException("CDR partner decrypt failed", e) }

  def encrypt(plaintext: String, keyInput: String): String = {
    val lib = new TransformDL()
    lib.setInputKey(keyInput)

    val keyField = classOf[TransformDL].getDeclaredField("key")
    keyField.setAccessible(true)
    val defaultKey = keyField.get(lib).asInstanceOf[Array[Byte]]

    val algorithmField = classOf[TransformDL].getDeclaredField("algorithm")
    algorithmField.setAccessible(true)
    val algorithm = algorithmField.get(lib).asInstanceOf[String]

    val deriveKey: Method = classOf[TransformDL].getDeclaredMethod("a", classOf[Array[Byte]], classOf[String])
    deriveKey.setAccessible(true)
    val secretKey = deriveKey.invoke(lib, defaultKey, algorithm).asInstanceOf[SecretKeySpec]

    val cipher = Cipher.getInstance(algorithm) // "AES" -> mặc định AES/ECB/PKCS5Padding
    cipher.init(Cipher.ENCRYPT_MODE, secretKey)
    Base64.getEncoder.encodeToString(cipher.doFinal(plaintext.getBytes))
  }

  /**
   * Tự-kiểm tra bằng test vector đã xác nhận với đối tác, chạy 1 lần lúc extension khởi tạo.
   * Nếu đối tác đổi version jar và hành vi lệch đi (vd đổi tên method private "a", đổi thuật
   * toán), lỗi sẽ hiện NGAY lúc khởi động thay vì âm thầm mã hoá/giải mã sai trong production.
   */
  def verifyCompatibility(): Unit = {
    val ciphertext = "MmQtOaOWlG1rRkt49Gn6Bw=="
    val keyInput = "231x#@Vie3" + "20260928123456"
    val expected = "978356099"
    val actualDecrypt = decrypt(ciphertext, keyInput)
    require(actualDecrypt == expected,
      s"CdrCipherCore self-check FAILED (decrypt): expected '$expected', got '$actualDecrypt'. " +
        "Jar đối tác có thể đã đổi version/hành vi — KHÔNG dùng cdr_encrypt/cdr_decrypt cho tới khi rà lại.")
    val actualEncrypt = encrypt(expected, keyInput)
    require(actualEncrypt == ciphertext,
      s"CdrCipherCore self-check FAILED (encrypt): expected '$ciphertext', got '$actualEncrypt'. " +
        "Logic sinh key qua reflection có thể không còn khớp jar đối tác.")
  }
}
```

## 3. `CdrCryptoExtension` — đăng ký SQL function

**Không đụng vào `column_encrypt`/`column_decrypt`.** Đăng ký hai hàm mới, tên riêng để tránh
va chạm: **`cdr_encrypt`**, **`cdr_decrypt`**.

Thiết kế chữ ký hàm **giữ nguyên UX** với `column_encrypt`/`column_decrypt` hiện có —
`cdr_decrypt(datasetName, value, fieldValue)` — để tái dùng **toàn bộ** hạ tầng lấy `keyPrefix`
đã xây (`PrefixSourceFactory`, `CachedPrefixSource`, `VaultPrefixSource` với cả 2 chế độ
`token`/`kubernetes`), không phải viết lại gì ở tầng Vault:

```scala
package vai.lakehouse.columncrypto.cdr

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal, ScalaUDF}
import org.apache.spark.sql.types.StringType
import vai.lakehouse.keyprefix.{ChainedConfigSource, EnvConfigSource, PrefixSource, PrefixSourceFactory, SparkConfSource}

/**
 * Đăng ký cdr_encrypt(table, value, fieldValue) / cdr_decrypt(table, value, fieldValue), bọc
 * CdrCipherCore. KHÁC ColumnCryptoExtension ở chỗ: builder trả về ScalaUDF (chạy code JVM
 * thật trên executor) thay vì cây biểu thức built-in — vì thuật toán của đối tác (vòng lặp cộng
 * dồn key + AES/ECB) không biểu diễn được bằng hàm built-in của Spark. Do đó:
 *   - Executor BẮT BUỘC có DataLakeSecurity_jv8.jar + key-prefix-lib.jar + cdr-crypto-udf.jar
 *     trên classpath (spark.jars) — khác hẳn cdr_encrypt/cdr_decrypt built-in hiện tại.
 *   - keyPrefix nằm trong CLOSURE của UDF (không phải Literal trong plan) — không cần
 *     redactKeyPrefixInPlans để che khỏi EXPLAIN, nhưng vẫn tồn tại trong bộ nhớ/task executor.
 *
 * Cấu hình Vault RIÊNG, namespace `spark.cdrcrypto.*` (KHÔNG dùng chung `spark.columncrypto.*`
 * của column_encrypt) — vì đối tác cấp prefix qua kênh/đường dẫn Vault khác với keyPrefix tự
 * quản của mình (docs/VDL - Mã hóa CDR.docx: "prefix sẽ được gửi riêng cho từng đơn vị"). Hai
 * loại prefix có thể cùng tồn tại trên 1 engine mà không đụng nhau.
 */
class CdrCryptoExtension(prefixSource: () => PrefixSource) extends (SparkSessionExtensions => Unit) {

  def this() = this(() => CdrCryptoExtension.sharedPrefixSource)

  import CdrCryptoExtension._

  private lazy val source: PrefixSource = prefixSource()

  private def register(ext: SparkSessionExtensions, name: String, usage: String, encryptMode: Boolean): Unit =
    ext.injectFunction((
      FunctionIdentifier(name),
      new ExpressionInfo(getClass.getName, null, name, usage, ""),
      (args: Seq[Expression]) => args match {
        case Seq(table, value, fieldValue) =>
          val prefix = source.read(datasetOf(name, table))
          val udfFn = (v: String, f: String) => {
            val keyInput = prefix + f
            if (encryptMode) CdrCipherCore.encrypt(v, keyInput) else CdrCipherCore.decrypt(v, keyInput)
          }
          ScalaUDF(udfFn, StringType, Seq(value, org.apache.spark.sql.catalyst.expressions.Cast(fieldValue, StringType)),
            udfDeterministic = true) // AES/ECB không IV -> tất định thật sự, không như GCM
        case other =>
          throw new IllegalArgumentException(s"$name expects 3 arguments (table, value, fieldValue) but got ${other.size}")
      }))

  override def apply(ext: SparkSessionExtensions): Unit = {
    CdrCipherCore.verifyCompatibility() // fail-fast nếu jar đối tác lệch hành vi
    register(ext, EncryptFunction, s"$EncryptFunction(table, value, fieldValue) - encrypts using the partner CDR algorithm.", encryptMode = true)
    register(ext, DecryptFunction, s"$DecryptFunction(table, value, fieldValue) - decrypts a value produced by the partner CDR algorithm.", encryptMode = false)
  }
}

object CdrCryptoExtension {
  val EncryptFunction = "cdr_encrypt"
  val DecryptFunction = "cdr_decrypt"

  private lazy val sharedPrefixSource: PrefixSource =
    PrefixSourceFactory.create(new SparkConfSource(SparkEnv.get.conf) {
      override def describe(key: String): String = s"spark.cdrcrypto.$key"
      override def get(key: String): Option[String] =
        SparkEnv.get.conf.getOption(describe(key)).map(_.trim).filter(_.nonEmpty)
    })

  private def datasetOf(function: String, arg: Expression): String = arg match {
    case Literal(v, StringType) if v != null => v.toString
    case _ => throw new IllegalArgumentException(
      s"$function: the first argument must be a constant string with the table/dataset name")
  }
}
```

Ghi chú kỹ thuật quan trọng: `SparkConfSource` hiện tại có `Prefix` cố định `"spark.columncrypto."`
(constant trong companion object). Cần refactor nhỏ ở `column-crypto-lib` để `SparkConfSource`
nhận `prefix` qua constructor thay vì hardcode, để `CdrCryptoExtension` tái dùng được với
namespace khác (`spark.cdrcrypto.`) mà không copy-paste lại toàn bộ `PrefixSourceFactory`.
Đây là thay đổi **tương thích ngược** (thêm tham số có giá trị mặc định), không ảnh hưởng
`column_encrypt`/`column_decrypt` đang chạy.

## 4. Test

Module `cdr-crypto-udf` có `src/test/scala`, viết theo đúng pattern `ColumnCryptoExtensionSpec`
đã có, **cộng thêm** phần đặc thù đối tác:

| Test | Nội dung |
|---|---|
| `CdrCipherCoreSpec` | `decrypt()` đọc đúng ciphertext mẫu; `encrypt()` ra đúng byte-for-byte ciphertext mẫu; round-trip 2 chiều; field rỗng (`nvl`); `verifyCompatibility()` pass |
| `CdrCryptoExtensionSpec` | `SparkSession.builder().withExtensions(new CdrCryptoExtension(() => stub))`; roundtrip qua SQL (`cdr_encrypt` rồi `cdr_decrypt`); `cdr_encrypt(...)` ra ciphertext mà `CdrCipherCore.decrypt` thật đọc được (cross-check với logic KHÔNG qua SQL); `DESCRIBE FUNCTION cdr_decrypt` |

Test chỉ chạy được khi đã `mvn install:install-file` jar đối tác cục bộ (mục 1.1) — ghi rõ
prerequisite này trong README của module, và trong CI (nếu có) đặt job build module này ở một
stage riêng, không chặn build chính nếu thiếu jar.

## 5. Đóng gói và triển khai

### 5.1. Build

```bash
mvn install:install-file -Dfile=DataLakeSecurity_jv8.jar \
  -DgroupId=com.viettel.datalake -DartifactId=datalake-security -Dversion=jv8 -Dpackaging=jar
mvn -pl cdr-crypto-udf -am clean package
# -> cdr-crypto-udf/target/cdr-crypto-udf-1.0-SNAPSHOT.jar
```

### 5.2. Nạp lên sql-engine — 3 jar, không phải 1

```
spark.jars = <path>/DataLakeSecurity_jv8.jar,<path>/key-prefix-lib-1.0-SNAPSHOT.jar,<path>/cdr-crypto-udf-1.0-SNAPSHOT.jar
spark.sql.extensions = <extension đang có>,vai.lakehouse.columncrypto.cdr.CdrCryptoExtension
```

`DataLakeSecurity_jv8.jar` cần được đặt cùng chỗ (HDFS/registry nội bộ) với `key-prefix-lib.jar`
— dùng lại đúng hạ tầng `hdfs://` đã dùng cho `column-crypto-lib` (xem
`COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md` mục 3, đã bổ sung phần nạp `cdr-crypto-udf`). **Cần xác nhận với đối tác/team pháp lý** việc đặt
jar của họ lên hạ tầng nội bộ có nằm trong phạm vi họ đã cấp phép hay không, trước khi làm việc này.

### 5.3. Cấu hình Vault cho `cdr_encrypt`/`cdr_decrypt`

Namespace **riêng**, độc lập với `spark.columncrypto.*`:

```
spark.cdrcrypto.source           = vault
spark.cdrcrypto.vault.addr       = <địa chỉ Vault chứa prefix CDR — có thể khác/giống Vault hiện tại>
spark.cdrcrypto.vault.authMethod = token
spark.cdrcrypto.vault.token      = <token riêng, chỉ đọc>
spark.cdrcrypto.vault.kvMount    = kv
spark.cdrcrypto.vault.kvBasePath = <path đối tác cấp, theo từng loại CDR>
spark.cdrcrypto.vault.keyField   = keyPrefix
spark.cdrcrypto.cacheTtlSeconds  = 300
```

## 6. Hướng dẫn sử dụng trên query console

```sql
DESCRIBE FUNCTION cdr_decrypt;   -- kiểm tra extension đã nạp

-- Giải mã CDR đối tác gửi, ví dụ dataset "sub_rel_product": key = 'sub_rel' + start_datetime
SELECT sub_id,
       cdr_decrypt('sub_rel_product', isdn, start_datetime) AS isdn
FROM cdr.sub_rel_product;

-- Mã hoá dữ liệu để gửi CHO đối tác (họ giải mã bằng chính TransformDL của họ)
SELECT sub_id,
       cdr_encrypt('sub_rel_product', isdn, start_datetime) AS isdn_encrypted
FROM staging.sub_rel_product;
```

Tham số thứ 3 (`start_datetime`, `province_code`, ...) là **field khác trên cùng dòng CDR**,
khác nhau theo từng loại CDR — đúng bảng ví dụ trong docx (mục 0). Phải hỏi lại đối tác field
nào áp dụng cho từng loại CDR nếu chưa có đủ danh sách.

## 7. So sánh với cách tiếp cận built-in (`column_encrypt`/`column_decrypt`)

| Tiêu chí | `column_encrypt`/`column_decrypt` (built-in, hiện có) | `cdr_encrypt`/`cdr_decrypt` (UDF, plan này) |
|---|---|---|
| Ai thực thi mã hoá/giải mã | `aes_encrypt`/`aes_decrypt` built-in của Spark | Code JVM thật — `TransformDL.decrypt()` (đối tác) và `CdrCipherCore.encrypt()` (tự viết) |
| Vì sao chọn cách này | Không có ràng buộc định dạng từ bên ngoài | **Bắt buộc** — phải khớp byte-for-byte với thuật toán đối tác, không thể tái tạo bằng built-in (vòng lặp cộng dồn key có độ dài động) |
| Executor cần jar nào | Không cần jar nào | **3 jar**: `DataLakeSecurity_jv8.jar`, `key-prefix-lib.jar`, `cdr-crypto-udf.jar` — qua `spark.jars` |
| Thuật toán mã hoá | AES-**256**-GCM (có xác thực, IV ngẫu nhiên) | AES-**128**-ECB/PKCS5 (không xác thực, không IV, **tất định**) |
| Sinh key | `SHA-256(prefix ‖ keyField)` | Cộng ký tự tuần hoàn (Vigenère-style), **không băm** |
| Catalyst tối ưu/codegen | Có — nút built-in tự sinh code | Không — `ScalaUDF` là hộp đen |
| Hiệu năng | Ngang hàm built-in khác | Chậm hơn (chuyển đổi kiểu dữ liệu qua lại, chưa benchmark) |
| `keyPrefix` trong plan | `Literal` — cần `redactKeyPrefixInPlans` để che khỏi `EXPLAIN`/Spark UI | Nằm trong **closure** của UDF — không hiện trong `EXPLAIN` mặc định, nhưng vẫn tồn tại trong bộ nhớ/task |
| Cấu hình Vault | `spark.columncrypto.*` | `spark.cdrcrypto.*` — **namespace riêng**, dùng chung hạ tầng `PrefixSourceFactory` nhưng độc lập, 2 loại prefix chạy song song không đụng nhau |
| Nguồn công thức | Một nguồn duy nhất (`CryptoExpressions`), dữ liệu ghi/đọc qua cả DataFrame API lẫn SQL luôn khớp | Công thức do **đối tác quyết định**, mình chỉ được bọc lại — không được tự sửa |
| Rủi ro đặc thù | Thấp — built-in đã kiểm chứng | Reflection vào **method private** của jar đối tác (`a`) — dễ vỡ nếu đối tác đổi version; đã có `verifyCompatibility()` để fail-fast |
| Bảo mật thuật toán bản thân | AES-256-GCM là chuẩn hiện đại | AES-128-ECB + KDF cộng dồn + key mặc định hard-code — **yếu về mặt mật mã học**, nhưng không phải quyết định của mình (đối tác đặt ra) |

## 8. Rủi ro và việc cần làm trước khi lên production

| Việc | Vì sao |
|---|---|
| Xác nhận **charset** đối tác dùng | `TransformDL` dùng default charset JVM cho `getBytes()`/`new String()`, không tường minh UTF-8. Nếu dữ liệu CDR có ký tự ngoài ASCII, kết quả có thể khác giữa các JVM |
| Xác nhận **danh sách field/prefix đầy đủ** theo từng loại CDR | docx chỉ cho 2 ví dụ (`sub_rel_product`, `tot_charge_mon`); production cần đủ mapping cho mọi loại CDR sẽ dùng |
| Xin phép phân phối `DataLakeSecurity_jv8.jar` lên hạ tầng nội bộ (HDFS/registry/image) | Đây là tài sản đối tác, cần rõ ràng về quyền sử dụng/phân phối trước khi đặt vào hạ tầng dùng chung |
| Đối tác tự chạy thử `cdr_encrypt(...)` do mình sinh ra | Round-trip nội bộ (mục 4) chứng minh tính đúng đắn thuật toán, nhưng xác nhận chéo từ hệ thống thật của đối tác là bước nghiệm thu cuối cùng |
| Theo dõi thay đổi version jar đối tác | Reflection vào method private dễ vỡ khi đối tác đổi nội bộ class; `verifyCompatibility()` giúp phát hiện sớm nhưng không ngăn được — cần quy trình báo trước khi đối tác đổi version |
| Benchmark hiệu năng UDF trên dữ liệu CDR thật | Chưa có số liệu; khối lượng CDR thường rất lớn, cần biết mức chênh so với built-in trước khi áp dụng diện rộng |

## 9. Thứ tự triển khai

1. `mvn install:install-file` jar đối tác (mục 1.1) trên máy dev.
2. Thêm module `cdr-crypto-udf` vào `pom.xml` gốc (mục 1).
3. Refactor `SparkConfSource` nhận `prefix` qua constructor (mục 3, ghi chú kỹ thuật) — thay đổi tương thích ngược trong `column-crypto-lib`.
4. Viết `CdrCipherCore` + test JVM thuần, đối chiếu với `tools/datalake-security-test/EncryptDecryptTest.java` (mục 2, 4).
5. Viết `CdrCryptoExtension` + test SQL (mục 3, 4).
6. `mvn -pl cdr-crypto-udf -am clean package`, kiểm tra jar sinh ra.
7. Xin xác nhận đối tác các điểm ở mục 8 trước khi đưa lên môi trường thật.
8. Cấu hình `spark.jars` + `spark.sql.extensions` + `spark.cdrcrypto.*` trên sql-engine thử nghiệm (mục 5).
9. Test qua query console (mục 6), đối chiếu cả hai chiều với hệ thống thật của đối tác.
10. Viết hướng dẫn vận hành/xử lý sự cố riêng cho `cdr_encrypt`/`cdr_decrypt` (đã có ở mục 6.2 và 9 của `COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md`) sau khi đã chạy ổn định.

## Cập nhật kiến trúc: hạ tầng lấy prefix tách thành `key-prefix-lib`

Ghi chú bổ sung sau khi triển khai: phần lấy keyPrefix (`PrefixSource`, `VaultPrefixSource`, `PrefixSourceFactory`,
`EnvConfigSource`, ...) đã được tách khỏi `column-crypto-lib` thành module riêng `key-prefix-lib` (package
`vai.lakehouse.keyprefix`). `cdr-crypto-udf` chỉ phụ thuộc `key-prefix-lib`, **không** phụ thuộc `column-crypto-lib` — nên phần
"3 jar" ở mục 5.2 là `DataLakeSecurity_jv8.jar` + `key-prefix-lib.jar` + `cdr-crypto-udf.jar`. `CdrCryptoExtension` đọc cấu hình
theo thứ tự `spark.cdrcrypto.*` rồi tới biến môi trường chung (`VAULT_*`, `CRYPTO_PREFIX_SOURCE`, ...). Image `cdr-crypto`
(Dockerfile target `cdr-crypto`) và manifest `k8s/spark-application-cdr.yaml` đóng gói/nạp đúng 3 jar này.

## Cập nhật: hỗ trợ cả DataFrame API

Ngoài SQL function `cdr_encrypt`/`cdr_decrypt`, module có API DataFrame `CdrCrypto` (đối ứng `ColumnCrypto` của `column-crypto-lib`):
`CdrCrypto.encryptColumns(df, datasetName, keyField, columns)` / `decryptColumns(...)` (hoặc dựng `CdrCryptoConfig` qua
`CdrCrypto.loadConfig`). Cả 2 đường dùng chung `CdrCrypto.transform` nên cho kết quả giống hệt nhau (có test đối chiếu
byte-for-byte với SQL và với `CdrCipherCore`). Vì là UDF thật nên executor vẫn cần `DataLakeSecurity_jv8.jar` + `cdr-crypto-udf.jar`.
`spark-app` KHÔNG dùng API này: nó chỉ gọi SQL function `cdr_encrypt`/`cdr_decrypt` theo tên (`CRYPTO_ENCRYPT_FUNCTION`), nên không phụ
thuộc `cdr-crypto-udf` lúc biên dịch và build mặc định không cần jar đối tác. DataFrame API dành cho code khác dùng khi cần.
