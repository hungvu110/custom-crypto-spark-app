# Kiến trúc `column-crypto-lib`: biểu thức built-in hiện tại và phương án UDF

Tài liệu này tổng hợp cách `column-crypto-lib` mã hoá/giải mã cột đang được triển khai (biểu
thức Catalyst dựng từ hàm built-in của Spark), kiến trúc sẽ ra sao nếu chuyển sang UDF, và so
sánh chi tiết hai hướng để làm căn cứ quyết định khi cần đổi thuật toán hoặc tích hợp thuật toán
của bên thứ ba (ví dụ nguồn dữ liệu gửi kèm một jar `decrypt` riêng).

Đây là tài liệu **kiến trúc/thiết kế**. Hướng dẫn cấu hình Vault, cú pháp SQL và xử lý sự cố nằm
ở [COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md](COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md).

## 1. Kiến trúc hiện tại: biểu thức Catalyst built-in

### 1.1. Catalyst là gì, "biểu thức" nằm ở đâu

Catalyst là khung biên dịch câu SQL của Spark, gồm nhiều giai đoạn xử lý trên **cây**:

```
SQL text
  │ Parser           → cây "chưa resolve" (tên hàm/cột còn là chữ)
  │ Analyzer         → resolve tên hàm/cột, kiểm tra kiểu   ◄── ColumnCryptoExtension chen vào đây
  │ Optimizer        → viết lại cây cho rẻ hơn
  │ Physical planner → chọn cách thực thi (join nào, scan nào)
  │ Codegen          → sinh code Java chạy trên executor
  ▼
Task chạy trên executor
```

Trong cây có hai loại nút: toán tử của plan (`Project`, `Filter`, `Join`, ...) và **biểu thức**
(`Expression`): `Add`, `Cast`, `Sha2`, `AesEncrypt`, `Literal`, `AttributeReference` (cột), v.v.
"Biểu thức Catalyst" nghĩa là một nút thuộc loại `Expression`. `column_encrypt(...)` không tồn
tại như một nút riêng — lúc Analyzer resolve tên hàm này, `ColumnCryptoExtension` **thay thế**
nó bằng một cây con chỉ gồm các nút built-in đã có sẵn trong Spark. Từ đó về sau (Optimizer,
Physical planner, Codegen) Spark không biết và không cần biết có `column_encrypt` — nó chỉ thấy
`Base64(AesEncrypt(...))`, y như người dùng tự gõ biểu thức đó trong SQL.

### 1.2. Công thức mã hoá (nguồn: [`CryptoExpressions.scala`](../column-crypto-lib/src/main/scala/vai/lakehouse/columncrypto/CryptoExpressions.scala))

```
key      = unhex(sha2(keyPrefix || cast(keyValue as string), 256))   -- SHA-256 → luôn đúng 32 byte (AES-256)
encrypt  = base64(aes_encrypt(cast(value as string), key, 'GCM'))
decrypt  = decode(aes_decrypt(unbase64(value), key, 'GCM'), 'UTF-8')
```

- `aes_encrypt`/`aes_decrypt` là hàm built-in của Spark SQL (có từ Spark 3.3). IV do Spark tự
  sinh ngẫu nhiên và gắn vào đầu ciphertext — lib không tự quản lý nonce.
- `keyPrefix` là bí mật lấy từ Vault/K8s Secret (xem [COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md](COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md)),
  `keyValue` là giá trị của cột `keyField` tại chính dòng đó (plaintext), nên **mỗi dòng một
  key khác nhau**.
- `CryptoExpressions` là nơi **duy nhất** dựng công thức này. Cả DataFrame API
  (`ColumnCrypto.encryptColumns`/`decryptColumns`) lẫn SQL function (`column_encrypt`/
  `column_decrypt`) đều gọi qua đây, nên dữ liệu ghi bằng đường này luôn giải mã được bằng
  đường kia (có test roundtrip 2 chiều đảm bảo).
- Các hàm built-in được dựng qua `FunctionRegistry.builtin.lookupFunction(...)` — đúng builder
  mà chính SQL parser dùng khi gặp `sha2(...)`, `aes_encrypt(...)` trong câu SQL — thay vì gọi
  trực tiếp constructor của từng class biểu thức, để không phải theo dõi chữ ký constructor có
  thể đổi giữa các phiên bản Spark.

### 1.3. Luồng chạy trên sql-engine (Spark Thrift Server)

```
Query console ──SQL──> STS (driver)
                         │  Analyzer: gặp column_encrypt('customers', name, id)
                         │     └─ ColumnCryptoExtension.register(...) chạy builder:
                         │          1. lấy tên bảng từ literal đầu tiên
                         │          2. source.read("customers") -> CachedPrefixSource -> VaultPrefixSource
                         │             (Vault: login → read → revoke, HOẶC đọc thẳng bằng token tĩnh — mục 1.5)
                         │          3. ColumnCrypto.redactKeyPrefixInPlans(...) che prefix khỏi EXPLAIN/UI/event log
                         │          4. trả về CryptoExpressions.encrypt(value, keyValue, prefix)
                         └─ cây con thay thế node column_encrypt trong logical plan
Physical planner + Codegen: sinh code Java cho aes_encrypt/sha2/... như mọi hàm built-in khác
Executor: chỉ chạy code built-in đã sinh — KHÔNG gọi Vault, KHÔNG cần jar column-crypto-lib.
```

Điểm mấu chốt: **toàn bộ việc gọi Vault chỉ diễn ra ở driver, đúng một lần cho mỗi lần cache
miss** (mặc định cache 300 giây theo tên bảng — xem `CachedPrefixSource`). Executor không có
đường nào chạm tới Vault hay tới code của lib.

### 1.4. Vì sao đường này không cần "tự viết crypto"

Lib không cài đặt thuật toán AES-GCM nào cả. Việc mã hoá do chính `aes_encrypt`/`aes_decrypt`
của Spark (một implementation đã được kiểm chứng rộng rãi) thực hiện. Lib chỉ quyết định **key
sinh ra từ đâu** (`sha2(prefix || keyValue)`) và **ghép các hàm built-in với nhau** thành đúng
biểu thức đó. Đây là lý do hướng này an toàn hơn so với việc tự implement một tầng mã hoá mới.

### 1.5. Nguồn `keyPrefix`: Kubernetes auth và token tĩnh

`VaultPrefixSource` hỗ trợ 2 cách xác thực với Vault (chọn qua `vault.authMethod`, xem
[`PrefixSourceFactory.scala`](../column-crypto-lib/src/main/scala/vai/lakehouse/columncrypto/prefix/PrefixSourceFactory.scala)):

| `vault.authMethod` | Luồng gọi Vault | Khi nào dùng |
| --- | --- | --- |
| `kubernetes` (mặc định) | `POST /v1/auth/<authMount>/login` (JWT của ServiceAccount) → token tạm → `GET` secret → `POST /v1/auth/token/revoke-self` | Vault đã bật Kubernetes auth cho ServiceAccount của driver |
| `token` | Chỉ 1 request: `GET` secret kèm `X-Vault-Token: <token tĩnh>`. **Không login, không revoke** | ClusterSecretStore/ExternalSecrets của Vault dùng `tokenSecretRef` (không có Kubernetes auth) — trường hợp thực tế đang áp dụng cho `sample-spark-application-privacy` |

Ở chế độ `token`, `VaultPrefixSource.read()` rẽ nhánh ngay từ đầu và bỏ hẳn 2 bước login/revoke
(xem `VaultPrefixSource.scala` dòng 62–71): revoke một token dùng chung sẽ làm hỏng mọi
ExternalSecret khác đang phụ thuộc token đó, nên lib tuyệt đối không đụng vào vòng đời của token
tĩnh. Chi tiết cấu hình, lệnh tạo token riêng chỉ đọc, và cách truyền token qua K8s Secret nằm ở
[COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md, mục 3.1](COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md#31-xác-thực-bằng-token-tĩnh-khi-vault-không-dùng-kubernetes-auth)
và README (mục "Xác thực bằng token Vault tĩnh").

### 1.6. Sơ đồ thành phần

```
                         ┌───────────────────────────────────────────┐
                         │            column-crypto-lib               │
                         │                                             │
 DataFrame API           │   ColumnCrypto.encryptColumns/decryptColumns│
 (SparkApp)  ───────────►│              │                              │
                         │              ▼                              │
 SQL (sql-engine) ──────►│   ColumnCryptoExtension (SparkSessionExt.)  │
                         │              │                              │
                         │              ▼                              │
                         │        CryptoExpressions  (công thức duy nhất)
                         │              │                              │
                         │              ▼                              │
                         │   PrefixSourceFactory → PrefixSource        │
                         │        (Cached → Chained → File | Vault)    │
                         └───────────────────────────────────────────┘
                                        │
                                        ▼
                              Vault (login/token) hoặc K8s Secret mount
```

## 2. Kiến trúc thay thế: UDF

### 2.1. Cách hoạt động

Thay vì dựng cây biểu thức built-in, `ColumnCryptoExtension` sẽ đăng ký một `ScalaUDF` bọc một
hàm JVM thuần do lib tự viết:

```
column_encrypt('customers', name, id)
        │  (lúc analyze, ở driver: vẫn tra prefix qua PrefixSource như hiện tại)
        ▼
ScalaUDF(CryptoCore.encrypt, Seq(name, id), prefix nằm trong CLOSURE của UDF)
        │  (KHÔNG bị thay bằng cây built-in — nút UDF tồn tại nguyên vẹn qua Optimizer)
        ▼
Executor: deserialize UDF, với MỖI DÒNG:
   1. chuyển giá trị Spark internal → kiểu Scala (String/Long/...)
   2. gọi CryptoCore.encrypt(value, keyValue, prefix)  -- code JVM thuần, tự quản lý Cipher/IV
   3. chuyển kết quả Scala → kiểu Spark internal
```

Khác biệt cốt lõi so với mục 1: **executor chạy code của lib**, không chỉ chạy hàm built-in. Vì
vậy prefix (nằm trong closure) và jar của lib đều phải có mặt ở executor.

### 2.2. Việc cần refactor nếu đi theo hướng này

| Việc | Chi tiết |
| --- | --- |
| Thêm `CryptoCore` | Hàm thuần `encrypt(plain, keyValue, prefix): String` / `decrypt(...)`, tự dùng `javax.crypto.Cipher` (AES/GCM/NoPadding). Phải tự sinh IV ngẫu nhiên 12 byte cho **mỗi lần mã hoá** và tự ghép `IV ‖ ciphertext ‖ tag` để giữ đúng định dạng đầu ra hiện tại của `aes_encrypt` — nếu lệch định dạng, **dữ liệu đã mã hoá bằng jar cũ sẽ không giải mã được nữa**. |
| Tối ưu theo luồng | `MessageDigest`/`Cipher` không thread-safe và tốn chi phí khởi tạo; nên giữ trong `ThreadLocal` thay vì tạo mới cho mỗi dòng. |
| Sửa `ColumnCryptoExtension` | Builder trả về `ScalaUDF(...)` bọc `CryptoCore` thay vì `CryptoExpressions.encrypt/decrypt`. Đánh dấu UDF là **không tất định** (`udfDeterministic = false`) vì IV ngẫu nhiên, để Optimizer không gộp/tính trước biểu thức. |
| Sửa `ColumnCrypto` (DataFrame API) | Để giữ một nguồn công thức duy nhất, cũng phải đổi sang gọi `CryptoCore` qua `functions.udf(...)` thay vì dựng biểu thức `expr(...)` như hiện tại. |
| Đưa jar xuống executor | Bắt buộc: UDF được serialize và chạy trên executor, nên `column-crypto-lib` phải có trên classpath của **cả driver lẫn executor** qua `spark.jars` (hiện tại chỉ cần ở driver). |
| Test mới | Roundtrip UDF, và test chứng minh `CryptoCore` giải mã được ciphertext do `aes_encrypt` built-in sinh ra (và ngược lại) nếu muốn dữ liệu cũ vẫn đọc được; test NULL; test benchmark tốc độ so với bản built-in. |

### 2.3. Trường hợp bên thứ ba gửi jar `decrypt` riêng

Nếu nguồn dữ liệu cung cấp một jar chứa logic decrypt của riêng họ (thuật toán, bố cục
ciphertext, cách sinh key có thể khác hoàn toàn với mục 1.2), khuyến nghị là **UDF bọc thẳng
jar của họ**, không tái tạo lại bằng biểu thức built-in:

- Built-in chỉ dùng được nếu bố cục ciphertext của họ tái tạo được bằng `aes_decrypt` + các hàm
  xử lý chuỗi nhị phân có sẵn, và bạn có đủ test vector để chứng minh tương đương ở mọi trường
  hợp biên (NULL, chuỗi rỗng, bảng mã ký tự...). Rủi ro là bản tái tạo lệch âm thầm khi họ đổi
  logic ở phiên bản sau.
- UDF bọc jar của họ luôn đúng theo định nghĩa, vì gọi thẳng code gốc. Chi phí đổi lại: executor
  cần thêm jar đó qua `spark.jars`, cần rà soát bảo mật/dependency của jar bên thứ ba, và tốc độ
  chậm hơn built-in.
- Nên tổ chức thành **hàm SQL riêng** (ví dụ `source_decrypt`) song song với `column_encrypt`/
  `column_decrypt` hiện có, thay vì thay thế toàn bộ — hai định dạng dữ liệu khác nhau nên cần
  hai hàm khác nhau. Cách này **không** buộc phải sửa gì ở `column_encrypt`/`column_decrypt`
  hiện tại hay ở SparkApp, trừ khi chính SparkApp cần đọc dữ liệu của họ.

## 3. Bảng so sánh chi tiết

| Tiêu chí | Built-in (hiện tại) | UDF |
| --- | --- | --- |
| Ai thực thi việc mã hoá/giải mã | `aes_encrypt`/`aes_decrypt` của Spark (đã kiểm chứng rộng rãi) | Code JVM tự viết trong lib (`CryptoCore`, dùng `javax.crypto`) |
| Lib có "tự viết crypto" không | Không — chỉ ghép hàm built-in có sẵn | Có phần keo nối tự viết: sinh IV, ghép/tách định dạng ciphertext, quản lý `Cipher`. Không phải tự cài thuật toán AES (JDK đã có), nhưng sai một chi tiết (vd tái dùng IV) là lỗi bảo mật nghiêm trọng |
| Executor có cần jar `column-crypto-lib` | Không | Có, bắt buộc qua `spark.jars` |
| Executor có gọi Vault | Không (prefix đã nhúng vào plan/closure từ driver) | Không (vẫn tra ở driver, nhưng prefix nằm trong closure gửi xuống cùng UDF) |
| Catalyst tối ưu / sinh code (codegen) | Có — nút built-in tự sinh code, gộp cùng cây xung quanh (whole-stage codegen) | Không — `ScalaUDF` là hộp đen, mỗi dòng phải chuyển đổi kiểu dữ liệu qua lại giữa Spark internal và Scala |
| Hiệu năng | Ngang các hàm built-in khác | Chậm hơn (chưa đo cụ thể trên dữ liệu thật của dự án — cần benchmark trước khi kết luận mức chênh) |
| Phụ thuộc API nội bộ Spark | Có: `FunctionRegistry.builtin`, `injectFunction`, `ExpressionInfo` — không phải API ổn định lâu dài, build/test lại khi đổi phiên bản Spark | Ít hơn: chỉ cần `ScalaUDF`/`udf(...)`, ổn định hơn qua các phiên bản |
| Tương thích dữ liệu cũ | Mặc định đúng (đang dùng chính công thức này) | Phải tự đảm bảo bằng test — nếu định dạng ciphertext tự viết lệch với `aes_encrypt`, dữ liệu cũ **không đọc được** |
| Kiểm soát định dạng ciphertext | Do Spark quy định, không đổi được | Tự do định nghĩa — cần khi phải khớp với thuật toán của bên thứ ba, muốn thêm AAD, hay đổi key theo phiên bản |
| Che `keyPrefix` khỏi `EXPLAIN`/Spark UI/event log | Cần cơ chế riêng (`redactKeyPrefixInPlans` set `spark.sql.redaction.string.regex`) vì prefix là `Literal` trong plan | Không cần cơ chế che — prefix nằm trong closure của UDF, không hiện trong `EXPLAIN` mặc định (nhưng vẫn nằm trong bộ nhớ/task gửi xuống executor) |
| Ảnh hưởng khi đổi từ built-in sang UDF | — | Ảnh hưởng cả `column-crypto-lib` (DataFrame API + extension) lẫn mọi nơi dùng nó (SparkApp, sql-engine) nếu áp dụng cho **toàn bộ** `column_encrypt`/`column_decrypt`. Nếu chỉ **thêm** hàm mới cho một định dạng khác (bên thứ ba) thì SparkApp hiện tại không bị ảnh hưởng |
| Khi nào nên dùng | Mặc định — không có lý do cụ thể để đổi | Cần định dạng ciphertext khác chuẩn của Spark, cần bọc thuật toán/jar bên thứ ba bắt buộc phải tuân theo, cần envelope encryption qua KMS thật, hoặc cần AAD/rotate key theo phiên bản |

## 4. Khuyến nghị

Giữ nguyên kiến trúc built-in (mục 1) cho `column_encrypt`/`column_decrypt` hiện có — nó nhanh
hơn, không cần jar trên executor, và không phải tự bảo trì định dạng ciphertext. Chỉ chuyển
sang UDF (mục 2) khi có yêu cầu cụ thể mà biểu thức built-in không đáp ứng được, điển hình nhất
là phải tuân theo thuật toán/jar `decrypt` của một nguồn dữ liệu khác. Trong trường hợp đó, nên
**thêm hàm SQL riêng bọc UDF** thay vì thay thế `column_encrypt`/`column_decrypt` đang chạy, để
không ảnh hưởng tới SparkApp và dữ liệu đã có.
