# Dùng `column-crypto-lib` trong sql-engine (Spark Thrift Server)

Hướng dẫn nạp jar `column-crypto-lib` vào sql-engine tạo trên giao diện UI và chạy mã hoá/giải mã
ngay trên query console SQL. Toàn bộ cấu hình đi qua **Spark conf** trên màn hình tạo engine —
không cần sửa StatefulSet, biến môi trường hay mount thêm Secret.

## 1. Cách hoạt động

```
Query console ──SQL──> STS (driver)
                         │  analyze: gặp column_encrypt('customers', name, id)
                         │     └─ ColumnCryptoExtension ─> CachedPrefixSource ─> Vault
                         │           keyPrefix của bảng 'customers' (chỉ driver gọi, cache theo TTL)
                         │           - authMethod=token      : 1 request GET secret kèm token tĩnh (mặc định của hướng dẫn này)
                         │           - authMethod=kubernetes : login (JWT) → GET secret → revoke-self
                         └─ thay hàm bằng biểu thức built-in của Spark:
                              base64(aes_encrypt(cast(v as string), unhex(sha2(keyPrefix||keyValue,256)), 'GCM'))
Executor: chỉ chạy hàm built-in (aes_encrypt, sha2, ...) — KHÔNG gọi Vault, KHÔNG cần jar của lib.
```

Chi tiết kiến trúc của luồng này (vì sao là "biểu thức built-in" chứ không phải UDF, so sánh với
phương án UDF) nằm ở [COLUMN_CRYPTO_ARCHITECTURE.md](COLUMN_CRYPTO_ARCHITECTURE.md).

Công thức là **cùng một** `CryptoExpressions` với DataFrame API của SparkApplication, nên dữ liệu
ghi bằng job SparkApplication đọc được bằng SQL và ngược lại (có test bảo đảm hai chiều).

## 2. Build và đặt jar

```bash
mvn -B clean package
# -> column-crypto-lib/target/column-crypto-lib-1.0-SNAPSHOT.jar   (~66 KB, chỉ chứa vai.lakehouse.columncrypto.*)
```

Đặt jar ở nơi engine đọc được lúc khởi động. Khuyến nghị dùng một thư mục trên HDFS của cụm
(Spark tự tải file `hdfs://` về — xem "Advanced Dependency Management" trong tài liệu Spark):

```bash
hdfs dfs -mkdir -p /libs/column-crypto-lib
hdfs dfs -put column-crypto-lib/target/column-crypto-lib-1.0-SNAPSHOT.jar /libs/column-crypto-lib/
```

`spark.jars` (built-in của Spark, không phải `spark.columncrypto.*`) hỗ trợ scheme `hdfs:`, `http:`,
`https:`, `ftp:` (Spark tự tải file) và `local:` (phải đã có sẵn trên từng node, không qua mạng) —
dùng `hdfs://` hoặc chỗ upload mà UI hỗ trợ, **không dùng** `spark.driver.extraLibraryPath`/
`spark.executor.extraLibraryPath` (hai config đó set `java.library.path` cho thư viện native
`.so`/`.dll`, không liên quan tới việc nạp jar chứa class Java/Scala). Vì cụm HKH bật Kerberos, driver
phải login Kerberos xong và namenode chứa `/libs/column-crypto-lib` phải có trong
`spark.kerberos.access.hadoopFileSystems`, nếu không request tải jar sẽ lỗi cùng kiểu access-denied
như các thao tác HDFS khác. Lib không đóng gói Spark/Jackson/snakeyaml nên không gây xung đột classpath.

## 3. Cấu hình trên màn hình tạo sql-engine

Điền vào ô Spark config của engine. Nhóm dưới đây là cấu hình dùng chung, áp dụng cho cách xác thực
**token tĩnh** (mặc định của hướng dẫn này — xem mục 3.1 vì sao):

| Key                                   | Giá trị                                                                    | Ghi chú                                                                                                                                                                                                                       |
| ------------------------------------- | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.jars`                          | `hdfs:///libs/column-crypto-lib/column-crypto-lib-1.0-SNAPSHOT.jar`        | Nạp lib vào driver (và executor); cần Kerberos đã login + namenode có trong `spark.kerberos.access.hadoopFileSystems` (xem mục 2)                                                                                             |
| `spark.sql.extensions`                | `<extension đang có>,vai.lakehouse.columncrypto.sql.ColumnCryptoExtension` | **Nối bằng dấu phẩy**, không ghi đè extension sẵn có (vd Ranger)                                                                                                                                                              |
| `spark.columncrypto.source`           | `vault` (hoặc `file`, `file,vault`)                                        | Mặc định `file`                                                                                                                                                                                                               |
| `spark.columncrypto.vault.addr`       | `http://vault.cyberspace.vn`                                               | Bắt buộc khi dùng vault. Đang là HTTP: token/keyPrefix đi trên mạng không mã hoá — nên chuyển HTTPS                                                                                                                           |
| `spark.columncrypto.vault.authMethod` | `token`                                                                    | Mặc định của lib là `kubernetes` nếu bỏ trống — **luôn đặt tường minh** `token`, vì Vault hiện tại (`ClusterSecretStore datalake-vault`) xác thực bằng `tokenSecretRef`, không có Kubernetes auth role cho Spark; xem mục 3.1 |
| `spark.columncrypto.vault.token`      | token Vault tĩnh (riêng, chỉ đọc)                                          | Bắt buộc. **Không dùng lại** token của ExternalSecrets (`datalake-vault-token`, có quyền ghi) — tạo token riêng chỉ đọc (mục 3.1)                                                                                             |
| `spark.columncrypto.vault.kvMount`    | `kv`                                                                       | Mặc định `kv`, có thể bỏ trống nếu đúng mặc định                                                                                                                                                                              |
| `spark.columncrypto.vault.kvBasePath` | `hla-datalake/datalake/spark-application`                                  | Bắt buộc; secret của bảng T nằm ở `<kvMount>/data/<kvBasePath>/T` (KV v2). **Không kèm tên bảng** ở đây — lib tự nối `/T`                                                                                                     |
| `spark.columncrypto.vault.keyField`   | `keyPrefix`                                                                | Field chứa prefix trong secret; mặc định `keyPrefix`. Sai tên field → lỗi `has no string field 'keyPrefix'`                                                                                                                   |
| `spark.columncrypto.cacheTtlSeconds`  | `300`                                                                      | Cache prefix trong bộ nhớ; `0` = tắt                                                                                                                                                                                          |

3 config dưới đây **chỉ dùng cho `authMethod=kubernetes`** (login bằng JWT của ServiceAccount) —
**không cần đặt và bị lib bỏ qua hoàn toàn khi `authMethod=token`** (không login nên không đọc tới):

| Key                                  | Giá trị                                               | Ghi chú                                                             |
| ------------------------------------ | ----------------------------------------------------- | ------------------------------------------------------------------- |
| `spark.columncrypto.vault.role`      | role Kubernetes auth                                  | Role phải bind với ServiceAccount `spark` + namespace của engine    |
| `spark.columncrypto.vault.authMount` | `kubernetes`                                          | Mount của Kubernetes auth trên Vault (`/v1/auth/<authMount>/login`) |
| `spark.columncrypto.vault.jwtPath`   | `/var/run/secrets/kubernetes.io/serviceaccount/token` | Đường dẫn JWT của ServiceAccount, đọc lúc login                     |

### 3.1. Xác thực bằng token tĩnh — cách đang áp dụng thực tế

Kế hoạch ban đầu của lib là xác thực bằng **Kubernetes auth** (`authMethod=kubernetes`, mặc định
nếu bỏ trống): JWT của ServiceAccount → login → token tạm → đọc secret → revoke. Cách đó **không
dùng được** với hạ tầng Vault hiện có, vì `ClusterSecretStore` (`datalake-vault`, dùng bởi
ExternalSecrets) xác thực bằng `tokenSecretRef` — tức Vault ở đây cấp quyền qua **token tĩnh**,
không có Kubernetes auth role cho ServiceAccount của Spark. Vì vậy cấu hình chuẩn cho engine là
`authMethod=token`, không phải `kubernetes`. Giá trị cụ thể cho từng key nằm ở bảng ở mục 3 —
3 key `role`/`authMount`/`jwtPath` ở bảng thứ hai của mục 3 **không cần đặt** khi dùng `token`.

Lưu ý khi dùng chế độ này:

- **Đừng dùng lại token của ExternalSecrets**: nó có quyền ghi (`ReadWrite`). Tạo token riêng chỉ đọc
  path `hla-datalake/datalake/spark-application/*` (lệnh cụ thể trong README, mục "Xác thực bằng
  token Vault tĩnh").
- Token nằm plaintext trong cấu hình engine trên UI (Spark chỉ che nó khi hiển thị/log). Ai xem được
  cấu hình engine là thấy được token; token có TTL thì phải rotate trước khi hết hạn.
- `vault.addr` đang là HTTP thì token đi trên mạng không mã hoá — nên chuyển HTTPS.
- Ở chế độ này lib không revoke token (khác Kubernetes auth), nên không ảnh hưởng dịch vụ khác dùng chung.

Các key này là conf **tĩnh**: sửa xong phải **restart engine** mới có hiệu lực.

Nếu engine có Secret mount sẵn (hiếm), dùng `spark.columncrypto.source=file` +
`spark.columncrypto.file.dir=<thư mục mount>` (mỗi bảng 1 file tên bảng). `file,vault` thử file
trước rồi fallback sang Vault.

## 4. Dùng trên query console

Kiểm tra extension đã nạp:

```sql
DESCRIBE FUNCTION column_encrypt;
```

Cú pháp: `column_encrypt('<bảng>', <cột>, <giá trị keyField>)` và
`column_decrypt('<bảng>', <cột đã mã hoá>, <giá trị keyField>)`. Tham số đầu là **tên bảng dạng
chuỗi hằng** (dùng để tìm `keyPrefix`); tham số thứ 3 là cột `keyField` của chính dòng đó
(cột này giữ plaintext, không được mã hoá).

```sql
-- Ghi: mã hoá cột name, city khi insert; id là keyField
INSERT INTO db.customers
SELECT id, column_encrypt('customers', name, id), column_encrypt('customers', city, id)
FROM staging_customers;

-- Đọc: giải mã
SELECT id,
       column_decrypt('customers', name, id) AS name,
       column_decrypt('customers', city, id) AS city
FROM db.customers;

-- View để người dùng cuối không phải nhớ cú pháp
CREATE VIEW db.customers_plain AS
SELECT id,
       column_decrypt('customers', name, id) AS name,
       column_decrypt('customers', city, id) AS city
FROM db.customers;
```

Cột đã mã hoá phải khai kiểu `STRING` trong DDL (giá trị là Base64 của ciphertext).

## 5. Giới hạn và lưu ý

- **Không so sánh được trên ciphertext**: IV ngẫu nhiên nên `WHERE name = 'x'` không chạy trên cột
  mã hoá. Chỉ lọc được qua `WHERE column_decrypt('customers', name, id) = 'x'`, và phải quét toàn bảng.
- **Truyền đúng cột `keyField`**: truyền sai thì `column_decrypt` lỗi `Tag mismatch`
  (GCM phát hiện sai key). Dùng view (mục 4) để cố định cách gọi.
- **Phân quyền**: mọi người dùng chạy được SQL trên engine đều gọi được hàm với mọi bảng, vì
  credential Vault (token tĩnh, hoặc role của Kubernetes auth) gắn với chính engine chứ không
  theo từng người dùng SQL. Nên: chỉ cấp SELECT bảng gốc cho pipeline, cấp SELECT trên view
  `*_plain` cho người được xem plaintext, và kiểm chứng Ranger plugin của bạn có kiểm tra bảng
  gốc bên dưới view hay không.
- **`keyPrefix` không lộ ra plan**: sau lần tra đầu, lib đặt `spark.sql.redaction.string.regex` của
  session để che prefix khỏi `EXPLAIN`, tab SQL của Spark UI và event log.
- **Lỗi Vault** hiện ngay ở bước analyze của câu query; thông báo không chứa JWT, token hay prefix.
- **Mất kết nối Vault khi cache đã có**: query vẫn chạy đến hết TTL, sau đó mới lỗi.

## 6. Xử lý sự cố

| Triệu chứng                                                    | Nguyên nhân thường gặp                                                                                                                 |
| -------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `Undefined function: column_encrypt`                           | Extension chưa nạp: sai tên class trong `spark.sql.extensions`, jar chưa nằm trên classpath lúc khởi động, hoặc chưa restart engine    |
| `ClassNotFoundException ... ColumnCryptoExtension`             | `spark.jars` không nạp kịp lúc khởi tạo session; đặt jar vào `/opt/spark/jars` của image hoặc dùng `spark.driver.extraClassPath`       |
| `Missing required setting: spark.columncrypto.vault.addr`      | Thiếu key bắt buộc trong Spark conf                                                                                                    |
| `Missing required setting: spark.columncrypto.vault.token`     | `authMethod=token` nhưng thiếu `vault.token`                                                                                           |
| `Invalid spark.columncrypto.vault.authMethod`                  | Giá trị khác `token`/`kubernetes`, hoặc gõ sai chính tả                                                                                |
| `Vault read of secret '...' failed: HTTP 403` (chế độ `token`) | Token thiếu quyền đọc path `kvBasePath` — kiểm tra lại policy, xem mục 3.1                                                             |
| `Vault login failed: HTTP 403` (chế độ `kubernetes`)           | Role không bind với ServiceAccount/namespace của engine                                                                                |
| `Vault read of secret '...' failed: HTTP 404`                  | Sai `kvBasePath`/`kvMount` hoặc chưa có secret cho bảng đó                                                                             |
| `has no string field 'keyPrefix'`                              | Secret tồn tại nhưng tên field khác `vault.keyField` (mặc định `keyPrefix`) — đổi tên field trong secret hoặc set lại `vault.keyField` |
| `Cannot read service account token`                            | Chỉ xảy ra ở chế độ `kubernetes` (pod tắt automount ServiceAccount token); chế độ `token` không đọc file này                           |
| `first argument must be a constant string`                     | Tham số đầu phải là literal `'tên_bảng'`, không phải cột hay biểu thức                                                                 |
| `Tag mismatch` (`AES_CRYPTO_ERROR`)                            | Sai bảng (sai prefix), sai cột `keyField`, hoặc giải mã cột chưa được mã hoá                                                           |

Xem log driver của engine để biết extension có nạp được không: lỗi khởi tạo extension in ra ngay
lúc engine khởi động.
