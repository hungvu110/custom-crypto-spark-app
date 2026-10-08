# Dùng `column-crypto-lib`, `cdr-crypto-udf` và `key-prefix-lib` trong sql-engine (Spark Thrift Server)

Hướng dẫn nạp các jar vào sql-engine tạo trên giao diện UI và chạy mã hoá/giải mã ngay trên query
console SQL, với **cả 2 loại lib crypto**:

- `column_encrypt`/`column_decrypt` — AES-256-GCM, module `column-crypto-lib`.
- `cdr_encrypt`/`cdr_decrypt` — thuật toán của đối tác (AES-128-ECB), module `cdr-crypto-udf`.
- Cả hai lấy `keyPrefix` qua **hạ tầng dùng chung** `key-prefix-lib` (DAK / K8s Secret mount / HashiCorp Vault).
  Cách đang triển khai là **DAK** (mục 4.6); Vault giữ lại làm fallback thủ công.

Toàn bộ cấu hình đi qua **Spark conf** trên màn hình tạo engine — không cần sửa StatefulSet, biến môi
trường hay mount thêm Secret.

## 1. Ba lib và vai trò

| Lib                 | Jar                                  | Vai trò                                                                               | Cần khi dùng                     |
| ------------------- | ------------------------------------ | ------------------------------------------------------------------------------------- | -------------------------------- |
| `key-prefix-lib`    | `key-prefix-lib-1.0-SNAPSHOT.jar`    | Hạ tầng lấy `keyPrefix` (Vault/file, có cache). Không có thuật toán mã hoá            | **Luôn cần**, cho mọi hàm crypto |
| `column-crypto-lib` | `column-crypto-lib-1.0-SNAPSHOT.jar` | Đăng ký `column_encrypt`/`column_decrypt`                                             | Hàm `column_*`                   |
| `cdr-crypto-udf`    | `cdr-crypto-udf-1.0-SNAPSHOT.jar`    | Đăng ký `cdr_encrypt`/`cdr_decrypt`; bọc lib đối tác                                  | Hàm `cdr_*`                      |
| Jar đối tác         | `DataLakeSecurity_jv8.jar`           | Lib mã hoá CDR của đối tác (`TransformDL`). Không phải của mình, chỉ nạp nguyên trạng | Hàm `cdr_*`                      |

`column-crypto-lib` và `cdr-crypto-udf` **độc lập nhau**: dùng một trong hai hoặc cả hai đều được, nhưng
`key-prefix-lib` luôn phải có mặt cùng.

| Bạn muốn dùng  | Jar cần nạp (`spark.jars`)                                                         | Extension cần khai (`spark.sql.extensions`)                                                              |
| -------------- | ---------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Chỉ `column_*` | `key-prefix-lib` + `column-crypto-lib`                                             | `vai.lakehouse.columncrypto.sql.ColumnCryptoExtension`                                                   |
| Chỉ `cdr_*`    | `key-prefix-lib` + `cdr-crypto-udf` + `DataLakeSecurity_jv8`                       | `vai.lakehouse.columncrypto.cdr.CdrCryptoExtension`                                                      |
| Cả hai         | `key-prefix-lib` + `column-crypto-lib` + `cdr-crypto-udf` + `DataLakeSecurity_jv8` | `vai.lakehouse.columncrypto.sql.ColumnCryptoExtension,vai.lakehouse.columncrypto.cdr.CdrCryptoExtension` |

## 2. Cách hoạt động

```
Query console ──SQL──> STS (driver)
                         │  analyze: gặp column_encrypt('customers', name, id)   hoặc   cdr_encrypt('users_cdr', isdn, start_datetime)
                         │     └─ Extension ─> key-prefix-lib (CachedPrefixSource) ─> Vault (hoặc file)
                         │           keyPrefix của bảng 'customers' / 'users_cdr' (chỉ driver gọi, cache theo TTL)
                         │           - authMethod=token      : 1 request GET secret kèm token tĩnh (mặc định của hướng dẫn này)
                         │           - authMethod=kubernetes : login (JWT) → GET secret → revoke-self
                         ├─ column_*: thay hàm bằng biểu thức built-in của Spark
                         │      base64(aes_encrypt(cast(v as string), unhex(sha2(keyPrefix||keyValue,256)), 'GCM'))
                         └─ cdr_*   : thay hàm bằng ScalaUDF gọi lib đối tác; keyPrefix nằm trong closure của UDF
                                      keyInput = keyPrefix + keyValue (nối chuỗi thô, không băm)
Executor:
  - column_*: chỉ chạy hàm built-in (aes_encrypt, sha2, ...) — KHÔNG gọi Vault, KHÔNG cần jar của lib.
  - cdr_*   : chạy UDF thật — KHÔNG gọi Vault, nhưng CẦN jar cdr-crypto-udf + jar đối tác trên classpath.
```

`spark.jars` áp dụng cho cả driver lẫn executor nên nạp đủ jar theo bảng mục 1 là đủ cho cả hai.

Chi tiết kiến trúc của luồng này (vì sao `column_*` là "biểu thức built-in" còn `cdr_*` bắt buộc là UDF) nằm ở
[COLUMN_CRYPTO_ARCHITECTURE.md](COLUMN_CRYPTO_ARCHITECTURE.md) và [PARTNER_CDR_CRYPTO_UDF_PLAN.md](PARTNER_CDR_CRYPTO_UDF_PLAN.md).

Công thức `column_*` là **cùng một** `CryptoExpressions` với DataFrame API của SparkApplication, và `cdr_*` cùng một hàm
biến đổi với DataFrame API `CdrCrypto`, nên dữ liệu ghi bằng job SparkApplication đọc được bằng SQL và ngược lại (có test
bảo đảm hai chiều).

## 3. Build và đặt jar

```bash
# Jar của column-crypto-lib + key-prefix-lib (không cần jar đối tác)
mvn -B clean package
# -> column-crypto-lib/target/column-crypto-lib-1.0-SNAPSHOT.jar   (chỉ chứa vai.lakehouse.columncrypto.*)
# -> key-prefix-lib/target/key-prefix-lib-1.0-SNAPSHOT.jar          (hạ tầng lấy keyPrefix, chứa vai.lakehouse.keyprefix.*)

# Jar của cdr-crypto-udf — cần cài jar đối tác vào ~/.m2 TRƯỚC (1 lần), rồi bật profile cdr-crypto
mvn install:install-file -Dfile=DataLakeSecurity_jv8.jar \
  -DgroupId=com.viettel.datalake -DartifactId=datalake-security -Dversion=jv8 -Dpackaging=jar
mvn -B -Pcdr-crypto -pl cdr-crypto-udf -am clean package -DskipTests
# -> cdr-crypto-udf/target/cdr-crypto-udf-1.0-SNAPSHOT.jar          (chỉ chứa vai.lakehouse.columncrypto.cdr.*)
```

Thiếu `key-prefix-lib.jar` thì cả 2 extension lỗi `NoClassDefFoundError: vai/lakehouse/keyprefix/...`. Jar của
`cdr-crypto-udf` **không** chứa jar đối tác — phải nạp riêng `DataLakeSecurity_jv8.jar` (file gốc ở thư mục gốc project,
không commit vào git).

Đặt jar ở nơi engine đọc được lúc khởi động. Khuyến nghị dùng một thư mục trên HDFS của cụm
(Spark tự tải file `hdfs://` về — xem "Advanced Dependency Management" trong tài liệu Spark):

```bash
hdfs dfs -mkdir -p /libs/column-crypto-lib
hdfs dfs -put key-prefix-lib/target/key-prefix-lib-1.0-SNAPSHOT.jar /libs/column-crypto-lib/
hdfs dfs -put column-crypto-lib/target/column-crypto-lib-1.0-SNAPSHOT.jar /libs/column-crypto-lib/
# Chỉ khi dùng cdr_*:
hdfs dfs -put cdr-crypto-udf/target/cdr-crypto-udf-1.0-SNAPSHOT.jar /libs/column-crypto-lib/
hdfs dfs -put DataLakeSecurity_jv8.jar /libs/column-crypto-lib/
```

**Cần xác nhận với đối tác/pháp lý** việc đặt `DataLakeSecurity_jv8.jar` lên hạ tầng nội bộ có nằm trong phạm vi họ đã cấp
phép hay không, trước khi đưa lên HDFS dùng chung.

`spark.jars` (built-in của Spark, không phải `spark.columncrypto.*`) hỗ trợ scheme `hdfs:`, `http:`,
`https:`, `ftp:` (Spark tự tải file) và `local:` (phải đã có sẵn trên từng node, không qua mạng) —
dùng `hdfs://` hoặc chỗ upload mà UI hỗ trợ, **không dùng** `spark.driver.extraLibraryPath`/
`spark.executor.extraLibraryPath` (hai config đó set `java.library.path` cho thư viện native
`.so`/`.dll`, không liên quan tới việc nạp jar chứa class Java/Scala). Vì cụm HKH bật Kerberos, driver
phải login Kerberos xong và namenode chứa `/libs/column-crypto-lib` phải có trong
`spark.kerberos.access.hadoopFileSystems`, nếu không request tải jar sẽ lỗi cùng kiểu access-denied
như các thao tác HDFS khác. Các jar này không đóng gói Spark/Jackson/snakeyaml nên không gây xung đột classpath.

## 4. Cấu hình trên màn hình tạo sql-engine

Điền vào ô Spark config của engine. Các key đều là conf **tĩnh**: sửa xong phải **restart engine** mới có hiệu lực.

### 4.1. Nạp jar và đăng ký extension

| Key                    | Giá trị (ví dụ dùng cả hai lib)                                                                                                                                                                                                                           | Ghi chú                                                                                                                     |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `spark.jars`           | `hdfs:///libs/column-crypto-lib/key-prefix-lib-1.0-SNAPSHOT.jar,hdfs:///libs/column-crypto-lib/column-crypto-lib-1.0-SNAPSHOT.jar,hdfs:///libs/column-crypto-lib/cdr-crypto-udf-1.0-SNAPSHOT.jar,hdfs:///libs/column-crypto-lib/DataLakeSecurity_jv8.jar` | Chọn jar theo bảng ở mục 1; cần Kerberos đã login + namenode có trong `spark.kerberos.access.hadoopFileSystems` (xem mục 3) |
| `spark.sql.extensions` | `<extension đang có>,vai.lakehouse.columncrypto.sql.ColumnCryptoExtension,vai.lakehouse.columncrypto.cdr.CdrCryptoExtension`                                                                                                                              | **Nối bằng dấu phẩy**, không ghi đè extension sẵn có (vd Ranger). Chỉ khai extension của lib nào bạn thật sự nạp jar        |

Khi khai `CdrCryptoExtension`, extension tự kiểm tra tương thích với jar đối tác **ngay lúc engine khởi động** (self-check
bằng test vector đã xác nhận). Thiếu `DataLakeSecurity_jv8.jar` hoặc jar đổi hành vi thì engine lỗi ngay lúc khởi tạo extension,
không đợi tới lúc chạy query — xem mục 9.

### 4.2. Cấu hình nguồn keyPrefix (`key-prefix-lib`)

Cả 2 extension đọc cấu hình lấy `keyPrefix` theo **cùng bộ key**, chỉ khác **tiền tố** (namespace) để 2 lib không đè cấu hình nhau:

| Lib dùng   | Namespace (tiền tố key) |
| ---------- | ----------------------- |
| `column_*` | `spark.columncrypto.`   |
| `cdr_*`    | `spark.cdrcrypto.`      |

Trong các bảng dưới, `<ns>` là một trong hai tiền tố trên (đã gồm dấu chấm cuối). Nếu dùng cả 2 lib thì **đặt cả 2 bộ**
(sql-engine không có biến môi trường để chia sẻ — khác SparkApplication, xem mục 8). Có thể cho 2 bộ trỏ cùng một Vault và cùng
một `kvBasePath`, hoặc tách riêng để mỗi lib lấy prefix từ path/token khác nhau.

Nhóm cấu hình dùng chung, áp dụng cho cách xác thực **token tĩnh** (mặc định của hướng dẫn này — xem mục 4.4 vì sao):

| Key                    | Giá trị                                   | Ghi chú                                                                                                                                        |
| ---------------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `<ns>source`           | `dak` (mục 4.6), `vault`, `file`, `file,vault` | Mặc định `file`. `dak` không được ghép với nguồn khác. Bảng này mô tả nguồn `vault`, giữ làm fallback thủ công                             |
| `<ns>vault.addr`       | `http://vault.cyberspace.vn`              | Bắt buộc khi dùng vault. Đang là HTTP: token/keyPrefix đi trên mạng không mã hoá — nên chuyển HTTPS                                            |
| `<ns>vault.authMethod` | `token`                                   | Mặc định của lib là `kubernetes` nếu bỏ trống — **luôn đặt tường minh** `token`, vì Vault hiện tại xác thực bằng `tokenSecretRef`; xem mục 4.4 |
| `<ns>vault.token`      | token Vault tĩnh (riêng, chỉ đọc)         | Bắt buộc. **Không dùng lại** token của ExternalSecrets (`datalake-vault-token`, có quyền ghi) — tạo token riêng chỉ đọc (mục 4.4)              |
| `<ns>vault.kvMount`    | `kv`                                      | Mặc định `kv`, có thể bỏ trống nếu đúng mặc định                                                                                               |
| `<ns>vault.kvBasePath` | `hla-datalake/datalake/spark-application` | Bắt buộc; secret của bảng T nằm ở `<kvMount>/data/<kvBasePath>/T` (KV v2). **Không kèm tên bảng** ở đây — lib tự nối `/T`                      |
| `<ns>vault.keyField`   | `keyPrefix`                               | Field chứa prefix trong secret; mặc định `keyPrefix`. Sai tên field → lỗi `has no string field 'keyPrefix'`                                    |
| `<ns>cacheTtlSeconds`  | `300`                                     | Cache prefix trong bộ nhớ; `0` = tắt                                                                                                           |

Ví dụ khi `<ns>` là `spark.cdrcrypto.`: `spark.cdrcrypto.source`, `spark.cdrcrypto.vault.addr`, `spark.cdrcrypto.vault.token`, ...

3 config dưới đây **chỉ dùng cho `authMethod=kubernetes`** (login bằng JWT của ServiceAccount) —
**không cần đặt và bị lib bỏ qua hoàn toàn khi `authMethod=token`** (không login nên không đọc tới):

| Key                   | Giá trị                                               | Ghi chú                                                             |
| --------------------- | ----------------------------------------------------- | ------------------------------------------------------------------- |
| `<ns>vault.role`      | role Kubernetes auth                                  | Role phải bind với ServiceAccount `spark` + namespace của engine    |
| `<ns>vault.authMount` | `kubernetes`                                          | Mount của Kubernetes auth trên Vault (`/v1/auth/<authMount>/login`) |
| `<ns>vault.jwtPath`   | `/var/run/secrets/kubernetes.io/serviceaccount/token` | Đường dẫn JWT của ServiceAccount, đọc lúc login                     |

### 4.3. Ví dụ cấu hình hoàn chỉnh (cả 2 lib, cùng Vault, xác thực token tĩnh)

```properties
# --- Nạp lib và extension ---
spark.jars=hdfs:///libs/column-crypto-lib/key-prefix-lib-1.0-SNAPSHOT.jar,hdfs:///libs/column-crypto-lib/column-crypto-lib-1.0-SNAPSHOT.jar,hdfs:///libs/column-crypto-lib/cdr-crypto-udf-1.0-SNAPSHOT.jar,hdfs:///libs/column-crypto-lib/DataLakeSecurity_jv8.jar
spark.sql.extensions=<extension đang có>,vai.lakehouse.columncrypto.sql.ColumnCryptoExtension,vai.lakehouse.columncrypto.cdr.CdrCryptoExtension

# --- keyPrefix cho column_encrypt/column_decrypt ---
spark.columncrypto.source=vault
spark.columncrypto.vault.addr=http://vault.cyberspace.vn
spark.columncrypto.vault.authMethod=token
spark.columncrypto.vault.token=<token chỉ đọc>
spark.columncrypto.vault.kvMount=kv
spark.columncrypto.vault.kvBasePath=hla-datalake/datalake/spark-application
spark.columncrypto.cacheTtlSeconds=300

# --- keyPrefix cho cdr_encrypt/cdr_decrypt (cùng Vault; có thể khác kvBasePath/token) ---
spark.cdrcrypto.source=vault
spark.cdrcrypto.vault.addr=http://vault.cyberspace.vn
spark.cdrcrypto.vault.authMethod=token
spark.cdrcrypto.vault.token=<token chỉ đọc>
spark.cdrcrypto.vault.kvMount=kv
spark.cdrcrypto.vault.kvBasePath=hla-datalake/datalake/spark-application
spark.cdrcrypto.cacheTtlSeconds=300
```

Chỉ dùng một lib thì bỏ khối `spark.<lib>.*`, jar và extension của lib còn lại (theo bảng mục 1).

### 4.4. Xác thực bằng token tĩnh — cách đang áp dụng thực tế

Kế hoạch ban đầu của lib là xác thực bằng **Kubernetes auth** (`authMethod=kubernetes`, mặc định
nếu bỏ trống): JWT của ServiceAccount → login → token tạm → đọc secret → revoke. Cách đó **không
dùng được** với hạ tầng Vault hiện có, vì `ClusterSecretStore` (`datalake-vault`, dùng bởi
ExternalSecrets) xác thực bằng `tokenSecretRef` — tức Vault ở đây cấp quyền qua **token tĩnh**,
không có Kubernetes auth role cho ServiceAccount của Spark. Vì vậy cấu hình chuẩn cho engine là
`authMethod=token`, không phải `kubernetes`. 3 key `role`/`authMount`/`jwtPath` **không cần đặt** khi dùng `token`.

Lưu ý khi dùng chế độ này:

- **Đừng dùng lại token của ExternalSecrets**: nó có quyền ghi (`ReadWrite`). Tạo token riêng chỉ đọc
  path `hla-datalake/datalake/spark-application/*` (lệnh cụ thể trong README, mục "Xác thực bằng
  token Vault tĩnh").
- Token nằm plaintext trong cấu hình engine trên UI (Spark chỉ che nó khi hiển thị/log). Ai xem được
  cấu hình engine là thấy được token; token có TTL thì phải rotate trước khi hết hạn. Nếu dùng cả 2 lib thì token
  xuất hiện ở **2 key** (`spark.columncrypto.vault.token` và `spark.cdrcrypto.vault.token`) — rotate cả hai.
- `vault.addr` đang là HTTP thì token đi trên mạng không mã hoá — nên chuyển HTTPS.
- Ở chế độ này lib không revoke token (khác Kubernetes auth), nên không ảnh hưởng dịch vụ khác dùng chung.

### 4.5. Nguồn `file` (K8s Secret mount)

Nếu engine có Secret mount sẵn (hiếm), dùng `<ns>source=file` + `<ns>file.dir=<thư mục mount>` (mỗi bảng 1 file tên
bảng, nội dung là prefix). `file,vault` thử file trước rồi fallback sang Vault.

### 4.6. Nguồn DAK (`source=dak`) — cách đang triển khai

Engine lấy key qua **DAK** bằng danh tính của team (client Keycloak riêng), thay cho token Vault dùng chung. Thiết kế:
[DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md](DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md); hợp đồng API: [DAK_API_SPEC.md](DAK_API_SPEC.md).

```properties
# --- Nguồn đang dùng. Fallback thủ công: đổi thành vault rồi restart engine ---
spark.columncrypto.source=dak

# --- DAK (giá trị do vận hành cấp khi tạo client Keycloak cho team) ---
spark.columncrypto.dak.addr=https://dak.<domain>
spark.columncrypto.dak.tokenUrl=https://keycloak.<domain>/realms/<realm của tenant>/protocol/openid-connect/token
spark.columncrypto.dak.clientId=<client_id của team>
spark.columncrypto.dak.clientSecret=<client_secret của team>
spark.columncrypto.cacheTtlSeconds=300

# --- Vault cũ: giữ nguyên cho fallback thủ công, KHÔNG được đọc khi source=dak (mục 4.3) ---
spark.columncrypto.vault.addr=http://vault.cyberspace.vn
spark.columncrypto.vault.authMethod=token
spark.columncrypto.vault.token=<token Vault chỉ đọc>
spark.columncrypto.vault.kvBasePath=hla-datalake/datalake/spark-application

# Dùng thêm cdr_*: lặp lại các khối trên với tiền tố spark.cdrcrypto. (cùng credential thì lib chỉ giữ 1 token)
```

| Key                       | Bắt buộc | Ghi chú                                                                                                   |
| ------------------------- | -------- | --------------------------------------------------------------------------------------------------------- |
| `<ns>dak.addr`            | Có       | Base URL của DAK, phải là `https://`                                                                      |
| `<ns>dak.tokenUrl`        | Có       | Token endpoint của **realm của tenant** sở hữu engine, phải là `https://`                                 |
| `<ns>dak.clientId`        | Có       | Client của team                                                                                           |
| `<ns>dak.clientSecret`    | Có       | Spark che giá trị trên UI/`SET` (tên key khớp `spark.redaction.regex`)                                    |
| `<ns>dak.allowInsecureHttp` | Không  | `true` cho phép `http://` — **chỉ** dùng khi test với server giả                                          |

**Cú pháp với `source=dak`**: tham số đầu của hàm bắt buộc là **`'database.table'`**, được chuẩn hoá chữ thường:

```sql
SELECT id, cdr_decrypt('demo_db.users_cdr', name, created_at) AS name FROM demo_db.users_cdr;
INSERT INTO demo_db.customers SELECT id, column_encrypt('demo_db.customers', name, id) FROM staging_customers;
```

Tên một phần (`'users_cdr'`) bị từ chối ngay ở bước analyze, không gọi DAK. Engine chỉ lấy được key của bảng trong
**workspace của chính nó**, và chỉ những bảng team đã được cấp quyền (còn hạn).

**Không** đặt `source=dak,vault` hay `file,dak`: lib từ chối, vì nguồn sau sẽ được dùng cả khi DAK trả 403, tức là bỏ qua
phân quyền và thời hạn quyền.

**Runbook fallback thủ công** (khi DAK hoặc Keycloak sự cố kéo dài):

1. Xin phê duyệt chuyển fallback, ghi nhận lý do và thời điểm.
2. Đổi `<ns>source=vault` trên màn hình cấu hình engine (với SparkApplication: `CRYPTO_PREFIX_SOURCE=vault`), restart engine
   (hoặc chạy lại job).
3. Kiểm tra một câu `cdr_decrypt`/`column_decrypt` trên bảng cũ. Fallback đọc secret `<kvBasePath>/<database>.<table>` ở Vault
   cũ: chỉ có cho các bảng đã có trước cutover; bảng tạo sau cutover chỉ có key ở DAK.
4. Khi DAK phục hồi, đổi lại `<ns>source=dak` và restart. Trong thời gian fallback, phân quyền và thời hạn quyền của DAK
   **không** có hiệu lực.

### 4.7. Gọi được Keycloak nội bộ từ sql-engine (hostAliases + CA nội bộ)

Với `source=dak`, **driver** của sql-engine (chính pod của StatefulSet, Thrift Server chạy client mode) gọi
`https://sso-lakehouse.cyberspace.vn/.../token` để xin token. Keycloak dùng **cert do CA nội bộ cấp**, nên JVM của driver phải
tin CA đó, và pod phải phân giải được tên `sso-lakehouse.cyberspace.vn` (ghim IP `10.221.148.42`). Executor không gọi Keycloak
nên không cần gì.

Cần 3 thứ: **ConfigMap CA**, **sửa StatefulSet** (hostAliases + initContainer dựng truststore + mount), và **Spark conf**
`spark.driver.extraJavaOptions`.

**1. ConfigMap chứa CA nội bộ** (một lần cho mỗi namespace; dak-mock và SparkApplication cùng namespace dùng chung):

```bash
NS=vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy
# internal-ca.pem: CA nội bộ dạng PEM (lấy từ team hạ tầng; có thể chứa nhiều cert, vd root + intermediate)
kubectl create configmap keycloak-ca -n "$NS" --from-file=ca.pem=internal-ca.pem
```

**2. Bổ sung vào pod template của StatefulSet sql-engine** (`spec.template.spec`):

```yaml
spec:
  template:
    spec:
      hostAliases:
        - ip: "10.221.148.42"
          hostnames:
            - sso-lakehouse.cyberspace.vn
      initContainers:
        # Truststore = cacerts của JDK (vẫn tin CA công khai) + mọi cert trong ca.pem.
        - name: build-truststore
          image: <image của container sql-engine>     # cần bash, awk, keytool và biến JAVA_HOME (image Spark có sẵn)
          command: ["bash", "-ec"]
          args:
            - |
              cp "$JAVA_HOME/lib/security/cacerts" /opt/truststore/truststore.p12
              awk '/BEGIN CERTIFICATE/ { n++ } n > 0 { print > ("/opt/truststore/ca-" n ".pem") }' /ca/ca.pem
              ls /opt/truststore/ca-*.pem > /dev/null
              for f in /opt/truststore/ca-*.pem; do
                keytool -importcert -noprompt -alias "internal-$(basename "$f" .pem)" -file "$f" \
                  -keystore /opt/truststore/truststore.p12 -storetype PKCS12 -storepass changeit
              done
              rm -f /opt/truststore/ca-*.pem
          volumeMounts:
            - name: keycloak-ca
              mountPath: /ca
              readOnly: true
            - name: truststore
              mountPath: /opt/truststore
      containers:
        - name: <container sql-engine>                # thêm vào container đang có, không tạo container mới
          volumeMounts:
            - name: truststore
              mountPath: /opt/truststore
              readOnly: true
      volumes:
        - name: keycloak-ca
          configMap:
            name: keycloak-ca
        - name: truststore
          emptyDir:
            sizeLimit: 16Mi
```

Nếu namespace áp Pod Security "restricted", thêm `securityContext` cho initContainer giống container chính
(`runAsNonRoot`, `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`).

**3. Spark conf trên màn hình engine** — **nối thêm** vào giá trị `spark.driver.extraJavaOptions` đang có, không ghi đè:

```properties
spark.driver.extraJavaOptions=<giá trị đang có> -Djavax.net.ssl.trustStore=/opt/truststore/truststore.p12 -Djavax.net.ssl.trustStoreType=PKCS12 -Djavax.net.ssl.trustStorePassword=changeit
```

Nếu cách khởi động engine không truyền `spark.driver.extraJavaOptions` vào JVM của Thrift Server, đặt các cờ trên vào biến
môi trường `JAVA_TOOL_OPTIONS` của container sql-engine thay thế.

Lưu ý:

- `javax.net.ssl.trustStore` áp dụng cho **mọi** kết nối TLS của driver. Truststore dựng từ `cacerts` cộng CA nội bộ nên các
  dịch vụ dùng CA công khai không bị ảnh hưởng. Nếu driver đang dùng một truststore riêng khác, phải nhập CA nội bộ vào chính
  truststore đó thay vì đổi sang file mới.
- StatefulSet do nền tảng (UI tạo engine) quản lý thì sửa tay có thể bị ghi đè khi engine được cập nhật từ UI — phối hợp với
  team nền tảng để đưa các mục trên vào template của engine, hoặc áp lại sau mỗi lần cập nhật.
- Kiểm tra: `kubectl logs <pod> -c build-truststore -n "$NS"` phải có `Certificate was added to keystore`. Lỗi
  `UnknownHostException: sso-lakehouse.cyberspace.vn` là thiếu `hostAliases`; `PKIX path building failed` là JVM chưa nhận
  truststore (kiểm `spark.driver.extraJavaOptions`).

SparkApplication: [k8s/spark-application-cdr.yaml](../k8s/spark-application-cdr.yaml) và
[k8s/spark-application-column.yaml](../k8s/spark-application-column.yaml) đã có sẵn cùng cấu hình ở `spec.driver`
(`hostAliases`, `initContainers`, `volumeMounts`), `spec.volumes` và `spark.driver.extraJavaOptions`. Spark Operator cần bật
mutating webhook thì các trường này mới được áp vào pod driver.

## 5. Chuẩn bị secret Vault theo bảng

`keyPrefix` được tra theo **tên bảng — tham số đầu của hàm** (`column_encrypt('customers', ...)`,
`cdr_encrypt('users_cdr', ...)`). Mỗi tên bảng cần 1 secret riêng:

```
<kvMount>/data/<kvBasePath>/<tên bảng>     field: <keyField>  (mặc định "keyPrefix")
```

Ví dụ với cấu hình ở mục 4.3, để dùng `cdr_encrypt('users_cdr', ...)`:

```
kv/hla-datalake/datalake/spark-application/users_cdr        keyPrefix = <prefix của bảng users_cdr>
```

Tên bảng chỉ được gồm `[A-Za-z0-9_.-]` và không bắt đầu bằng `.` (chặn path traversal). Thiếu secret cho bảng nào thì query
dùng bảng đó lỗi `HTTP 404` (mục 9). Nếu cùng một tên bảng dùng cho cả `column_*` lẫn `cdr_*` mà 2 bộ cấu hình cùng `kvBasePath` thì
**2 hàm dùng chung một prefix** — hoặc trỏ 2 bộ cấu hình tới 2 `kvBasePath` khác nhau để tách.

## 6. Dùng trên query console

### 6.1. `column_encrypt` / `column_decrypt`

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

### 6.2. `cdr_encrypt` / `cdr_decrypt`

Kiểm tra extension đã nạp:

```sql
DESCRIBE FUNCTION cdr_encrypt;
```

Cú pháp: `cdr_encrypt('<bảng>', <cột>, <giá trị keyField>)` và `cdr_decrypt('<bảng>', <cột đã mã hoá>, <giá trị keyField>)` — **cùng
hình dạng 3 tham số** với `column_*`. Khác ở cách dùng `keyField`: key được tạo bằng `keyPrefix + keyField` **nối chuỗi thô** (không
băm), nên `keyField` phải khớp **chính xác từng ký tự** với hệ thống của đối tác (không thêm dấu cách, không đổi định dạng ngày).

```sql
-- Giải mã CDR do đối tác giao: cột isdn đã mã hoá, start_datetime là keyField (giữ plaintext)
SELECT start_datetime,
       cdr_decrypt('sub_rel_product', isdn, start_datetime) AS isdn
FROM db.cdr_from_partner;

-- Ghi CDR mã hoá theo đúng thuật toán đối tác
INSERT INTO db.cdr_out
SELECT start_datetime, cdr_encrypt('sub_rel_product', isdn, start_datetime)
FROM staging_cdr;

-- View cho người dùng cuối
CREATE VIEW db.cdr_plain AS
SELECT start_datetime, cdr_decrypt('sub_rel_product', isdn, start_datetime) AS isdn
FROM db.cdr_from_partner;
```

Cột đã mã hoá phải khai kiểu `STRING` trong DDL (giá trị là Base64 của ciphertext) cho cả 2 loại hàm.

### 6.3. Đọc bảng do SparkApplication ghi (manifest `spark-application-cdr.yaml`)

Job `k8s/spark-application-cdr.yaml` ghi bảng `${DB_NAME}.${TABLE_NAME}` gồm `id, name, city, created_at`, mã hoá 2 cột
`name`, `city` (`CRYPTO_ENCRYPTED_COLUMNS`) bằng `cdr_encrypt`, lấy giá trị cột `created_at` (`CRYPTO_KEY_FIELD`, giữ plaintext)
làm `keyField`. Để đọc lại trên sql-engine, gọi `cdr_decrypt` với **cùng tên bảng và cùng cột `keyField`**:

```sql
-- Giả sử manifest đặt DB_NAME=demo_db, TABLE_NAME=users_cdr (khớp secret .../spark-application/users_cdr)
SELECT id,
       cdr_decrypt('users_cdr', name, created_at) AS name,
       cdr_decrypt('users_cdr', city, created_at) AS city,
       created_at
FROM demo_db.users_cdr
ORDER BY id;

-- View để người dùng cuối không phải nhớ cú pháp
CREATE VIEW demo_db.users_cdr_plain AS
SELECT id,
       cdr_decrypt('users_cdr', name, created_at) AS name,
       cdr_decrypt('users_cdr', city, created_at) AS city,
       created_at
FROM demo_db.users_cdr;
```

Quy tắc khớp với manifest:

| Trong manifest                           | Trong câu SQL                                                                                                                                        |
| ---------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TABLE_NAME`                             | Tham số **đầu** của hàm (`'users_cdr'`) — quyết định `keyPrefix` nào được tra. Phải **bằng `TABLE_NAME` lúc ghi**, không phải tên bảng nào cũng được |
| `DB_NAME` + `TABLE_NAME`                 | Tên bảng trong `FROM` (`demo_db.users_cdr`)                                                                                                          |
| `CRYPTO_ENCRYPTED_COLUMNS` (`name,city`) | Các cột bọc trong `cdr_decrypt(...)`; cột khác (`id`, `created_at`) đọc thẳng                                                                        |
| `CRYPTO_KEY_FIELD` (`created_at`)        | Tham số **thứ 3** của hàm — truyền đúng cột `created_at` của chính dòng đó                                                                           |

Vì `cdr_*` **tất định** (cùng đầu vào → cùng ciphertext), có thể lọc trực tiếp trên cột đã mã hoá khi biết cả giá trị lẫn
`created_at` của dòng, không phải quét giải mã toàn bảng:

```sql
SELECT id, created_at,
       cdr_decrypt('users_cdr', name, created_at) AS name
FROM demo_db.users_cdr
WHERE created_at = '2025-01-01 08:00:00'
  AND name = cdr_encrypt('users_cdr', 'Alice Nguyen', '2025-01-01 08:00:00');
```

Nếu bảng đã ghi bằng manifest column (`spark-application-column.yaml`, hàm `column_encrypt`), dùng cùng khuôn nhưng với
`column_decrypt('<TABLE_NAME>', name, created_at)`.

**Điều kiện để query chạy được** (đã nạp theo mục 4): engine phải có jar + extension `CdrCryptoExtension`, và cấu hình
`spark.cdrcrypto.*` phải trỏ **đúng Vault/`kvBasePath` mà manifest đã dùng để ghi**, nếu không sẽ tra ra prefix khác hoặc lỗi 404.
Ánh xạ từ các biến môi trường của manifest sang Spark conf của engine:

| Biến trong manifest        | Spark conf trên engine             | Ví dụ giá trị                             |
| -------------------------- | ---------------------------------- | ----------------------------------------- |
| `CRYPTO_PREFIX_SOURCE`     | `spark.cdrcrypto.source`           | `vault`                                   |
| `VAULT_ADDR`               | `spark.cdrcrypto.vault.addr`       | `http://vault.cyberspace.vn`              |
| `VAULT_AUTH_METHOD`        | `spark.cdrcrypto.vault.authMethod` | `token`                                   |
| `VAULT_TOKEN`              | `spark.cdrcrypto.vault.token`      | token Vault chỉ đọc                       |
| `VAULT_KV_MOUNT`           | `spark.cdrcrypto.vault.kvMount`    | `kv`                                      |
| `VAULT_KV_PATH`            | `spark.cdrcrypto.vault.kvBasePath` | `hla-datalake/datalake/spark-application` |
| `VAULT_KEY_FIELD` (nếu có) | `spark.cdrcrypto.vault.keyField`   | `keyPrefix`                               |

Lưu ý:

- Bảng phải chỉ chứa dữ liệu mã hoá bằng `cdr_*` với cùng prefix. Nếu bảng còn dòng do `column_encrypt` ghi từ lần chạy trước
  (job dùng `INSERT_MODE=append`), `cdr_decrypt` lỗi `Input length must be multiple of 16` ở dòng đó — dùng bảng riêng cho từng loại
  hoặc `INSERT_MODE=overwrite`.
- Với `created_at` dạng `2025-01-01 08:00:00` (19 ký tự), `keyPrefix` **không còn ảnh hưởng tới key** do đặc tính thuật toán đối tác
  (xem mục 8) — vẫn phải cấu hình đủ để hàm chạy, nhưng đừng coi prefix là lớp bảo vệ trong trường hợp này.

## 7. So sánh khi dùng hai bộ hàm

| Điểm                      | `column_*`                                        | `cdr_*`                                                                    |
| ------------------------- | ------------------------------------------------- | -------------------------------------------------------------------------- |
| Thuật toán                | AES-256-GCM, IV ngẫu nhiên, có tag xác thực       | AES-128-ECB do đối tác quy định, không IV, không tag                       |
| Sinh key                  | `SHA-256(keyPrefix ‖ keyValue)`                   | `keyPrefix + keyValue` nối thô rồi cộng ký tự vào key mặc định của đối tác |
| Cùng đầu vào → ciphertext | **Khác nhau** mỗi lần                             | **Giống nhau** (tất định)                                                  |
| Lọc theo cột đã mã hoá    | Không (`WHERE name = 'x'` không chạy)             | Có thể so sánh ciphertext, vì tất định                                     |
| Sai key khi giải mã       | Lỗi `Tag mismatch`                                | Lỗi `CDR partner decrypt failed` (thường do `BadPaddingException`)         |
| Jar trên executor         | Không cần                                         | Cần `cdr-crypto-udf` + `DataLakeSecurity_jv8`                              |
| Lộ prefix qua `EXPLAIN`   | Được lib che (`spark.sql.redaction.string.regex`) | Không xảy ra (prefix nằm trong closure UDF, không phải literal của plan)   |

## 8. Giới hạn và lưu ý

- **Không so sánh được trên ciphertext của `column_*`**: IV ngẫu nhiên nên `WHERE name = 'x'` không chạy trên cột
  mã hoá. Chỉ lọc được qua `WHERE column_decrypt('customers', name, id) = 'x'`, và phải quét toàn bảng. Cột mã hoá bằng `cdr_*`
  thì tất định nên có thể so sánh trực tiếp ciphertext.
- **Truyền đúng cột `keyField`**: truyền sai thì `column_decrypt` lỗi `Tag mismatch` (GCM phát hiện sai key); `cdr_decrypt` thì lỗi giải mã
  hoặc ra dữ liệu rác. Dùng view (mục 6) để cố định cách gọi.
- **Đừng trộn 2 thuật toán trên cùng một bảng/cột**: cột đã ghi bằng `column_*` mà giải bằng `cdr_decrypt` (hoặc ngược lại) sẽ lỗi
  như `Input length must be multiple of 16` / `Tag mismatch`. Dùng bảng riêng cho từng loại.
- **Độ dài `keyInput` của `cdr_*`**: thuật toán của đối tác ghi đè vòng tròn trên key 16 byte, nên **chỉ 16 ký tự cuối** của
  `keyPrefix + keyField` còn ảnh hưởng tới key. Nếu `keyField` dài từ 16 ký tự trở lên thì `keyPrefix` **không còn tác dụng** (key chỉ
  phụ thuộc `keyField`). Đây là đặc tính của thuật toán đối tác, không sửa được ở phía mình — trao đổi với đối tác về độ dài prefix/field.
- **Phân quyền**: mọi người dùng chạy được SQL trên engine đều gọi được hàm với mọi bảng, vì
  credential Vault (token tĩnh, hoặc role của Kubernetes auth) gắn với chính engine chứ không
  theo từng người dùng SQL. Nên: chỉ cấp SELECT bảng gốc cho pipeline, cấp SELECT trên view
  `*_plain` cho người được xem plaintext, và kiểm chứng Ranger plugin của bạn có kiểm tra bảng
  gốc bên dưới view hay không.
- **`keyPrefix` không lộ ra plan**: với `column_*`, sau lần tra đầu, lib đặt `spark.sql.redaction.string.regex` của
  session để che prefix khỏi `EXPLAIN`, tab SQL của Spark UI và event log; với `cdr_*` prefix nằm trong closure UDF nên không hiện trong plan.
- **sql-engine chỉ cấu hình qua Spark conf**: khác SparkApplication (đọc thêm biến môi trường `VAULT_*`/`CRYPTO_*` làm phương án
  dự phòng), engine tạo trên UI thường không sửa được env của pod, nên phải đặt đủ `spark.columncrypto.*` và/hoặc `spark.cdrcrypto.*`
  như mục 4.
- **Lỗi Vault** hiện ngay ở bước analyze của câu query; thông báo không chứa JWT, token hay prefix.
- **Mất kết nối Vault khi cache đã có**: query vẫn chạy đến hết TTL, sau đó mới lỗi.

## 9. Xử lý sự cố

| Triệu chứng                                                                                   | Nguyên nhân thường gặp                                                                                                                              |
| --------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Undefined function: column_encrypt` / `cdr_encrypt`                                          | Extension chưa nạp: sai tên class trong `spark.sql.extensions`, jar chưa nằm trên classpath lúc khởi động, hoặc chưa restart engine                 |
| `ClassNotFoundException ... ColumnCryptoExtension` / `CdrCryptoExtension`                     | `spark.jars` không nạp kịp lúc khởi tạo session; đặt jar vào `/opt/spark/jars` của image hoặc dùng `spark.driver.extraClassPath`                    |
| `NoClassDefFoundError: vai/lakehouse/keyprefix/...`                                           | Thiếu `key-prefix-lib.jar` trong `spark.jars` (mọi hàm crypto đều cần)                                                                              |
| `NoClassDefFoundError: com/viettel/datalake/security/TransformDL`                             | Dùng `cdr_*` nhưng thiếu `DataLakeSecurity_jv8.jar` trong `spark.jars` (driver lẫn executor)                                                        |
| Engine lỗi lúc khởi động: `CdrCipherCore self-check FAILED`                                   | Jar đối tác đổi version/hành vi so với test vector đã xác nhận — không dùng được cho tới khi kiểm tra lại với đối tác                               |
| `Missing required setting: spark.columncrypto.vault.addr` (hoặc `spark.cdrcrypto.vault.addr`) | Thiếu key bắt buộc trong Spark conf — **đúng namespace của lib đang dùng** (`column_*` đọc `spark.columncrypto.*`, `cdr_*` đọc `spark.cdrcrypto.*`) |
| `Missing required setting: <ns>vault.token`                                                   | `authMethod=token` nhưng thiếu `vault.token` ở namespace đó                                                                                         |
| `Invalid <ns>vault.authMethod`                                                                | Giá trị khác `token`/`kubernetes`, hoặc gõ sai chính tả                                                                                             |
| `Vault read of secret '...' failed: HTTP 403` (chế độ `token`)                                | Token thiếu quyền đọc path `kvBasePath` — kiểm tra lại policy, xem mục 4.4                                                                          |
| `Vault login failed: HTTP 403` (chế độ `kubernetes`)                                          | Role không bind với ServiceAccount/namespace của engine                                                                                             |
| `Vault read of secret '...' failed: HTTP 404`                                                 | Sai `kvBasePath`/`kvMount` hoặc chưa có secret cho **tên bảng** đó (mục 5)                                                                          |
| `has no string field 'keyPrefix'`                                                             | Secret tồn tại nhưng tên field khác `vault.keyField` (mặc định `keyPrefix`) — đổi tên field trong secret hoặc set lại `vault.keyField`              |
| `Cannot read service account token`                                                           | Chỉ xảy ra ở chế độ `kubernetes` (pod tắt automount ServiceAccount token); chế độ `token` không đọc file này                                        |
| `first argument must be a constant string`                                                    | Tham số đầu phải là literal `'tên_bảng'`, không phải cột hay biểu thức                                                                              |
| `With source=dak the table must be '<database>.<table>'`                                      | Đang dùng `source=dak` mà tham số đầu là tên một phần (`'users_cdr'`) hoặc có ký tự lạ — viết lại thành `'demo_db.users_cdr'`                       |
| `'dak' cannot be combined with other sources`                                                 | Cấu hình `source=dak,vault` / `file,dak` — đặt `source=dak`; fallback về Vault phải đổi thủ công                                                    |
| `Invalid <ns>dak.addr: must be an absolute https:// URL` (hoặc `dak.tokenUrl`)                | Sai URL hoặc dùng `http://`. `http://` chỉ được phép khi đặt `<ns>dak.allowInsecureHttp=true` (test local)                                          |
| `Keycloak token request failed: HTTP 401 (invalid_client)`                                    | Sai `dak.clientId`/`dak.clientSecret`, hoặc `dak.tokenUrl` trỏ nhầm realm (client không tồn tại ở realm đó)                                         |
| `Keycloak token request failed: HTTP 400 (unauthorized_client)`                               | Client của team chưa bật Service Accounts — báo vận hành sửa client theo checklist                                                                  |
| `Keycloak token request failed: cannot reach ...`                                             | Không kết nối được Keycloak: kiểm tra mạng/DNS, truststore nếu Keycloak dùng CA nội bộ                                                              |
| `DAK denied key for '...' (HTTP 403 access_denied, requestId=...)`                            | Team chưa được cấp quyền, **quyền đã hết hạn**, hoặc bảng chưa đăng ký trong workspace. Gửi `requestId` cho admin DAK để tra nguyên nhân             |
| `DAK key request ... failed (HTTP 403 key_disabled, ...)`                                     | Key của bảng đang bị khoá trên DAK                                                                                                                  |
| `DAK rejected the access token ... (HTTP 401 ...)`                                            | Client chưa được đăng ký hoặc đã bị khoá trên DAK, hoặc realm chưa có trong registry của DAK                                                        |
| `DAK key request ... failed: cannot reach ...` / `HTTP 503`                                   | DAK hoặc Vault phía DAK không phản hồi. Sự cố kéo dài: fallback thủ công (mục 4.6)                                                                  |
| `Tag mismatch` (`AES_CRYPTO_ERROR`) — `column_decrypt`                                        | Sai bảng (sai prefix), sai cột `keyField`, giải mã cột chưa được mã hoá, hoặc cột mã hoá bằng `cdr_*`                                               |
| `CDR partner decrypt failed` — `cdr_decrypt`                                                  | Sai bảng (sai prefix), sai `keyField`, hoặc dữ liệu không phải do thuật toán CDR mã hoá                                                             |
| `Input length must be multiple of 16 when decrypting`                                         | `cdr_decrypt` gặp giá trị không phải ciphertext ECB, thường do bảng lẫn dữ liệu mã hoá bằng `column_*` hoặc cột chưa mã hoá                         |

Xem log driver của engine để biết extension có nạp được không: lỗi khởi tạo extension in ra ngay
lúc engine khởi động.
