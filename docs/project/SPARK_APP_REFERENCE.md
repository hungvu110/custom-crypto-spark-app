# Tham chiếu `spark-app` (bài toán 1) — biến môi trường, flow, logging, deploy & verify

> Tách ra từ README gốc. `spark-app` là app mẫu minh hoạ patch block token (bài toán 1) và cách nạp lib crypto (bài toán 2/3).
> Nguồn keyPrefix (file/Vault/DAK) ở [`KEY_PREFIX_SOURCES.md`](../key-access/KEY_PREFIX_SOURCES.md).

App không nhận tham số dòng lệnh — toàn bộ cấu hình đọc qua biến môi trường (tất cả optional,
có default; validate lúc khởi động, sai giá trị sẽ `exit(2)` — cùng logic với
`vlp-spark-sample/MainApp.scala`):

| Biến                                                                      | Mặc định                                                                               | Giá trị hợp lệ               | Mô tả                                                                                                                        |
| ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `APP_NAME`                                                                | `SampleSparkApplicationPrivacy`                                                        | chuỗi bất kỳ                 | Tên hiển thị trên Spark UI                                                                                                   |
| `DB_NAME`                                                                 | `demo_db`                                                                              | chuỗi bất kỳ; khi bật mã hoá với `CRYPTO_PREFIX_SOURCE=dak`: chỉ `[A-Za-z0-9_]`, 1–128 ký tự | Database/schema sẽ được tạo. Cùng `TABLE_NAME` tạo thành tên key `database.table` (chữ thường) để tra keyPrefix |
| `TABLE_NAME`                                                              | `sample_table`                                                                         | như `DB_NAME`                | Bảng sẽ được tạo                                                                                                             |
| `TABLE_TYPE`                                                              | `delta`                                                                                | `delta` / `iceberg` / `hive` | `hive` → `STORED AS PARQUET`; còn lại → `USING <type>`                                                                       |
| `INSERT_MODE`                                                             | `append`                                                                               | `append` / `overwrite`       | Chế độ ghi dữ liệu mẫu                                                                                                       |
| `ENABLE_HIVE_SUPPORT`                                                     | `true`                                                                                 | `true` / `false`             | Bật `enableHiveSupport()` cho SparkSession                                                                                   |
| `CRYPTO_ENCRYPT_FUNCTION`, `CRYPTO_DECRYPT_FUNCTION` | (rỗng = KHÔNG mã hoá) | tên hàm SQL, vd `column_encrypt`/`column_decrypt` hoặc `cdr_encrypt`/`cdr_decrypt` | Hàm do extension (nạp qua `spark.sql.extensions`) đăng ký; phải đặt CẢ 2 hoặc để trống CẢ 2. App không nhúng lib crypto, chỉ gọi hàm theo tên — xem mục "Mã hoá cột" |
| `CRYPTO_ENCRYPTED_COLUMNS` | (bắt buộc khi bật mã hoá) | danh sách cột, phân cách bằng dấu phẩy | Các cột bị mã hoá, vd `name,city` |
| `CRYPTO_KEY_FIELD` | (bắt buộc khi bật mã hoá) | tên cột | Cột KHÔNG mã hoá, giá trị của nó (theo từng dòng) dùng làm nguyên liệu sinh key; không được nằm trong `CRYPTO_ENCRYPTED_COLUMNS` |
| `CRYPTO_PREFIX_SOURCE` | `file` | `dak` / `file` / `vault` / `file,vault`. **`dak` không được ghép** với nguồn khác (`dak,vault` bị từ chối) | **Do extension đọc, không phải app**: nguồn keyPrefix — DAK (manifest mẫu đang dùng), thư mục file (K8s Secret) hoặc HashiCorp Vault. Dùng chung cho `column_*` lẫn `cdr_*` — xem mục "Chọn nguồn keyPrefix" |
| `CRYPTO_CACHE_TTL_SECONDS` | `300` | số nguyên ≥ 0 (`0` = tắt cache) | Thời gian cache keyPrefix theo bảng trên driver; áp dụng cho mọi nguồn |
| `DAK_ADDR` | (bắt buộc khi `dak`) | URL `https://…`; `http://` chỉ khi `DAK_ALLOW_INSECURE_HTTP=true` | Base URL của DAK, vd `http://dak-mock:8085` (dak-mock cùng namespace). Lib gọi `GET <DAK_ADDR>/api/v1/keys/<database>/<table>` |
| `DAK_TOKEN_URL` | (bắt buộc khi `dak`) | URL `https://…` (`http://` như trên) | Token endpoint Keycloak của **realm của tenant**, vd `https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/token` |
| `DAK_CLIENT_ID` | (bắt buộc khi `dak`) | chuỗi | Client Keycloak của team (`client_credentials`), vd `team-de-ws8` |
| `DAK_CLIENT_SECRET` | (bắt buộc khi `dak`) | chuỗi | **Nhạy cảm**: lấy từ K8s Secret `spark-dak-client` (key `clientSecret`) qua `secretKeyRef`, không ghi thẳng vào manifest |
| `DAK_ALLOW_INSECURE_HTTP` | `false` | `true` / `false` | `true` cho phép `DAK_ADDR`/`DAK_TOKEN_URL` dùng `http://` — **chỉ** khi test với dak-mock; DAK thật phải bỏ biến này |
| `CRYPTO_KEY_PREFIX_DIR` | `conf/key-prefix` (đọc đĩa rồi classpath; jar app không còn bản mẫu — luôn đặt tường minh) | đường dẫn thư mục | Chỉ khi `file`: thư mục chứa keyPrefix, mỗi bảng 1 file `<database>.<table>` (nhạy cảm, mount K8s Secret `key-prefix`) |
| `VAULT_ADDR`, `VAULT_KV_PATH`                                             | (bắt buộc khi `vault`)                                                                 | chuỗi                        | Địa chỉ Vault, đường dẫn KV chứa prefix (không gồm tên key; secret con là `<VAULT_KV_PATH>/<database>.<table>`)              |
| `VAULT_AUTH_METHOD`                                                       | `kubernetes`                                                                           | `kubernetes` / `token`       | Cách đăng nhập Vault                                                                                                         |
| `VAULT_ROLE`                                                              | (bắt buộc khi `kubernetes`)                                                            | chuỗi                        | Tên role Kubernetes auth                                                                                                     |
| `VAULT_TOKEN`                                                             | (bắt buộc khi `token`)                                                                 | chuỗi                        | Token Vault tĩnh, nhạy cảm — lấy qua `secretKeyRef`                                                                          |
| `VAULT_KV_MOUNT`, `VAULT_AUTH_MOUNT`, `VAULT_KEY_FIELD`, `VAULT_JWT_PATH` | `kv`, `kubernetes`, `keyPrefix`, `/var/run/secrets/kubernetes.io/serviceaccount/token` | chuỗi                        | Mount KV v2, mount auth, tên field chứa prefix trong secret, file JWT của service account                                    |
| `HDFS_SASL_DEBUG`                                                         | `false`                                                                                | `true` / `false`             | Bật DEBUG cho 2 logger SASL/Token của Hadoop — in ra hex dump wrap/unwrap rất dài, chỉ bật khi cần trace lỗi wire encryption |

Flow nghiệp vụ (`SparkApp.runCreateTableAndInsert`):

1. `CREATE DATABASE IF NOT EXISTS ${DB_NAME}`
2. `CREATE TABLE IF NOT EXISTS ${DB_NAME}.${TABLE_NAME} (id BIGINT, name STRING, city STRING, created_at STRING) USING delta` (hoặc `STORED AS PARQUET` nếu `TABLE_TYPE=hive`)
3. Nếu bật mã hoá (`CRYPTO_ENCRYPT_FUNCTION`): dựng DataFrame đã mã hoá các cột `CRYPTO_ENCRYPTED_COLUMNS` bằng hàm SQL của extension — bước này chạy TRƯỚC mọi thao tác ghi, nên hàm chưa nạp hoặc keyPrefix hỏng thì fail sớm (extension tự tra keyPrefix và che khỏi plan/Spark UI); insert 5 dòng dữ liệu mẫu bằng `insertInto` (mode = `INSERT_MODE`). Không bật thì ghi plaintext
4. `SELECT * FROM ${DB_NAME}.${TABLE_NAME} ORDER BY id`, giải mã lại các cột đó, in kết quả plaintext

- Trước khi chạy nghiệp vụ, app in mục **HDFS SECURITY DIAGNOSTICS**: các config bảo mật,
  `encryptDataTransfer` của server, và **class đang xử lý `HDFS_BLOCK_TOKEN`**
  (patch có đang hoạt động không).
- Sau mỗi bước (CREATE DATABASE / CREATE TABLE / INSERT), app log permission/owner của
  thư mục HDFS tương ứng (`DB_DIR`, `TABLE_DIR`) — hữu ích để trace lỗi quyền trên Isilon.
- Khi job fail vì block token, app tự nhận diện và chỉ thẳng tới nguyên nhân/patch chưa nạp.

### Logging

`log4j2.properties`: root logger `warn`; chỉ logger nghiệp vụ `VLP` (dùng bởi `Log.scala`)
ở `info`; toàn bộ nhánh `org.apache.spark`/`org.apache.hadoop`/`io.delta` hạ xuống `warn`,
scheduler K8s + fabric8 client xuống `error`. Hai logger
`org.apache.hadoop.hdfs.protocol.datatransfer.sasl` và
`org.apache.hadoop.security.token.Token` giữ `warn` làm baseline — chỉ được app nâng lên
`DEBUG` khi set `HDFS_SASL_DEBUG=true`.

#### Trên Kubernetes: BẮT BUỘC trỏ bằng `-Dlog4j.configurationFile`

Đây là điểm dễ sai nhất. Khi chạy trên K8s, Spark tự tạo ConfigMap
(`spark-conf-volume-driver`/`spark-conf-volume-exec`) và **mount đè lên `/opt/spark/conf`**
của pod (hằng số `SPARK_CONF_DIR_INTERNAL` trong `spark-kubernetes` jar). Volume mount của
K8s thay thế **toàn bộ** nội dung thư mục → `log4j2.properties` bake trong image tại đó **bị
che hoàn toàn**. Log4j2 không thấy config nào nên rơi về bản mặc định nhúng trong `spark-core`
(`org/apache/spark/log4j2-defaults.properties`, `rootLogger.level = info`) → log INFO của
`TaskSetManager`/`DAGScheduler`/`MemoryStore`... tràn ra dù `log4j2.properties` của project đã
cấu hình đúng.

Vì vậy `Dockerfile` copy file vào **2 chỗ**, và `k8s/spark-application-*.yaml` trỏ tường minh
tới bản không bị che:

| Path trong image                    | Dùng khi                                                                                                                                                           |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `/opt/app/log4j2.properties`        | Chạy trên K8s — trỏ tới qua `-Dlog4j.configurationFile=file:/opt/app/log4j2.properties` ở cả `spark.driver.extraJavaOptions` lẫn `spark.executor.extraJavaOptions` |
| `/opt/spark/conf/log4j2.properties` | Chạy KHÔNG qua K8s (`docker run`, spark-submit local trong container) — lúc đó không có ConfigMap mount đè                                                         |

Kiểm chứng nhanh trên cluster: `kubectl exec <driver-pod> -- ls -la /opt/spark/conf` sẽ **không**
thấy `log4j2.properties` (chỉ có `spark.properties` của ConfigMap) — đúng như mô tả ở trên.

## Deploy & verify trên Kubernetes

```bash
kubectl apply -f k8s/spark-application-column.yaml    # hoặc k8s/spark-application-cdr.yaml, khớp với image target
kubectl logs <driver-pod> -n vlp-tenantw1xjixm-wsytjjtr0-ingestion
```

Các config quan trọng trong `k8s/spark-application-*.yaml`:

| Config                                                                              | Lý do                                                                                                                      |
| ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `mainClass: org.example.SparkApp` + `driver/executor.env`                           | Entry point; app đọc config qua env (`DB_NAME`, `TABLE_NAME`, `TABLE_TYPE`, `INSERT_MODE`, ... — sai giá trị sẽ `exit(2)`) |
| `spark.sql.extensions` / `spark.sql.catalog.spark_catalog`                          | Delta extension bắt buộc khi `TABLE_TYPE=delta` (mặc định); NỐI THÊM extension crypto bằng dấu phẩy (`ColumnCryptoExtension` hoặc `CdrCryptoExtension`) |
| `spark.jars`                                                                        | Đường dẫn `local:///opt/app/...` tới các jar lib crypto (khớp target image); jar cdr cần đủ 3 jar và executor cũng nạp     |
| `CRYPTO_ENCRYPT_FUNCTION` / `CRYPTO_DECRYPT_FUNCTION` / `CRYPTO_ENCRYPTED_COLUMNS` / `CRYPTO_KEY_FIELD` | Chọn hàm SQL và cột mã hoá; trống cả 2 tên hàm = không mã hoá                                          |
| `hadoopConfigMap: hdfs-hadoop-hkh`                                                  | Cấp `core-site.xml`/`hdfs-site.xml` của cụm HKH                                                                            |
| `spark.kerberos.principal / keytab / access.hadoopFileSystems`                      | Kerberos login và lấy delegation token cho HDFS                                                                            |
| `spark.hadoop.hadoop.security.authentication / authorization`                       | Tiền tố `hadoop.` lặp lại là **đúng**: Spark strip `spark.hadoop.` rồi set phần còn lại vào Hadoop Configuration           |
| `spark.driver.userClassPathFirst` / `spark.executor.userClassPathFirst` = `"false"` | **Bắt buộc** — bật `true` đảo thứ tự classpath và patch mất tác dụng                                                       |
| `-Dsun.security.krb5.debug=true` (driver/executor)                                  | Debug Kerberos khi verify                                                                                                  |

### Checklist verify

1. Mục `HDFS SECURITY DIAGNOSTICS` trong log driver phải có:
   - `server encryptDataTransfer : true`
   - `HDFS_BLOCK_TOKEN handler : vai.lakehouse.hdfs.LenientBlockTokenIdentifier`
   - `[ OK ] Patch ĐANG hoạt động`
2. Các mục `CREATE DATABASE`, `CREATE TABLE`, `INSERT SAMPLE DATA`, `QUERY KẾT QUẢ` xuất hiện
   tuần tự; `row count` ở bước cuối khớp số dòng vừa insert (5 dòng, hoặc bội số của 5 nếu
   `INSERT_MODE=append` và chạy job nhiều lần).
3. Log executor (DEBUG) có `SASL client doing encrypted handshake` và
   `Handshake secret is null, sending without handshake secret`. Nếu thấy
   `SASL client skipping handshake` → mã hoá **đã bị tắt**, sai yêu cầu, phải điều tra lại.
4. Kiểm tra dữ liệu qua Spark SQL hoặc trực tiếp trên HDFS:
   ```bash
   hdfs dfs -ls /user/hive/warehouse/demo_db.db/sample_table
   ```

Nếu mục `HDFS SECURITY DIAGNOSTICS` báo `Patch KHÔNG hoạt động`: kiểm tra jar có trong `spark.jars`/`mainApplicationFile`
chưa, `userClassPathFirst` có bị bật không — xem
[`SPARK_HDFS_WIRE_ENCRYPTION_TASK.md`](../hdfs-block-token/SPARK_HDFS_WIRE_ENCRYPTION_TASK.md) mục 15
(fallback bằng `spark.plugins: vai.lakehouse.hdfs.BlockTokenFixPlugin`).
