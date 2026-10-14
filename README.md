# sample-spark-application-privacy

Bộ module bảo mật dữ liệu cho **Spark 3.5.1 (Scala 2.12)** chạy trên Kubernetes (Spark Operator), đọc/ghi HDFS trên
Dell PowerScale/Isilon. Giải 4 bài toán:

| # | Bài toán | Giải pháp | Module |
| --- | --- | --- | --- |
| 1 | Isilon ép wire encryption nhưng block token sai chuẩn Apache → Hadoop client ≥ 3.2.1 fail mọi đọc/ghi HDFS | Patch `LenientBlockTokenIdentifier`, giữ nguyên mã hoá và Kerberos | `spark-app` |
| 2 | Mã hoá cột nhạy cảm, dùng được cả Spark job lẫn SQL console, hai đường cho cùng kết quả | `column_encrypt`/`column_decrypt` (Catalyst built-in AES-256-GCM), key theo từng dòng | `column-crypto-lib` |
| 3 | Đọc/ghi CDR theo thuật toán riêng của đối tác (chỉ có thư viện decrypt) | UDF bọc thư viện đối tác + tự viết encrypt | `cdr-crypto-udf` |
| 4 | Khử định danh (giả danh hoá) số thuê bao / IMEI / mã KH mà vẫn liên kết được bản ghi | HASH / ENCRYPT / MAPPING qua UDF; app khử định danh bảng hoặc file; utils kiểm tra | `pseudonym-udf`, `pseudonymize-table-app`, `pseudonym-verify-utils` |

Mọi lib lấy khoá/keyPrefix qua hạ tầng chung `key-prefix-lib` (K8s Secret / Vault / DAK).

## 1. Cấu trúc

| Module / thư mục | Vai trò | Chi tiết |
| --- | --- | --- |
| `key-prefix-lib` | Hạ tầng lấy keyPrefix/khoá: file (Secret mount), Vault, DAK; có cache. Không chứa thuật toán mã hoá | [`docs/key-access/KEY_PREFIX_SOURCES.md`](./docs/key-access/KEY_PREFIX_SOURCES.md) |
| `spark-app` | App mẫu + patch block token (`vai.lakehouse.hdfs.*`); gọi hàm crypto theo tên, không nhúng lib crypto | [`docs/project/SPARK_APP_REFERENCE.md`](./docs/project/SPARK_APP_REFERENCE.md) |
| `column-crypto-lib` | Bài toán 2 | [`docs/column-crypto/COLUMN_CRYPTO_ARCHITECTURE.md`](./docs/column-crypto/COLUMN_CRYPTO_ARCHITECTURE.md) |
| `cdr-crypto-udf` | Bài toán 3 (profile `cdr-crypto`, cần jar đối tác) | [`docs/cdr-crypto/PARTNER_CDR_CRYPTO_UDF_PLAN.md`](./docs/cdr-crypto/PARTNER_CDR_CRYPTO_UDF_PLAN.md) |
| `pseudonym-udf` | Bài toán 4: lib giả danh (DataFrame API + SQL `pseudo_hash`/`pseudo_encrypt`) | [`docs/pseudonymization/`](./docs/pseudonymization/README.md) |
| `pseudonymize-table-app` | Bài toán 4: khử định danh **bảng hoặc file/thư mục**, mỗi cột một chế độ → bảng đích. Dockerfile + 2 manifest | [`pseudonymize-table-app/README.md`](./pseudonymize-table-app/README.md) |
| `pseudonym-verify-utils` | Bài toán 4: so bảng nguồn ↔ đích, và `selftest` trên dữ liệu tự sinh. Jar mỏng, không Docker/manifest | [`pseudonym-verify-utils/README.md`](./pseudonym-verify-utils/README.md) |
| `hdfs-load-stress-test` | Job sinh tải ghi HDFS (chỉ patch block token) | [`hdfs-load-stress-test/README.md`](./hdfs-load-stress-test/README.md) |
| `dak-mock` | Mock DAK cho test lấy key | [`dak-mock/README.md`](./dak-mock/README.md) |
| `k8s/` | Manifest `spark-app` (column / cdr), Secret mẫu, StatefulSet sql-engine, pod lấy jar | — |
| `Dockerfile`, `Dockerfile.base`, `Dockerfile.jar-carrier` | Image `spark-app` (target `no-crypto`/`column-crypto`/`cdr-crypto`), base có sẵn patch, image mang jar vào prod | [`docs/project/BUILD_AND_TEST.md`](./docs/project/BUILD_AND_TEST.md) |
| `internal-repo/` | Cấu hình Nexus/Maven để build ở máy nội bộ công ty | [`internal-repo/README.md`](./internal-repo/README.md) |
| `docs/` | Tài liệu kỹ thuật (mục 5) | — |

Jar của `spark-app` và các app **không nhúng** lib crypto: lib là jar rời, nạp lúc deploy (`spark.jars` +
`spark.sql.extensions`); loại crypto nào được dùng do image target + manifest quyết định.

## 2. Build & test

JDK 11+, Maven 3.8+. Chạy ở thư mục gốc; dùng `-am` khi build một module để Maven tự build module phụ thuộc.

```bash
mvn -B clean package                  # mọi module mặc định (không cần jar đối tác)
mvn -B -Pcdr-crypto clean package     # thêm cdr-crypto-udf (cần DataLakeSecurity_jv8.jar đã cài vào ~/.m2)
mvn -B -pl pseudonymize-table-app -am clean package     # một module + phụ thuộc
```

| Artifact | Dùng cho |
| --- | --- |
| `spark-app/target/sample-spark-application-privacy-1.0-SNAPSHOT.jar` (+ `-block-token-patch.jar`) | Chạy app mẫu; jar patch nhét được vào image khác (vd Spark History Server) |
| `key-prefix-lib`, `column-crypto-lib`, `cdr-crypto-udf`, `pseudonym-udf` `-1.0-SNAPSHOT.jar` | Jar lib thuần, nạp rời qua `spark.jars` (cùng `key-prefix-lib`) |
| `pseudonymize-table-app/target/pseudonymize-table-app.jar` | Chạy SparkApplication khử định danh |
| `pseudonym-verify-utils/target/pseudonym-verify-utils.jar` | `spark-submit`/spark-shell kiểm tra bảng đã khử định danh |

Lệnh từng module, bảng test, build & push image: [`docs/project/BUILD_AND_TEST.md`](./docs/project/BUILD_AND_TEST.md).
Test chạy `local[*]`, không cần cluster; phần I/O thật với Isilon và sql-engine phải verify trên cluster.

## 3. Các bài toán, tóm tắt

**1. Wire encryption + block token (Isilon).** Isilon ép `AES/CTR` 256-bit; block token dùng định dạng nội bộ nên client
≥ 3.2.1 parse lỗi. Patch đăng ký `LenientBlockTokenIdentifier` đè entry `HDFS_BLOCK_TOKEN` (ServiceLoader), coi token là
chuỗi byte đục; mã hoá và Kerberos **không đổi**. Đây là workaround — khi Dell sửa OneFS thì gỡ package
`vai.lakehouse.hdfs`. Yêu cầu build: shade có `ServicesResourceTransformer`, không relocate `org.apache.hadoop.*`.
Phân tích: [`SPARK_HDFS_WIRE_ENCRYPTION_TASK.md`](./docs/hdfs-block-token/SPARK_HDFS_WIRE_ENCRYPTION_TASK.md),
[`HDFS_BLOCK_TOKEN_PATCH_FLOW.md`](./docs/hdfs-block-token/HDFS_BLOCK_TOKEN_PATCH_FLOW.md),
[`HDFS_PATCH_AT_SCALE.md`](./docs/hdfs-block-token/HDFS_PATCH_AT_SCALE.md).

**2. Mã hoá cột.** `key = SHA-256(keyPrefix ‖ giá trị cột keyField của chính dòng)`, AES-256-GCM qua `aes_encrypt`.
Cùng một công thức cho DataFrame API và SQL function nên hai đường giải mã được của nhau. `keyField` không được nằm trong
cột mã hoá. [`COLUMN_CRYPTO_ARCHITECTURE.md`](./docs/column-crypto/COLUMN_CRYPTO_ARCHITECTURE.md),
[`COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md`](./docs/column-crypto/COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md).

**3. CDR đối tác.** `cdr_encrypt`/`cdr_decrypt` gọi đúng `decrypt()` của thư viện đối tác và tự viết `encrypt()`
(AES-128-ECB). Là UDF thật nên executor cũng cần đủ jar. Jar đối tác không commit; chỉ build với profile `cdr-crypto`.

**4. Khử định danh.** Ba chế độ, chọn theo từng cột:

| Chế độ | Output | Đảo ngược |
| --- | --- | --- |
| `HASH` (mặc định) | `k1_<32 hex>` — HMAC-SHA256 có khoá | Không |
| `ENCRYPT` | `e1_<base64url>` — AES-256-SIV tất định | Có, bằng khoá |
| `MAPPING` | `t_<32 hex>` ngẫu nhiên + bảng ánh xạ Delta lưu riêng | Join ngược bảng ánh xạ |

Cùng giá trị gốc → cùng định danh giả (join/đếm/gom nhóm được). **Không nạp extension `pseudo_*` lên sql-engine của
analyst**: ai gọi được `pseudo_hash` thì liệt kê không gian SĐT là đảo ngược được.

```
nguồn (bảng hoặc INPUT_PATH) ──pseudonymize-table-app──▶ bảng đích ──pseudonym-verify-utils──▶ PASS/FAIL
```

- Khử định danh: `PSEUDO_COLUMNS=isdn:HASH,imei:ENCRYPT`, nguồn bảng (`DB_NAME`+`TABLE_NAME`) hoặc file
  (`INPUT_PATH`+`INPUT_FORMAT`: parquet/orc/avro/json/csv/txt…). Env của lib, manifest, exit code: README của app.
- Kiểm tra: số dòng, `NULL`, 1-1, định dạng, không sót giá trị gốc, cột khác không đổi; `selftest` tự sinh dữ liệu
  và nghiệm thu lib bằng khoá tạm.
- Cơ chế từng thuật toán kèm ví dụ: [`docs/pseudonymization/ALGORITHMS.md`](./docs/pseudonymization/ALGORITHMS.md).

## 4. Deploy & verify (spark-app)

```bash
kubectl apply -f k8s/spark-application-column.yaml    # hoặc k8s/spark-application-cdr.yaml, khớp image target
kubectl logs <driver-pod> -n <namespace>
```

Log driver phải có `server encryptDataTransfer : true`, `HDFS_BLOCK_TOKEN handler : …LenientBlockTokenIdentifier` và
`[ OK ] Patch ĐANG hoạt động`. Nếu `Patch KHÔNG hoạt động`: kiểm tra jar trong `spark.jars`/`mainApplicationFile` và
`userClassPathFirst`. Biến môi trường, flow, logging, checklist đầy đủ:
[`docs/project/SPARK_APP_REFERENCE.md`](./docs/project/SPARK_APP_REFERENCE.md). Với app khử định danh: `pseudonymize-table-app/README.md`
mục 6.

**Tuyệt đối không:**

| Không làm | Lý do |
| --- | --- |
| Hạ `dfs.data.transfer.protection` | Vi phạm yêu cầu bắt buộc |
| `spark.*.extraClassPath` cho jar patch | Nạp trước Hadoop → patch thua last-write-wins |
| `spark.*.userClassPathFirst=true` | Đảo classpath → patch mất tác dụng |
| Relocate `org.apache.hadoop.*` trong shade | Class con kế thừa sai class → `instanceof` fail |
| Đóng gói Spark/Hadoop vào fat jar | Xung đột runtime, vỡ `META-INF/services` |
| Bỏ `ServicesResourceTransformer` | File đăng ký ServiceLoader biến mất → patch vô hiệu |
| Sửa `hadoop-client-api-3.3.4.jar` trong image | Checksum lạ → cờ đỏ supply-chain |
| Hạ Hadoop client xuống 3.2.0 | Spark 3.5 build trên API 3.3.x, vỡ chỗ khác |

## 5. Tài liệu

Mục lục theo bài toán: [`docs/README.md`](./docs/README.md).

| Cần gì | Đọc |
| --- | --- |
| Build, test, image | [`docs/project/BUILD_AND_TEST.md`](./docs/project/BUILD_AND_TEST.md) |
| Env, flow, deploy `spark-app` | [`docs/project/SPARK_APP_REFERENCE.md`](./docs/project/SPARK_APP_REFERENCE.md) |
| Nguồn keyPrefix (file / Vault / DAK) | [`docs/key-access/KEY_PREFIX_SOURCES.md`](./docs/key-access/KEY_PREFIX_SOURCES.md) |
| Bài toán 1 — block token | [`docs/hdfs-block-token/`](./docs/hdfs-block-token/SPARK_HDFS_WIRE_ENCRYPTION_TASK.md) |
| Bài toán 2 — mã hoá cột | [`docs/column-crypto/`](./docs/column-crypto/COLUMN_CRYPTO_ARCHITECTURE.md) |
| Bài toán 3 — CDR đối tác | [`docs/cdr-crypto/`](./docs/cdr-crypto/PARTNER_CDR_CRYPTO_UDF_PLAN.md) |
| Bài toán 4 — khử định danh | [`docs/pseudonymization/`](./docs/pseudonymization/ALGORITHMS.md) |
| DAK (API, plan) | [`docs/key-access/`](./docs/key-access/DAK_API_SPEC.md) |
| Vận hành (Ranger, Airflow, History Server) | [`docs/operations/`](./docs/operations/RANGER_AIRFLOW_MONITORING_GUIDE.md), [`docs/hdfs-block-token/spark-history-server-hdfs-hkh-kerberos.md`](./docs/hdfs-block-token/spark-history-server-hdfs-hkh-kerberos.md) |
| Stress test HDFS | [`docs/hdfs-load-stress-test/PLAN.md`](./docs/hdfs-load-stress-test/PLAN.md) |
