# Dùng `cdr_encrypt` / `cdr_decrypt` trên sql-engine

Hướng dẫn cho người cấu hình và người dùng sql-engine (Spark Thrift Server) cần mã hoá/giải mã CDR theo thuật toán của đối tác,
kèm audit log mỗi lần hàm được dùng. Bản đầy đủ cho cả `column_*` lẫn `cdr_*` (Vault, Keycloak CA, so sánh hai bộ hàm):
[COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md](../column-crypto/COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md). Thiết kế audit:
[CDR_CRYPTO_AUDIT_LOG_PLAN.md](CDR_CRYPTO_AUDIT_LOG_PLAN.md).

Hai hàm:

| Hàm | Cú pháp | Kết quả |
|---|---|---|
| `cdr_encrypt` | `cdr_encrypt('<database>.<table>', '<tên cột>', <giá trị cần mã hoá>, <cột keyField>)` | Ciphertext dạng Base64 (`STRING`) |
| `cdr_decrypt` | `cdr_decrypt('<database>.<table>', '<tên cột>', <giá trị đã mã hoá>, <cột keyField>)` | Giá trị gốc (`STRING`) |

**Mỗi cột một key.** Tham số 2 (hằng chuỗi, trong dấu nháy) là tên cột dùng để xin key từ DAK; tham số 3 là dữ liệu của từng dòng
(thường chính là cột đó, nhưng có thể là biểu thức hay hằng số). Ví dụ `cdr_encrypt('demo_db.sub_rel_product', 'isdn', isdn,
start_datetime)`: `'isdn'` chọn key, `isdn` là số điện thoại của từng dòng. Tách hai tham số để key vẫn đúng khi cột đã bị đổi tên
(alias, view) hoặc khi tìm theo một giá trị cụ thể.

Hàm chạy theo **từng dòng**: key của mỗi dòng là `key của cột + giá trị cột keyField của dòng đó`. Cú pháp 3 tham số cũ (key theo
bảng) không còn: gọi 3 tham số sẽ báo lỗi kèm ví dụ cú pháp mới. Thiết kế:
[CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md](CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md).

## 1. Chuẩn bị jar

Cần **3 jar**, nạp cho cả driver lẫn executor:

| Jar | Vai trò |
|---|---|
| `key-prefix-lib-1.0-SNAPSHOT.jar` | Lấy `keyPrefix` từ DAK / Vault / file |
| `cdr-crypto-udf-1.0-SNAPSHOT.jar` | Đăng ký `cdr_encrypt`/`cdr_decrypt`, audit log |
| `DataLakeSecurity_jv8.jar` | Thư viện mã hoá của đối tác, nạp nguyên trạng |

Build (một lần cài jar đối tác vào `~/.m2` theo [BUILD_AND_TEST.md](../project/BUILD_AND_TEST.md), rồi):

```bash
mvn -Pcdr-crypto -pl cdr-crypto-udf -am clean package
# -> key-prefix-lib/target/key-prefix-lib-1.0-SNAPSHOT.jar
# -> cdr-crypto-udf/target/cdr-crypto-udf-1.0-SNAPSHOT.jar
```

Đưa cả 3 jar lên HDFS, ví dụ `hdfs:///libs/column-crypto-lib/`. Khi cập nhật bản mới phải thay **cả** `key-prefix-lib` và `cdr-crypto-udf`
cùng lúc (bản `cdr-crypto-udf` key theo cột cần `key-prefix-lib` bản có `ColumnKeySource`, gọi DAK `/api/v1/keys/by-column`).

## 2. Cấu hình cần truyền vào sql-engine

### 2.1. Đặt cấu hình ở đâu

Điền vào ô Spark config của engine. Trên engine dạng StatefulSet, cấu hình này nằm trong `spark-properties.conf.template`
(ConfigMap `en-<engineId>-spark-properties`), được `entrypoint.sh` render bằng `envsubst` lúc khởi động.

- Mọi key đều là cấu hình **tĩnh**: sửa xong phải **restart engine**.
- Vì đi qua `envsubst`, giá trị có ký tự `$` (thường gặp trong secret) sẽ bị thay thành rỗng hoặc giá trị biến môi trường. Kiểm tra
  `clientSecret` trước khi dán; nếu có `$` thì đưa secret vào biến môi trường của pod thay vì Spark conf.

### 2.2. Bộ cấu hình đầy đủ (DAK + audit)

Đây là cấu hình đang dùng: lấy key theo cột qua DAK bằng **client nội bộ** DAK cấp cho luồng này, bật audit ghi vào log của pod.

```properties
# --- 1. Nạp lib và đăng ký hàm ---
spark.jars=hdfs:///libs/column-crypto-lib/key-prefix-lib-1.0-SNAPSHOT.jar,hdfs:///libs/column-crypto-lib/cdr-crypto-udf-1.0-SNAPSHOT.jar,hdfs:///libs/column-crypto-lib/DataLakeSecurity_jv8.jar
spark.sql.extensions=<extension đang có>,vai.lakehouse.columncrypto.cdr.CdrCryptoExtension

# --- 2. Lấy key theo cột qua DAK (GET /api/v1/keys/by-column) ---
spark.cdrcrypto.source=dak
spark.cdrcrypto.dak.addr=https://dak.<domain>
spark.cdrcrypto.dak.tokenUrl=https://keycloak.<domain>/realms/<realm của tenant>/protocol/openid-connect/token
spark.cdrcrypto.dak.clientId=<client_id nội bộ>
spark.cdrcrypto.dak.clientSecret=<client_secret nội bộ>
spark.cdrcrypto.dak.tag=internal
spark.cdrcrypto.dak.caller=en-sqlzqfu6n7
# 0 = không cache key: DAK khoá key của cột thì query kế tiếp lỗi ngay (mục 2.6)
spark.cdrcrypto.cacheTtlSeconds=0

# --- 3. Audit log ---
spark.cdrcrypto.audit.enabled=true
spark.cdrcrypto.audit.sinks=log
spark.cdrcrypto.audit.tenant=tenantw1xjixm
spark.cdrcrypto.audit.workspace=wsw7vtwvi
spark.cdrcrypto.audit.team=teamtscauiy
spark.cdrcrypto.audit.engineId=en-sqlzqfu6n7
```

Thay các giá trị `<...>` và bốn giá trị định danh engine (mục 2.4) theo engine của bạn. Nếu engine đã dùng `column_encrypt`, giữ nguyên
các key `spark.columncrypto.*` và jar/extension của nó; chỉ thêm các dòng trên (nối `CdrCryptoExtension` vào `spark.sql.extensions`
bằng dấu phẩy, không ghi đè).

### 2.3. Ý nghĩa từng key

**Nhóm 1 — nạp lib**

| Key | Bắt buộc | Ghi chú |
|---|---|---|
| `spark.jars` | Có | Đủ 3 jar ở mục 1, ngăn cách bằng dấu phẩy. Đọc từ HDFS cần Kerberos đã login (xem [mục 3 của hướng dẫn chung](../column-crypto/COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md#3-build-và-đặt-jar)) |
| `spark.sql.extensions` | Có | Thêm `vai.lakehouse.columncrypto.cdr.CdrCryptoExtension`, **nối** vào extension sẵn có (vd Ranger) |

**Nhóm 2 — lấy key theo cột qua DAK** (namespace `spark.cdrcrypto.`, riêng với `spark.columncrypto.` của `column_*`)

| Key | Bắt buộc | Ghi chú |
|---|---|---|
| `spark.cdrcrypto.source` | Có | `dak`. Key theo cột chỉ có ở DAK: giá trị khác bị từ chối, không có fallback về Vault (mục 2.5) |
| `spark.cdrcrypto.dak.addr` | Có | Base URL của DAK, `https://` |
| `spark.cdrcrypto.dak.tokenUrl` | Có | Token endpoint của realm Keycloak của tenant, `https://` |
| `spark.cdrcrypto.dak.clientId` | Có | **Client nội bộ** của luồng `by-column`, do DAK cấp (không phải client của team) |
| `spark.cdrcrypto.dak.clientSecret` | Có | Spark che giá trị này trên UI và `SET` |
| `spark.cdrcrypto.dak.tag` | Có (client nội bộ) | `internal`. Thiếu thì DAK trả `400 invalid_tag`. Client team (khi DAK có grant theo cột) thì để trống |
| `spark.cdrcrypto.dak.caller` | Không | Tên engine, gửi làm header `X-Dak-Caller` để DAK ghi audit. `[A-Za-z0-9._-]{1,128}`, sai định dạng thì lỗi cấu hình |
| `spark.cdrcrypto.cacheTtlSeconds` | Không | Cache key theo `(bảng, cột)` trong bộ nhớ, mặc định `300`; `0` = tắt. Quyết định key bị khoá trên DAK có hiệu lực sau bao lâu (mục 2.6) |

Cấu hình phía lib và bảng lỗi đầy đủ: [KEY_PREFIX_SOURCES.md](../key-access/KEY_PREFIX_SOURCES.md#key-theo-cột-qua-dak-by-column-columnkeysource).

**Nhóm 3 — audit log** (mặc định tắt)

| Key | Mặc định | Ghi chú |
|---|---|---|
| `spark.cdrcrypto.audit.enabled` | `false` | `true` để bật |
| `spark.cdrcrypto.audit.sinks` | (bắt buộc khi bật) | `log` — ghi vào log của pod. `http` (Elasticsearch/collector) chưa hỗ trợ |
| `spark.cdrcrypto.audit.tenant` | (bắt buộc khi bật) | Định danh engine, ghi vào mọi dòng audit (mục 2.4). Chỉ gồm chữ, số, `.`, `_`, `-`, tối đa 128 ký tự |
| `spark.cdrcrypto.audit.workspace` | (bắt buộc khi bật) | Như trên |
| `spark.cdrcrypto.audit.team` | (bắt buộc khi bật) | Như trên |
| `spark.cdrcrypto.audit.engineId` | (bắt buộc khi bật) | Như trên |
| `spark.cdrcrypto.audit.log.mode` | `logger` | `logger`: qua log4j2, vào cả log container lẫn file audit trên PVC. `direct`: ghi thẳng stdout, chỉ vào log container |
| `spark.cdrcrypto.audit.log.prefix` | `CDR_AUDIT` | Dấu hiệu để lọc dòng audit. Phải chứa chữ `AUDIT` thì mới vào file audit của nền tảng |
| `spark.cdrcrypto.audit.log.loggerName` | `vai.lakehouse.cdrcrypto.audit` | Logger log4j2 dùng ở chế độ `logger` |
| `spark.cdrcrypto.audit.coalesceMs` | `0` | Chờ tối đa bao lâu để gom các event đến sát nhau; `0` = ghi ngay |
| `spark.cdrcrypto.audit.maxQueueSize` | `10000` | Hàng đợi đầy thì bỏ event cũ nhất và ghi số bị bỏ (`dropped`) vào event kế tiếp |
| `spark.cdrcrypto.audit.maxUsagesPerEvent` | `5` | Một câu query dùng nhiều hàm hơn thì tách thành nhiều dòng (`part`/`parts`) |
| `spark.cdrcrypto.audit.usageTtlSec` | `3600` | Lần dùng hàm không có execution sau thời gian này được ghi `EXPIRED` |
| `spark.cdrcrypto.audit.maxUsages` | `10000` | Giới hạn số lần dùng hàm giữ trong bộ nhớ driver |
| `spark.cdrcrypto.audit.statementUserTtlSec` | `3600` | Thời gian nhớ user của một câu lệnh JDBC |
| `spark.cdrcrypto.audit.shutdownFlushTimeoutSec` | `10` | Thời gian chờ ghi nốt hàng đợi khi tắt engine |

Thường chỉ cần đặt 6 key đầu của nhóm 3; các key còn lại để mặc định. Cấu hình audit sai (thiếu key bắt buộc, giá trị không hợp lệ)
thì **engine không khởi động được**, lỗi nêu đúng tên key — xem mục 6.

### 2.4. Lấy giá trị định danh engine

Bốn giá trị `tenant`/`workspace`/`team`/`engineId` cố định theo engine, lấy từ namespace và tên StatefulSet của engine:

```text
namespace:   vlp-<tenant>-<workspace>-<team>     vd vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy
StatefulSet: sql-engine-<engineId>               vd sql-engine-en-sqlzqfu6n7
```

```bash
kubectl get statefulset -n <namespace> -o name | grep sql-engine
```

Mỗi engine thuộc đúng một team, nên một engine chỉ có một bộ giá trị này.

### 2.5. Khi DAK gặp sự cố

Không có đường quay về Vault cũ cho `cdr_*`: key theo cột chỉ có ở DAK (Vault cũ chỉ có key cấp bảng, không giải mã được dữ liệu
mã hoá bằng key cột). Đặt `source` khác `dak` thì mọi query `cdr_*` lỗi `Column keys are only available from DAK`. Khi DAK sập, query
vẫn chạy được với các cột đã có key trong cache cho tới hết `cacheTtlSeconds` (mục 2.6).

### 2.6. Key bị khoá trên DAK và `cacheTtlSeconds`

DAK kiểm tra ở **mỗi** request và không cache. Nhưng engine cache key theo `(bảng, cột)` trong `cacheTtlSeconds`, và trong
khoảng đó `cdr_*` dùng key trong cache mà không hỏi lại DAK. Key của một cột **không bao giờ đổi**, nên cache không có rủi ro dùng key
sai; điều cần cân nhắc chỉ là khi DAK khoá key của một cột (hoặc thu hồi quyền của client):

| `cacheTtlSeconds` | Query chạy sau khi thu hồi |
|---|---|
| `300` (mặc định) | Vẫn chạy được tới khi cache của bảng đó hết hạn, **tối đa 300 giây**, rồi mới lỗi |
| `N` | Như trên, tối đa `N` giây |
| `0` | **Lỗi ngay** ở query kế tiếp, vì mỗi lần dùng hàm đều hỏi DAK |

Lỗi khi đó có dạng `DAK has no usable key for 1 column(s) of 'demo_db.sub_rel_product' (requestId=...): 'isdn' DISABLED: ...`
(hoặc `HTTP 403 access_denied` khi quyền bị thu hồi); gửi `requestId` cho admin DAK để tra nguyên nhân. Kết quả lỗi không bị cache:
mọi query sau đều lỗi cho tới khi key được mở khoá, và chạy lại được ngay khi DAK mở khoá (mở khoá trả lại đúng key cũ).

**"Query kế tiếp" là câu query được phân tích sau lúc thu hồi.** Kể cả với `0`:

- `keyPrefix` được lấy trên driver lúc phân tích câu query rồi gửi kèm hàm xuống executor. Query **đang chạy** lúc thu hồi vẫn chạy
  tới hết, không bị dừng giữa chừng.
- DataFrame đã tạo và giữ lại trong session (vd `df = spark.sql(...)` rồi gọi `df.show()` nhiều lần) dùng lại plan cũ, không hỏi lại
  DAK. Câu SQL gửi qua query console/JDBC thì lần nào cũng được phân tích lại; view cũng vậy.
- Dữ liệu đã giải mã rồi lưu lại (`CACHE TABLE ... AS SELECT cdr_decrypt(...)`, `df.cache()`, đã ghi ra bảng) không bị ảnh hưởng.

**Đánh đổi khi giảm TTL:**

- Mỗi lần hàm xuất hiện trong một câu query là khoảng 1 request tới DAK lúc phân tích, và DAK đọc Vault ở mỗi request. Executor
  không gọi DAK nên số dòng không làm tăng số request.
- DAK chậm thì query chậm theo. DAK sập thì mọi query `cdr_*` lỗi ngay, không còn khoảng đệm như khi có cache. Keycloak sập thì
  query vẫn chạy tới khi token hiện tại hết hạn (token được cache riêng, không phụ thuộc `cacheTtlSeconds`).

Chọn giá trị: cần thu hồi có hiệu lực ngay thì đặt `0` (cấu hình ở mục 2.2). Nếu chấp nhận trễ vài chục giây để giảm tải lên DAK
thì đặt `10`–`30`. Đổi giá trị phải restart engine. Engine dùng cả `column_*` thì `spark.columncrypto.cacheTtlSeconds` là key riêng,
đặt cả hai nếu muốn cùng hành vi.

### 2.7. Ngoài Spark conf

- Pod sql-engine phải gọi được Keycloak và DAK: `hostAliases` + truststore chứa CA nội bộ —
  [mục 4.7 của hướng dẫn chung](../column-crypto/COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md#47-gọi-được-keycloak-nội-bộ-từ-sql-engine-hostaliases--ca-nội-bộ).
- Nếu namespace có NetworkPolicy chặn egress, mở đường từ pod engine tới DAK và Keycloak.
- Audit ở chế độ `logger` dựa vào cấu hình log4j2 có sẵn của sql-engine (appender `audit` lọc `.*AUDIT.*`, ghi `/opt/spark/logs/audit/`).
  Không cần sửa gì, chỉ cần không bỏ appender đó và không nâng root logger lên `WARN`.

## 3. Kiểm tra sau khi restart

```sql
DESCRIBE FUNCTION cdr_decrypt;   -- có mô tả nghĩa là extension đã nạp
```

Log driver khi engine khởi động phải có dòng `CDR audit enabled: sinks=log, log.mode=logger, engineId=...` (nếu đã bật audit).
Nếu engine không lên, xem log driver: lỗi cấu hình và lỗi self-check jar đối tác đều in ra ngay lúc khởi tạo extension.

## 4. Dùng trên query console

Tham số đầu **bắt buộc** là `'database.table'`, tham số 2 là tên cột (cả hai là hằng chuỗi, không phân biệt hoa thường, chỉ gồm
chữ, số, `_`). `keyField` nối thẳng sau key không qua băm, nên giá trị cột keyField phải khớp **từng ký tự** với lúc mã hoá (cùng
định dạng ngày, không thêm dấu cách).

```sql
-- Giải mã CDR: isdn đã mã hoá bằng key của cột isdn, start_datetime là keyField (giữ plaintext)
SELECT start_datetime,
       cdr_decrypt('demo_db.sub_rel_product', 'isdn', isdn, start_datetime) AS isdn
FROM demo_db.sub_rel_product;

-- Ghi CDR mã hoá: mỗi cột dùng key của chính nó
INSERT INTO demo_db.cdr_out
SELECT start_datetime,
       cdr_encrypt('demo_db.cdr_out', 'isdn', isdn, start_datetime),
       cdr_encrypt('demo_db.cdr_out', 'imsi', imsi, start_datetime)
FROM demo_db.staging_cdr;

-- View cho người dùng cuối, để không phải nhớ cú pháp
CREATE VIEW demo_db.sub_rel_product_plain AS
SELECT start_datetime, cdr_decrypt('demo_db.sub_rel_product', 'isdn', isdn, start_datetime) AS isdn
FROM demo_db.sub_rel_product;
```

- Cột đã mã hoá khai kiểu `STRING`.
- Bảng và cột trong tham số 1, 2 là nơi **lưu** dữ liệu mã hoá, vì key gắn với chúng. Ghi vào `cdr_out` thì mã hoá bằng key của
  `cdr_out`, và giải mã sau này cũng phải dùng `'demo_db.cdr_out', 'isdn'`.
- `cdr_*` là tất định (cùng đầu vào → cùng ciphertext), nên lọc được trực tiếp trên cột đã mã hoá khi biết giá trị và keyField:
  `WHERE isdn = cdr_encrypt('demo_db.sub_rel_product', 'isdn', '978356099', start_datetime)`.
- **Lần mã hoá đầu tiên của một cột** luôn lỗi `PENDING` kèm mã YC: DAK chưa có key cho cột đó và đã mở yêu cầu tạo key. Người có
  quyền bên DAK tạo key (với dak-mock: `POST /mock-admin/yc/<mã>/fulfill`) rồi chạy lại câu lệnh. Giải mã cột chưa có key thì lỗi
  `MISSING (reason=not_requested)` và không mở YC.
- Chỉ **16 ký tự cuối** của `keyPrefix + keyField` ảnh hưởng tới key (đặc tính thuật toán đối tác): keyField từ 16 ký tự trở lên thì
  `keyPrefix` không còn tác dụng bảo vệ.

## 5. Audit log

### 5.1. Ghi gì

Mỗi câu query có dùng `cdr_encrypt`/`cdr_decrypt` sinh **hai dòng**:

| Dòng | Khi nào | Nội dung chính |
|---|---|---|
| `START` | Ngay khi query bắt đầu, trước khi dữ liệu được xử lý | Ai chạy (`user` = tên đăng nhập JDBC/Keycloak), hàm, bảng, cột |
| `END` | Ngay khi query kết thúc | `SUCCEEDED`/`FAILED`, thời lượng, số dòng đã xử lý, số dòng có giá trị NULL |

Hiếm hơn: `EXPIRED` — hàm đã vào plan nhưng không có query nào chạy nó (chỉ `EXPLAIN`, hoặc chạy ngoài SQL execution như `df.rdd`).

**Không bao giờ ghi**: giá trị dữ liệu, `keyPrefix`, ciphertext, token, nội dung câu SQL. Biểu thức tham số được ghi dạng text, mọi
literal bị thay bằng `'***'`.

### 5.2. Xem ở đâu

| Nơi | Lệnh | Lưu giữ |
|---|---|---|
| Log container (stderr) | `kubectl logs -n <ns> <pod> -c engine-server \| grep 'CDR_AUDIT '` | Mất khi pod bị xoá; chỉ giữ thêm lần chạy ngay trước khi restart |
| File audit trên PVC | `kubectl exec -n <ns> <pod> -c engine-server -- grep 'CDR_AUDIT ' /opt/spark/logs/audit/spark-audit.log` | 7 ngày (file cũ nén `spark-audit-YYYY-MM-DD-N.log.gz`), còn sau khi pod restart |

Việc thu log về hệ thống audit do team DAK đảm nhiệm.

### 5.3. Đọc một dòng audit

Layout của log4j2 thêm thời gian và tên logger vào trước, nên `CDR_AUDIT` nằm **giữa dòng**; phần sau `CDR_AUDIT ` là một object JSON:

```text
26/10/19 15:15:30 INFO audit: CDR_AUDIT {"auditVersion":2,"eventId":"0b1f...","event":"START","ts":"2026-10-19T08:15:30.123Z","appId":"spark-application-...","executionId":7,"user":"alice","userSource":"thrift","statementId":"c3a1...","tenant":"tenantw1xjixm","workspace":"wsw7vtwvi","team":"teamtscauiy","engineId":"en-sqlzqfu6n7","usages":[{"usageId":"u9f3a1c2b4d5e","function":"cdr_decrypt","dataset":"demo_db.sub_rel_product","column":"isdn","valueExpr":"spark_catalog.demo_db.sub_rel_product.isdn","keyFieldExpr":"spark_catalog.demo_db.sub_rel_product.start_datetime"}]}
26/10/19 15:23:02 INFO audit: CDR_AUDIT {"auditVersion":2,"eventId":"7d02...","event":"END","ts":"2026-10-19T08:23:02.456Z","appId":"spark-application-...","executionId":7,"tenant":"tenantw1xjixm","workspace":"wsw7vtwvi","team":"teamtscauiy","engineId":"en-sqlzqfu6n7","status":"SUCCEEDED","durationMs":452333,"usages":[{"usageId":"u9f3a1c2b4d5e","rows":1200000000,"nullRows":3400}]}
```

| Trường | Ý nghĩa |
|---|---|
| `eventId` | Định danh duy nhất của dòng, dùng để loại trùng |
| `event` | `START` / `END` / `EXPIRED` |
| `ts` | Thời điểm, UTC (dùng trường này, không dùng thời gian ở đầu dòng — đầu dòng theo giờ địa phương và không đủ năm) |
| `executionId` | Ghép `START` với `END` của cùng một query |
| `user`, `userSource` | Người chạy query. `userSource=thrift` là tên đăng nhập JDBC; `sparkUser` nghĩa là không lấy được user thật (mục 6) |
| `statementId` | Id câu lệnh của Thrift Server |
| `tenant`, `workspace`, `team`, `engineId` | Định danh engine (mục 2.4) |
| `status`, `durationMs` | Chỉ có ở `END` |
| `usages[]` | Mỗi lần dùng hàm trong query: `function`, `dataset` (tham số 1), `column` (tham số 2, cột dùng để chọn key), `valueExpr`, `keyFieldExpr`; ở `END` là `rows`, `nullRows`. `column` có từ `auditVersion` 2 |
| `part`, `parts` | Có khi một query dùng nhiều hàm hơn `maxUsagesPerEvent` và bị tách thành nhiều dòng |
| `dropped` | Có khi hàng đợi audit từng đầy và phải bỏ event |

`rows`/`nullRows` chỉ tính các task chạy thành công; query lỗi thể hiện bằng `status=FAILED`.

### 5.4. Ảnh hưởng tới người dùng

- Tên cột kết quả **không đổi** khi bật audit.
- `EXPLAIN` và tab SQL của Spark UI hiện thêm `[cdr_audit:u…]` sau biểu thức hàm, ví dụ `UDF(isdn, start_datetime)[cdr_audit:u9f3a1c2b4d5e]`.
- Query không phải chờ ghi audit; lỗi ghi audit chỉ là cảnh báo trong log, query vẫn chạy.
- Chi phí đếm dòng không đo được so với chi phí giải mã (~0,5 µs/dòng).

## 6. Xử lý sự cố

Lỗi chung của `cdr_*` (thiếu jar, lỗi kết nối DAK/Keycloak, sai keyField…):
[mục 9 của hướng dẫn chung](../column-crypto/COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md#9-xử-lý-sự-cố). Lỗi khi xin key theo cột (`PENDING`,
`MISSING`, `REJECTED`, `DISABLED`, `invalid_tag`…): [bảng lỗi của KEY_PREFIX_SOURCES.md](../key-access/KEY_PREFIX_SOURCES.md#key-theo-cột-qua-dak-by-column-columnkeysource).
Lỗi riêng của hàm:

| Triệu chứng | Nguyên nhân / cách xử lý |
|---|---|
| `cdr_encrypt expects 4 arguments (table, column, value, fieldValue)` | Đang dùng cú pháp 3 tham số cũ; thêm tên cột ở tham số 2 |
| `the second argument must be a constant string with the column name` | Tham số 2 phải là hằng chuỗi trong dấu nháy (`'isdn'`), không phải cột |
| `Column keys are only available from DAK` | `spark.cdrcrypto.source` khác `dak` |

Lỗi riêng của audit:

| Triệu chứng | Nguyên nhân / cách xử lý |
|---|---|
| Engine không khởi động: `Missing required setting: spark.cdrcrypto.audit.sinks` (hoặc `audit.tenant`, `audit.team`…) | Bật audit nhưng thiếu key bắt buộc (mục 2.3, nhóm 3) |
| `Invalid spark.cdrcrypto.audit.<key>: ...` | Giá trị sai dạng: `enabled` không phải `true`/`false`, số âm/không phải số, định danh có ký tự lạ, `log.mode` khác `logger`/`direct` |
| `sink 'http' (Elasticsearch/log collector) is not supported yet` | Chỉ dùng được `sinks=log` |
| Log driver có `Logger '...' has INFO disabled, so CDR audit events would be lost` | Cấu hình log4j2 chặn level INFO của logger audit. Bật INFO cho `vai.lakehouse.cdrcrypto.audit`, hoặc đặt `spark.cdrcrypto.audit.log.mode=direct` |
| Không thấy dòng `CDR_AUDIT` nào | Kiểm tra lần lượt: `audit.enabled=true`; log driver có dòng `CDR audit enabled`; query thực sự gọi hàm (ví dụ `SELECT count(*)` trên view giải mã không chạy hàm nên không có audit); jar `cdr-crypto-udf` trên engine là bản có audit |
| Có dòng trong `kubectl logs` nhưng không có trong `spark-audit.log` | `log.mode=direct`, hoặc `log.prefix` không chứa chữ `AUDIT`, hoặc appender `audit` của log4j2 đã bị bỏ |
| `user` là user dịch vụ (vd `presto`), `userSource=sparkUser` | Không nhận được event của Thrift Server cho câu lệnh đó. Báo lại kèm `statementId` để kiểm tra |
| Có `dropped` trong dòng audit | Hàng đợi audit từng đầy (ghi log quá chậm hoặc quá nhiều query cùng lúc). Tăng `audit.maxQueueSize` hoặc kiểm tra log driver |
| Dòng `START` không có `END` | Engine bị dừng giữa query (restart, pod bị xoá). `START` đã ghi trước khi dữ liệu được xử lý nên vẫn còn dấu vết |
