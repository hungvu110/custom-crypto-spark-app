# Tiến độ: audit log cho `cdr_encrypt` / `cdr_decrypt`

Thiết kế và các bước triển khai: [CDR_CRYPTO_AUDIT_LOG_PLAN.md](CDR_CRYPTO_AUDIT_LOG_PLAN.md). File này chỉ ghi trạng thái, kết quả kiểm tra
và các thay đổi so với thiết kế ban đầu.

Cập nhật lần cuối: 2026-10-19. Branch `feat/cdr-crypto-audit-log`, chưa commit.

## 1. Trạng thái theo bước

Số bước theo mục 8 của plan.

| Bước | Trạng thái | Ghi chú |
|---|---|---|
| Phase 0 (a)(b)(c)(e) — hành vi Spark 3.5.1 | ✅ Xong | `SparkAuditContractSpec`, giữ làm test hồi quy; kết quả ở mục 2 |
| Phase 0 (d) — user Thrift ở mức event | ⏳ Chờ deploy jar | Cần sql-engine thật |
| Phase 0 (f) — sql-engine | ✅ Đã kiểm tra cấu hình | Mục 6.4 của plan; dòng `CDR_AUDIT` thật kiểm tra ở H1-4 |
| Phase 0 (f) — pod driver SparkApplication | ⏳ | Tên container, log4j2 của image, PID 1 |
| Phase 1, bước 1–7 — lõi + `START` | ✅ Xong | Package `vai.lakehouse.columncrypto.cdr.audit`; đã gắn vào `CdrCryptoExtension` và `CdrCrypto` |
| Phase 2, bước 8 — `AuditMeter` | ✅ Xong | Hai accumulator `rows`, `nullRows` |
| Phase 2, bước 9 — `END` | ✅ Xong | Test trên Spark thật: query thành công, query lỗi, hai query chạy song song (mỗi query đúng cặp START/END, số dòng và user riêng; đạt 3/3 lần chạy) |
| Phase 2, bước 10 — overhead đếm dòng | ✅ Xong | 0,457 µs/dòng có meter so với 0,457 µs/dòng không meter (đo bằng `CdrCrypto.transform` thật + accumulator thật) |
| H1-1 — `LogAuditSink` chế độ `logger` | ✅ Xong | Test với appender log4j2 thật, message khớp `.*AUDIT.*` |
| H1-2 — chế độ `direct` | ✅ Xong | Nhiều thread ghi không xen dòng |
| H1-3 — tài liệu hợp đồng cho DAK | ⏳ | Mục 6.3 của plan là bản nháp |
| H1-4 — kiểm tra trên cluster | ⏳ Chờ deploy jar | Cấu hình để thử: mục 4 |
| H1-5 — `tini -g`/`exec` cho entrypoint | Đề xuất cho nền tảng | Không chặn H1 |
| Phase 4, bước 11 — tài liệu cấu hình | ⏳ | `COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md`, `SPARK_APP_REFERENCE.md` |
| Phase 4, bước 12 — chạy thử trên cluster, 1–2 GB | ⏳ | |
| H2-1 → H2-6 — Elasticsearch / collector | Chưa làm | `audit.sinks=http` hiện báo lỗi "not supported yet" |

## 2. Kết quả Phase 0

Chạy ngày 2026-10-19 trên Spark 3.5.1 `local[2]`, giữ lại làm test hồi quy
`cdr-crypto-udf/src/test/scala/vai/lakehouse/columncrypto/cdr/audit/SparkAuditContractSpec.scala` (8/8 đạt):

| Kiểm tra | Kết quả | Hệ quả cho thiết kế |
|---|---|---|
| (a) `usageId` trong `physicalPlanDescription` | Có, với SQL (AQE bật/tắt × whole-stage codegen bật/tắt), DataFrame API, `INSERT ... SELECT`, CTAS; kết quả tính đúng | Cơ chế ghép dùng được |
| (a) Nhét `usageId` vào tên UDF | Tên cột tự sinh thành `cdr_decrypt#u…(name)`, đổi theo từng lần chạy | Dùng `AuditTag` (plan mục 4.2); tên cột không đổi |
| (a) CTAS | Sinh 2 execution: gốc và con (`rootExecutionId` trỏ về gốc), cả hai chứa cùng `usageId` | Chỉ xử lý execution gốc, nếu không sẽ ra 2 cặp `START`/`END` |
| Builder hàm | Chạy 1 lần mỗi câu SQL, trong thread gọi `sql()`, đọc được `spark.jobGroup.id` | Cách lấy `statementId` dùng được |
| (b) `executionId` so với `QueryExecution.id` | **Khác nhau** | Không dùng `QueryExecutionListener`; không cần vì (c) đủ |
| (c) `errorMessage` của END | Thành công: `Some("")`; lỗi: `Some("Job aborted …")` | Rỗng → `SUCCEEDED`, có nội dung → `FAILED`; không cần trạng thái `UNKNOWN` |
| (e) `df.rdd.count` | Không có `SQLExecutionStart` | Rơi vào `EXPIRED`, kèm số đếm của meter |
| DataFrame dùng lại cho nhiều action | Cùng `usageId` ở mỗi lần; `count()` cắt bỏ cột UDF nên không có usage trong plan | Đúng mong đợi: không giải mã thì không audit |

(d) chưa kiểm chứng ở mức event: mới xác nhận Spark UI của sql-engine hiện user Keycloak ở trang JDBC/ODBC Server (2026-10-19).
(f) với sql-engine: cấu hình đã kiểm tra (plan mục 6.4), dòng `CDR_AUDIT` thật chờ H1-4.

## 3. Chạy test

```bash
mvn -o -Pcdr-crypto -pl cdr-crypto-udf -am test
```

Kết quả lần chạy gần nhất: `cdr-crypto-udf` 97/97, `key-prefix-lib` 87/87.

Phải có `-am` (hoặc `mvn install` cho `key-prefix-lib` trước). Thiếu nó, Maven dùng bản `key-prefix-lib` cũ trong `~/.m2` (trước commit
`2410c5c`, gọi DAK bằng mã phiếu) và `CdrCryptoExtensionDakSpec` đỏ — không liên quan tới audit.

## 4. Cấu hình để thử trên sql-engine

Jar cần thay vào `spark.jars`: `cdr-crypto-udf/target/cdr-crypto-udf-1.0-SNAPSHOT.jar` và `key-prefix-lib/target/key-prefix-lib-1.0-SNAPSHOT.jar`
(build: `mvn -o -Pcdr-crypto -pl cdr-crypto-udf -am package -DskipTests`).

Thêm vào `spark-properties.conf.template`, cùng chỗ với `spark.sql.extensions`:

```properties
spark.cdrcrypto.audit.enabled=true
spark.cdrcrypto.audit.sinks=log
spark.cdrcrypto.audit.tenant=tenantw1xjixm
spark.cdrcrypto.audit.workspace=wsw7vtwvi
spark.cdrcrypto.audit.team=teamtscauiy
spark.cdrcrypto.audit.engineId=en-sqlzqfu6n7
```

Sau khi chạy một query có `cdr_decrypt` qua JDBC bằng user Keycloak, kiểm tra (H1-4 và Phase 0 d — `user` phải là tên Keycloak, `userSource` là `thrift`):

```bash
kubectl logs -n $NS $POD -c engine-server | grep 'CDR_AUDIT '
kubectl exec -n $NS $POD -c engine-server -- grep 'CDR_AUDIT ' /opt/spark/logs/audit/spark-audit.log
```

## 5. Thay đổi so với thiết kế ban đầu

Các thay đổi dưới đây đã được sửa vào nội dung plan; mục này giữ lý do.

| Ngày | Thay đổi | Lý do |
|---|---|---|
| 2026-10-19 | Bỏ HDFS; hai hướng H1 (log của pod) và H2 (Elasticsearch / collector) | Quyết định của team: DAK không lấy log từ HDFS |
| 2026-10-19 | H1 ghi qua log4j2 (`log.mode=logger` mặc định) thay vì ghi thẳng stdout | log4j2 của sql-engine có sẵn appender `audit` lọc `.*AUDIT.*`, ghi file trên PVC giữ 7 ngày (plan mục 6.4) |
| 2026-10-19 | Đánh dấu lần dùng hàm bằng `AuditTag` thay vì nhét `usageId` vào tên UDF | Phase 0: nhét vào tên UDF làm tên cột tự sinh đổi theo từng lần chạy |
| 2026-10-19 | Bỏ `QueryExecutionListener`, bỏ trạng thái `UNKNOWN` | Phase 0: `executionId` khác `QueryExecution.id`; `SparkListenerSQLExecutionEnd.errorMessage` đủ để biết thành công/lỗi |
| 2026-10-19 | Chỉ xử lý execution gốc (`rootExecutionId`) | Phase 0: CTAS sinh execution con chứa cùng `usageId`, sẽ ra 2 cặp START/END |
| 2026-10-19 | Bỏ `failedRows` | Accumulator thường của Spark bỏ phần cộng của task lỗi (API bật đếm cả task lỗi là API nội bộ), nên con số luôn là 0. Lỗi thể hiện bằng `status=FAILED` |
| 2026-10-19 | Không đặt `udfName` cho UDF | Tên cột kết quả giống hệt khi tắt audit; `EXPLAIN` hiện `UDF(...)[cdr_audit:u…]` |
| 2026-10-19 | `maxUsagesPerEvent` mặc định 5 (thay vì 20) | Với 20 usage có biểu thức dài tối đa, một event vượt 8 KB |
| 2026-10-19 | Che mọi literal (chuỗi, số, ngày…) trên cây expression | Bản đầu chỉ che literal chuỗi bằng regex; số điện thoại dạng số sẽ lọt vào `valueExpr`/`keyFieldExpr` |
| 2026-10-19 | Kiểm tra cấu hình sớm chỉ khi nạp qua `spark.sql.extensions` | `SparkSession.Builder.withExtensions` gọi extension trước khi có SparkContext; khi đó cấu hình được kiểm tra ở lần dùng hàm đầu tiên |
| 2026-10-19 | `log.prefix` mặc định `CDR_AUDIT`, code tự thêm dấu cách trước JSON | `ConfigSource` cắt khoảng trắng ở cuối giá trị nên không đặt được `CDR_AUDIT ` |

## 6. Ngoài phạm vi nhưng cần báo nền tảng

- ConfigMap `ranger-spark-audit.xml` của sql-engine chứa user và mật khẩu DB của Ranger ở dạng plain text — nên chuyển sang Secret, và đổi mật khẩu nếu là mật khẩu thật.
- PID 1 của sql-engine là `bash` không `exec` nên SIGTERM không tới JVM (H1-5).
