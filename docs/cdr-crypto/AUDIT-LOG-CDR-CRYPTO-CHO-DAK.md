# Audit log khi dùng `cdr_encrypt` / `cdr_decrypt`

Mỗi lần người dùng mã hoá/giải mã một cột bằng `cdr_encrypt` / `cdr_decrypt` trên sql-engine, thư viện `cdr-crypto-udf` ghi lại **ai, lúc nào,
bảng nào, cột nào, bao nhiêu dòng, thành công hay lỗi**. Log chỉ được ghi vào **log của pod**; Elasticsearch của cụm K8s tự thu từ log pod.
Tài liệu này mô tả nội dung và định dạng dòng log.

## 1. Phạm vi

| | |
|---|---|
| Có ghi | Mỗi câu query dùng hàm: người chạy (tên đăng nhập JDBC/Keycloak), bảng (`dataset`), cột chọn key (`column`), số dòng, số dòng NULL, trạng thái, thời lượng |
| **Không** ghi | Giá trị dữ liệu, `keyPrefix`, ciphertext, token, nội dung câu SQL. Mọi literal trong biểu thức bị thay bằng `'***'` |
| Không thuộc log này | Việc lấy key từ DAK (`GET /api/v1/keys/by-column`), phần đó được audit riêng phía DAK |

Không log từng dòng dữ liệu (hàng tỷ dòng); chỉ log mỗi lần hàm được dùng trong một câu query, kèm số dòng đã xử lý.
Ghi bất đồng bộ trên driver; lỗi ghi audit chỉ cảnh báo, không làm query fail.

## 2. Mỗi query sinh mấy dòng

| Event | Khi nào | Nội dung chính |
|---|---|---|
| `START` | Ngay khi query bắt đầu, trước khi xử lý dữ liệu | `user`, hàm, bảng, cột |
| `END` | Khi query kết thúc | `status` = `SUCCEEDED`/`FAILED`, `durationMs`, `rows`, `nullRows` |
| `EXPIRED` (hiếm) | Hàm đã vào plan nhưng không có query nào chạy (chỉ `EXPLAIN`, hoặc chạy ngoài SQL như `df.rdd`) | Như START; có `rows` > 0 nếu hàm thực sự đã chạy |

`START` và `END` của cùng một query ghép bằng `executionId` + `appId`. Khoảng 2 dòng mỗi query, mỗi dòng dưới 8 KB.

## 3. Log nằm ở đâu trong pod

| | |
|---|---|
| Pod / container | `sql-engine-<engineId>-0` / container `engine-server`. Executor không có dòng audit |
| Luồng | stderr của container (appender `console` của log4j2), nên collector log của cụm đọc được như log container thông thường |
| Bản sao cục bộ | `/opt/spark/logs/audit/spark-audit.log` trên PVC, giữ 7 ngày; chỉ để tra cứu khi cần, không bắt buộc thu |

## 4. Định dạng dòng log

Dòng có tiền tố của log4j2 nên **`CDR_AUDIT ` nằm giữa dòng**; phần sau đó là một object JSON.

```text
26/10/19 15:15:30 INFO audit: CDR_AUDIT {"auditVersion":2,"eventId":"0b1f...","event":"START","ts":"2026-10-19T08:15:30.123Z","appId":"spark-application-...","executionId":7,"user":"alice","userSource":"thrift","statementId":"c3a1...","tenant":"tenantw1xjixm","workspace":"wsw7vtwvi","team":"teamtscauiy","engineId":"en-sqlzqfu6n7","usages":[{"usageId":"u9f3a1c2b4d5e","function":"cdr_decrypt","dataset":"demo_db.sub_rel_product","column":"isdn","valueExpr":"spark_catalog.demo_db.sub_rel_product.isdn","keyFieldExpr":"spark_catalog.demo_db.sub_rel_product.start_datetime"}]}
26/10/19 15:23:02 INFO audit: CDR_AUDIT {"auditVersion":2,"eventId":"7d02...","event":"END","ts":"2026-10-19T08:23:02.456Z","appId":"spark-application-...","executionId":7,"tenant":"tenantw1xjixm","workspace":"wsw7vtwvi","team":"teamtscauiy","engineId":"en-sqlzqfu6n7","status":"SUCCEEDED","durationMs":452333,"usages":[{"usageId":"u9f3a1c2b4d5e","rows":1200000000,"nullRows":3400}]}
```

Khi lọc/parse (ví dụ pipeline Elasticsearch):

- Lọc dòng chứa `CDR_AUDIT ` (log Spark thường còn nhiều dòng khác), rồi parse JSON phía sau.
- Lấy thời gian từ trường `ts` (UTC) trong JSON, **không** dùng thời gian đầu dòng (không đủ năm, theo giờ địa phương).
- Dedupe theo `eventId` nếu cần (giao hàng at-least-once; có thể dùng làm `_id`).

| Trường | Ý nghĩa |
|---|---|
| `auditVersion` | Hiện là 2. Đổi tương thích ngược chỉ thêm trường; đổi phá vỡ thì tăng số này |
| `eventId` | Định danh duy nhất của dòng |
| `event`, `ts`, `executionId`, `appId` | Loại event, thời điểm (UTC), khoá ghép START/END |
| `user`, `userSource` | Người chạy query. `thrift` = tên đăng nhập JDBC (đáng tin nhất); `ugi`; `sparkUser` = **không lấy được user thật** (chỉ còn user dịch vụ chung) |
| `statementId` | Id câu lệnh của Thrift Server |
| `tenant`, `workspace`, `team`, `engineId` | Định danh engine, có trong **mọi** dòng |
| `usages[]` | Mỗi lần dùng hàm: `function`, `dataset` (tham số 1), `column` (tham số 2 — cột chọn key), `valueExpr`, `keyFieldExpr`; ở `END` là `rows`, `nullRows` |
| `status`, `durationMs` | Chỉ ở `END` |
| `part`, `parts` | Query dùng nhiều hơn 5 hàm bị tách thành nhiều dòng cùng `executionId` |
| `dropped` | Hàng đợi audit từng đầy và đã bỏ event (hiếm) |

`rows`/`nullRows` chỉ tính task chạy thành công; query lỗi thể hiện bằng `status=FAILED`.

## 5. Lưu ý

1. Đây là audit best-effort, không fail-closed: nếu ghi audit lỗi thì query vẫn chạy.
2. Log container bị mất khi pod bị xoá nếu chưa kịp thu; phụ thuộc vào tốc độ thu của collector.
3. Job SparkApplication (`spark-app`) cũng ghi audit cùng định dạng, ở log của pod driver (tên container chưa kiểm chứng trên cluster).
4. Dòng `CDR_AUDIT` thật trên cluster chưa được kiểm tra (chờ deploy jar); mẫu ở mục 4 lấy theo test.
