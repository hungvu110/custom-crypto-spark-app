# Tài liệu kỹ thuật

Mỗi thư mục ứng với một bài toán (đánh số như [README gốc](../README.md)). Đọc file đầu tiên của từng nhóm trước.

| Thư mục | Bài toán | Đọc đầu tiên | Còn lại |
| --- | --- | --- | --- |
| [`hdfs-block-token/`](./hdfs-block-token) | 1. Wire encryption + block token Isilon | [`SPARK_HDFS_WIRE_ENCRYPTION_TASK.md`](./hdfs-block-token/SPARK_HDFS_WIRE_ENCRYPTION_TASK.md) — root cause, thiết kế patch | [`HDFS_BLOCK_TOKEN_PATCH_FLOW.md`](./hdfs-block-token/HDFS_BLOCK_TOKEN_PATCH_FLOW.md) luồng lỗi/đã vá; [`HDFS_PATCH_AT_SCALE.md`](./hdfs-block-token/HDFS_PATCH_AT_SCALE.md) phân phối cho nhiều app; [`BLOCK_TOKEN_GUIDE.md`](./hdfs-block-token/BLOCK_TOKEN_GUIDE.md) ghi file rỗng với SeaTunnel Zeta; [`spark-history-server-hdfs-hkh-kerberos.md`](./hdfs-block-token/spark-history-server-hdfs-hkh-kerberos.md) History Server đọc event log |
| [`column-crypto/`](./column-crypto) | 2. Mã hoá cột | [`COLUMN_CRYPTO_ARCHITECTURE.md`](./column-crypto/COLUMN_CRYPTO_ARCHITECTURE.md) — built-in vs UDF | [`COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md`](./column-crypto/COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md) nạp vào sql-engine |
| [`cdr-crypto/`](./cdr-crypto) | 3. CDR đối tác | [`PARTNER_CDR_CRYPTO_UDF_PLAN.md`](./cdr-crypto/PARTNER_CDR_CRYPTO_UDF_PLAN.md) | [`CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md`](./cdr-crypto/CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md) plan key theo cột (`cdr_encrypt` 4 tham số); [`CDR_CRYPTO_SQL_ENGINE_GUIDE.md`](./cdr-crypto/CDR_CRYPTO_SQL_ENGINE_GUIDE.md) dùng trên sql-engine + config cần truyền + audit log; [`CDR_CRYPTO_AUDIT_LOG_PLAN.md`](./cdr-crypto/CDR_CRYPTO_AUDIT_LOG_PLAN.md) thiết kế audit, [`CDR_CRYPTO_AUDIT_LOG_PROGRESS.md`](./cdr-crypto/CDR_CRYPTO_AUDIT_LOG_PROGRESS.md) tiến độ; `VDL - Mã hóa CDR.docx` tài liệu gốc của đối tác |
| [`pseudonymization/`](./pseudonymization/README.md) | 4. Khử định danh | [`ALGORITHMS.md`](./pseudonymization/ALGORITHMS.md) — cơ chế + ví dụ | [`LIB_PLAN.md`](./pseudonymization/LIB_PLAN.md) thiết kế lib |
| [`key-access/`](./key-access) | Lấy keyPrefix/khoá (dùng cho bài toán 2, 3, 4) | [`KEY_PREFIX_SOURCES.md`](./key-access/KEY_PREFIX_SOURCES.md) — file / Vault / DAK | [`DAK_API_SPEC.md`](./key-access/DAK_API_SPEC.md) API; [`DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md`](./key-access/DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md) plan; [`API-LAY-KEY-DUONG-DAN-COT.md`](./key-access/API-LAY-KEY-DUONG-DAN-COT.md) **hợp đồng mới nhất của DAK** cho `by-path` và `by-column` ([`TRA-LOI-DAK-BY-PATH.md`](./key-access/TRA-LOI-DAK-BY-PATH.md) SA trả lời hình dạng `path`); [`DAK_BY_COLUMN_CLIENT_PLAN.md`](./key-access/DAK_BY_COLUMN_CLIENT_PLAN.md) plan lấy key theo cột qua `by-column`; `dak_response/` lịch sử trao đổi với DAK / sql-engine |
| [`dak-mock-api/`](./dak-mock-api) | Mock DAK để test | [`DAK_MOCK_BY_COLUMN_PLAN.md`](./dak-mock-api/DAK_MOCK_BY_COLUMN_PLAN.md) — mô phỏng `by-column`, YC, endpoint quản trị | — |
| [`project/`](./project) | Build, chạy, image | [`BUILD_AND_TEST.md`](./project/BUILD_AND_TEST.md) | [`SPARK_APP_REFERENCE.md`](./project/SPARK_APP_REFERENCE.md) env + deploy `spark-app` |
| [`operations/`](./operations) | Vận hành | [`RANGER_AIRFLOW_MONITORING_GUIDE.md`](./operations/RANGER_AIRFLOW_MONITORING_GUIDE.md) | — |
| [`hdfs-load-stress-test/`](./hdfs-load-stress-test/PLAN.md) | Stress test HDFS | [`PLAN.md`](./hdfs-load-stress-test/PLAN.md) | — |
| `images/` | Ảnh minh hoạ dùng trong các tài liệu trên | — | — |

Tài liệu của từng module (README riêng) nằm cạnh code: `key-prefix-lib/`, `cdr-crypto-udf/`, `pseudonymize-table-app/`, `pseudonym-verify-utils/`,
`hdfs-load-stress-test/`, `dak-mock/`, `internal-repo/`.
