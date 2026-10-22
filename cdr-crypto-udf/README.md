# cdr-crypto-udf

`cdr_encrypt` / `cdr_decrypt`: mã hoá CDR theo thuật toán của đối tác, **mỗi cột một key** lấy qua DAK `by-column`
([key-prefix-lib](../key-prefix-lib/README.md)). Hướng dẫn dùng và cấu hình:
[docs/cdr-crypto/CDR_CRYPTO_SQL_ENGINE_GUIDE.md](../docs/cdr-crypto/CDR_CRYPTO_SQL_ENGINE_GUIDE.md); thiết kế:
[CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md](../docs/cdr-crypto/CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md).

```sql
cdr_encrypt('<database>.<table>', '<tên cột>', <giá trị>, <cột keyField>)
cdr_decrypt('<database>.<table>', '<tên cột>', <giá trị>, <cột keyField>)
```

## Cấu trúc thư mục

Source chia thư mục theo chức năng, nhưng **mọi class vẫn ở package `vai.lakehouse.columncrypto.cdr`** (và `.audit`). Tên đầy đủ
`vai.lakehouse.columncrypto.cdr.CdrCryptoExtension` nằm trong cấu hình đã triển khai (`spark.sql.extensions`), nên không được đổi.
Thư mục `src/test` phản chiếu đúng cấu trúc này.

```text
src/main/scala/vai/lakehouse/columncrypto/cdr/
├── crypto/      lõi thuật toán đối tác
├── sql/         đăng ký hàm SQL
├── dataframe/   DataFrame API
└── audit/       audit log
```

| Thư mục      | File                       | Nội dung                                                                                                                                  |
| ------------ | -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `crypto/`    | `CdrCipherCore.scala`      | Bọc `TransformDL` của đối tác: `decrypt`, `encrypt` (tự viết), `verifyCompatibility` (self-check lúc khởi động)                           |
| `sql/`       | `CdrCryptoExtension.scala` | Đăng ký `cdr_encrypt`/`cdr_decrypt` (4 tham số), xin key theo cột, bọc `AuditTag` khi bật audit                                           |
| `dataframe/` | `CdrCrypto.scala`          | `encryptColumns`/`decryptColumns`, `loadConfig`, `sharedColumnKeySource`; dùng chung `transform` với SQL                                  |
|              | `CdrCryptoConfig.scala`    | Key của từng cột + `keyField` + cột cần mã hoá; `toString` che key                                                                        |
| `audit/`     | `CdrAudit.scala`           | Runtime dùng chung cả tiến trình, đọc `spark.cdrcrypto.audit.*`                                                                            |
|              | `CdrAuditRuntime.scala`    | Đăng ký usage, ghép với SQL execution, dựng START/END/EXPIRED                                                                              |
|              | `AuditEvent.scala`         | Event và JSON (`auditVersion` 2, có `column`)                                                                                              |
|              | các file còn lại           | `AuditConfig`, `AuditDispatcher`, `AuditSink`/`LogAuditSink`, `AuditMeter`, `AuditTag`, `AuditText`, `UsageRegistry`, `CdrAuditListener` |

Hai đường mã hoá (SQL và DataFrame) dùng chung `CdrCrypto.transform`, nên dữ liệu ghi bằng đường này giải mã được bằng đường kia.

## Build và test

Module nằm trong profile `cdr-crypto` vì phụ thuộc `DataLakeSecurity_jv8.jar` của đối tác (không commit vào repo,
xem [BUILD_AND_TEST.md](../docs/project/BUILD_AND_TEST.md)):

```bash
mvn -Pcdr-crypto -pl cdr-crypto-udf -am test
```
