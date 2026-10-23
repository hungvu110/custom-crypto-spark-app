# Plan: `cdr_encrypt` / `cdr_decrypt` dùng key theo từng cột

Ngày: 2026-10-22. Phạm vi: `cdr-crypto-udf` (hàm SQL, DataFrame API, audit) và `spark-app` (`CryptoStep`). Việc lấy key qua DAK nằm ở
[../key-access/DAK_BY_COLUMN_CLIENT_PLAN.md](../key-access/DAK_BY_COLUMN_CLIENT_PLAN.md); mock DAK để test ở
[../dak-mock-api/DAK_MOCK_BY_COLUMN_PLAN.md](../dak-mock-api/DAK_MOCK_BY_COLUMN_PLAN.md).

**Trạng thái (2026-10-22):** C1–C6 xong (`mvn -Pcdr-crypto -pl cdr-crypto-udf test`: 105 test; `mvn -pl spark-app test`: 17 test).
C7 (chạy thật với dak-mock trên cluster) chưa làm.

## 0. Thay đổi một câu

Hôm nay cả bảng dùng **một** `keyPrefix`: `cdr_encrypt('db.table', value, keyField)`. Sau thay đổi, **mỗi cột một key**, và hàm nhận thêm
**tên cột** để biết xin key nào:

```sql
cdr_encrypt('db.table', 'column', value, keyField)
cdr_decrypt('db.table', 'column', value, keyField)
```

| Tham số    | Kiểu                | Ý nghĩa                                                            |
| ---------- | ------------------- | ------------------------------------------------------------------ |
| `db.table` | hằng chuỗi          | Bảng chứa cột (như hiện nay)                                       |
| `column`   | **hằng chuỗi, mới** | Tên cột để xin key từ DAK. Chỉ dùng để chọn key, không đọc dữ liệu |
| `value`    | cột hoặc biểu thức  | Dữ liệu cần mã hoá / giải mã của từng dòng                         |
| `keyField` | cột                 | Giá trị ghép sau key để ra `keyInput` của từng dòng (không đổi)    |

Thuật toán không đổi: `keyInput = keyPrefix(cột) + keyField`, AES/ECB của đối tác (`CdrCipherCore`).

Ví dụ: `cdr_encrypt('demo_db.sub_rel_product', 'isdn', isdn, start_datetime)`. Chữ `'isdn'` trong nháy là tên để xin key; `isdn` không nháy
là số điện thoại của từng dòng. Thường hai chỗ trùng nhau; chúng khác khi `value` là hằng số (tìm kiếm) hoặc cột đã bị đổi tên.

## 1. Quyết định

| #  | Quyết định                                                                                                                                                                                       |
| -- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| D1 | **Tên cột truyền tường minh** (phương án B), không suy từ biểu thức `value`. Suy từ biểu thức sai khi cột bị alias/view/CTE đổi tên, và không làm được khi `value` là hằng số                    |
| D2 | **Bỏ dạng 3 tham số**. Chưa có dữ liệu thật nên không cần giữ tương thích. Gọi 3 tham số báo lỗi chỉ rõ cú pháp mới                                                                             |
| D3 | Hai tham số đầu **bắt buộc là hằng chuỗi**, như tham số bảng hiện nay. Không cho tên cột lấy từ dữ liệu                                                                                          |
| D4 | Mã hoá xin key với `onMissing=request` (cột mới thì DAK mở YC), giải mã với `onMissing=none` (thiếu key là lỗi cấu hình)                                                                         |
| D5 | Key lấy qua `ColumnKeySource` (chỉ nguồn `dak`). Không còn đọc `keyPrefix` cấp bảng cho CDR                                                                                                      |
| D6 | `column_encrypt` (`column-crypto-lib`) **không đổi**, vẫn 3 tham số và key cấp bảng                                                                                                             |

## 2. Thay đổi trong `cdr-crypto-udf`

### 2.1 Hàm SQL: `CdrCryptoExtension`

- Builder khớp `Seq(table, column, value, fieldValue)`. Dạng khác báo:
  `cdr_encrypt expects 4 arguments (table, column, value, fieldValue), e.g. cdr_encrypt('demo_db.sub_rel_product', 'isdn', isdn, start_datetime)`.
- `datasetOf` (đã có) đọc `table`; thêm `columnOf` đọc `column` theo cùng quy tắc hằng chuỗi.
- Lấy key: `source.readColumns(dataset, Seq(column), intent)` rồi lấy theo tên cột chữ thường; `intent` theo hàm (`Encrypt` / `Decrypt`).
- Key vẫn nằm trong closure của `ScalaUDF` (không thành `Literal` trong plan), `udfDeterministic = true` như hiện nay.
- Lỗi `ColumnKeysUnavailableException` từ lib đi thẳng ra người dùng lúc analyze. Thông báo có `state`, `requestCode` (ví dụ "cột `isdn`
  đang chờ DAK tạo key, YC-2026-000002"), không có key.

Mô tả hàm (`ExpressionInfo`) cập nhật theo cú pháp mới để `DESCRIBE FUNCTION` hiển thị đúng.

### 2.2 DataFrame API: `CdrCrypto`, `CdrCryptoConfig`

```scala
case class CdrCryptoConfig(
  keyPrefixes: Map[String, String],   // cột (chữ thường) -> keyPrefix; thay cho keyPrefix đơn
  keyField: String,
  encryptedColumns: Seq[String],
  datasetName: String = "")
```

- `toString` che toàn bộ `keyPrefixes` (chỉ in tên cột).
- `loadConfig(datasetName, keyField, encryptedColumns, intent, source)` gọi `readColumns` **một lần cho mọi cột** của bảng (lib tự chia lô).
- `encryptColumns` / `decryptColumns` truyền `KeyIntent` tương ứng; mỗi cột dùng key của chính nó.
- Tên cột của DataFrame là tên dùng để xin key. Đổi tên cột trước khi gọi thì phải đổi lại cho khớp tên lưu ở DAK (ghi rõ trong scaladoc).
- `validate` giữ nguyên các kiểm tra hiện có, thêm: mọi cột trong `encryptedColumns` có mặt trong `keyPrefixes`.

### 2.3 Nguồn key dùng chung

`CdrCrypto.sharedPrefixSource: PrefixSource` đổi thành `sharedColumnKeySource: ColumnKeySource`, dựng bằng `ColumnKeySourceFactory` từ cùng
chuỗi cấu hình (`spark.cdrcrypto.*` rồi tới biến môi trường). Vẫn là một instance cho cả tiến trình để chỉ có một cache.

### 2.4 Audit

| Thay đổi                          | Chi tiết                                                                                       |
| --------------------------------- | ---------------------------------------------------------------------------------------------- |
| `UsageInfo` thêm `column`         | Tên cột dùng để xin key (chữ thường)                                                           |
| `auditVersion` 1 → 2              | Event có thêm field, nên tăng version để nơi đọc log phân biệt                                 |
| Không ghi key                     | Như hiện nay: không ghi `keyPrefix`, chỉ ghi `dataset`, `column`, biểu thức `value`/`keyField` |
| `AuditTag`, `CdrAuditRuntime.tag` | Nhận thêm `column`                                                                             |

Cập nhật [CDR_CRYPTO_AUDIT_LOG_PLAN.md](CDR_CRYPTO_AUDIT_LOG_PLAN.md) phần định dạng event.

## 3. Thay đổi trong `spark-app`

`CryptoStep` hiện gọi chung hợp đồng 3 tham số `fn('db.table', value, keyField)` cho cả `column_encrypt` và `cdr_encrypt`
([CryptoStep.scala](../../spark-app/src/main/scala/org/example/CryptoStep.scala)). Sau thay đổi hai hàm khác số tham số, nên:

| Thay đổi                                                                | Chi tiết                                                                                                                                       |
| ----------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Biến môi trường mới `CRYPTO_KEY_SCOPE`                                  | `table` (mặc định, 3 tham số, cho `column_encrypt`) hoặc `column` (4 tham số, chèn tên cột chữ thường làm tham số 2, cho `cdr_encrypt`)        |
| `CryptoSettings` thêm `keyScope`                                        | `parse` kiểm giá trị hợp lệ                                                                                                                    |
| `applyFunction`                                                         | `column`: gọi `fn(lit(keyName), lit(<tên cột chữ thường>), col(c), keyCol)`                                                                    |
| [k8s/spark-application-cdr.yaml](../../k8s/spark-application-cdr.yaml) | Thêm `CRYPTO_KEY_SCOPE=column`, `DAK_TAG=internal`, `DAK_CALLER=<tên job>`; `DAK_CLIENT_ID` / Secret `spark-dak-client` trỏ tới **client nội bộ**; sửa các chú thích còn nhắc `GET /api/v1/keys/<DB_NAME>/<TABLE_NAME>` |
| ConfigMap của dak-mock ([dak-mock/k8s/dak-mock.yaml](../../dak-mock/k8s/dak-mock.yaml)) | Client nội bộ ở trên phải được khai với `kind: internal`, không thì mọi lời gọi `401`              |

Job dùng client nội bộ **không** gọi được `column_encrypt` (client nội bộ gọi `/{database}/{table}` nhận `403`), vì hai lib đọc chung bộ
`DAK_*` trên SparkApplication. Xem [plan key-access mục 2.3](../key-access/DAK_BY_COLUMN_CLIENT_PLAN.md#23-cấu-hình).

Phương án khác là `spark-app` tự đoán theo tên hàm (`cdr_*` thì 4 tham số). Không chọn vì app được thiết kế không biết lib crypto nào
đang nạp.

## 4. Các bước (TDD)

Làm sau khi xong K1–K5 của plan key-access, vì cdr-crypto phụ thuộc `ColumnKeySource`.

| #  | Việc                                                                           | Test                                                                  |
| -- | ------------------------------------------------------------------------------ | --------------------------------------------------------------------- |
| C1 | `CdrCryptoExtension` 4 tham số, `columnOf`, `KeyIntent`                        | `CdrCryptoExtensionSpec` (nguồn key giả), sửa các ca 3 tham số thành 4 |
| C2 | `CdrCryptoConfig.keyPrefixes`, `loadConfig`, `encryptColumns`/`decryptColumns` | `CdrCryptoSpec`                                                       |
| C3 | Nguồn dùng chung `sharedColumnKeySource`                                       | `CdrCryptoExtensionDakSpec` đổi sang stub `by-column`                 |
| C4 | Audit thêm `column`, `auditVersion` 2                                          | `AuditEventSpec`, `CdrAuditIntegrationSpec`, `SparkAuditContractSpec` |
| C5 | `spark-app` `CRYPTO_KEY_SCOPE`                                                 | test của `CryptoStep`                                                 |
| C6 | Tài liệu (mục 5)                                                               | —                                                                     |
| C7 | Chạy thật với dak-mock trên cluster                                            | Kịch bản ở [plan mock mục 4](../dak-mock-api/DAK_MOCK_BY_COLUMN_PLAN.md#4-triển-khai) |

Ca test bắt buộc:

- Hai cột cùng bảng → hai key khác nhau; mã hoá cột A rồi giải mã bằng key cột B **không** ra giá trị gốc.
- SQL và DataFrame API cho cùng kết quả trên cùng cột (dữ liệu ghi đường này giải mã được đường kia).
- `cdr_encrypt` gửi `onMissing=request`, `cdr_decrypt` gửi `onMissing=none`.
- Hằng số làm `value`: `WHERE isdn = cdr_encrypt('t', 'isdn', '978356099', start_datetime)` dùng key cột `isdn`.
- Cột trong alias: `SELECT cdr_decrypt('t', 'isdn', x, k) FROM (SELECT isdn AS x, k FROM t)` dùng key cột `isdn`.
- Gọi 3 tham số, tham số 2 không phải hằng chuỗi, tên cột sai định dạng → lỗi rõ ràng lúc analyze.
- Cột `PENDING` / `REJECTED` / `DISABLED` → query lỗi, thông báo có `requestCode`, không có key.
- `CdrCryptoConfig.toString` không lộ key.
- `CdrCipherCore.verifyCompatibility` vẫn chạy.
- Mã hoá rồi giải mã đúng với `keyPrefix` dạng key mà dak-mock và DAK sinh: Base64 của 32 byte ngẫu nhiên (44 ký tự, có thể chứa
  `+`, `/`, `=`). Thuật toán đối tác nhận chuỗi bất kỳ (test vector dùng `231x#@Vie3`), test này để chốt lại điều đó.

## 5. Tài liệu cần cập nhật

| Tài liệu                                                               | Nội dung                                                            |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------- |
| [CDR_CRYPTO_SQL_ENGINE_GUIDE.md](CDR_CRYPTO_SQL_ENGINE_GUIDE.md)       | Cú pháp 4 tham số, ví dụ, cấu hình `dak.tag` / `dak.caller`, lỗi YC |
| [PARTNER_CDR_CRYPTO_UDF_PLAN.md](PARTNER_CDR_CRYPTO_UDF_PLAN.md)       | Ghi chú: key cấp bảng đã thay bằng key theo cột, trỏ sang plan này  |
| [../project/SPARK_APP_REFERENCE.md](../project/SPARK_APP_REFERENCE.md) | `CRYPTO_KEY_SCOPE`, `DAK_TAG`, `DAK_CALLER`                         |
| [../../README.md](../../README.md)                                     | Ví dụ gọi hàm                                                       |

## 6. Rủi ro

| Rủi ro                                                                                           | Cách xử lý                                                                                              |
| ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| Ghi nhầm tên cột ở tham số 2 khi mã hoá → DAK mở YC và tạo key cho cột sai                        | Lỗi chỉ lộ ra khi giải mã bằng tên đúng. Guide nhấn mạnh; audit ghi `column` để tra lại                |
| Lần mã hoá đầu tiên của một cột luôn lỗi (`PENDING`) cho tới khi người có quyền bên DAK tạo key   | Quy trình: chạy thử một lần để mở YC, chờ DAK tạo key, rồi mới chạy job thật. Ghi vào guide. Trên dak-mock: `POST /mock-admin/yc/{code}/fulfill`, hoặc bật `dak.yc.auto-fulfill: true` ở môi trường test để bỏ bước chờ |
| Nhiều lời gọi hàm trong một query → nhiều request DAK khi cache trống                             | Cache theo cột trong lib. DataFrame API gom một lời gọi cho cả bảng                                    |
