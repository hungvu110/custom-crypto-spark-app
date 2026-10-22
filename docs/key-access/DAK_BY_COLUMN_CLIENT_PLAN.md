# Plan: `key-prefix-lib` lấy key theo cột qua DAK `by-column`

Ngày: 2026-10-22. Phạm vi: `key-prefix-lib` (client gọi DAK). Phần dùng key trong hàm SQL ở
[../cdr-crypto/CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md](../cdr-crypto/CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md); mock DAK để test ở
[../dak-mock-api/DAK_MOCK_BY_COLUMN_PLAN.md](../dak-mock-api/DAK_MOCK_BY_COLUMN_PLAN.md).

Hợp đồng API lấy từ trao đổi với DAK: [dak_response/PHAN-HOI-YC-DAK-VONG-2.md](dak_response/PHAN-HOI-YC-DAK-VONG-2.md) §4 (hình dạng response),
[VONG-4](dak_response/PHAN-HOI-YC-DAK-VONG-4.md) (`reason`, `REJECTED`, bảng `tag`/`onMissing`), [VONG-6](dak_response/PHAN-HOI-YC-DAK-VONG-6.md)
(key cột không bao giờ đổi, hai tầng trạng thái).

**Trạng thái (2026-10-22):** K1–K6 xong (`mvn -pl key-prefix-lib test`: 121 test). K7 (chạy thật với dak-mock trên cluster) chưa
làm, cần bản `cdr-crypto-udf` key theo cột để có bên gọi.

## 0. Bối cảnh và quyết định đã chốt

Trước đây mỗi **bảng** có một `keyPrefix`, lấy qua `GET /api/v1/keys/{database}/{table}`. Nay mỗi **cột** có key riêng, lấy qua
`GET /api/v1/keys/by-column`.

| Chủ đề                | Quyết định                                                                                              |
| --------------------- | ------------------------------------------------------------------------------------------------------- |
| Định danh key         | `(database, table, column)`, chữ thường. Tenant cố định phía DAK, không có workspace                    |
| Client                | Client nội bộ của DAK cấp cho luồng này (`tag=internal`). Nhánh client team để sau                      |
| Key có đổi không      | **Không bao giờ** (DAK xác nhận vòng 6). Lib không dùng `keyVersion` để chọn hay lưu key, chỉ ghi log   |
| Dữ liệu cũ            | Không có (mới chỉ test). Không có migration, không cần fallback về key cấp bảng                         |
| Luồng `/{db}/{table}` | **Giữ nguyên** trong lib cho `column-crypto-lib` (`column_encrypt`), không đụng tới                     |
| `by-file`             | Ngoài phạm vi                                                                                           |

## 1. Hợp đồng `by-column` mà lib cài đặt

### 1.1 Request

```http
GET /api/v1/keys/by-column?database=demo_db&table=users_cdr&columns=msisdn,imei&onMissing=request&tag=internal
Authorization: Bearer <access_token của client>
Accept: application/json
X-Request-Id: <uuid do lib sinh>
X-Dak-Caller: <tên engine/job>        (tuỳ chọn)
```

| Tham số             | Lib gửi                                                                                     |
| ------------------- | ------------------------------------------------------------------------------------------- |
| `database`, `table` | Từ tham số `'database.table'`, đã kiểm `[A-Za-z0-9_]{1,128}` và chữ thường                  |
| `columns`           | Đã kiểm cùng quy tắc, chữ thường, **loại trùng**, giữ thứ tự xuất hiện đầu tiên             |
| `onMissing`         | **Luôn gửi tường minh**: `request` khi mã hoá, `none` khi giải mã                           |
| `tag`               | Giá trị của cấu hình `dak.tag`. Không đặt thì **không gửi** tham số (luồng client team)     |

Giới hạn mỗi lời gọi: **100 cột** và **4096 ký tự** cho chuỗi `columns` sau khi gộp, **tính cả dấu phẩy** (độ dài của
`columns.mkString(",")`, đúng cách dak-mock đếm). Lib tự chia lô để không vượt.

### 1.2 Response `200`

```json
{
  "database": "demo_db",
  "table": "users_cdr",
  "complete": false,
  "columns": [
    { "column": "msisdn",    "state": "READY",    "keyPrefix": "9f2c...", "keyVersion": 1 },
    { "column": "imei",      "state": "DISABLED" },
    { "column": "device_id", "state": "PENDING",  "requestCode": "YC-2026-000002" },
    { "column": "cell_id",   "state": "MISSING",  "reason": "not_requested" },
    { "column": "lac",       "state": "REJECTED", "requestCode": "YC-2026-000005", "reasonText": "..." }
  ]
}
```

| `state`    | Lib làm gì                                                                                              |
| ---------- | ------------------------------------------------------------------------------------------------------- |
| `READY`    | Dùng `keyPrefix` (phải là chuỗi khác rỗng, nếu không là lỗi dữ liệu)                                    |
| `PENDING`  | Lỗi: YC đã mở, chờ người có quyền tạo key. Nêu `requestCode`                                            |
| `MISSING`  | Lỗi. `reason=not_requested`: sai tên cột hoặc cột chưa từng được mã hoá. `yc_limit_reached`: thử lại sau |
| `REJECTED` | Lỗi kết thúc, không retry. Nêu `requestCode`                                                            |
| `DISABLED` | Lỗi: key đang bị khoá                                                                                   |
| khác       | Lỗi: response không hợp lệ (DAK thêm trạng thái mới mà lib chưa biết)                                   |

**Quy tắc "tất cả hoặc không":** chỉ trả key khi **mọi** cột được hỏi đều `READY`. Có một cột khác `READY` thì báo lỗi cho cả lời gọi,
liệt kê từng cột lỗi kèm `state`, `requestCode`, `reason`. Không bao giờ mã hoá một nửa số cột.

Lib còn kiểm tra: `database`/`table` trong response khớp request; mỗi cột được hỏi xuất hiện đúng một lần. `complete` chỉ dùng để đối
chiếu (nếu `complete=true` mà có cột khác `READY` thì coi là response không hợp lệ).

Field không áp dụng cho trạng thái của cột **không có mặt** trong JSON (dak-mock dùng `NON_NULL`, không trả `null`). Lib đọc mọi field
phụ (`keyVersion`, `requestCode`, `reason`, `reasonText`) như tuỳ chọn, chỉ bắt buộc `keyPrefix` khi `state=READY`.

### 1.3 Mã lỗi khác `200`

Giữ nguyên cách xử lý của `DakPrefixSource` hiện có:

| HTTP     | `error`                                                                | Lib                                                     |
| -------- | ---------------------------------------------------------------------- | ------------------------------------------------------- |
| 400      | `invalid_column`, `invalid_tag`, `invalid_on_missing`, `invalid_table` | Báo lỗi cấu hình, không retry                           |
| 401      | `invalid_token`                                                        | Bỏ token cache, xin token mới, thử lại **đúng 1 lần**   |
| 403      | `access_denied`                                                        | Báo lỗi (client team chưa có grant, hoặc gửi sai `tag`) |
| 429, 5xx | `rate_limited`, `internal_error`, `upstream_unavailable`               | Báo lỗi kèm `requestId`, không retry                    |

Thông báo lỗi gợi ý nguyên nhân theo đúng hành vi của dak-mock / DAK:

| Lỗi                                 | Gợi ý trong thông báo                                                                                   |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `400 invalid_tag`                   | Client nội bộ phải gửi `tag=internal`: kiểm `dak.tag` (`DAK_TAG`)                                        |
| `403 access_denied` từ `by-column`  | Client team thiếu grant trên ít nhất một cột, grant hết hạn, hoặc client team lại đặt `dak.tag=internal` |
| `MISSING` + `not_requested`         | Khi giải mã: sai tên bảng/cột, hoặc cột chưa từng được mã hoá                                           |
| `MISSING` + `yc_limit_reached`      | Đã đủ YC đang mở cho bảng; chờ người có quyền bên DAK xử lý rồi thử lại                                 |
| `PENDING`                           | Chờ người có quyền bên DAK tạo key cho `requestCode`; với dak-mock là `POST /mock-admin/yc/{code}/fulfill` |

Thông báo lỗi **không bao giờ** chứa token, secret, `keyPrefix` hay body thô của DAK; chỉ mã lỗi, `requestId`, tên bảng/cột, `state`,
`requestCode`, `reason`. `reasonText` (text tự do của người từ chối) không đưa vào thông báo, chỉ ghi log mức `DEBUG` sau khi cắt độ dài.

## 2. Thiết kế trong `key-prefix-lib`

### 2.1 API mới, tách khỏi `PrefixSource`

`PrefixSource.read(datasetName): String` giữ nguyên cho `column-crypto-lib`. Thêm trait mới:

```scala
/** Mục đích lời gọi: quyết định onMissing gửi lên DAK. */
sealed trait KeyIntent
object KeyIntent {
  case object Encrypt extends KeyIntent   // onMissing=request
  case object Decrypt extends KeyIntent   // onMissing=none
}

trait ColumnKeySource {
  /** Key của từng cột (khoá là tên cột chữ thường). Ném lỗi nếu có cột nào không READY. */
  def readColumns(datasetName: String, columns: Seq[String], intent: KeyIntent): Map[String, String]
}
```

### 2.2 Các lớp

| Lớp                              | Việc                                                                                                                                                                                                     |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DakColumnRef`                   | Kiểm và chuẩn hoá tên cột như `DakTableRef`                                                                                                                                                              |
| `DakClient` (tách từ `DakPrefixSource`) | Gửi GET có Bearer, `X-Request-Id`, `X-Dak-Caller`; 401 → xin token mới, thử lại 1 lần; trích `requestId`; dựng thông báo lỗi chung. `DakPrefixSource` chuyển sang dùng lớp này, **hành vi không đổi** |
| `DakColumnKeySource`             | Chuẩn hoá, loại trùng, chia lô, gọi `by-column`, parse, áp quy tắc "tất cả hoặc không"                                                                                                                   |
| `ColumnBatches`                  | Hàm thuần: chia danh sách cột theo 100 cột / 4096 ký tự                                                                                                                                                  |
| `ColumnKeysUnavailableException` | Lỗi riêng cho ca có cột không `READY`, mang danh sách `(column, state, requestCode, reason)` để caller (cdr-crypto, audit) đọc được                                                                     |
| `CachedColumnKeySource`          | Cache theo `(database.table, column)`, TTL `cacheTtlSeconds`. Cache miss chỉ gọi DAK cho **các cột còn thiếu**, gộp một lô. Không cache khi lời gọi lỗi                                                  |
| `ColumnKeySourceFactory`         | Dựng nguồn từ `ConfigSource`; dùng chung `KeycloakTokenProvider.shared`                                                                                                                                  |

Vì key cột không bao giờ đổi, cache không có rủi ro dùng key cũ. TTL chỉ giới hạn thời gian một key đã bị **khoá** (`DISABLED`) vẫn còn
dùng được trong engine. Giữ mặc định 300 giây như hiện nay.

### 2.3 Cấu hình

Thêm hai khoá logic (ánh xạ trong `EnvConfigSource` và `spark.cdrcrypto.*`):

| Khoá logic   | Spark conf       | Biến môi trường | Ý nghĩa                                                                                      |
| ------------ | ---------------- | --------------- | -------------------------------------------------------------------------------------------- |
| `dak.tag`    | `<ns>dak.tag`    | `DAK_TAG`       | Gửi làm `tag`. Client nội bộ đặt `internal`. Để trống = không gửi                             |
| `dak.caller` | `<ns>dak.caller` | `DAK_CALLER`    | Gửi làm `X-Dak-Caller`. Phải khớp `[A-Za-z0-9._-]{1,128}`, sai thì lỗi cấu hình lúc khởi tạo |

Các khoá `dak.addr`, `dak.tokenUrl`, `dak.clientId`, `dak.clientSecret`, `dak.allowInsecureHttp`, `cacheTtlSeconds` dùng lại.
`clientId`/`clientSecret` là của **client nội bộ** DAK cấp (trên dak-mock: một phần tử `clients` có `kind: internal`).

**Ràng buộc một client:** client nội bộ không thuộc team nào nên gọi `/{database}/{table}` sẽ nhận `403` (DAK vòng 2, dak-mock làm
đúng như vậy). Trên sql-engine không sao vì `column_encrypt` và `cdr_encrypt` có namespace cấu hình riêng (`spark.columncrypto.*` /
`spark.cdrcrypto.*`). Trên SparkApplication hai lib đọc **chung** bộ biến `DAK_*`, nên một job không dùng được cả `column_encrypt`
(cần client team) lẫn `cdr_encrypt` (cần client nội bộ). Hiện mỗi job chỉ dùng một trong hai nên chấp nhận; ghi rõ trong
[SPARK_APP_REFERENCE.md](../project/SPARK_APP_REFERENCE.md).

**Nguồn key cột chỉ có `dak`.** `ColumnKeySourceFactory` báo lỗi rõ nếu `source` là `file` hoặc `vault`: không có key cột nào trong Vault cũ
và không có dữ liệu cũ cần fallback. Fallback thủ công về Vault (trong [DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md](DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md))
chỉ còn áp dụng cho `column_encrypt`.

## 3. Các bước (TDD: viết test trước, chạy đỏ, rồi cài đặt)

| #  | Việc                                                                  | Test                                                         |
| -- | --------------------------------------------------------------------- | ------------------------------------------------------------ |
| K1 | Tách `DakClient` khỏi `DakPrefixSource`                               | `DakPrefixSourceSpec` hiện có phải xanh, không sửa test      |
| K2 | `DakColumnRef` + `ColumnBatches`                                      | `DakColumnRefSpec`, `ColumnBatchesSpec`                      |
| K3 | `DakColumnKeySource`: request, parse, "tất cả hoặc không", mã lỗi     | `DakColumnKeySourceSpec` dùng `StubHttpServer` có sẵn        |
| K4 | `CachedColumnKeySource`                                               | `CachedColumnKeySourceSpec` (đồng hồ tiêm vào)               |
| K5 | `ColumnKeySourceFactory` + khoá `dak.tag`, `dak.caller`               | `ColumnKeySourceFactorySpec`, cập nhật `EnvConfigSourceSpec` |
| K6 | Cập nhật [KEY_PREFIX_SOURCES.md](KEY_PREFIX_SOURCES.md)               | —                                                            |
| K7 | Chạy thật với dak-mock trên cluster                                   | Kịch bản ở [plan mock mục 4](../dak-mock-api/DAK_MOCK_BY_COLUMN_PLAN.md#4-triển-khai) |

Stub HTTP của K3 trả **đúng các body mà dak-mock trả** (lấy từ `dak-mock/src/test/.../ColumnKeyApiTest.java`), để test hai phía đối
chiếu cùng một hợp đồng.

Ca test bắt buộc cho K3:

- Mọi cột `READY` → trả map đúng; request có đúng `onMissing` theo `KeyIntent`, có `tag` khi cấu hình, không có `tag` khi để trống.
- Một cột `PENDING` / `MISSING` (hai `reason`) / `REJECTED` / `DISABLED` / trạng thái lạ → `ColumnKeysUnavailableException`, thông báo có
  `requestCode`/`reason`, **không** có `keyPrefix` của các cột `READY` khác.
- `READY` mà `keyPrefix` rỗng hoặc thiếu; thiếu cột trong response; cột lặp; `database`/`table` lệch; `complete=true` mâu thuẫn → lỗi response.
- Tên cột trùng khác hoa thường → gửi một lần, map trả về đúng.
- 120 cột → hai lô; tổng ký tự vượt 4096 → chia lô theo độ dài.
- 401 rồi 200 → xin token mới đúng một lần; 401 hai lần → lỗi; 403, 400, 429, 503 → lỗi đúng mã, không retry.

Mục tiêu coverage ≥ 80% cho các lớp mới.

## 4. Rủi ro và việc còn mở

| Rủi ro / việc mở                                                                                                              | Cách xử lý                                                                                                    |
| ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Bản hợp đồng đính kèm vòng 6 (`YEU-CAU-TU-SA-YC.md` bản vòng 3) **chưa có trong repo**; tên trường JSON lấy từ ví dụ vòng 2 §4 | dak-mock là bản cài đặt tham chiếu của hợp đồng này. Gom toàn bộ phần parse vào một hàm của `DakColumnKeySource`; khi có bản chính thức thì đối chiếu và sửa lib cùng mock |
| DAK trả thêm field mới                                                                                                        | Bỏ qua field lạ (giống luồng hiện có)                                                                         |
| Nhiều lời gọi `cdr_encrypt` trong một query tạo nhiều request khi cache trống                                                 | Cache theo cột; DataFrame API gom các cột của cùng bảng thành một lời gọi (xem plan cdr-crypto)               |
| Lộ secret của client nội bộ là lộ key mọi cột                                                                                 | Secret chỉ ở K8s Secret, không log; rotate bằng client thứ hai theo DAK                                       |
