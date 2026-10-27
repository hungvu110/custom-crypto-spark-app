# Plan: `dak-mock` mô phỏng `GET /api/v1/keys/by-column`

Ngày: 2026-10-22. Phạm vi: module [`dak-mock/`](../../dak-mock/README.md) (Spring Boot, Java 17). Mục đích: có một DAK giả chạy trong cluster
test để `key-prefix-lib` ([../key-access/DAK_BY_COLUMN_CLIENT_PLAN.md](../key-access/DAK_BY_COLUMN_CLIENT_PLAN.md)) và `cdr-crypto`
([../cdr-crypto/CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md](../cdr-crypto/CDR_CRYPTO_PER_COLUMN_KEY_PLAN.md)) chạy được đầu-cuối trước khi DAK thật
xong nhánh nội bộ.

Hợp đồng mô phỏng theo [../key-access/API-LAY-KEY-DUONG-DAN-COT.md](../key-access/API-LAY-KEY-DUONG-DAN-COT.md) (bản tổng hợp mới nhất của DAK,
thay các vòng trao đổi trước trong [../key-access/dak_response/](../key-access/dak_response/)). Mock chỉ mô phỏng **những gì
bên gọi thấy được**, không mô phỏng thiết kế nội bộ của DAK.

## 0. Mock làm gì và không làm gì

| Có                                                                                | Không                                                         |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `GET /api/v1/keys/by-column` đúng hình dạng request/response/mã lỗi của hợp đồng  | Phiếu PYC: grant của client team khai tĩnh trong `dak.grants` |
| Hai loại client: **nội bộ** (`tag=internal`) và **team**                          | Giao diện quản trị, phiếu PYC, cảnh báo hết hạn               |
| YC: `PENDING` → tạo key hoặc từ chối; giới hạn 50 YC mở; `REJECTED` dính theo cột | DB: YC giữ trong bộ nhớ, mất khi pod restart                  |
| Key cột lưu ở Vault KV v2, **ghi với `cas=0`** nên không bao giờ ghi đè           | Rate limit (`429`)                                            |
| `by-path` (key theo đường dẫn): xem [dak-mock/README.md](../../dak-mock/README.md) mục 2.3 |                                                  |
| Endpoint quản trị riêng của mock để giả lập "người có quyền bên DAK"              |                                                               |
| Endpoint cũ `GET /api/v1/keys/{database}/{table}` **giữ nguyên**                  |                                                               |

## 1. Hợp đồng mock phục vụ

### 1.1 Request

```http
GET /api/v1/keys/by-column?database=demo_db&table=users_cdr&columns=msisdn,imei&onMissing=request&tag=internal
Authorization: Bearer <access_token>
X-Request-Id: ...        (tuỳ chọn)
X-Dak-Caller: ...        (tuỳ chọn)
```

Thứ tự kiểm tra (như endpoint cũ: định dạng **trước** token, để `400` không cần gọi Keycloak):

1. `database`, `table`: `[A-Za-z0-9_]{1,128}` → chữ thường. Sai: `400 invalid_table`.
2. `columns`: tách dấu phẩy, mỗi tên cùng quy tắc → chữ thường → **gộp trùng**, giữ thứ tự xuất hiện đầu. Rỗng, sai tên, quá **100 cột** hoặc
   quá **4096 ký tự** (sau gộp): `400 invalid_column`, thông báo nói rõ vượt cái nào.
3. `onMissing`: `request` | `none`, không gửi = `none`. Khác: `400 invalid_on_missing`.
4. `tag`: không gửi hoặc `internal`. Khác: `400 invalid_tag`.
5. Xác minh token (dùng lại `TokenVerifier`). Lỗi: `401 invalid_token` / `503 upstream_unavailable`.
6. Bảng `tag` × loại client:

| Ca                                  | Mock trả                                                     |
| ----------------------------------- | ------------------------------------------------------------ |
| Client nội bộ, `tag=internal`       | Luồng nội bộ (mục 1.2)                                       |
| Client nội bộ, không `tag`          | `400 invalid_tag` ("internal client must send tag=internal") |
| Client team, `tag=internal`         | `403 access_denied`                                          |
| Client team, không `tag`            | Luồng team (mục 1.2b)                                        |
| Client team gửi `onMissing=request` | Bỏ qua, coi là `none` (client team không bao giờ mở YC)      |

`X-Dak-Caller` khớp `[A-Za-z0-9._-]{1,128}` thì ghi vào audit; sai định dạng thì **bỏ qua header**, không từ chối.

### 1.2 Luồng nội bộ: trạng thái từng cột

Với mỗi cột, xét theo thứ tự:

| Điều kiện                                                               | `state`    | Field kèm                             |
| ----------------------------------------------------------------------- | ---------- | ------------------------------------- |
| Cột nằm trong danh sách khoá                                            | `DISABLED` | —                                     |
| Có key trong Vault                                                      | `READY`    | `keyPrefix`, `keyVersion`             |
| Có YC `REJECTED` cho `(database, table, column)`                        | `REJECTED` | `requestCode`, `reasonText`           |
| Có YC `OPEN` cho cột                                                    | `PENDING`  | `requestCode` (cùng mã, không mở mới) |
| `onMissing=none`                                                        | `MISSING`  | `reason: not_requested`               |
| `onMissing=request`, đã có 50 YC `OPEN` cho `(client, database, table)` | `MISSING`  | `reason: yc_limit_reached`            |
| `onMissing=request`                                                     | `PENDING`  | `requestCode` mới (`YC-<năm>-<6 số>`) |

`REJECTED` khớp theo `(database, table, column)`, **không theo client**: client nội bộ thứ hai (sau rotate) cũng nhận `REJECTED`.

Cột bị khoá chỉ có nghĩa khi cột đã có key: `DISABLED` thay cho `READY`, key trong Vault giữ nguyên.

### 1.2b Luồng client team

Theo vòng 2 của DAK: grant theo **cột**, kiểm **trước** khi tra key.

1. Mọi cột được hỏi phải có grant còn hạn của team trong `dak.grants` (`team`, `database`, `table`, `columns`, `expires-at`). Thiếu
   một cột, hoặc grant đã hết hạn: `403 access_denied` **cả lô**, cùng một thân, kể cả khi cột chưa có key (không dò được cột nào
   đã có key).
2. Có đủ grant: mỗi cột `READY` (hoặc `DISABLED` nếu bị khoá), chưa có key thì `MISSING` + `reason: not_requested`. **Không bao giờ
   mở YC**, `onMissing` bị bỏ qua.

`dak.grants` thay cho phiếu được duyệt của DAK: DAK chưa làm grant theo cột (vòng 2 §3), nên mock mô phỏng đúng hành vi đã thống nhất
nhưng nguồn grant là cấu hình tĩnh.

### 1.3 Response

Luôn `200` khi qua được mục 1.1:

```json
{
  "database": "demo_db",
  "table": "users_cdr",
  "complete": false,
  "columns": [
    {
      "column": "msisdn",
      "state": "READY",
      "keyPrefix": "9f2c...",
      "keyVersion": 1
    },
    { "column": "imei", "state": "PENDING", "requestCode": "YC-2026-000002" }
  ]
}
```

`complete = true` khi mọi cột `READY`. Field không áp dụng thì **không có mặt** (không trả `null`). `X-Request-Id` và `Cache-Control: no-store`
do `RequestIdFilter` sẵn có thêm vào mọi response. Body lỗi giữ định dạng `{"error","message","requestId"}`.

## 2. Thiết kế trong `dak-mock`

### 2.1 Cấu hình (`dak.*`)

```yaml
dak:
  clients:
    - realm: vlp-tenantw1xjixm
      client-id: <client-noi-bo>
      kind: internal # mới: internal | team (mặc định team). internal không cần workspace/team
    - realm: vlp-tenantw1xjixm
      client-id: <client-cua-team>
      workspace: wsw7vtwvi
      team: teamtscauiy
  grants: # mới: grant theo cột của client team
    - team: teamtscauiy
      database: demo_db
      table: users_cdr
      columns: [msisdn, imei]
      expires-at: 2027-10-22T00:00:00Z # ISO-8601; để trống = không hết hạn
  vault:
    # mới: path key cột. Mỗi phần là MỘT đoạn đường dẫn riêng
    column-path-template: hla-datalake/datalake/column-keys/{database}/{table}/{column}
  yc:
    max-open-per-table: 50 # theo (client, database, table)
    auto-fulfill: false # true: onMissing=request tạo key ngay và trả READY (chỉ để chạy e2e nhanh)
  admin:
    token: ${MOCK_ADMIN_TOKEN:} # để trống = tắt endpoint quản trị
```

`DakProperties` kiểm thêm lúc khởi động: `kind` hợp lệ; client `team` vẫn bắt buộc `workspace`/`team`; `column-path-template` chứa đủ
`{database}`, `{table}`, `{column}`.

### 2.2 Lưu key cột

- **Đọc**: `GET {vault}/v1/{kvMount}/data/{column-path}`, field `keyPrefix`; `404` nghĩa là chưa có key. `keyVersion` lấy từ `metadata.version`.
- **Ghi** (khi tạo key cho YC): `POST {vault}/v1/{kvMount}/data/{column-path}` với `{"options":{"cas":0},"data":{"keyPrefix":"..."}}`. `cas=0`
  nghĩa là Vault chỉ cho ghi khi path **chưa tồn tại**, nên mock không thể ghi đè key đã có, giống cam kết của DAK ở vòng 6. Vault từ chối vì
  đã tồn tại thì mock dùng key đang có, không sinh key khác.
- Giá trị sinh: 32 byte ngẫu nhiên (`SecureRandom`) mã hoá Base64. Tên cột đã qua kiểm tra `[a-z0-9_]` nên không cần escape.
- Token Vault của mock (`VAULT_TOKEN`) cần quyền `read` trên `kv/data/hla-datalake/datalake/column-keys/*`, và thêm `create` nếu muốn mock tự tạo key khi `fulfill` YC. Path nằm dưới
  `hla-datalake/datalake/` để người vận hành tạo được secret tay.

### 2.3 YC trong bộ nhớ

`YcRegistry` (thread-safe): map `(database, table, column)` → YC `{requestCode, state OPEN|REJECTED, clientId, createdAt, reasonText}`, và bộ đếm YC
`OPEN` theo `(client, database, table)`. Mã YC tăng dần trong tiến trình. Mất khi restart: lời gọi sau mở YC mới với mã mới. Chấp nhận được cho
môi trường test; Deployment giữ **1 replica**.

### 2.4 Endpoint quản trị của mock

Chỉ bật khi đặt `MOCK_ADMIN_TOKEN`; mọi request phải có header `X-Mock-Admin-Token` đúng giá trị, không thì `404` (không để lộ có endpoint).

| Endpoint                                                           | Việc                                                                                     |
| ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------- |
| `GET  /mock-admin/yc`                                              | Liệt kê YC (không có key)                                                                |
| `POST /mock-admin/yc/{requestCode}/fulfill`                        | Sinh key (hoặc nhận `{"keyValue": "..."}`), ghi Vault `cas=0`, xoá YC. Không trả lại key |
| `POST /mock-admin/yc/{requestCode}/reject` `{"reasonText": "..."}` | Đổi YC sang `REJECTED`                                                                   |
| `POST /mock-admin/columns/{database}/{table}/{column}/disable`     | Thêm cột vào danh sách khoá (trong bộ nhớ)                                               |
| `POST /mock-admin/columns/{database}/{table}/{column}/enable`      | Gỡ khoá; cột trả lại **chính key cũ**                                                    |

### 2.5 Lớp mới / sửa

| Lớp                           | Việc                                                                                                 |
| ----------------------------- | ---------------------------------------------------------------------------------------------------- |
| `ColumnKeyController`         | `GET /api/v1/keys/by-column`: kiểm tham số theo mục 1.1, gọi `ColumnKeyService`                      |
| `ColumnKeyRequest`            | Parse và chuẩn hoá `database`/`table`/`columns`/`onMissing`/`tag`                                    |
| `ColumnKeyService`            | Bảng `tag` × client, trạng thái từng cột (mục 1.2), dựng response, audit                             |
| `ColumnKeyStore`              | Đọc/ghi key cột ở Vault (`cas=0`)                                                                    |
| `YcRegistry`                  | YC và giới hạn 50                                                                                    |
| `MockAdminController`         | Mục 2.4                                                                                              |
| `TokenVerifier.Caller`        | Thêm `kind`; client nội bộ có `workspace`/`team` rỗng                                                |
| `DakApiException`             | Thêm `invalidColumn`, `invalidTag`, `invalidOnMissing`                                               |
| `KeyController`, `KeyService` | **Không đổi** hành vi. Client nội bộ gọi endpoint cũ nhận `403 access_denied` (không thuộc team nào) |

### 2.6 Audit

Một dòng cho **mỗi cột** (logger `AUDIT`), không bao giờ ghi key, token hay secret:

```text
KEY_FETCH_COLUMN state=PENDING httpStatus=200 clientId=... kind=internal caller=<X-Dak-Caller|-> database=demo_db table=users_cdr column=imei requestCode=YC-2026-000002 reason=-
```

Lỗi cả lời gọi (`400`/`401`/`403`) ghi một dòng `result=ERROR` như endpoint cũ.

## 3. Các bước (TDD)

Làm **trước** hai plan còn lại để có chỗ chạy thử đầu-cuối.

| #   | Việc                                                           | Test (`ColumnKeyApiTest`, kiểu `KeyApiTest`: MockWebServer giả Keycloak JWKS và Vault)                                         |
| --- | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| M1  | `DakProperties`: `kind`, `column-path-template`, `yc`, `admin` | Cấu hình sai thì ứng dụng không khởi động                                                                                      |
| M2  | `ColumnKeyRequest` + mã lỗi `400`                              | Mỗi ca `400` ở mục 1.1, và không có request nào tới JWKS                                                                       |
| M3  | Bảng `tag` × client, grant theo cột của client team            | Năm ca của mục 1.1 bước 6; mục 1.2b: đủ grant, thiếu một cột, grant hết hạn, team không có grant, không mở YC                  |
| M4  | `ColumnKeyStore` + `YcRegistry` + `ColumnKeyService`           | Mọi dòng của bảng mục 1.2; gọi lại trả cùng `requestCode`; giới hạn 50; `REJECTED` dính khi đổi client                         |
| M5  | `MockAdminController`                                          | fulfill → lời gọi sau `READY`; fulfill lần hai không đổi key (`cas=0`); reject; disable/enable trả key cũ; thiếu token → `404` |
| M6  | `auto-fulfill`                                                 | `onMissing=request` trả `READY` ngay                                                                                           |
| M7  | k8s + tài liệu (mục 4)                                         | —                                                                                                                              |

Ca test xuyên suốt: không response hay dòng log nào chứa `keyPrefix` ngoài trường `keyPrefix` của cột `READY`.

## 4. Triển khai

| Việc                                                                                     | Chi tiết                                                                                                                              |
| ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| [k8s/dak-mock.yaml](../../dak-mock/k8s/dak-mock.yaml) ConfigMap                          | Thêm client nội bộ (`kind: internal`), `vault.column-path-template`, `yc.*`                                                           |
| Secret                                                                                   | `MOCK_ADMIN_TOKEN` mới; `VAULT_TOKEN` cần `read` (và `create` nếu mock tự tạo key) trên `kv/data/hla-datalake/datalake/column-keys/*` |
| Keycloak                                                                                 | Tạo một client `client_credentials` đóng vai client nội bộ trong realm test                                                           |
| [README.md](../../dak-mock/README.md), [sa-fetch-key.sh](../../dak-mock/sa-fetch-key.sh) | Mô tả endpoint mới, endpoint quản trị, ví dụ `curl` cho `by-column`                                                                   |

Kịch bản chạy thử đầu-cuối trên cluster (sau khi xong cả ba plan):

1. Job `spark-application-cdr` mã hoá cột `name,city` lần đầu → lỗi `PENDING` kèm hai mã YC.
2. `GET /mock-admin/yc`, rồi `fulfill` hai YC.
3. Chạy lại job → mã hoá thành công; truy vấn `cdr_decrypt('demo_db.users_cdr', 'name', name, created_at)` ra giá trị gốc.
4. `disable` cột `city` → job sau lỗi `DISABLED` (sau khi cache 300 giây hết hạn); `enable` → chạy lại được với key cũ.
5. `reject` một YC của cột mới → lời gọi sau `REJECTED`, không mở YC mới.

## 5. Việc còn mở

| Việc                                                                            | Ảnh hưởng                                                                        |
| ------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Bản hợp đồng đính kèm vòng 6 chưa có trong repo; tên field theo ví dụ vòng 2 §4 | Khi có bản chính thức, đối chiếu và sửa mock cùng lúc với lib                    |
| Nhánh client team (grant theo cột) chưa làm ở DAK                               | Mock dùng `dak.grants` tĩnh; khi DAK có grant theo cột thì đối chiếu lại hành vi |
