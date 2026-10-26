# Hợp đồng: phiếu yêu cầu khử định danh (PYC)

Phần này là thứ **đưa team DAK**. Chỉ mô tả hành vi bên ngoài nhìn thấy được; không mô tả thiết kế nội bộ của mock.
Bối cảnh và các quyết định còn mở: [README.md](./README.md). Cách mock hiện thực: [MOCK_IMPLEMENTATION_PLAN.md](./MOCK_IMPLEMENTATION_PLAN.md).

> **Tên "PYC":** ở đây PYC là **phiếu yêu cầu khử định danh**. Trong [docs/key-access/](../key-access/) "PYC" còn là phiếu cấp quyền key theo bảng của DAK; hai loại phiếu khác nhau.

**Người dùng không gọi API.** Team được DAK cấp tài khoản nội bộ, đăng nhập portal và upload file phiếu. Vì vậy hợp đồng này mô tả:
**nội dung phiếu và quy tắc kiểm** (mục 1–3), **hành vi của portal** (mục 4), **việc của admin** (mục 5), **key khử định danh** (mục 6) và **gửi job** (mục 7).
Các URL `/mock-portal/**` và `/mock-admin/**` ở mục 4–5 chỉ là bề mặt HTTP **của mock** để chạy thử; DAK thật hiện thực bằng giao diện của mình, **giữ nguyên hành vi và chuyển trạng thái**.

## 1. Phiếu

```json
{
  "requestCode": "PYC-2026-000001",
  "state": "PENDING_APPROVAL",
  "database": "demo_db",
  "table": "users_cdr",
  "columns": [
    { "column": "msisdn", "mode": "HASH" },
    { "column": "imei", "mode": "HASH" }
  ],
  "dagId": "deid_pseudonymize_table",
  "purpose": "Chia sẻ dữ liệu cho đối tác X phục vụ phân tích churn",
  "team": "teamtscauiy",
  "createdBy": "hungvt",
  "createdAt": "2026-10-23T08:05:00Z",
  "sourceFile": { "name": "phieu-khu-dinh-danh.json", "sha256": "9f2c…" }
}
```

`createdBy` là **tên tài khoản** đã đăng nhập, `team` là team của tài khoản đó (người dùng không tự khai team). `sourceFile.name` chỉ để hiển thị:
bỏ phần đường dẫn, bỏ ký tự điều khiển, tối đa 255 ký tự. `sourceFile.sha256` là băm của đúng các byte đã upload (DAK thật nên giữ cả file gốc để đối chiếu khi cần).

Field theo trạng thái (chỉ có mặt khi áp dụng, không trả `null`):

| Field                           | Có khi                                                                                             |
| ------------------------------- | -------------------------------------------------------------------------------------------------- |
| `decidedAt`, `decidedBy`        | `APPROVED` trở đi, hoặc `REJECTED`                                                                 |
| `reasonText`                    | `REJECTED` (lý do admin ghi)                                                                       |
| `outputDatabase`, `outputTable` | `APPROVED` trở đi (DAK quyết tên, người dùng không đặt)                                            |
| `keyVersion`                    | `JOB_SUBMITTED` trở đi: version của key bảng trong Vault đã giao cho job (không phải giá trị key)  |
| `jobRef` = `{ "kind", "name" }` | `JOB_SUBMITTED` trở đi. `kind` = `SparkApplication` hoặc `AirflowDagRun`                           |
| `submitError`                   | `SUBMIT_FAILED` (tóm tắt, không chứa token/secret/key)                                             |
| `jobState`, `jobFinishedAt`     | `RUNNING`, `SUCCEEDED`, `FAILED`                                                                   |

`requestCode`: `PYC-<năm>-<6 số>`, tăng dần. Phiếu **thuộc team** (không theo từng người), và khác `YC-…` (yêu cầu tạo key theo cột của luồng nội bộ).

## 2. Nội dung file phiếu

Các trường **logic** DAK thật cần thu thập, bất kể portal dùng form, Excel hay JSON (định dạng thật: [README](./README.md) Q2). Mock nhận JSON:

```json
{
  "database": "demo_db",
  "table": "users_cdr",
  "columns": [{ "column": "msisdn", "mode": "HASH" }, { "column": "imei" }],
  "dagId": "deid_pseudonymize_table",
  "purpose": "Chia sẻ dữ liệu cho đối tác X phục vụ phân tích churn"
}
```

| Trường              | Quy tắc                                                                                                                                                                                                              | Mã lỗi (`code`)   |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------- |
| `database`, `table` | Bắt buộc. `[A-Za-z0-9_]{1,128}`, chuẩn hoá chữ thường                                                                                                                                                                | `invalid_table`   |
| `columns`           | Bắt buộc, 1..100 phần tử. Mỗi tên cùng quy tắc, chữ thường, **gộp trùng**                                                                                                                                              | `invalid_column`  |
| `columns[].mode`    | `HASH`; bỏ trống = `HASH`. `ENCRYPT`/`MAPPING` chưa hỗ trợ ([README](./README.md) Q5, O2): từ chối                                                                                                                      | `invalid_mode`    |
| `dagId`             | **Bắt buộc khi DAK trigger job bằng Airflow**, người dùng chọn. Khớp `[A-Za-z0-9_][A-Za-z0-9_.-]{0,249}` (không bắt đầu bằng `.` hay `-`) **và nằm trong danh sách cho phép** của DAK (`allowed-dag-ids`). Khi DAK không dùng Airflow: được phép khai nhưng bị bỏ qua | `invalid_dag`     |
| `purpose`           | Bắt buộc, 1..500 ký tự sau `strip`                                                                                                                                                                                    | `invalid_purpose` |
| (cả file)           | Tối đa **64 KB**, đúng định dạng, **không có trường lạ** (trường lạ bị từ chối, không bỏ qua im lặng)                                                                                                                  | `invalid_file`    |

`dagId` ngoài danh sách cho phép bị từ chối **với cùng một thông điệp** dù DAG có tồn tại trên Airflow hay không, để người dùng không dò được tên DAG của hệ thống.
Danh sách `dagId` được phép có thể hiển thị cho người dùng trên giao diện (mock: `GET /mock-portal/deid-dags`).

Kiểm file **gom mọi lỗi một lượt** (người dùng thấy hết lỗi để sửa file một lần, không phải upload nhiều lần).
Có lỗi nào thì **không tạo phiếu**. Lỗi trả theo dạng:

```json
{
  "error": "invalid_file",
  "message": "The uploaded request has 2 problems",
  "problems": [
    { "field": "columns[1].column", "code": "invalid_column", "message": "name must match [A-Za-z0-9_]{1,128}" },
    { "field": "dagId", "code": "invalid_dag", "message": "dagId is not allowed" }
  ],
  "requestId": "…"
}
```

## 3. Giới hạn và chống trùng

- Tối đa **20** phiếu chưa ở trạng thái cuối cho mỗi team (`dak.deid.max-active-per-team`); vượt: từ chối upload (`too_many_active_requests`).
- Không cho hai phiếu **đang hoạt động** của cùng team trùng cả `database`, `table` và tập cột: từ chối (`duplicate_request`) kèm `requestCode` của phiếu cũ.
  Nhờ vậy bấm upload hai lần (hoặc tải lại trang) không tạo hai phiếu.

## 4. Hành vi portal

Mọi thao tác yêu cầu đăng nhập; người dùng chỉ thấy và thao tác trên phiếu **của team mình**. Phiếu của team khác hoặc không tồn tại: cùng một kết quả
"không tìm thấy" (không lộ phiếu có tồn tại).

| Thao tác                 | Việc                                                                                       | Trạng thái nguồn → đích          |
| ------------------------ | ------------------------------------------------------------------------------------------ | -------------------------------- |
| Upload phiếu             | Kiểm file (mục 2) và giới hạn (mục 3); hợp lệ thì tạo phiếu                                 | → `PENDING_APPROVAL`             |
| Huỷ phiếu                | Người dùng huỷ phiếu đang chờ duyệt                                                         | `PENDING_APPROVAL` → `CANCELLED` |
| Xem một phiếu            | Nếu job đang chạy, DAK làm mới `jobState` từ hệ thống chạy job trước khi hiển thị (mục 7.5) | —                                |
| Danh sách phiếu của team | Mới nhất trước, lọc theo `state`, phân trang                                               | —                                |
| Xem `dagId` được phép    | Danh sách để người dùng chọn khi điền phiếu                                                | —                                |

Thao tác sai trạng thái (vd huỷ phiếu đã duyệt) bị từ chối (`invalid_state`, nêu trạng thái hiện tại). Huỷ lặp lại trên phiếu đã `CANCELLED` **không phải lỗi**
(trả lại phiếu, idempotent).

Không có sửa phiếu: muốn đổi nội dung thì huỷ rồi upload file mới.

**Bề mặt HTTP của mock** (để chạy thử bằng `curl`, header `X-Mock-Portal-Token` thay cho đăng nhập):

| Endpoint                                               | Tương ứng                                              | Thành công |
| ------------------------------------------------------ | ------------------------------------------------------ | ---------- |
| `POST /mock-portal/deid-requests` (multipart `file`)   | Upload phiếu                                           | `201`      |
| `POST /mock-portal/deid-requests/{code}/cancel`        | Huỷ phiếu                                              | `200`      |
| `GET /mock-portal/deid-requests/{code}`                | Xem một phiếu                                          | `200`      |
| `GET /mock-portal/deid-requests?state=&limit=&cursor=` | Danh sách phiếu của team; `limit` 1..100 (mặc định 20) | `200`      |
| `GET /mock-portal/deid-dags`                           | `dagId` được phép                                      | `200`      |

Danh sách trả `{ "requests": [ … ], "nextCursor": "PYC-2026-000007" }`, mới nhất trước. `nextCursor` là mã phiếu cuối của trang và **chỉ có mặt khi còn phiếu phía sau**;
truyền lại làm `cursor` để lấy trang kế (vẫn dùng được khi phiếu đó không còn khớp bộ lọc `state`). `state` không phân biệt hoa thường. `GET /mock-portal/deid-dags` trả `{ "dagIds": [ … ] }`
(rỗng khi DAK không dùng Airflow). Mã phiếu sai định dạng (không phải `PYC-<năm>-<6 số>`) trả `404 not_found` giống phiếu không tồn tại.

## 5. Việc của admin

Trong mock: `/mock-admin/deid-requests/**` với header `X-Mock-Admin-Token` (sai/thiếu → `404`, như `/mock-admin/yc`). Ở DAK thật đây là
giao diện duyệt phiếu với phân quyền admin; **hành vi và chuyển trạng thái giữ nguyên bảng dưới**. Admin thấy đủ nội dung phiếu, gồm `dagId` và `purpose`.

| Endpoint (mock)                                                 | Việc                                                                                                            | Nguồn → đích                                                           |
| --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `GET  /mock-admin/deid-requests?state=PENDING_APPROVAL`         | Liệt kê phiếu mọi team                                                                                          | —                                                                      |
| `GET  /mock-admin/deid-requests/{code}`                         | Xem phiếu                                                                                                       | —                                                                      |
| `POST /mock-admin/deid-requests/{code}/approve`                 | Duyệt: chốt `outputDatabase/outputTable`, **lấy key bảng từ Vault và giao cho job** (mục 6), **gửi job** (mục 7) | `PENDING_APPROVAL` → `APPROVED` → `JOB_SUBMITTED` hoặc `SUBMIT_FAILED` |
| `POST /mock-admin/deid-requests/{code}/reject` `{"reasonText"}` | Từ chối, `reasonText` bắt buộc, ≤ 500 ký tự                                                                     | `PENDING_APPROVAL` → `REJECTED`                                        |
| `POST /mock-admin/deid-requests/{code}/retry-submit`            | Làm lại bước lấy key + gửi job sau `SUBMIT_FAILED`                                                              | `SUBMIT_FAILED` → `JOB_SUBMITTED`                                      |

`approve` trả `200` với phiếu **sau khi đã thử lấy key và gửi job**: `JOB_SUBMITTED` nếu được, `SUBMIT_FAILED` kèm `submitError` nếu không (vẫn là `200`,
vì việc duyệt đã thành công; lỗi nằm ở trạng thái phiếu). `approve` lặp lại trên phiếu đã duyệt trả `200` cùng phiếu, **không giao key và không gửi job lần hai**.
Duyệt phiếu đã `CANCELLED` bị từ chối (`invalid_state`): người dùng huỷ trước thì admin không duyệt được.

## 6. Key khử định danh theo bảng

Phiếu **không** kiểm key cột, cũng không kiểm cột đã mã hoá hay chưa ([README](./README.md) Q1, Q7). Key khử định danh là key **theo bảng**, do **DAK backend** đọc
từ **Vault của DAK** lúc duyệt (Q13), cùng nguồn với key bảng của luồng `GET /api/v1/keys/{database}/{table}`.

1. **Đọc:** lấy `keyPrefix` và version của bảng `(database, table)`. Bảng chưa có key → `SUBMIT_FAILED` với `submitError="table key not found"` (tạo key rồi `retry-submit`); phiếu **không** bị từ chối.
2. **Kiểm định dạng:** job HASH cần đúng **32 byte** (HMAC). `keyPrefix` phải là base64 của đúng 32 byte; khác thì `SUBMIT_FAILED` với `submitError="table key has unsupported format"`. Thông báo **không bao giờ** chứa key.
3. **Giao key cho job bằng tham chiếu, không bằng giá trị.** DAK tạo một **Secret riêng cho phiếu** (tên `deid-key-<mã phiếu>`, chữ thường) trong namespace chạy job, chứa key ở dạng file
   `pseudonym.hmac_v1` với giá trị `base64:<keyPrefix>` (đúng định dạng nguồn `file` mà job đọc: [mẫu](../../pseudonymize-table-app/k8s/pseudonym-keys-secret.yaml)). Job nhận **tên Secret** và mount nó.
   Key **không** xuất hiện trong phiếu, `conf` của Airflow, biến môi trường của SparkApplication, log, audit hay response. (Airflow lưu `conf` trong cơ sở dữ liệu và hiện trên giao diện nên không dùng để chuyển key.)
4. **Dọn dẹp:** DAK xoá Secret khi phiếu thấy job kết thúc (`SUCCEEDED`, `FAILED` hoặc `jobState=UNKNOWN`) và khi lấy key/gửi job lỗi giữa chừng. Secret gắn nhãn `dak.vai/request-code` để quét dọn thủ công nếu DAK mất trạng thái.
5. **Chỉ `HASH`.** `ENCRYPT` cần khoá 64 byte và `MAPPING` cần bảng ánh xạ; một key bảng 32 byte không đủ cho chúng nên các mode này bị từ chối ở mục 2.

Hệ quả cho bên dùng dữ liệu đầu ra: nếu cột đã mã hoá không tất định thì `HASH` trên ciphertext không còn liên kết được bản ghi của cùng một chủ thể.

## 7. Hợp đồng "gửi job"

Team DAK tự chọn cách hiện thực (kubectl, Kubernetes API, Airflow). Điều bắt buộc về hành vi:

1. **Idempotent theo `requestCode`**: gửi lần hai cho cùng phiếu không tạo job thứ hai. Tên job suy ra từ mã phiếu: `deid-2026-000001`
   (chữ thường, hợp DNS-1123; Airflow dùng làm `dag_run_id`). Hệ thống báo đã tồn tại (`409`) được coi là **đã gửi**, lấy lại tham chiếu.
2. **Người dùng chỉ chọn được** `database`, `table`, danh sách cột, `mode` (đã qua kiểm tra) và `dagId` (nằm trong danh sách cho phép, chỉ có nghĩa khi dùng Airflow). Image, env, khoá, đường dẫn
   đều do DAK (template cấu hình, người dùng không sửa được). Giá trị chèn vào template phải qua bộ ký tự `[a-z0-9_]` (cột) trước khi thay. `purpose` và `sourceFile` **không bao giờ** đi vào manifest.
3. Job nhận đúng các tham số của [`pseudonymize-table-app`](../../pseudonymize-table-app/README.md), **truyền lúc trigger** (Airflow: `conf` của DAG run; SparkApplication: biến môi trường).
   Tên bảng đích do DAK quyết, người dùng không đặt. Airflow xác thực bằng `client_id`/`client_secret` của Keycloak (luồng tự động, không dùng tài khoản người) và nhận `Authorization: Bearer <access token>`:

| Tham số job      | Giá trị từ phiếu                                                                                                                      |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `DB_NAME`        | `database`                                                                                                                            |
| `TABLE_NAME`     | `table`                                                                                                                               |
| `PSEUDO_COLUMNS` | `msisdn:HASH,imei:HASH` (mỗi cột `tên:MODE`)                                                                                          |
| `OUTPUT_DB`      | `outputDatabase`                                                                                                                      |
| `OUTPUT_TABLE`   | `outputTable` (**không** được trùng bảng nguồn)                                                                                       |
| `OUTPUT_MODE`    | `overwrite` (mỗi phiếu có bảng đích riêng)                                                                                            |
| `VERIFY`         | `true`                                                                                                                                |
| `KEY_SECRET`     | **Tên** Secret chứa key của phiếu (mục 6). Job mount Secret này làm thư mục khoá (`/etc/pseudonym-keys`) và đặt nguồn khoá `file`    |

4. Job gắn **nhãn truy vết** `dak.vai/request-code=<requestCode>` (SparkApplication: `metadata.labels`; Airflow: `conf.requestCode`) để từ
   job tìm lại phiếu kể cả khi DAK mất trạng thái.
5. Theo dõi trạng thái:

| Hệ thống chạy job | Nguồn trạng thái                      | Ánh xạ                                                                                                                          |
| ----------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| SparkApplication  | `status.applicationState.state`       | `SUBMITTED`/`PENDING_RERUN`/`RUNNING` → `RUNNING`; `COMPLETED` → `SUCCEEDED`; `FAILED`/`SUBMISSION_FAILED`/`FAILING` → `FAILED` |
| Airflow DAG run   | `GET /dagRuns/{dag_run_id}` → `state` | `queued`/`running` → `RUNNING`; `success` → `SUCCEEDED`; `failed` → `FAILED`                                                    |

SparkApplication chưa có `status` (mới tạo, Operator chưa xử lý) coi là `RUNNING`. SparkApplication đã bị xoá (do `timeToLiveSeconds`) mà phiếu chưa
ghi nhận trạng thái cuối: `jobState=UNKNOWN`, `state` của phiếu giữ nguyên. Đặt `timeToLiveSeconds` ≥ 604800 trong template để tránh trường hợp này.
Với Airflow: `dagId` không tồn tại (`404` khi trigger) → `SUBMIT_FAILED` với `submitError="airflow dag not found"`.

Exit code của driver `pseudonymize-table-app`: `0` xong và đối chiếu đạt, `1` lỗi chạy, `2` sai cấu hình, `3` đã ghi nhưng đối chiếu **không** đạt.
Với SparkApplication, mã `3` hiện ra ở trạng thái `FAILED`; phân biệt cần đọc log driver, nên `FAILED` là đủ ở mức phiếu.

## 8. Mã lỗi

Áp dụng cho cả bề mặt HTTP của mock; DAK thật hiển thị tương đương trên giao diện.

| HTTP | `error`                    | Khi                                                                       |
| ---- | -------------------------- | ------------------------------------------------------------------------- |
| 400  | `invalid_file`             | File sai định dạng, quá 64 KB, có trường lạ, hoặc có lỗi trong `problems` |
| 400  | `invalid_request`          | Tham số danh sách sai: `limit` ngoài 1..100 hoặc không phải số, `state` lạ, `cursor` lạ hoặc của team khác |
| 401  | `invalid_credentials`      | Chưa đăng nhập / token portal của mock sai (DAK thật: phiên hết hạn)      |
| 404  | `not_found`                | Phiếu không tồn tại **hoặc** thuộc team khác                              |
| 409  | `invalid_state`            | Thao tác không hợp lệ ở trạng thái hiện tại                               |
| 409  | `duplicate_request`        | Đã có phiếu đang hoạt động trùng bảng và tập cột                          |
| 429  | `too_many_active_requests` | Vượt giới hạn phiếu đang hoạt động của team                               |

Lỗi nội dung file (mục 2) nằm trong `problems[].code` (`invalid_table`, `invalid_column`, `invalid_mode`, `invalid_dag`, `invalid_purpose`), riêng `invalid_file` ở mức ngoài khi cả file hỏng.
Lỗi lấy key và gửi job **không** là lỗi HTTP: chúng đưa phiếu vào `SUBMIT_FAILED` (mục 5, 6).

## 9. Audit

Một dòng cho **mỗi lần chuyển trạng thái** (logger `AUDIT`), không bao giờ ghi token, key hay secret:

```text
DEID_TRANSITION requestCode=PYC-2026-000001 from=PENDING_APPROVAL to=APPROVED actor=admin team=teamtscauiy database=demo_db table=users_cdr columns=msisdn,imei dagId=deid_pseudonymize_table result=OK
```

Upload ghi `from=- to=PENDING_APPROVAL actor=<tên tài khoản> fileSha256=<băm>`. Thao tác bị từ chối ghi `result=ERROR error=<mã>`. Lấy key ghi `KEY_DELIVERY requestCode=… keyVersion=<n> result=OK|ERROR` (không có giá trị key).
`actor` của admin = `admin` trong mock (token dùng chung); DAK thật ghi tài khoản admin. `purpose` không vào audit (văn bản tự do của người dùng).
