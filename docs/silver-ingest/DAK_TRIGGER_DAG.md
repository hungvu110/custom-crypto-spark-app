# DAK ↔ Airflow: phần đặt lịch và trigger DAG cho phiếu chuẩn hoá silver

Tài liệu cho đội DAK. Mô tả **DAK phải làm gì với Airflow** khi xử lý phiếu chuẩn hoá silver, để code của DAK chạy đúng với các DAG đã có.
Bản chạy thử đã làm sẵn là `dak-mock` (xem mục 10); có thể dùng làm mẫu và làm đề bài kiểm thử.

## 1. Tổng quan

Mỗi **dataset** (một thư mục nguồn như `nims`) có đúng **một DAG** trên Airflow. Người dùng gửi phiếu cho cả dataset, admin **duyệt một lần**.
DAK không chạy job; DAK chỉ làm bốn việc với Airflow:

| # | Việc | Khi nào | Mục |
|---|---|---|---|
| 1 | **Đặt lịch**: ghi cấu hình đã duyệt vào Airflow Variable, rồi bỏ pause DAG | Admin duyệt phiếu (và khi gửi lại phiếu lỗi) | 5.1 |
| 2 | **Gỡ lịch**: pause DAG, xoá Variable | Admin dừng phiếu | 5.2 |
| 3 | **Chạy bù một ngày** ngoài lịch (tuỳ chọn) | Admin yêu cầu | 5.3 |
| 4 | **Đọc kết quả lần chạy gần nhất** để hiện trên phiếu | Khi xem phiếu | 5.4 |

Phần còn lại (đọc file, giải mã, ghi bảng) do DAG và Spark app làm. DAK **không** biết và **không** cần biết chi tiết đó.

```
Người dùng gửi phiếu ─► admin duyệt ─► DAK ghi Variable ─► DAK bỏ pause DAG ─► Airflow chạy DAG theo cron của phiếu
                                         (cấu hình)          (bật lịch)          mỗi lần: nạp ngày/giờ liền trước
```

## 2. Dataset và quy ước đặt tên

| source | `path` (tên dataset) | DAG id = tên Variable | Có cột mã hoá? | Cron hằng giờ? |
|---|---|---|---|---|
| `VTT` | `f001-mpre-call` | `silver_ingest_vtt_f001_mpre_call` | không | có |
| `VTNET` | `ebs_3g_4g` | `silver_ingest_vtnet_ebs_3g_4g` | không | có |
| `VTNET` | `nims` | `silver_ingest_vtnet_nims` | có (có thể để rỗng) | có |

Quy tắc đặt tên: `silver_ingest_<source viết thường>_<path, đổi "-" thành "_">`. **DAG id và tên Variable là cùng một chuỗi.**
Danh sách dataset là danh sách cố định (khai trong file DAG); thêm dataset mới phải sửa cả DAG lẫn DAK.

## 3. Phiếu

### 3.1 Trường của phiếu

| Trường | Bắt buộc | Quy tắc |
|---|---|---|
| `source` | có | `VTT` hoặc `VTNET` (không phân biệt hoa thường) |
| `path` | có | **Đúng tên một dataset của source** (bảng mục 2). Không phải ngày, lô hay thư mục con |
| `cron` | có | Chỉ hai dạng, múi giờ `Asia/Ho_Chi_Minh`: hằng ngày `<phút> <giờ> * * *` (vd `30 2 * * *`), hằng giờ `<phút> * * * *` (vd `5 * * * *`). Phút 0–59, giờ 0–23, **đúng một dấu cách giữa các trường**. Dạng khác (`*/5 * * * *`, `0 */2 * * *`, `0 3 * * 1`…) phải bị `400` |
| `table` | có | `[a-z0-9_]{1,128}`, bắt đầu bằng `vtt_` (source VTT) hoặc `vtnet_` (source VTNET) |
| `outputMode` | không | `overwrite-partitions` hoặc `append`; bỏ trống = `overwrite-partitions` |
| `encryptColumns` | không | Chỉ cho dataset có cột mã hoá (`nims`). Tối đa 100 tên khác nhau (không phân biệt hoa thường), mỗi tên `[A-Za-z_][A-Za-z0-9_]{0,127}`. Thiếu, `null` hoặc `[]` đều hợp lệ (chỉ giải mã file) |
| `keyField` | bắt buộc nếu có cột | Cùng quy tắc tên cột, **không** nằm trong `encryptColumns` |
| `purpose` | có | 1–500 ký tự, không ký tự điều khiển |
| `batchId` | **cấm** | Mỗi lần chạy tự lấy ngày làm lô |

Dataset không có cột mã hoá mà phiếu có `encryptColumns` hoặc `keyField` → `400`. Trường lạ → `400`. Trả **mọi lỗi một lượt**
(`problems[]` gồm `field`, `code`, `message`), thông điệp cố định, **không lặp lại giá trị người dùng nhập**.

Mã phiếu phải có dạng **`SLV-<năm 4 số>-<6 số>`** (vd `SLV-2026-000001`): DAG kiểm đúng định dạng này và từ chối cấu hình nếu sai.

### 3.2 Chặn trùng

Mỗi dataset chỉ có **một phiếu đang mở** (chưa ở trạng thái kết thúc), tính trên **mọi team**: một dataset chỉ có một lịch và một cấu hình ở Airflow.
Gửi phiếu thứ hai khi phiếu cũ còn mở → `409 duplicate_request`.

### 3.3 Trạng thái

```
PENDING_APPROVAL ──duyệt──► APPROVED ──đặt lịch được──► SCHEDULED ──admin dừng──► STOPPED
   │   │                        └──đặt lịch lỗi──► SUBMIT_FAILED ──gửi lại──► APPROVED
   │   └── admin từ chối ──► REJECTED
   └────── người gửi huỷ ──► CANCELLED
```

- Trạng thái kết thúc (không đổi nữa, không tính vào chống trùng): `REJECTED`, `CANCELLED`, `STOPPED`.
- Người gửi chỉ huỷ được phiếu `PENDING_APPROVAL`. Phiếu `SUBMIT_FAILED` **không** huỷ được, chỉ gửi lại.
- `APPROVED` chỉ thoáng qua: duyệt xong là đặt lịch ngay.
- Phiếu `SCHEDULED` có thêm `lastRun`: `SUCCEEDED`, `FAILED`, `RUNNING`, hoặc trống nếu chưa chạy lần nào.
- Phiếu `SUBMIT_FAILED` có `submitError` (mục 7).

## 4. Điều Airflow làm với cấu hình

Hiểu phần này để biết vì sao DAK phải ghi đúng cấu hình:

- Airflow đọc lại file DAG khoảng **30 giây một lần**. Mỗi lần đọc, DAG của dataset đọc Variable cùng tên và dựng lịch từ `cron` trong đó.
- **Không có Variable** → DAG vẫn tồn tại nhưng **không có lịch**, không bao giờ tự chạy.
- **Variable sai định dạng** → DAG cũng không có lịch; task đầu `validate_conf` báo lý do cụ thể khi có ai chạy tay. DAK ghi đúng thì không bao giờ gặp.
- Mỗi lần chạy theo lịch nạp **một đơn vị dữ liệu**: cron hằng ngày nạp **ngày hôm trước**; cron hằng giờ nạp **giờ liền trước lúc chạy** (chạy 11:05 nạp giờ 10).
- **Không tự chạy bù** khi duyệt: duyệt xong không có dag run nào cho các ngày cũ.
- Mỗi DAG chỉ chạy **một dag run một lúc**; các run tới sau xếp hàng.

## 5. Bốn thao tác

Mọi lời gọi dùng Airflow REST **`/api/v1`** (Airflow 2.x; Airflow 3 khác API, chưa hỗ trợ). `BASE` là địa chỉ webserver, không có `/api/v1`.

### 5.1 Đặt lịch (duyệt, và gửi lại)

Thứ tự **bắt buộc**: ghi cấu hình **trước**, bỏ pause **sau**, để DAG đã bật luôn thấy đủ cấu hình.

1. Ghi Variable (cập nhật trước, chưa có thì tạo):

   ```
   PATCH {BASE}/api/v1/variables/{DAG id}      body: {"key": "{DAG id}", "value": "<chuỗi JSON của cấu hình, mục 6>"}
   404 → POST {BASE}/api/v1/variables          cùng body
   ```

   `value` là **một chuỗi** chứa JSON (JSON được mã hoá thành chuỗi), không phải đối tượng lồng.
2. Bỏ pause DAG:

   ```
   PATCH {BASE}/api/v1/dags/{DAG id}           body: {"is_paused": false}
   ```

   `404` nghĩa là Airflow chưa có DAG này (file DAG chưa deploy hoặc đang lỗi parse).

Cả hai bước thành công → phiếu `SCHEDULED`. Bước nào lỗi → phiếu `SUBMIT_FAILED` kèm `submitError` (mục 7). Việc gửi lại chạy lại **cả hai bước**, nên phải an toàn khi lặp (ghi đè Variable, bỏ pause DAG đã bỏ pause đều không sao).

### 5.2 Gỡ lịch (dừng phiếu)

Chỉ cho phiếu `SCHEDULED`. Thứ tự ngược lại: **pause trước, xoá Variable sau**.

```
PATCH  {BASE}/api/v1/dags/{DAG id}          body: {"is_paused": true}
DELETE {BASE}/api/v1/variables/{DAG id}     (404 coi như đã xoá)
```

Cả hai thành công → `STOPPED`. Lỗi → báo `503`, phiếu **vẫn `SCHEDULED`**, admin dừng lại được (các lệnh trên lặp lại an toàn). Các lần chạy đang dở không bị huỷ.

### 5.3 Chạy bù một ngày (tuỳ chọn, admin)

Chỉ cho phiếu `SCHEDULED`. Trigger một dag run của DAG đó ngoài lịch; phiếu và lịch không đổi.

```
POST {BASE}/api/v1/dags/{DAG id}/dagRuns    body: {"dag_run_id": "<mã duy nhất>", "conf": {"date": "20261013"}}
```

- `date`: ngày thật, `yyyyMMdd`; sai → `400`, phiếu chưa `SCHEDULED` → `409`.
- `dag_run_id` phải **mới mỗi lần gọi** để chạy lại cùng một ngày là một lần chạy mới. Gợi ý: `<mã phiếu viết thường>-<ngày>-<yyyyMMddHHmmss UTC>`, ký tự `[A-Za-z0-9_.-]`, tối đa 250.
- Airflow trả `409` (trùng `dag_run_id`) thì coi là **đã gửi**, không phải lỗi.
- Muốn chạy chỉ vài giờ của ngày, thêm `"fromHour": "10", "toHour": "12"` vào `conf` (bắt buộc kèm `date`). Không bắt buộc làm.
- Chạy **một khoảng ngày/giờ** không phải việc của DAK: ops trigger DAG `silver_ingest_backfill` trực tiếp trên Airflow (xem `BACKFILL.md`).

### 5.4 Đọc lần chạy gần nhất

Dùng khi trả về phiếu `SCHEDULED`:

```
GET {BASE}/api/v1/dags/{DAG id}/dagRuns?order_by=-start_date&limit=1
```

| Kết quả | `lastRun` |
|---|---|
| `404`, hoặc `dag_runs` rỗng | trống |
| `state = success` | `SUCCEEDED` |
| `state = failed` | `FAILED` |
| trạng thái khác (`running`, `queued`…) | `RUNNING` |

Nếu **đọc lỗi** (mạng, 5xx…) thì giữ nguyên giá trị cũ, không làm hỏng việc xem phiếu.

## 6. Nội dung Airflow Variable

JSON, **chỉ những khoá dưới đây** (khoá lạ làm DAG bỏ cả cấu hình). Không chứa key hay bí mật nào.

| Khoá | Khi nào | Lấy từ | DAG kiểm |
|---|---|---|---|
| `ticket` | luôn | mã phiếu | `SLV-\d{4}-\d{6}` |
| `table` | luôn | `table` | `[a-z0-9_]{1,128}`, đúng tiền tố của source |
| `cron` | luôn | `cron`, **nguyên văn** | hai dạng ở mục 3.1 |
| `outputMode` | chỉ khi phiếu có | `outputMode` | `overwrite-partitions` hoặc `append` |
| `encryptColumns` | chỉ dataset có cột mã hoá | `encryptColumns` (có thể `[]`) | danh sách tên cột hợp lệ |
| `keyField` | chỉ khi có cột mã hoá | `keyField` | tên cột hợp lệ, không nằm trong `encryptColumns` |

Với dataset **không** có cột mã hoá (`ebs_3g_4g`, `f001-mpre-call`) **không** ghi `encryptColumns`/`keyField`.
Với dataset có cột mã hoá mà phiếu không chọn cột nào: ghi `"encryptColumns": []` và **không** ghi `keyField`.

Ví dụ:

```json
{"ticket": "SLV-2026-000001", "table": "vtnet_nims_infra_cell_4g", "cron": "0 3 * * *", "encryptColumns": []}
```

```json
{"ticket": "SLV-2026-000003", "table": "vtnet_nims_infra_cell_4g_enc", "cron": "15 4 * * *", "outputMode": "append",
 "encryptColumns": ["lng", "lat"], "keyField": "eci"}
```

```json
{"ticket": "SLV-2026-000002", "table": "vtnet_ebs_3g_4g", "cron": "5 * * * *"}
```

## 7. Lỗi: từ Airflow sang phiếu

Quy về **ba thông điệp cố định**. **Không bao giờ** đưa nội dung response của Airflow, URL hay chi tiết kỹ thuật vào phiếu hay log công khai.

| Điều kiện từ Airflow | `submitError` (phiếu `SUBMIT_FAILED`) | Thường do |
|---|---|---|
| `404` khi bỏ pause hoặc trigger | `airflow dag not found` | File DAG chưa deploy, đang lỗi parse, hoặc sai DAG id |
| `401` / `403` | `airflow denied the request` | Sai user/mật khẩu, hoặc user **thiếu quyền** (mục 8) |
| Lỗi khác (5xx, mạng, quá thời gian) | `airflow unavailable` | Sai `BASE`, Airflow hoặc mạng hỏng |

Lỗi không lường trước (ngoại lệ trong code DAK) → `internal error while scheduling the dag`, chi tiết chỉ ghi log phía DAK (không kèm nội dung ngoại lệ). Với thao tác dừng và chạy bù, lỗi Airflow trả `503` kèm thông điệp trên.

## 8. Tài khoản Airflow cho DAK

- Dùng **một user riêng của Airflow** với xác thực Basic (`api.auth_backends` có `basic_auth`). Đây không phải user Keycloak. Mật khẩu để trong Secret, không ghi vào ConfigMap hay log.
- Role `User` mặc định **không đủ**: nó không ghi được Variable. Cần role có đủ các quyền:

| Tài nguyên | Quyền |
|---|---|
| `Variables` | `can read`, `can create`, `can edit`, `can delete` |
| `DAGs` | `can read`, `can edit` |
| `DAG Runs` | `can read`, `can create` |

Thiếu quyền Variable là lỗi hay gặp nhất: thao tác duyệt trả `airflow denied the request`.

## 9. Điều cần giữ khi implement

- **Thứ tự**: đặt lịch = Variable rồi bỏ pause; gỡ lịch = pause rồi xoá Variable.
- **Lặp lại an toàn**: gửi lại, dừng lại, trigger trùng `dag_run_id` đều không gây hại.
- **Một phiếu mở mỗi dataset**, kiểm trên mọi team lúc tạo phiếu. Phiếu `SUBMIT_FAILED` vẫn tính là đang mở, nên khi gửi lại không thể bị trùng.
- **`cron` ghi nguyên văn**: DAK không đổi, không chuẩn hoá. Muốn chỉnh dạng thì phải kiểm bằng đúng quy tắc mục 3.1, vì DAG chỉ nhận đúng hai dạng đó.
- **Thông điệp lỗi cố định**, không lặp lại giá trị người dùng, không lộ response của Airflow.
- **Nhật ký (audit)**: ghi mã phiếu, trạng thái cũ → mới, người thao tác, `source`, `path`, `cron`, `table`, `dagId`; **không** ghi `purpose` hay bất kỳ key nào.
- **Trạng thái phiếu nằm ở DAK**; Airflow chỉ giữ Variable và DAG. Nếu DAK khởi động lại mà mất trạng thái, Variable và lịch trên Airflow vẫn còn (bản mock giữ trong bộ nhớ nên có hạn chế này; bản thật nên lưu bền).

## 10. Kiểm thử chấp nhận

| # | Tình huống | Kỳ vọng |
|---|---|---|
| 1 | Duyệt phiếu `nims` hợp lệ | Có Variable `silver_ingest_vtnet_nims` đúng nội dung mục 6, DAG hết pause, phiếu `SCHEDULED` |
| 2 | Duyệt khi Airflow trả `403` | Phiếu `SUBMIT_FAILED`, `submitError = airflow denied the request`; sửa quyền rồi gửi lại → `SCHEDULED` |
| 3 | Duyệt khi chưa có DAG (`404`) | `SUBMIT_FAILED`, `airflow dag not found` |
| 4 | Dừng phiếu `SCHEDULED` | DAG bị pause, Variable bị xoá, phiếu `STOPPED`; sau đó gửi được phiếu mới cho cùng dataset |
| 5 | Dừng khi Airflow lỗi | `503`, phiếu vẫn `SCHEDULED`; dừng lại thành công sau khi Airflow phục hồi |
| 6 | Gửi phiếu thứ hai cho dataset đang có phiếu mở | `409 duplicate_request` |
| 7 | Phiếu có `cron` sai (`*/5 * * * *`, `03:00`), có `batchId`, `path` là một ngày | `400`, `problems[]` liệt kê đúng các trường |
| 8 | Dataset không có cột mã hoá mà phiếu có `encryptColumns` | `400` ở `encryptColumns`/`keyField` |
| 9 | Chạy bù ngày `20261013` hai lần | Hai dag run khác `dag_run_id`; ngày sai → `400`; phiếu chưa `SCHEDULED` → `409` |
| 10 | Xem phiếu `SCHEDULED` khi dag run gần nhất `success`/`failed`/`running` | `lastRun` = `SUCCEEDED`/`FAILED`/`RUNNING` |
| 11 | Phiếu có cron hằng giờ `5 * * * *` | Variable có `"cron": "5 * * * *"`; DAG chạy theo giờ |

**Kiểm Airflow thủ công** (thay `$U` bằng user DAK; `curl` sẽ hỏi mật khẩu, đừng gõ mật khẩu vào dòng lệnh):

```bash
curl -s -o /dev/null -w "%{http_code}\n" -u "$U" -X PATCH "$BASE/api/v1/variables/silver_probe" -H 'Content-Type: application/json' -d '{"key":"silver_probe","value":"1"}'
```

Kết quả `404` hoặc `200` là quyền ghi Variable ổn (404 vì Variable thử chưa có); `403` là thiếu quyền; `401` là sai user hoặc mật khẩu.

**Mẫu tham chiếu**: `dak-mock/src/main/java/vai/lakehouse/dakmock/silver/` (`SilverRequestParser` = quy tắc phiếu, `SilverRequestService` = trạng thái, `SilverConfig` = bốn thao tác với Airflow, `SilverDagTrigger.scheduleConfig` = nội dung Variable)
và `dak-mock/src/main/java/vai/lakehouse/dakmock/deid/AirflowDagRuns.java` (các lời gọi REST). Mẫu phiếu: `dak-mock/k8s/silver/samples/`. Kịch bản chạy tay từng bước: `TEST_SCENARIO_VTNET_EBS.md`.

## 11. Ngoài phạm vi tài liệu này

- Cấp key cho job: DAK vẫn là nơi cấp key theo đường dẫn (giải mã cả file) và theo cột (mã hoá cột) cho dataset `nims` lúc job chạy. Hợp đồng riêng, xem `docs/key-access/`.
- Chạy bù một khoảng ngày/giờ: làm trên Airflow, xem `BACKFILL.md`.
- Build và deploy file DAG, image Spark.
