# Yêu cầu từ đội dak: key global cho job chuẩn hoá silver

Ngày: 2026-11-16. Gửi: đội SA / sql-engine.

Tài liệu này báo **một thay đổi ở Airflow Variable của DAG silver** và **một endpoint mới để lấy
key**. Cuối tài liệu có hai câu hỏi. Các anh/chị trả lời thẳng dưới từng số là đủ.

---

## 0. Bối cảnh

Người dùng cần mã hoá **nhiều dataset bằng chung một key**, để sau này query còn nối được dữ liệu
giữa các dataset đó. Hiện mỗi dataset silver lấy key riêng theo đường dẫn/cột (`by-path`,
`by-column`), nên hai dataset không bao giờ cùng key.

dak thêm một loại key mới: **key global**, định danh bằng tên **vùng** (vd `khach_hang`). Admin
dak tạo key global; người gửi phiếu silver tick "Vùng dữ liệu Global?" rồi chọn vùng. Mọi phiếu
chọn cùng vùng dùng cùng một key.

Giá trị key global **không bao giờ đổi** sau khi tạo — dak chặn xoay key — nên job không phải lo
dữ liệu cũ/mới của cùng vùng mã hoá bằng hai giá trị.

---

## 1. Airflow Variable của DAG silver: hai khoá mới

Variable `silver_ingest_<source>_<path>` (hợp đồng `DAK_TRIGGER_DAG.md` §6) thêm hai khoá **chỉ
khi** phiếu chọn key global:

```json
{
  "ticket": "SLV-2026-000042",
  "table": "subscriber",
  "cron": "0 2 * * *",
  "encryptColumns": ["msisdn", "imei"],
  "keyField": "id",
  "globalKey": true,
  "globalZone": "khach_hang"
}
```

| Khoá | Kiểu | Ý nghĩa |
|---|---|---|
| `globalKey` | boolean, luôn là `true` khi có | Job mã hoá các cột `encryptColumns` bằng key global, **không** lấy key theo path/cột như hiện nay |
| `globalZone` | string `^[a-z0-9_]{1,64}$` | Vùng của key global — tham số cho endpoint ở §2 |

Phiếu **không** chọn key global: Variable **giữ nguyên như hiện tại**, không có hai khoá trên —
job chạy đúng như bây giờ. dak không bao giờ gửi `"globalKey": false`.

Khi có `globalKey`, phiếu luôn có `encryptColumns` không rỗng (dak chặn lúc gửi phiếu).

---

## 2. Endpoint lấy key global

```
GET /api/v1/keys/global?zone=<vùng>
Authorization: Bearer <access token Keycloak>
```

Phản hồi 200 (`Cache-Control: no-store`):

```json
{ "zone": "khach_hang", "keyPrefix": "<giá trị key>", "keyVersion": 1 }
```

dak phát key khi **có ít nhất một phiếu silver đang hiệu lực (đã duyệt hoặc đang chạy lịch) chọn
đúng vùng đó**, và token thuộc về bên được phép:

- token của **client chính của team** gửi phiếu đó (cùng client các anh/chị dùng cho
  `/api/v1/keys/{db}/{table}`), hoặc
- token của **client nội bộ** đang dùng cho `by-path` / `by-column`.

| Mã | Khi nào |
|---|---|
| 200 | như trên |
| 401 | token sai / hết hạn |
| 403 `access_denied` | không có phiếu silver hiệu lực nào cho vùng này mà token được phép — **cùng một câu trả lời** với vùng không tồn tại |
| 403 `key_disabled` | key global đang bị khoá |
| 429 | gọi quá nhanh — thử lại sau |

Mọi lần lấy key đều ghi audit bên dak.

---

## 3. Luồng khử định danh: KHÔNG đổi gì

Cùng đợt này, phiếu khử định danh cho phép chọn "dùng lại key của phiếu trước". dak xử lý hoàn
toàn bên mình: key của phiếu mới mang đúng giá trị key phiếu cũ. Job deid vẫn lấy key qua
`GET /api/v1/keys/deid` theo mã phiếu mới như hiện tại — `conf` và endpoint **không đổi**.

---

## 4. Câu hỏi

1. Job silver hiện lấy token bằng client nào — client chính của team, hay client nội bộ chung của
   các anh/chị? (dak chấp nhận cả hai, nhưng cần biết để ghi đúng vào hướng dẫn vận hành.)
2. Bao giờ DAG silver đọc được `globalKey`/`globalZone`? Trước thời điểm đó dak sẽ dặn người dùng
   chưa tick "Vùng dữ liệu Global?" — DAG cũ bỏ qua hai khoá lạ hay chết ở bước đọc Variable?
