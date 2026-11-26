# Lỗi: `GET /api/v1/keys/deid` trả `keyVersion = null`

Job khử định danh gọi `GET /api/v1/keys/deid` và nhận `keyVersion` là `null`, nên job dừng. Nhờ DAK kiểm tra và sửa.

## 1. Hiện tượng

| | |
|---|---|
| Endpoint | `GET /api/v1/keys/deid?database=demo_db&table=cdr_voice&requestCode=<mã phiếu>` |
| Bảng | `demo_db.cdr_voice` |
| requestId (để DAK tra log) | `9bb43461fca29537` |
| Token | `client_credentials` của client nội bộ, header `X-Dak-Caller: pseudonymize-table` |
| Kết quả | HTTP 200, nhưng `keyVersion` là JSON `null` |

Job báo:

```text
DAK returned an invalid deid key response for 'demo_db.cdr_voice'
('keyVersion' is not a positive integer, got JSON type NULL, requestId=9bb43461fca29537)
```

Các trường khác trong response đã được job kiểm tra và hợp lệ: `requestCode` khớp, `database`/`table` khớp, `keyPrefix` là base64 của đúng 32 byte.

## 2. Mong đợi

Theo `YEU-CAU-TU-DAK-BO-K8S-SECRET.md` mục 2, `keyVersion` là **số nguyên dương** (kiểu JSON number), ví dụ:

```json
{
  "requestCode": "REQ-2026-000011",
  "database": "demo_db",
  "table": "cdr_voice",
  "keyPrefix": "<base64 của 32 byte>",
  "keyVersion": 3
}
```

## 3. Nhờ DAK kiểm tra

1. Với `requestId` trên, key đọc từ Vault có version không? Nguyên nhân hay gặp:
   - đọc từ Vault KV v1 hoặc đường dẫn không có metadata version;
   - key được tạo ngoài luồng thông thường nên thiếu metadata;
   - trường `keyVersion` chưa được ánh xạ từ metadata Vault vào response.
2. Sửa để response luôn có `keyVersion` là số nguyên dương. Nếu có key không có version thì trả lỗi rõ ràng (ví dụ `403 key_not_ready`) thay vì `null`.
3. Báo lại khi đã sửa để chạy lại job.

## 4. Vì sao job không bỏ qua trường này

`keyVersion` được ghi vào log/metadata của lần chạy để sau truy ra lần khử định danh dùng key version nào (cần khi key được xoay). Nếu nhận `null`, job không biết lần chạy đó dùng key nào, nên dừng thay vì chạy tiếp.

## 5. Log phía job (để đối chiếu)

Job ghi hai dòng cho mỗi lần gọi (không in `keyPrefix`):

```text
[DAK deid key] Sending request: GET http://<dak>/api/v1/keys/deid?database=demo_db&table=cdr_voice&requestCode=...
[DAK deid key] Response: HTTP 200, requestId=9bb43461fca29537; body fields (name (JSON type) = value): {..., keyPrefix (STRING) = <hidden, length=44, base64 of 32 bytes=true>, keyVersion (NULL) = null}
```
