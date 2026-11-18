# Thông báo từ đội dak: một phiếu khai thác nhiều bảng — nhiều dag run song song

Ngày: 2026-11-16. Gửi: đội SA / sql-engine.

Tài liệu này báo **một thay đổi về số lượng dag run** của DAG khử định danh. Hình dạng `conf` của
mỗi run **không đổi**. Cuối tài liệu có hai câu cần các anh/chị xác nhận.

---

## 0. Bối cảnh

Trước đây một phiếu khai thác dữ liệu chỉ khai **một bảng**, dak trigger **một** dag run. Từ nay
người dùng khai được **nhiều bảng, từ nhiều database** trong một phiếu. Kết quả của cả phiếu ghi
vào **một database kết quả** do dak đặt tên.

---

## 1. Một bảng một dag run, chạy song song

Phiếu N bảng → dak trigger **N dag run** cùng `dagId`, gần như cùng lúc (dak gửi tuần tự, Airflow
chạy song song). Mỗi run khử định danh đúng **một** bảng nguồn.

`conf` của mỗi run **giữ nguyên hình dạng hiện tại** (API_CONTRACT §7.3):

```json
{
  "requestCode": "REQ-2026-000123",
  "database": "db_a",
  "table": "t1",
  "pseudoColumns": "msisdn:HASH,imei:HASH",
  "env": {
    "OUTPUT_DB": "vtnet_team01_req_2026_000123",
    "OUTPUT_TABLE": "db_a_t1",
    "INPUT_FROM": "20261001",
    "INPUT_TO": "20261007",
    "REQUEST_CODE": "REQ-2026-000123",
    "DAK_CLIENT_ID": "sa-vlp-datalake-team01-deid",
    "DAK_CLIENT_SECRET": "…"
  }
}
```

| Điểm | Quy ước |
|---|---|
| `OUTPUT_DB` | **Giống nhau** cho mọi run của phiếu: `<đơn vị>_<mã team>_<mã phiếu>`, chỉ gồm `[a-z0-9_]`, tối đa 128 ký tự. dak đặt, người dùng không sửa được. |
| `OUTPUT_TABLE` | Mỗi run một bảng: **tên bảng nguồn**. Hai database nguồn có bảng trùng tên → **mọi** bảng mang tên đó đổi thành `<database>_<bảng>` (vd `db_a_t1`, `db_b_t1`). dak bảo đảm không hai run nào trong một phiếu ghi cùng một bảng kết quả. |
| `dag_run_id` | Phiếu **một bảng**: không đổi (`deid-2026-000123`, `…-r2` khi chạy lại). Phiếu **nhiều bảng**: thêm hậu tố `-t<n>` theo thứ tự bảng (`deid-2026-000123-t1`, `…-t2`; chạy lại: `…-r2-t2`). |
| Lấy key | Mỗi run gọi `GET /api/v1/keys/deid?database=&table=&requestCode=` với **database/table của chính run đó** — như hiện tại. Mỗi bảng có key riêng. |
| Credential | `DAK_CLIENT_ID`/`DAK_CLIENT_SECRET` **dùng chung** cho mọi run cùng một lượt. dak không xoay secret giữa các run của một lượt — run sau không làm credential của run trước hết hiệu lực. |
| Lỗi | Mỗi run sống chết riêng. dak đọc trạng thái từng `dag_run_id`. Bảng lỗi chạy lại riêng (run mới cho đúng bảng đó); bảng đã xong không chạy lại. |
| Xoá vùng khi hết hạn | dak yêu cầu xoá **từng bảng kết quả** (`OUTPUT_DB` + `OUTPUT_TABLE` của từng run) như hiện tại. |

Phiếu một bảng (gần như toàn bộ phiếu đang có) **không thấy khác gì**.

---

## 2. Câu hỏi

1. N run cùng ghi vào **một** `OUTPUT_DB` chưa tồn tại: job có tự tạo database và chịu được việc
   nhiều run cùng tạo một lúc (`CREATE DATABASE IF NOT EXISTS` hay tương đương) không?
2. Hậu tố `-t<n>` của `dag_run_id` có vi phạm ràng buộc nào phía DAG / tên SparkApplication không
   (độ dài, ký tự)? Dạng dài nhất: `deid-2026-000123-r9-t50`.

(Đã biết: SA không giới hạn số job gửi — chốt 2026-11-16.)
