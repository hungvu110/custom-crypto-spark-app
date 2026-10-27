# Trả lời đội dak — hình dạng `path` của `by-path`

Ngày: 2026-10-27. Gửi: đội dak. Từ: đội SA.

Trả lời câu hỏi mục 6 của [API-LAY-KEY-DUONG-DAN-COT.md](API-LAY-KEY-DUONG-DAN-COT.md), góp ý về `onMissing` (mục 3.2) và về `by-path` khi chưa có key. Đọc xong là bắt đầu code `by-path` được.
Phần còn lại của tài liệu đó (mục 2 `by-path`, mục 3 `by-column`, mục 4 YC) SA đồng ý.

## 1. Câu trả lời

| Câu hỏi | Trả lời |
| --- | --- |
| Tuyệt đối hay tương đối? | **Tương đối theo một gốc** của nguồn dữ liệu, tổ chức theo bảng, theo ngày. Gốc do SA quản lý, **không** gửi cho dak |
| Ai chuẩn hoá? | **SA tự chuẩn hoá** trước khi gửi. dak lưu và so khớp nguyên chuỗi, không cần tự chuẩn hoá |
| Hoa thường | **Giữ nguyên.** `VTNET_cdr` và `vtnet_cdr` là hai đường dẫn khác nhau |

## 2. Dạng chuẩn SA luôn gửi

- Tương đối: **không** có `/` ở đầu, **không** có scheme hay máy chủ (`hdfs://...`).
- **Không** có `/` ở cuối, **không** có `//` liên tiếp.
- Không có `..`, không có ký tự điều khiển (đúng như mục 2 của dak).

| Đường dẫn trên HDFS (SA có) | Gốc của nguồn | `path` SA gửi dak |
| --- | --- | --- |
| `hdfs://<namenode>:8020/landing/vtnet/vtnet_cdr/20261027` | `/landing/vtnet` | `vtnet_cdr/20261027` |
| `hdfs://<namenode>:8020/landing/vtnet/vtnet_cdr/20261027/` | `/landing/vtnet` | `vtnet_cdr/20261027` |
| `/landing/vtnet//vtnet_cdr/20261027/part-00001.csv` | `/landing/vtnet` | `vtnet_cdr/20261027/part-00001.csv` |

## 3. Việc cần ở phía dak

1. **Người tạo key ở màn quản trị** phải nhập đúng dạng chuẩn ở mục 2, nếu không key sẽ không tra ra. Đề nghị ô nhập từ chối chuỗi có `/`
   ở đầu hoặc cuối, có `//` hoặc có `://`, để sai sót lộ ra ngay lúc tạo key thay vì lúc SA gọi.
2. **Một đường dẫn có thể là một thư mục** (một lô file). SA dùng **một** key của đường dẫn đó để giải mã **mọi** file trong thư mục. Luồng mã
   hoá ghi file vào thư mục đó phải dùng đúng key này cho cả thư mục. Nếu phía dak có ý định cấp key riêng cho từng file trong một thư mục,
   xin báo để SA gọi `by-path` theo từng file.

## 4. Góp ý về `onMissing` của `by-column` (mục 3.2)

Trong luồng chuẩn hoá, SA gọi `by-column` để **mã hoá** các cột trước khi ghi vào bảng silver, không phải để giải mã. Bảng silver mới thì cột
chưa có key là chuyện bình thường. Vì vậy SA gọi với **`onMissing=request`**:

1. Lần đầu, cột chưa có key → dak mở YC (`PENDING`), tác vụ dừng.
2. Admin dak duyệt YC, sinh key cho cột đó.
3. SA chạy lại → cột `READY`, tác vụ mã hoá và ghi bảng.

Xin dak xác nhận luồng này. `onMissing=none` SA chỉ dùng khi giải mã.

> **Đã chốt (2026-10-27):** dak xác nhận ở hợp đồng mục 3.2. SA gọi lại trong cùng tác vụ theo exponential backoff tới khi admin duyệt.

## 5. Góp ý về `by-path` khi chưa có key (mục 2)

Key theo đường dẫn do **luồng mã hoá file** tạo ra. Đường dẫn không có key nghĩa là file của nó **không mã hoá**, không phải là thiếu
key. Vì vậy SA đề nghị:

- Chưa có key → dak trả **rỗng**: `204 No Content`, **không** mở YC.
- SA đọc file nguyên trạng và chạy tiếp. Có key thì SA giải mã rồi chạy tiếp. Cả hai trường hợp tác vụ đều thành công.

> **Đã chốt (2026-10-27):** dak đồng ý `204`, không mở YC cho `by-path`. Phía SA coi `202` là lỗi (dừng tác vụ, không đọc file như
> plaintext), vì `202` nghĩa là đường dẫn cần key mà chưa có.

## 6. Key `by-path` bị khoá (`DISABLED`)

SA đồng ý đề xuất của dak: key của đường dẫn tồn tại nhưng đang bị khoá thì `by-path` trả **`403` với `error = "key_disabled"`**, không trả
`204`. Lý do trùng với mục 5: `204` nghĩa là file không mã hoá, nên trả `204` cho key bị khoá sẽ làm SA đọc dữ liệu mã hoá như nguyên trạng.

- SA phân biệt hai loại `403` bằng mã lỗi: `key_disabled` (key bị khoá, dừng tác vụ, báo admin dak mở khoá) và `access_denied` (sai loại client).
- Tác vụ SA dừng với lỗi, **không** chờ và không tự thử lại: khoá là quyết định của admin dak.
- Xin dak ghi `403 key_disabled` vào hợp đồng mục 2 (bảng mã lỗi của `by-path`).
