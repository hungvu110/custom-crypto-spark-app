# Trả lời đội dak — một phiếu khai thác nhiều bảng, nhiều dag run song song

Ngày: 2026-11-18. Gửi: đội dak. Từ: đội SA.

Trả lời hai câu cuối [YEU-CAU-TU-DAK-DEID-NHIEU-BANG.md](YEU-CAU-TU-DAK-DEID-NHIEU-BANG.md). SA đồng ý toàn bộ mục 1 (một bảng một dag run, `conf` giữ nguyên hình dạng, `OUTPUT_DB` chung, `OUTPUT_TABLE` mỗi run một bảng, credential dùng chung).

## 1. Câu trả lời

| # | Câu hỏi | Trả lời |
|---|---|---|
| 1 | N run cùng ghi vào một `OUTPUT_DB` chưa tồn tại: job có tự tạo database và chịu được nhiều run cùng tạo không? | **Có.** Mỗi job chạy `CREATE DATABASE IF NOT EXISTS <OUTPUT_DB>` trước khi ghi, nên không cần tạo trước, và lệnh này bỏ qua khi database đã có (kể cả khi run khác vừa tạo). Với mode `HASH` (mode dak đang gửi) mỗi run chỉ ghi bảng của nó (`OUTPUT_TABLE`) và không dùng chung thư mục tạm hay trạng thái nào khác. Mode `MAPPING` dùng chung một bảng ánh xạ giữa các run nên SA chưa bảo đảm chạy song song cho mode đó. SA **chưa thử** N run đồng thời trên metastore thật; nếu thấy lỗi tạo database trùng, báo SA kèm `dag_run_id` |
| 2 | Hậu tố `-t<n>` của `dag_run_id` có vi phạm ràng buộc phía DAG / tên SparkApplication không? | **Không vi phạm ràng buộc tên** (dạng dài nhất `deid-2026-000123-r9-t50` chỉ 23 ký tự, tên SparkApplication và pod driver `…-driver` còn xa giới hạn 63 ký tự của DNS-1123; chỉ có chữ thường, số, `-`). **Nhưng DAG cũ từ chối** vì đang kiểm `dag_run_id` theo `deid-<năm>-<6 số>[-r<n>]`: run `…-t1` bị `validate_conf` báo lỗi và failed ngay. Bản DAG mới đã nhận `[-r<n>][-t<n>]` (mỗi hậu tố 1–3 chữ số). **dak chỉ gửi `-t<n>` sau khi SA báo DAG mới đã lên Airflow** |

## 2. Thay đổi phía DAG

1. `dag_run_id`: `deid-<năm>-<6 số>[-r<n>][-t<n>]`. Thứ tự hậu tố **cố định** `-r` rồi `-t` (`…-r2-t2`, không phải `…-t2-r2`).
2. **Chạy song song.** DAG cũ có `max_active_runs=1` (chạy từng run một vì trước đây các run của một bảng ghi cùng bảng kết quả). DAG mới đặt `max_active_runs=64`: phiếu N bảng chạy N run song song, tối đa 64 run cùng lúc của DAG này; run vượt quá chờ ở trạng thái `queued`. SA không đặt giới hạn số job như đã chốt 2026-11-16; con số 64 chỉ vì Airflow cần một số, và giới hạn thật là quota của namespace.
3. **Điều kiện an toàn đã chuyển sang dak:** Airflow không tự tuần tự hoá các run ghi **cùng một bảng kết quả** (chỉ đếm run của DAG). Nên dak giữ đúng điều đã cam kết: không hai run nào của một phiếu ghi cùng `OUTPUT_DB` + `OUTPUT_TABLE`, và chỉ chạy lại bảng mà run trước đã kết thúc.

## 3. Hai điều SA đề nghị dak xác nhận

1. **Chữ hoa trong tên bảng.** DAG kiểm `OUTPUT_DB`/`OUTPUT_TABLE` theo `[a-z0-9_]{1,128}` (chữ thường), trong khi `database`/`table` nguồn cho phép `[A-Za-z0-9_]`. Bảng nguồn tên `Subscriber` thì `OUTPUT_TABLE=Subscriber` bị DAG từ chối. Đề nghị dak **đổi sang chữ thường** khi đặt `OUTPUT_TABLE` (kể cả dạng `<database>_<bảng>`). Hive/Spark vốn không phân biệt hoa thường nên không mất thông tin.
2. **Độ dài.** `<database>_<bảng>` có thể dài tới 257 ký tự (mỗi tên tối đa 128), trong khi `OUTPUT_TABLE` tối đa 128. Đề nghị dak bảo đảm tên bảng kết quả ≤ 128 ký tự (và vẫn không trùng trong phiếu).

## 4. Ngoài phạm vi

DAG xoá vùng khi hết hạn (`deid_maintenance.py`) không đổi: dak yêu cầu xoá từng bảng kết quả như hiện tại (`OUTPUT_DB` + `OUTPUT_TABLE` của từng run). DAG này cũng đang chạy từng run một (`max_active_runs=1`), nên phiếu N bảng hết hạn thì N lần xoá chạy lần lượt. Cần chạy song song thì SA sẽ đổi sau.

## 5. Đính chính: một phiếu nhiều bảng dùng chung MỘT key

Ngày: 2026-11-23. Đính chính một câu dễ gây hiểu nhầm ở [YEU-CAU-TU-DAK-DEID-NHIEU-BANG.md](YEU-CAU-TU-DAK-DEID-NHIEU-BANG.md), dòng "Lấy key" của bảng mục 1, viết *"Mỗi bảng có key riêng"*.

**Đúng là: mọi bảng của cùng một phiếu khai thác được khử định danh bằng cùng MỘT key.**

- Mỗi run vẫn gọi `GET /api/v1/keys/deid?database=&table=&requestCode=` với database và bảng của chính run đó. Điều này không đổi.
  Nhưng các run của cùng một `requestCode` nhận về **cùng một khoá**, không phải mỗi bảng một khoá.
- Hệ quả: cùng một giá trị gốc (ví dụ `isdn`) ở các bảng khác nhau của một phiếu cho **cùng một định danh giả**, nên các bảng kết quả **join được với nhau**.
- Đã kiểm chứng bằng test phiếu nhiều bảng trên dak: các bảng kết quả join được.
- Chưa kiểm chứng: hai phiếu khác nhau có dùng chung khoá không. Tạm coi là **không**, tức chỉ join được các bảng trong cùng một phiếu.
- Điều kiện join còn lại không thuộc phạm vi key: giá trị gốc phải có cùng định dạng ở các bảng (job băm nguyên trạng, `0903…` khác `84903…`).
