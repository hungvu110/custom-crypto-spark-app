# Phản hồi của đội dak — vòng 4, về `PHAN-HOI-YC-SQL-ENGINE-VONG-3.md`

Ngày: 2026-10-21. Gửi: đội sql-engine / SparkApplication.

Cảm ơn vòng 3. Tin quan trọng nhất với chúng tôi là **dữ liệu mã hoá bằng key cột là mới hoàn
toàn** — nó xoá toàn bộ đợt migration nạp key cũ khỏi kế hoạch của bên này, và như các anh/chị
nói, cho phép bật `by-column` ngay khi nhánh nội bộ chạy được ở môi trường thử.

Tài liệu này trả lời ⑤ theo yêu cầu §5.1 của các anh/chị, ghi nhận việc tự rà theo ①–④ và ⑥, và
xác nhận **bản hợp đồng đã sửa** — mục §5 của vòng này chính là bảng đối chiếu thay cho việc gửi
lại toàn văn.

---

## 1. Tóm tắt một trang

|                     |                                                                                                                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Trả lời ⑤**       | `MISSING` thêm `reason` mã cố định — hai giá trị `not_requested` · `yc_limit_reached`. YC `REJECTED` khớp theo `(database, table, column)`, **không theo client** — đồng ý đúng như đề nghị |
| **② (tự rà)**       | dak **hỗ trợ cả hai**: nút Sinh key (dak sinh AES-256) và ô Giá trị key nhập tay cho key **tồn tại từ nguồn trước** — dak lưu vào chứ không sinh lại                                        |
| **①–④, ⑥**          | Không cần trả lời các anh/chị — chúng tôi đã tự rà và chốt, ghi ở §3 để hai bên vẫn đọc được quyết định                                                                                     |
| **Hợp đồng đã sửa** | `YEU-CAU-TU-SA-YC.md` cập nhật vòng 3 — thay thế mọi phiên bản trước. Thay đổi so với bản các anh/chị đọc lần cuối nằm gọn ở §5 dưới đây                                                    |
| **Không migration** | Xác nhận — mọi việc nạp key cột theo lô (R3, R4) bỏ khỏi phạm vi bên này                                                                                                                    |
| **Việc tiếp theo**  | Chúng tôi code nhánh nội bộ của `by-column` và báo khi chạy được ở môi trường thử                                                                                                           |

---

## 2. Trả lời ⑤ — hai ý

### ⑤a. `MISSING` phân biệt nguyên nhân bằng `reason` mã cố định

**Nhận.** `reason` là mã cố định để thư viện so bằng giá trị, không đọc mô tả:

| `state`   | `reason`           | Nghĩa                                                         | Bên gọi làm gì                       |
| --------- | ------------------ | ------------------------------------------------------------- | ------------------------------------ |
| `MISSING` | `not_requested`    | chưa có key và lời gọi không yêu cầu mở YC (`onMissing=none`) | lỗi cấu hình nếu đây là luồng mã hoá |
| `MISSING` | `yc_limit_reached` | đã đủ 50 YC `OPEN` cho `(client, database, table)`            | chờ, thử lại sau                     |

Các mã khác trong hợp đồng giữ nguyên tên: `invalid_tag` · `invalid_column` · `invalid_on_missing`.

`REJECTED` cũng kèm lý do cho **người** đọc (`reasonText` — text tự do người từ chối nhập); chúng
tôi không thêm mã cố định cho nó vì thư viện bên các anh/chị đã chốt "lỗi kết thúc, không retry" —
không phân nhánh theo lý do.

### ⑤b. YC `REJECTED` khớp theo `(database, table, column)` — đồng ý

Chính xác như đề nghị của các anh/chị: **không theo client**. Người có quyền từ chối vì "cột này
không nên mã hoá" thì mọi lời gọi sau cho cột đó — kể cả từ client nội bộ thứ hai sau rotate —
nhận lại `REJECTED` kèm `requestCode` cũ, không mở YC mới.

Người có quyền đổi ý thì xử từ giao diện dak (mở lại/làm mới), không phải lời gọi bên các anh/chị.

---

## 3. Tự rà và chốt ① ② ③ ④ (theo đề nghị "dak tự rà" của các anh/chị)

Không cần các anh/chị trả lời — ghi ở đây để hai bên vẫn thấy quyết định:

| #   | Điểm                                 | dak chốt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| --- | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ①   | Cột đã có dữ liệu mã hoá             | Giao diện tạo key cho YC hiển thị cảnh báo "cột này có thể đã có dữ liệu mã hoá" và yêu cầu xác nhận trước khi tạo. Nếu sau này cần đưa dữ liệu cũ vào: sẽ nạp prefix cũ bằng đường lô **trước** khi bật `by-column` cho cột đó và chặn mở YC cho cột đã nạp (`MISSING` + `reason=not_requested`) — đúng trình tự các anh/chị đề nghị. Giai đoạn này dữ liệu mới hoàn toàn nên chỉ là phòng ngừa.                                                                                                                                                                                                                         |
| ②   | Ai sinh giá trị key                  | **Cả hai đường, người có quyền chọn** (cùng hình dạng màn tạo key hiện có của dak — Tên key · Danh tính DB · Quyền READ/WRITE · Giá trị key · nút **Sinh key**): (a) bấm Sinh key — dak sinh (AES-256, 32 byte → base64, thuật toán theo từng dòng); (b) **điền tay ô Giá trị key** — cho key mã hoá **đã tồn tại từ trước** theo nguồn ngoài dak; việc của dak khi đó là **lưu vào** (Vault + bản ghi key), không sinh lại. Endpoint đóng YC nhận `keyValue` **tuỳ chọn** theo hai đường đó; response không trả lại giá trị.                                                                                             |
| ③   | Unique khi `column_name` NULL        | Key cả-bảng biểu diễn bằng `NULL` trong `column_name`; ràng buộc unique dùng `coalesce(column_name, '')` nên **không có ngoại lệ NULL**: hai key cùng `(tenant, database, table)` đều bị chặn dù cả hai là key cả-bảng, và một key cả-bảng không thể chồng lên key theo cột của cùng bảng ở cùng vị trí cột rỗng. Bất biến "một bảng/một cột một key" giữ cho cả hai loại.                                                                                                                                                                                                                                                |
| ④   | Gạch dưới trong `VaultPath`          | Hai cam kết bằng văn bản: **(a)** mỗi phần `database`, `table`, `column` là một đoạn đường dẫn riêng `.../{database}/{table}/{column}` — không nối bằng ký tự có thể xuất hiện trong tên; **(b)** ánh xạ tên → đoạn path là một-một trong bảng tên hiện cho phép: tên `[A-Za-z0-9_]` đổi `_` thành `-`, và vì tên không chứa `-` nên hai tên khác nhau không thể cho cùng đoạn. Có test chốt (`a_b` so với tên hợp lệ khác phải ra hai path; `a_b`+`c` và `a`+`b_c` là hai path khác nhau). Nếu sau này nới tên cột cho `-`, chúng tôi đổi ánh xạ sang giữ nguyên `_` kèm migrate — sẽ nói với các anh/chị trước khi nới. |
| ⑥   | `tag` / `onMissing` theo loại client | Bảng sáu ca các anh/chị đưa đã vào hợp đồng nguyên văn (§5.1b của bản mới), kể cả hai ca in đậm: client team gửi `tag=internal` → `403 access_denied`; client nội bộ thiếu `tag` → `400 invalid_tag`; client team gửi `onMissing=request` → **bỏ qua coi `none`**, không `400`.                                                                                                                                                                                                                                                                                                                                           |

---

## 4. Xác nhận lại các điểm các anh/chị sẽ làm (§4 của vòng 3)

Để đối chiếu khi test hai đầu:

| Điểm của các anh/chị                               | Bên này kỳ vọng nhận được                                                                                  |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Chỉ dùng key khi mọi cột `READY`                   | đúng — có cột `PENDING`/`MISSING`/`REJECTED`/`DISABLED` nào thì không dùng key nào của lô                  |
| Mã hoá gửi `onMissing=request`, giải mã gửi `none` | đúng — và mặc định khi không gửi cũng là `none`                                                            |
| `REJECTED` là lỗi kết thúc                         | đúng — nhận lại cùng `requestCode` nếu gọi lại, không mở YC mới                                            |
| `X-Dak-Caller` `[A-Za-z0-9._-]{1,128}`             | sai định dạng thì chúng tôi **bỏ qua header**, không từ chối lời gọi                                       |
| Chia lô 4096 ký tự / 100 cột sau gộp               | đúng — vượt một trong hai là `400 invalid_column` nói rõ cái nào                                           |
| Cache chỉ cột `READY`                              | nhất quán với vòng đời key bên này: key không đổi im lặng, chỉ đổi khi tạo mới (YC mới → `keyVersion` mới) |
| Lỗi 4xx/5xx không chứa token/secret/keyPrefix      | cảm ơn — bên này cũng không bao giờ trả các giá trị đó trong thân lỗi                                      |

---

## 5. Hợp đồng đã sửa — thay đổi so với bản các anh/chị đọc

`YEU-CAU-TU-SA-YC.md` (cùng thư mục) đã cập nhật vòng 3 và **thay thế mọi phiên bản trước**. Nếu
các anh/chị đối chiếu với bản vòng 1 từng gửi, khác biệt cần biết:

| Chỗ                   | Bản trước                     | Bản mới                                                   |
| --------------------- | ----------------------------- | --------------------------------------------------------- |
| `by-column` mã trả về | `200` / `207` / `202` theo ca | **luôn `200`**, mỗi cột một `state`, cờ `complete`        |
| `tag`                 | bắt buộc mọi lời gọi          | **theo loại client** (§5.1b) — bảng sáu ca của vòng 3     |
| `onMissing`           | chưa có                       | `request`/`none`, mặc định `none`; client team bị bỏ qua  |
| `MISSING`             | không phân biệt               | kèm `reason: not_requested \| yc_limit_reached`           |
| `REJECTED`            | chỉ mô tả                     | khớp theo `(database, table, column)`, không theo client  |
| `by-file`             | làm trước                     | **lùi** — các anh/chị chưa dùng ở giai đoạn này           |
| Giới hạn `columns`    | 100 cột                       | 100 cột **và** 4096 ký tự sau gộp                         |
| Trạng thái YC         | `OPEN` vô hạn                 | `OPEN` / `REJECTED` (dừng, không retry) / đóng khi có key |

Mọi mục khác giữ nguyên.

Thứ tự bên này (đã ghi ở hợp đồng §8): sửa truy vấn key bảng → migration cột + bảng YC →
`by-column` **nhánh client nội bộ** → giao diện quản trị YC (dak sinh key) → báo các anh/chị khi
chạy được ở môi trường thử. Nhánh client team và `by-file` theo sau.

---

## 6. Việc tiếp theo

1. Chúng tôi code theo hợp đồng vòng 3 và **báo khi nhánh nội bộ chạy được ở môi trường thử**.
2. Các anh/chị viết thư viện với stub theo hợp đồng vòng 2 (như kế hoạch §5.2 của vòng 3).
3. Hai bên bật `by-column` cho job/engine ở môi trường thử ngay khi (1) xong — không còn chờ gì.
