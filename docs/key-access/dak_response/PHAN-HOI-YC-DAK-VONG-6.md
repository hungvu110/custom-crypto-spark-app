# Phản hồi của đội dak — vòng 6, về `PHAN-HOI-YC-SQL-ENGINE-VONG-5.md`

Ngày: 2026-10-22. Gửi: đội sql-engine / SparkApplication.

Cảm ơn hai câu hỏi — cả hai đều đúng chỗ cần chốt trước khi các anh/chị viết thư viện, và câu ②
nhắc chúng tôi một câu viết thiếu chuẩn ở vòng 4.

Tài liệu này: gửi lại hợp đồng đúng (①) và xác nhận bằng văn bản key của một cột không bao giờ
đổi, kèm cách dak sẽ chặn được (②). Không điểm nào còn treo.

---

## 1. Tóm tắt một trang

|                    |                                                                                                                                                                                                                                      |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **① Hợp đồng**     | Bản đúng là `YEU-CAU-TU-SA-YC.md` bản vòng 3, **đính kèm thư này** — gửi lại trọn bộ để khỏi tra đường dẫn. Bảng sáu ca `tag`/`onMissing` ở §5.1b. **Hai tầng trạng thái tách bạch** ở §2 dưới                                       |
| **② Key của cột**  | **Xác nhận bằng văn bản: một cột đã có key thì KHÔNG BAO GIỜ có key khác.** Vòng đời đủ: không ghi đè · không rotate · `keyVersion` không đổi. Câu "YC mới → keyVersion mới" ở vòng 4 §4 **rút lại** — viết sai; cách dùng đúng ở §3 |
| **Chặn tại dak**   | Có, không chỉ quy ước: ràng buộc unique một-cột-một-key ở DB chặn cả tạo mới lẫn đóng YC thứ hai; đường `rotate` bị **khoá** cho key cột. Chi tiết §4                                                                                |
| **Việc tiếp theo** | Các anh/chị viết lib theo hợp đồng đính kèm; chúng tôi code nhánh nội bộ và báo khi chạy được                                                                                                                                        |

---

## 2. Trả lời ① — hợp đồng đúng, và hai tầng trạng thái

Bản các anh/chị có là bản vòng 1 (trước cả vòng 2) — bản cập nhật chỉ nằm trong thư mục nội bộ
chúng tôi chứ chưa gửi lại. Lỗi phân phối là của chúng tôi. **Bản đúng đính kèm thư này**:
`YEU-CAU-TU-SA-YC.md` (bản vòng 3, cập nhật 2026-10-21), thay thế mọi bản trước. Những chỗ bảng
so sánh của các anh/chị nêu — `200`/`state`/`complete`, `tag` theo loại client, `onMissing`,
`reason`, `by-column` nội bộ trước — đều có trong bản đó.

**Hai tầng trạng thái, tách bạch như yêu cầu:**

| Tầng                                        | Trường                                                                                                  | Giá trị                                                                                                                         | Nơi xuất hiện                                        |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| **YC** (bản ghi "có người hỏi key này")     | `state` trong thân `202` của `by-file`, hoặc `requestCode` kèm cột `PENDING`/`REJECTED` của `by-column` | `OPEN` · `REJECTED` · `FULFILLED` (đóng khi có key — **không bao giờ xuất hiện trong response**, cột thành `READY` thay vì vậy) | `by-file` thân 202; `by-column` chỉ lộ `requestCode` |
| **Cột** (trong `columns[]` của `by-column`) | `state` của **từng cột**                                                                                | `READY` · `PENDING` · `MISSING` · `REJECTED` · `DISABLED`                                                                       | `by-column` thân 200                                 |

Vòng 4 §4 gộp hai tầng trong một bảng — dễ đọc nhầm như các anh/chị nói; bảng trên là bản chuẩn
và đã thay thế trong hợp đồng đính kèm (§5.3).

---

## 3. Trả lời ② — xác nhận: key của một cột KHÔNG BAO GIỜ đổi

### 3.1 Xác nhận bằng văn bản (yêu cầu 1 của các anh/chị)

> **Một cột đã có key thì không bao giờ có key khác.** Không có đường nào — API, giao diện, hay
> thao tác vận hành — tạo ra key thứ hai cho cùng `(database, table, column)` đang sống.
> `keyVersion` của cột đó **luôn giữ nguyên giá trị đầu** từ lúc key được tạo tới khi key bị
> khoá/xoá (khoá/xoá làm cột thành `DISABLED`/`MISSING`, không tạo key mới).

### 3.2 Câu "YC mới → `keyVersion` mới" — RÚT LẠI (yêu cầu 2)

Các anh/chị bắt đúng: câu đó ở vòng 4 §4 (dòng Cache) **viết sai** — nó mô tả luồng key ACCESS
của phiếu PYC (nơi rotate là tính năng có chủ ý, BR-2.11), không phải key cột của luồng YC.
Sao chép sang ngữ cảnh YC là lỗi của chúng tôi. Câu đúng cho `by-column`:

> **Key cột không đổi im lặng và cũng không đổi có chủ ý.** Tuyệt đối — không có "YC mới",
> không có rotate, không có version mới cho cùng một cột. `keyVersion` đọc được chỉ dùng để ghi
> log/đối chiếu như các anh/chị dùng — nó không bao giờ tăng cho một cột.

### 3.3 Vì sao không có ca "cột nhận key thứ hai"

Hai cam kết trước vẫn đúng và giờ chặt hơn:

- **Vòng 1–2 "không bao giờ ghi đè"**: đúng và mạnh hơn — không chỉ không ghi đè giá trị, mà
  **không cả dòng key thứ hai** cho cột đang sống (xem §4: unique index).
- **Vòng 4 ⑤b "REJECTED không mở YC mới"**: đúng — và nếu người có quyền **đổi ý sau khi từ
  chối** muốn cấp key cho cột, xử trên giao diện dak bằng cách làm mới YC đó; key sinh ra vẫn là
  **key đầu tiên và duy nhất** của cột (cột chưa từng có key), `keyVersion` = 1.

Trường hợp duy nhất một cột "đổi" key: **không tồn tại**. Kể cả key bị khoá (`DISABLED`) rồi mở
lại — mở lại trả **chính key cũ**, không sinh mới.

---

## 4. Chặn tại dak — không chỉ quy ước (yêu cầu 3 của các anh/chị)

Kiểm tra trên code hiện có, ba lớp:

| Lớp          | Cơ chế                                                                                                                                                                                             | Chặn gì                                                                                                         |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| **DB**       | Unique index một-`(tenant, database, table, column)`-một-key-sống (`WHERE status <> 'DELETED' …`) — chính index mà vòng 2 đã thống nhất                                                            | Tạo key thứ hai cho cột đang sống, **bất kể đường nào** (API, giao diện, đóng YC, nạp lô) — trùng là DB từ chối |
| **Ứng dụng** | `fulfill` (đóng YC bằng key) tra key theo cột trước khi ghi — cột đã có key thì gắn key **hiện có** vào YC, không sinh dòng mới                                                                    | Đường "đóng YC lần hai cho cùng cột" không tạo key                                                              |
| **API**      | Đường `rotate` (`POST /keys/{id}/rotate`) tồn tại cho key của luồng PYC — **khoá cho key cột của luồng YC**: từ chối `409` kèm thông báo "key cột không đổi". Không đường nào khác ghi lại giá trị | Nguồn duy nhất có thể "đổi" giá trị bị bịt cho key cột                                                          |

Lớp DB là lời đảm bảo cứng — kể cả lỗi tương lai ở ứng dụng cũng không qua được index. Lớp API
(chặn rotate) là việc chúng tôi thêm trong đợt code `by-column`, ghi vào kế hoạch (docs/29 §6
bước 6) để không trôi.

Ngoài ra bản ghi key mang `request_code` = mã YC sinh nó, nên khi điều tra "cột này có bao giờ
đổi key không", đối chiếu một cột là đủ — nếu thấy request_code khác lúc khác thì có lỗi, và
chúng tôi muốn biết.

---

## 5. Việc tiếp theo

1. Hợp đồng vòng 3 đính kèm thư này là bản duy nhất các anh/chị cần — viết thư viện theo nó.
2. Chúng tôi code nhánh nội bộ `by-column` (đã) bao gồm chặn rotate cho key cột; báo khi chạy
   được ở môi trường thử.
3. Hai bên bật `by-column` cho job/engine ở môi trường thử ngay sau (2) — không còn chờ gì khác.
