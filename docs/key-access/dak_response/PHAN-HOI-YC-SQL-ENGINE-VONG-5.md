# Phản hồi của đội sql-engine / SparkApplication — vòng 5, về `PHAN-HOI-YC-DAK-VONG-4.md`

Ngày: 2026-10-22. Gửi: đội DAK.

Cảm ơn vòng 4. Chúng tôi **đồng ý** với trả lời ⑤ (`reason` mã cố định `not_requested` / `yc_limit_reached`; YC `REJECTED` khớp theo
`(database, table, column)`, không theo client), với kết quả tự rà ①–④ và ⑥, và với thứ tự làm ở §5 (nhánh nội bộ của `by-column` trước,
nhánh client team và `by-file` sau). Bảng kỳ vọng ở §4 khớp với những gì thư viện của chúng tôi sẽ làm.

Còn **hai điểm cần các anh/chị trả lời** trước khi chúng tôi viết thư viện gọi `by-column`. Cả hai đều ảnh hưởng trực tiếp đến cách
sql-engine và SparkApplication lấy và dùng key.

---

## 1. Tóm tắt

|                       |                                                                                                                                  |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| **Cần trả lời**       | ① Hợp đồng đã sửa chưa đến phía chúng tôi · ② Xác nhận key của một cột không bao giờ đổi, và sửa câu "YC mới → `keyVersion` mới" |
| **Không cần trả lời** | Các ý còn lại ở vòng 4, thuộc thiết kế nội bộ của DAK                                                                            |

---

## 2. Điểm cần trả lời

### ① Hợp đồng đã sửa chưa đến phía chúng tôi

Vòng 4 §5 nói `YEU-CAU-TU-SA-YC.md` (cùng thư mục) "đã cập nhật vòng 3 và thay thế mọi phiên bản trước", có mục §5.1b chứa bảng sáu ca của
`tag` / `onMissing`. Bản chúng tôi có **chưa phải bản đó**:

| Chỗ                   | Bản chúng tôi đang có             | Vòng 4 nói bản mới                             |
| --------------------- | --------------------------------- | ---------------------------------------------- |
| Mã trả về `by-column` | `200` / `207` / `202` theo ca     | luôn `200`, mỗi cột một `state`, cờ `complete` |
| `tag`                 | bắt buộc mọi lời gọi              | theo loại client (§5.1b)                       |
| `onMissing`, `reason` | không có                          | có                                             |
| Thứ tự làm            | `by-file` trước (§8), "chưa code" | `by-column` nhánh nội bộ trước                 |
| Mục §5.1b             | không có                          | có                                             |

Xin gửi lại bản đúng (hoặc cho biết đường dẫn nếu nó nằm ở chỗ khác). Chúng tôi **chưa viết gì dựa trên bản cũ**, và sẽ viết stub theo bản
mới. Hai chỗ xin ghi rõ khi gửi:

- Bảng sáu ca của `tag` / `onMissing` có trong bản mới, đúng như vòng 4 §3 ⑥ nói.
- Tên trạng thái ở hai tầng: YC có `OPEN` / `REJECTED`, còn mỗi cột trong `columns[]` có `state` `READY` / `PENDING` / `MISSING` / `REJECTED` /
  `DISABLED`. Vòng 4 §5 gộp hai tầng này trong một bảng nên dễ đọc nhầm.

### ② Key của một cột không bao giờ đổi: xin xác nhận, và sửa câu ở vòng 4

Cách chúng tôi dùng key: một cột được mã hoá bằng **một key duy nhất**. Các lần sau (mã hoá thêm, giải mã) chỉ cần xin đúng key đó. Vì key
không bao giờ đổi nên thư viện **không dùng `keyVersion`** để chọn hay lưu key; chúng tôi chỉ đọc nó để ghi log.

Vòng 4 §4, dòng Cache, viết: key "không đổi im lặng, chỉ đổi khi tạo mới (**YC mới → `keyVersion` mới**)". Câu này ngụ ý một cột đã có key
vẫn có thể nhận key khác, **trái** với hai cam kết trước của DAK:

- Vòng 1 và vòng 2: giá trị key đã ghi **không bao giờ bị ghi đè**.
- Vòng 4 §2 ⑤b: sau `REJECTED` thì **không mở YC mới** cho cùng `(database, table, column)`.

Nếu thật sự có một cột nhận key thứ hai thì dữ liệu mã hoá bằng key đầu sẽ giải mã sai (AES/ECB có thể cho ra kết quả rác hoặc lỗi padding mà
không báo rõ), và chúng tôi không có cách nào biết dữ liệu nào dùng key nào. Vì vậy xin DAK:

1. **Xác nhận bằng văn bản:** một cột đã có key thì **không bao giờ có key khác**. `keyVersion` của cột đó luôn không đổi.
2. **Giải thích hoặc sửa** câu "YC mới → `keyVersion` mới" ở vòng 4 §4, để nó không bị hiểu là cột đổi được key.
3. **Chặn ở phía DAK:** không cho tạo key thứ hai cho cột đã có key (kể cả qua giao diện), thay vì chỉ dựa vào quy ước.

Hiện chưa có dữ liệu thật nào được mã hoá (mới chỉ test), nên bây giờ là lúc rẻ nhất để chốt điều này.

---

## 3. Việc tiếp theo

1. DAK gửi bản hợp đồng đúng (①) và xác nhận ② (key của cột không bao giờ đổi).
2. Chúng tôi viết thư viện và stub theo bản hợp đồng đó.
3. Hai bên bật `by-column` ở môi trường thử khi DAK báo nhánh nội bộ chạy được.
