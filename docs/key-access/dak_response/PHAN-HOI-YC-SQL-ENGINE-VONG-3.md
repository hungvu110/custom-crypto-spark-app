# Phản hồi của đội sql-engine / SparkApplication — vòng 3, về `PHAN-HOI-YC-DAK-VONG-2.md`

Ngày: 2026-10-21. Gửi: đội DAK.

Cảm ơn vòng 2. Chúng tôi **đồng ý với hình dạng API ở §4** (luôn `200`, `state` từng cột, `complete`), với trạng thái YC ở ③, `onMissing` ở ④,
cột khoá nằm trong `columns[]` ở ⑤, gộp cột trùng ở ⑥ và giới hạn 4096 ký tự ở ⑧. Thư viện của chúng tôi sẽ viết theo đúng các điểm đó.

**Một thay đổi so với các vòng trước:** dữ liệu mã hoá bằng key cột là dữ liệu **hoàn toàn mới**. Không có dữ liệu nào đã mã hoá bằng prefix
cấp bảng cần giữ lại, nên **không có đợt migration** nạp key cũ. Các việc R3, R4 (nạp idempotent, nạp / đối soát theo lô) không cần cho
đường `by-column`. Điều kiện phòng ngừa còn lại nằm ở ① bên dưới.

Tài liệu này gồm: trả lời 3 câu ở §5, và **sáu chỗ chúng tôi thấy chưa ổn hoặc còn thiếu**.

---

## 1. Tóm tắt

|                |                                                                                                                                                                                                                         |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Trả lời §5** | Câu 1: nhánh team chờ được · Câu 2: mặc định `none` được, chúng tôi luôn gửi tường minh · Câu 3: không có migration, bỏ câu này                                                                                         |
| **Cần làm rõ** | ① cột đã có dữ liệu không được nhận key mới · ② ai sinh giá trị key · ③ unique khi `column_name` rỗng · ④ gạch dưới trong `VaultPath` · ⑤ `MISSING` và phạm vi của YC `REJECTED` · ⑥ `tag` và `onMissing` ở client team |

---

## 2. Trả lời ba câu ở §5

1. **Nhánh client team chờ được không?** **Được.** Giai đoạn này mọi engine / job của chúng tôi dùng client nội bộ. Các anh/chị làm nhánh
   nội bộ trước, nhánh team sau khi có grant theo cột.
2. **`onMissing` mặc định `none`:** **được.** Thư viện **luôn gửi tường minh** (`request` cho mã hoá, `none` cho giải mã), nên mặc định
   không ảnh hưởng chúng tôi. Giữ `none` là lựa chọn an toàn hơn.
3. **Ai nạp key migration:** **không còn câu hỏi này.** Mọi dữ liệu là mới, key cột sinh ra lần đầu từ YC, không cần nạp prefix cũ.

---

## 3. Điểm chưa ổn hoặc còn thiếu

### ① Cột đã có dữ liệu mã hoá không được nhận key mới

Vì dữ liệu hiện là mới hoàn toàn, điểm này **không cần việc nạp key cũ**. Nhưng chúng tôi vẫn ghi lại để không ai vô tình gặp lại nó:

Với `onMissing=request`, cột chưa có key sẽ mở YC và người có quyền tạo key. Nếu **có một cột nào đó đã có dữ liệu mã hoá bằng key khác**
(ví dụ ai đó thử mã hoá bằng một key tạm rồi mới đăng ký cột với DAK, hoặc một bảng được đem vào sau này) mà DAK tạo **key mới** cho cột
đó, thì key mới **không bao giờ ghi đè** (bất biến của DAK), nên dữ liệu cũ và mới trong cùng một cột **không giải mã được bằng cùng một key**.

Đề nghị:

1. Trong giao diện tạo key cho YC, nếu DAK biết cột có thể đã có dữ liệu, hiển thị cảnh báo và yêu cầu người tạo xác nhận.
2. Nếu sau này có cột cần đưa dữ liệu cũ vào, dùng đường nạp lô (R3, R4) với giá trị prefix cũ **trước** khi bật `by-column` cho cột đó,
   và chặn mở YC cho cột nằm trong danh sách đó (trả `MISSING` kèm `reason`).

**Phản hồi của chúng tôi:** dữ liệu mã hoá bằng key cột là **hoàn toàn mới**, không có dữ liệu nào từ trước, nên không có đợt nạp key cũ. Điểm
này chỉ để phòng ngừa, không chặn việc nào của hai bên.

### ② Ai sinh giá trị key khi người có quyền "tạo key" cho YC?

§3 của vòng 1 nói "người có quyền bên dak tạo key". Xin chốt: giá trị `keyPrefix` do **DAK sinh ngẫu nhiên** (nguồn ngẫu nhiên an toàn, độ dài
cố định), hay do người có quyền **nhập tay**? Key nhập tay dễ yếu hoặc dùng lại giữa các cột. Vì không còn đường migration cần nhập giá trị
cụ thể, chúng tôi đề nghị **chỉ có một đường duy nhất**: DAK sinh, người có quyền chỉ bấm tạo và không nhìn thấy giá trị.

**Phản hồi của chúng tôi:** chúng tôi không yêu cầu DAK trả lời điểm này trong vòng này; cách sinh key do DAK quyết định.

### ③ Ràng buộc unique `(tenant, database_name, table_name, column_name)` khi `column_name` rỗng

Key cấp bảng của luồng `/{database}/{table}` không có cột. Nếu `column_name` để `NULL`, nhiều hệ quản trị CSDL coi hai `NULL` là **khác nhau**
nên ràng buộc unique **không chặn được hai key cùng bảng**, tức mất bất biến "một bảng một key" đang dựa vào. Xin cho biết key cấp bảng được
biểu diễn thế nào (giá trị rỗng cố định, hay bảng riêng) và ràng buộc unique vẫn chặn trùng ở cả hai loại key.

**Phản hồi của chúng tôi:** đây là thiết kế nội bộ của DAK, chúng tôi chưa cần quan tâm và không cần DAK trả lời. Nêu ra để DAK tự rà.

### ④ `VaultPath` không nhận `_` nhưng tên cột lại cho phép `_`

Ở ⑦ vòng 2, DAK nói `VaultPath` chỉ nhận chữ thường, số và gạch ngang, trong khi `[A-Za-z0-9_]` có **gạch dưới**. Có hai khả năng và cả hai
đều cần nói rõ:

- Gạch dưới được đổi sang ký tự khác: khi đó `a_b` và `a-b` có thể trỏ cùng một đường dẫn. Hôm nay tên cột không có `-` nên chưa trùng,
  nhưng xin ghi rõ phép ánh xạ **một-một** và có test.
- Ba phần `database`, `table`, `column` ghép vào cùng một đoạn đường dẫn: `a_b` + `c` và `a` + `b_c` có thể ra cùng chuỗi. Xin xác nhận
  mỗi phần là **một đoạn riêng** (`.../{database}/{table}/{column}`), không nối bằng ký tự có thể xuất hiện trong tên.

Hai điều này quyết định "hai tên khác nhau không bao giờ ra cùng một key", nên xin trả lời bằng văn bản, không chỉ trong code.

**Phản hồi của chúng tôi:** đây là thiết kế nội bộ của DAK, chúng tôi chưa cần quan tâm và không cần DAK trả lời. Nêu ra để DAK tự rà.

### ⑤ `MISSING` còn lẫn nguyên nhân, và phạm vi của YC `REJECTED`

- `MISSING` được dùng cho hai việc: `onMissing=none`, và **vượt giới hạn 50 YC `OPEN`**. Bên gọi cần phân biệt (cái đầu là lỗi cấu hình,
  cái sau là chờ rồi thử lại). Xin thêm trường **`reason` có mã cố định** (ví dụ `not_requested`, `yc_limit_reached`) thay vì câu chữ tự do.
- YC `REJECTED` "vẫn khớp lời gọi đó": khớp theo gì? `(database, table, column)` hay thêm `client`? Nếu một YC bị từ chối bởi người có quyền
  vì cột này không nên mã hoá, thì client nội bộ thứ hai (sau khi rotate) cũng phải nhận `REJECTED`, không mở YC mới. Chúng tôi đề nghị khớp
  theo `(database, table, column)`, không theo client.

### ⑥ `tag` và `onMissing` ở client team

Hợp đồng gốc (YEU-CAU-TU-SA-YC.md §5.1) quy định `tag` **bắt buộc, chỉ nhận `internal`**, nhưng vòng 2 cho phép **client team** gọi `by-column`.
DAK đã trả lời: **luồng client team không truyền `tag`; luồng nội bộ (SA với client nội bộ) mới truyền `tag=internal`**. Chúng tôi đồng ý, và
thư viện sẽ gửi `tag` từ khoá cấu hình `dak.tag`: có giá trị thì gửi, **không đặt thì không gửi tham số**. Xin sửa YEU-CAU §5.1 cho khớp
(`tag` không còn "bắt buộc").

Vì `tag` do bên gọi tự khai nên **không được làm ranh giới quyền**. Xin chốt các ca lệch giữa `tag` và danh tính client:

| Ca                                  | Đề nghị                                                                                                                    |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Client nội bộ gửi `tag=internal`    | Luồng nội bộ: không cần grant, được `onMissing=request`                                                                    |
| Client team không gửi `tag`         | Luồng team: kiểm grant theo cột, không tạo YC                                                                              |
| **Client team gửi `tag=internal`**  | `403 access_denied` (không coi là client nội bộ, không rơi về luồng nội bộ). Quyền theo danh tính client, không theo `tag` |
| **Client nội bộ không gửi `tag`**   | `400 invalid_tag` kèm nói rõ thiếu `tag`, vì client này không thuộc team nào nên không thể đi luồng team                   |
| `tag` có giá trị khác `internal`    | `400 invalid_tag`                                                                                                          |
| Client team gửi `onMissing=request` | Bỏ qua và coi là `none` (hoặc `400 invalid_on_missing`); chúng tôi nghiêng về bỏ qua                                       |

Lý do cho hai dòng in đậm: nếu `tag=internal` đủ để vào luồng nội bộ thì bất kỳ client team nào cũng tự nâng quyền được chỉ bằng cách thêm
một tham số. Thứ quyết định luồng phải là danh tính client; `tag` chỉ xác nhận ý định và giúp bắt lỗi cấu hình.

**Phản hồi của chúng tôi:** vấn đề đã được giải quyết theo trả lời của DAK (client team không truyền `tag`, client nội bộ truyền
`tag=internal`). Bảng trên chỉ để ghi vào hợp đồng, không cần DAK trả lời thêm.

---

## 4. Việc chúng tôi sẽ làm theo hợp đồng vòng 2

| Điểm                 | Thư viện làm                                                                                                         |
| -------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `state` từng cột     | Chỉ dùng key khi mọi cột cần dùng đều `READY`; có cột khác thì lỗi, nêu `state` và `requestCode`, không chạy một nửa |
| `onMissing`          | Mã hoá gửi `request`, giải mã gửi `none`                                                                             |
| `REJECTED`           | Lỗi kết thúc, không retry                                                                                            |
| `X-Dak-Caller`       | Gửi tên engine / job, đúng định dạng `[A-Za-z0-9._-]{1,128}`                                                         |
| Giới hạn độ dài      | Chia lô theo tổng 4096 ký tự và 100 cột, sau khi loại trùng chữ thường                                               |
| Cache                | Chỉ cache cột `READY`, không cache `PENDING` / `MISSING` / `REJECTED` / `DISABLED`                                   |
| Mã lỗi `4xx` / `5xx` | Báo lỗi theo mã, không chứa token, secret hay `keyPrefix`                                                            |

---

## 5. Việc tiếp theo

1. DAK trả lời ⑤ (các ý ①, ②, ③, ④, ⑥ không cần trả lời) và gửi bản hợp đồng đã sửa như §7 của vòng 2.
2. Chúng tôi viết thư viện và test với stub theo hợp đồng vòng 2.
3. Vì không có đợt migration, hai bên có thể bật `by-column` cho job / engine ngay khi nhánh nội bộ chạy được ở môi trường thử.
