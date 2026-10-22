# Phản hồi của đội sql-engine / SparkApplication về `YEU-CAU-TU-SA-YC.md`

Ngày: 2026-10-21. Gửi: đội DAK.

Cảm ơn các anh/chị đã gửi hợp đồng trước khi code. Tài liệu này **chỉ bàn `GET /api/v1/keys/by-column`**. Luồng `by-file` chúng tôi chưa
dùng ở giai đoạn này nên chưa có ý kiến; các anh/chị cứ làm sau.

Chúng tôi sẽ chuyển `cdr_encrypt`/`cdr_decrypt` sang **một key cho mỗi cột** và dùng `by-column` làm đường lấy key chính, không còn
`/{database}/{table}`. Vì vậy các điểm dưới đây ảnh hưởng thẳng tới luồng chạy thật, không chỉ luồng YC.

---

## 1. Tóm tắt

|                            |                                                                                                                                                                                                          |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Cần sửa trước khi code** | ① §6 bỏ cổng quyền: client nội bộ không cần grant, client team cần grant theo cột ② định danh key cột khi cố định tenant, bỏ workspace ③ YC không có trạng thái kết thúc ④ YC sinh ra cả khi chỉ giải mã |
| **Cần làm rõ**             | ⑤ cột bị khoá ⑥ trùng cột trong `columns` ⑦ ký tự trong tên cột ⑧ độ dài URL                                                                                                                             |
| **Trả lời 4 câu ở §7**     | Mục 3                                                                                                                                                                                                    |

---

## 2. Điểm cần sửa hoặc làm rõ

### ① 🔴 §6 bỏ cổng quyền: ranh giới chuyển sang client riêng của luồng nội bộ

§6 chấp nhận không có cổng quyền vì "luồng YC là nội bộ giữa hai đội". Theo trao đổi gần nhất, DAK sẽ cấp **một `client_id` / secret
riêng cho luồng nội bộ** để lấy key. Chúng tôi đồng ý hướng này: nó thay "token hợp lệ của realm" (ai trong realm cũng gọi được) bằng một
ranh giới cụ thể. Nhưng `by-column` **trả `keyPrefix` thật** (§5.3) và chúng tôi dùng nó cho mọi lần mã hoá/giải mã, nên cần chốt rõ:

1. **Hai loại người gọi `by-column`, kiểm theo client ngay từ ngày đầu** (không đợi "client thứ hai" như §6 viết):

   | Người gọi                                                                      | Điều kiện lấy key                                  | Khi không đủ điều kiện                                 |
   | ------------------------------------------------------------------------------ | -------------------------------------------------- | ------------------------------------------------------ |
   | **Client nội bộ** (client riêng DAK cấp cho luồng này)                         | Không cần grant. Được phép tạo YC khi chưa có key  | —                                                      |
   | **Client của team khác** (client team, `client_id` / secret của chính team đó) | Chỉ khi team có **grant còn hạn trên đúng cột** đó | `403 access_denied`, cùng một thân cho mọi nguyên nhân |

   Nghĩa là khi DAK cấp quyền lấy key trên một cột cho team khác (qua phiếu được duyệt như luồng hiện có), team đó dùng **client của
   chính mình** gọi `by-column` và lấy được key của cột được cấp. Không cần thêm credential nào. Phía chúng tôi không phải đổi gì:
   thư viện chỉ cần `dak.clientId` / `dak.clientSecret` của người gọi.

   Xin DAK chốt các chi tiết của nhánh team:
   - **Grant theo cột**, không chỉ theo bảng: team được cấp `msisdn` không tự có `imei` của cùng bảng. Thời hạn và cảnh báo hết hạn
     áp dụng như PHAN-HOI-DAK.md §3.1.
   - Team không có grant nhận `403` **kể cả khi cột chưa có key**, để không dò được cột nào đã có key. Có grant thì key chắc chắn tồn tại.
   - Client team **không tạo YC** (YC chỉ mở bởi client nội bộ, hoặc team phải đi đường phiếu như hiện nay). Xin xác nhận.
   - Request nhiều cột mà team chỉ có grant một phần thì trả `403` cả lô hay `state` từng cột? Đề nghị cả lô `403`, vì cùng thân với
     ca không có quyền, bên gọi không phải xử lý "đủ một nửa".
   - Client nội bộ có gọi được `/{database}/{table}` không?

2. **Ghi vào hợp đồng đây là thay đổi có chủ đích với client nội bộ.** Với client này, phiếu được duyệt, thời hạn 1 năm, cảnh báo 30
   ngày và `denyReason` (PHAN-HOI-DAK.md §3.1, §6) **không áp dụng**; ranh giới là bí mật của client. Hai bên từng nói đó là lý do hệ
   thống tồn tại ("ai cho phép, vì sao, đến bao giờ"), nên cần ghi lại là bỏ có chủ ý, không để nó trôi. Client team vẫn đi đủ cổng phiếu.
3. **Hệ quả của client nội bộ dùng chung.** Mọi driver sql-engine và SparkApplication của luồng này dùng chung một secret, nên DAK không
   phân biệt được engine/job nào hỏi key, và lộ secret là lộ key của mọi cột. Phía chúng tôi giữ secret trong K8s Secret. Xin DAK:
   - cho phép **rotate bằng client thứ hai** (R5) như luồng team, vì engine/job phải restart để nhận credential mới;
   - ghi audit mọi lần trả `keyPrefix` theo `clientId` và `X-Request-Id`;
   - tuỳ chọn: nhận thêm một header **chỉ để audit** (tên engine/job do chúng tôi gửi), không dùng để cấp quyền. Nếu DAK thấy header
     này làm bên gọi tự khai thì bỏ, vì nó không đáng tin.

### ② Định danh key cột sau khi cố định tenant và bỏ workspace

Theo trao đổi gần nhất, DAK **cố định tenant** và **không xét workspace** nữa. Chúng tôi đồng ý, và các câu "tenant/workspace suy từ
token" trong PHAN-HOI-DAK.md §2, §6 và DAK_API_SPEC.md được thay bằng quy ước dưới đây. Xin xác nhận bằng văn bản cho `by-column`:

- Định danh key cột là `(database, table, column)` trong tenant cố định của DAK. Không có tham số tenant hay workspace nào, và `tag` không
  tham gia định danh.
- Mọi team cùng hỏi `demo_db.users_cdr.msisdn` nhận **cùng một key**. Đây là chủ ý: một cột một key, không phải một key mỗi team.
- Client nội bộ không thuộc team nào (①). Với client team, team vẫn xác định từ token (`azp` → team trong DB của DAK) để kiểm grant theo
  cột. Chỉ bỏ workspace, không bỏ team.

Hệ quả cần nói thẳng: khi không còn workspace thì ranh giới giữa các team chỉ là **grant theo cột** (client team) và **client nội bộ**
(①). Cổng workspace trong DAK_API_SPEC.md §5.3 không còn chặn được gì. Vì vậy ① phải được thực thi ngay, và `200` so với `202` chỉ được để
lộ cho client nội bộ (client team không có grant nhận `403` trước khi biết cột nào có key).

Với luồng `/{database}/{table}` hiện có, xin cho biết ràng buộc unique `(tenant, workspace, database_name, table_name)` đổi thành gì, và key
đã nạp theo workspace được gộp ra sao. Nếu cùng một bảng đang có hai key ở hai workspace khác nhau thì bắt buộc phải chọn một, và chúng
tôi cần biết trước để dữ liệu đã mã hoá không mất khả năng giải mã.

### ③ YC chưa có trạng thái kết thúc

§8 bước 6 nói người có quyền có thể **từ chối** YC, nhưng response chỉ có `state: "OPEN"`. Bên gọi gặp YC bị từ chối sẽ thấy `202` mãi
mãi và retry mãi. Xin chốt:

- Danh sách `state` đầy đủ (ví dụ `OPEN`, `REJECTED`, `CLOSED`) và lời gọi nào trả mã nào.
- Khi YC bị từ chối: trả lỗi kết thúc (`403` hoặc `409` kèm `requestCode` và lý do ngắn), không phải `202`.
- Sau khi từ chối, gọi lại có mở **YC mới** không? Nếu có thì cho chúng tôi biết để không tạo vòng lặp YC.

### ④ Giải mã không nên sinh YC

Hôm nay `cdr_decrypt('db.table', isdn, ...)` trên bảng không có key là **lỗi cấu hình** (sai tên bảng, sai tên cột, chưa migrate). Theo §3, mỗi
lần như vậy dak mở một YC. Hậu quả: gõ sai tên cột trong 1 câu SQL tạo 1 YC mới trong hàng đợi của người có quyền; YC không có người
duyệt nên không có gì ngăn hàng đợi phình ra.

Đề nghị: thêm tham số để bên gọi nói rõ ý định, ví dụ `onMissing=request|none`.

- Mã hoá (chưa có key là chuyện bình thường): `onMissing=request`.
- Giải mã: `onMissing=none`, chưa có key thì trả `404 key_not_found` như một lỗi kết thúc, không tạo YC.

Dù chọn gì, xin thêm: giới hạn số YC `OPEN` tối đa mỗi client / mỗi bảng, và rate limit riêng cho thao tác tạo YC.

### ⑤ Cột có key nhưng đang bị khoá

Luồng cũ có `403 key_disabled`. Hợp đồng mới có ba trạng thái cho mỗi cột (có key / chưa có key / YC) nhưng **không có ô cho cột bị khoá**.
Xin chốt: một cột bị khoá làm cả lời gọi `403 key_disabled`, hay nằm trong `columns[]` với `state: "DISABLED"`? Chúng tôi cần quy tắc rõ để
không chạy mã hoá nửa chừng với một nửa số cột.

### ⑥ Trùng cột sau khi chuẩn hoá chữ thường

`columns=MSISDN,msisdn` chuẩn hoá thành cùng một cột. Xin chốt: gộp thành một mục trong response, hay `400 invalid_column`. Chúng tôi sẽ
loại trùng ở phía mình, nhưng cần biết để test cả hai phía khớp.

### ⑦ Ký tự trong tên cột

Tên bảng/cột Spark có thể chứa ký tự ngoài `[A-Za-z0-9_]` khi được bọc trong dấu backtick (ví dụ `start-time`, `số_điện_thoại`). Với
`by-column` ràng buộc `[A-Za-z0-9_]{1,128}` nghĩa là những cột đó **không thể có key**. Chúng tôi sẽ báo lỗi rõ ở phía mình và hiện giả
định bảng CDR không có tên như vậy. Xin xác nhận ràng buộc này là chủ ý.

### ⑧ Độ dài URL

100 cột × 128 ký tự có thể vượt 8 KB, mức mặc định của nhiều ingress / máy chủ cho dòng request. Chúng tôi sẽ **chia lô** theo giới hạn
bên các anh/chị. Xin ghi giới hạn tổng độ dài `columns` (hoặc xác nhận ingress chấp nhận bao nhiêu) thay vì chỉ giới hạn số cột.

---

## 3. Trả lời 4 câu ở §7

1. **Mã `207`**: thư viện chúng tôi tự viết nên xử được `202`/`207`, nhưng đề nghị đổi: **luôn `200`** cho `by-column`, mỗi cột có
   `state` (`READY` / `PENDING` / `REJECTED` / `DISABLED`) và `requestCode` khi cần, kèm một cờ tổng `complete: true|false`. Lý do:
   bên gọi chỉ phải đọc một hình dạng, không phụ thuộc mã HTTP; proxy, gateway hay client HTTP khác coi `2xx` là thành công sẽ không nhầm
   "chưa có key" thành "có key". (Bản thân thư viện hiện tại của chúng tôi coi mọi `2xx` là thành công, nên chúng tôi cũng sẽ sửa.)
2. **Đường dẫn file phân biệt hoa thường**: không áp dụng ở giai đoạn này (chưa dùng `by-file`).
3. **100 cột**: đủ cho một lời gọi, chúng tôi chia lô khi cần. Giới hạn thật nên là tổng độ dài, xem ⑧.
4. **Biết YC đã xong chưa**: **hỏi lại cùng lời gọi**, đồng ý với hướng nghiêng của các anh/chị. Không cần endpoint tra trạng thái miễn
   là ③ được chốt (có trạng thái kết thúc).

---

## 4. Bổ sung nhỏ

- Mọi response của `by-column`, kể cả có cột `PENDING`, trả `X-Request-Id`; `Cache-Control: no-store`. Chúng tôi **không cache** kết quả có cột
  chưa sẵn sàng và chỉ cache từng cột đã có key.
- Audit của `by-column` cần ghi từng cột (cột nào trả key, cột nào mở YC) và `clientId` (cùng header audit tuỳ chọn ở ①) sinh ra YC. Với client team ghi thêm team
  và grant đã dùng. Người có quyền cần biết ai đã yêu cầu.
- Tên mã lỗi cho các trường hợp mới (`tag` thiếu hoặc sai, `columns` rỗng, quá giới hạn) xin ghi trong hợp đồng; hiện chỉ có `invalid_column`.
- Key của cột phải theo cùng bất biến với key bảng: **không bao giờ ghi đè**, `keyVersion` là version đã thực sự đọc. Xin xác nhận
  để dữ liệu đã mã hoá không bị mất khả năng giải mã.
- **Migration**: dữ liệu CDR hiện đã mã hoá bằng prefix cấp bảng. Để giải mã tiếp, mỗi cột đã mã hoá cần được nạp key cột **có giá trị bằng
  prefix cũ**. Xin xác nhận nạp theo lô (R3, R4 ở PHAN-HOI-SQL-ENGINE.md) áp dụng được cho key cột, và nạp lại cùng giá trị là idempotent.
