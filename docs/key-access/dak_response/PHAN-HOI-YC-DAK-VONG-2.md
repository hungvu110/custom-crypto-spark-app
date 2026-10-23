# Phản hồi của đội dak — vòng 2, về `PHAN-HOI-YC-SQL-ENGINE.md`

Ngày: 2026-10-21. Gửi: đội sql-engine / SparkApplication.

Tám điểm các anh/chị nêu đều đúng, và bốn trong số đó chỉ ra lỗi thật trong hợp đồng chúng tôi
gửi — không phải chỗ cần làm rõ. Cảm ơn vì đã đọc kỹ trước khi hai bên code.

Tài liệu này: **chúng tôi nhận gì, sửa gì, và ba chỗ cần các anh/chị quyết**.

---

## 1. Tóm tắt một trang

|                             |                                                                                                                             |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **Nhận, sửa ngay**          | ① hai loại client kiểm từ ngày đầu · ③ YC có trạng thái kết thúc · ④ `onMissing` · ⑥ gộp cột trùng · ⑧ giới hạn theo độ dài |
| **Nhận, đổi hình dạng API** | Câu 1 — bỏ `207`, luôn `200` với `state` từng cột                                                                           |
| **Xác nhận**                | ② định danh `(database, table, column)` · ⑤ cột khoá nằm trong `columns[]` · ⑦ ràng buộc tên cột là chủ ý                   |
| **Cần các anh/chị quyết**   | 3 câu ở §5                                                                                                                  |
| **Việc lớn phát sinh**      | **Grant xuống mức cột** — chưa có, cần một đợt riêng (§3)                                                                   |

**Điểm quan trọng nhất**: ① làm lộ ra một việc chúng tôi chưa có — grant hiện chỉ tới mức **bảng**,
không xuống **cột**. Nó không sửa được trong vòng này; xem §3.

---

## 2. Tám điểm

### ① Hai loại client — nhận, kiểm từ ngày đầu

Đồng ý toàn bộ, gồm cả việc **không đợi "client thứ hai"**. Chúng tôi sửa `docs/29 §5.3`: thay
"tạm để mở, siết sau" bằng kiểm client ngay từ phiên bản đầu.

Trả lời năm chi tiết các anh/chị hỏi:

| Câu hỏi                                               | Trả lời                                                                                       |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Grant theo cột, không chỉ bảng                        | **Đúng, nhưng chưa có** — xem §3. Đây là việc lớn nhất phát sinh từ vòng này                  |
| Team không grant nhận `403` kể cả khi cột chưa có key | **Đồng ý.** Kiểm grant TRƯỚC khi tra key, nên không dò được cột nào đã có                     |
| Client team không tạo YC                              | **Xác nhận.** YC chỉ mở bởi client nội bộ. Team thiếu quyền đi đường phiếu như hiện nay       |
| Nhiều cột mà team chỉ có grant một phần               | **Đồng ý `403` cả lô.** Cùng thân với ca không quyền, bên gọi không phải xử "đủ một nửa"      |
| Client nội bộ gọi `/{database}/{table}` được không    | **Không.** Đường đó đi qua bốn cổng phiếu và cần một team; client nội bộ không thuộc team nào |

**Ghi lại là bỏ có chủ ý** (mục 2 của các anh/chị): đồng ý, và chúng tôi ghi vào `docs/29` chứ
không chỉ ở đây. Với client nội bộ, phiếu được duyệt / thời hạn / cảnh báo hết hạn / `denyReason`
**không áp dụng**; ranh giới duy nhất là bí mật của client. Các anh/chị nói đúng rằng đó chính là
thứ hệ thống này sinh ra để ghi lại — nên nó phải nằm trong tài liệu, không trôi đi.

**Hệ quả client dùng chung** (mục 3): nhận cả ba.

- **Rotate bằng client thứ hai**: đồng ý. Cùng cách R5 của luồng team — dak cấp client mới, hai
  client cùng sống một thời gian, engine/job restart dần, rồi thu hồi client cũ.
- **Audit theo `clientId` + `X-Request-Id`**: đã có sẵn cho mọi lời gọi luồng máy, sẽ áp dụng
  nguyên cho hai đường mới.
- **Header chỉ-để-audit**: chúng tôi **nhận**, nhưng ghi rõ giới hạn của nó. Giá trị do bên gọi
  khai nên không xác minh được — nó giúp người vận hành thu hẹp phạm vi khi điều tra, và không bao
  giờ tham gia cấp quyền. Tên: `X-Dak-Caller`, tối đa 128 ký tự, chỉ `[A-Za-z0-9._-]`. Sai định
  dạng thì **bỏ qua header**, không từ chối lời gọi: một header phụ trợ không đáng làm hỏng một
  lượt lấy key.

### ② Định danh key cột — xác nhận bằng văn bản

**Xác nhận cả ba gạch đầu dòng:**

- Định danh key cột là `(database, table, column)` trong tenant cố định. Không tham số tenant hay
  workspace. `tag` **không** tham gia định danh.
- Mọi team hỏi `demo_db.users_cdr.msisdn` nhận **cùng một key**. Một cột một key, không phải một
  key mỗi team.
- Client nội bộ không thuộc team nào. Client team vẫn xác định team từ token (`azp`) để kiểm
  grant. Bỏ workspace, **không** bỏ team.

Các anh/chị nói đúng ở chỗ: bỏ workspace là bỏ một lớp chặn, nên ① phải thực thi ngay chứ không
để sau. Chúng tôi đã sửa kế hoạch nội bộ theo hướng đó.

**Về ràng buộc unique và key đã nạp** — câu hỏi cuối của ②:

- `uq_keys_table_scope` hiện là `(tenant, workspace, database_name, table_name)`, đổi thành
  `(tenant, database_name, table_name, column_name)`.
- **Đo trên dữ liệu môi trường thử 2026-10-21: không bảng nào có key ở hai workspace khác nhau.**
  Nên việc gộp không làm hai key va nhau.
- Chúng tôi sẽ **đo lại trên production trước khi áp**, và nếu có bảng nào ở hai workspace thì báo
  các anh/chị trước, kèm danh sách bảng — để bên các anh/chị biết dữ liệu nào cần giải mã lại bằng
  key nào. Không tự chọn một key rồi bỏ cái kia.

### ③ YC có trạng thái kết thúc — nhận, chúng tôi viết thiếu

Đúng là lỗi của hợp đồng vòng 1: có "từ chối" ở §8 nhưng response chỉ có `OPEN`. Bên gọi sẽ retry
vô hạn.

| `state`    | Nghĩa                                   | Bên gọi làm gì                                   |
| ---------- | --------------------------------------- | ------------------------------------------------ |
| `OPEN`     | đã ghi nhận, chờ người có quyền tạo key | thử lại sau                                      |
| `REJECTED` | người có quyền từ chối, kèm lý do       | **dừng**, không retry                            |
| `CLOSED`   | key đã tạo xong                         | lời gọi tiếp theo trả key, không còn `state` này |

Theo câu 1 (bỏ `207`), `REJECTED` nằm trong `state` của từng cột chứ không phải mã HTTP riêng —
xem §4.

**Gọi lại sau khi bị từ chối**: **không** mở YC mới. YC `REJECTED` vẫn khớp lời gọi đó, nên bên
gọi nhận lại `REJECTED` kèm lý do, không tạo vòng lặp. Người có quyền muốn mở lại thì làm từ giao
diện dak.

### ④ Giải mã không nên sinh YC — nhận hoàn toàn

Điểm này chúng tôi không nghĩ tới, và nó đúng: gõ sai tên cột trong một câu SQL không nên tạo một
mục trong hàng đợi của người có quyền. YC không có người duyệt nên không có gì ngăn hàng đợi phình.

**Nhận `onMissing`:**

| Giá trị   | Chưa có key thì                   | Dùng khi                                   |
| --------- | --------------------------------- | ------------------------------------------ |
| `request` | mở YC, `state: PENDING`           | mã hoá — chưa có key là chuyện bình thường |
| `none`    | `state: MISSING`, **không** mở YC | giải mã — chưa có key là lỗi cấu hình      |

Mặc định khi không gửi: **`none`**. Chọn mặc định an toàn hơn — quên gửi tham số thì không âm thầm
tạo YC.

**Giới hạn và rate limit**: nhận cả hai.

- Tối đa **50 YC `OPEN`** cho mỗi `(client, database, table)`. Vượt thì `state: MISSING` kèm lý do,
  không mở thêm.
- Rate limit riêng cho thao tác **tạo** YC, tách khỏi rate limit lấy key: một lời gọi 100 cột chưa
  có key không nên ăn hết hạn mức lấy key của cả engine.

### ⑤ Cột bị khoá — nằm trong `columns[]`

Theo câu 1 (một hình dạng duy nhất): `state: "DISABLED"`, **không** làm hỏng cả lời gọi.

Lý do: khoá một key là thao tác của người vận hành trên **một** cột; để nó làm `403` cả lô nghĩa là
khoá một cột chặn luôn chín cột còn lại đang hoạt động bình thường. Bên các anh/chị đọc `state` của
từng cột và quyết định dừng hay chạy phần còn lại — chúng tôi không quyết hộ.

### ⑥ Trùng cột sau chuẩn hoá — gộp, không báo lỗi

`columns=MSISDN,msisdn` → **một** mục trong `columns[]`, không `400`.

Lý do chọn gộp: bên gọi ghép danh sách cột từ nhiều nguồn là chuyện thường, và từ chối cả lời gọi
vì một trùng lặp vô hại là bắt họ viết thêm bước lọc mà dak làm được rẻ hơn. Thứ tự trong response
theo lần xuất hiện **đầu tiên**.

### ⑦ Ràng buộc tên cột — xác nhận là chủ ý

`[A-Za-z0-9_]{1,128}` là chủ ý, hai lý do:

1. Tên cột đi vào **đường dẫn Vault**. `VaultPath` chỉ nhận chữ thường, số và gạch ngang; ký tự
   khác phải escape, và escape trong đường dẫn bí mật là chỗ hai cách viết trỏ cùng một key.
2. Nó khớp ràng buộc đã có cho `database`/`table` (PHAN-HOI-DAK.md §6.A), nên một quy tắc cho cả
   ba.

Nếu bảng CDR về sau có cột như `start-time`, báo chúng tôi — khi đó bàn cách ánh xạ, chứ không nới
quy tắc rồi sửa Vault path sau.

### ⑧ Giới hạn theo ĐỘ DÀI, không chỉ số cột — nhận

Các anh/chị đúng: 100 cột × 128 ký tự vượt 8 KB.

| Giới hạn                  | Giá trị                                   |
| ------------------------- | ----------------------------------------- |
| Số cột một lời gọi        | 100                                       |
| **Tổng độ dài `columns`** | **4096 ký tự** (sau khi gộp trùng)        |
| Vượt một trong hai        | `400 invalid_column`, nói rõ vượt cái nào |

4096 để dòng request còn chỗ cho `database`, `table`, `tag`, `onMissing` và header — tổng vẫn dưới
8 KB với biên an toàn.

---

## 3. 🔴 Việc lớn phát sinh: grant xuống mức cột

Điểm ① yêu cầu: _"team được cấp `msisdn` không tự có `imei` của cùng bảng"_.

**dak chưa có điều này.** Hôm nay grant gắn với một **key**, và key gắn tối đa tới mức **bảng**.
Không có chỗ nào ghi "team X được đọc cột Y".

Việc phải làm để có nó:

1. Key xuống mức cột — **đang làm**, là phần chúng tôi đã lên kế hoạch cho `by-column`.
2. Phiếu yêu cầu chọn được **cột**, không chỉ bảng. Màn tạo phiếu, màn duyệt, và bản ghi phạm vi
   đều phải đổi.
3. Grant sinh ra theo cột, và truy vấn kiểm quyền của luồng máy tra theo `(team, database, table,
column)`.

Mục 1 nằm trong vòng này. Mục 2 và 3 là **một đợt riêng**, và chúng đổi màn hình mà người dùng dak
đang dùng hằng ngày — không gộp vào việc của `by-column` được.

**Hệ quả cho lịch**: `by-column` cho **client nội bộ** làm được ngay (không cần grant). Nhánh
**client team** phải chờ mục 2 và 3. Chúng tôi đề nghị tách làm hai bước và báo các anh/chị khi
nhánh nội bộ chạy được, thay vì chờ cả hai xong.

Nếu các anh/chị cần nhánh team sớm hơn, nói để chúng tôi xếp lại thứ tự — nhưng nó không nhanh hơn
bằng cách bỏ bớt việc.

---

## 4. Trả lời bốn câu, và hình dạng API sau khi sửa

### Câu 1 — bỏ `207`, luôn `200`: **nhận**

Lý do các anh/chị đưa thuyết phục hơn lý do chúng tôi chọn `207`: bên gọi chỉ phải đọc **một** hình
dạng, và proxy coi mọi `2xx` là thành công sẽ không nhầm "chưa có key" thành "có key".

```http
GET /api/v1/keys/by-column?database=demo_db&table=users_cdr
    &columns=msisdn,imei,device_id&tag=internal&onMissing=request
Authorization: Bearer <access_token>
```

```http
HTTP/1.1 200 OK
Cache-Control: no-store
X-Request-Id: 7f3e...

{
  "database": "demo_db",
  "table": "users_cdr",
  "complete": false,
  "columns": [
    { "column": "msisdn",    "state": "READY",    "keyPrefix": "9f2c...", "keyVersion": 3 },
    { "column": "imei",      "state": "DISABLED" },
    { "column": "device_id", "state": "PENDING",  "requestCode": "YC-2026-000002" }
  ]
}
```

| `state`    | Có `keyPrefix` | Nghĩa                                                                  |
| ---------- | -------------- | ---------------------------------------------------------------------- |
| `READY`    | có             | dùng được ngay                                                         |
| `PENDING`  | không          | chưa có key, đã mở YC (`onMissing=request`)                            |
| `MISSING`  | không          | chưa có key, **không** mở YC (`onMissing=none`, hoặc vượt giới hạn YC) |
| `REJECTED` | không          | YC bị từ chối, kèm `reason`. **Không retry**                           |
| `DISABLED` | không          | key tồn tại nhưng đang bị khoá                                         |

`complete: true` khi **mọi** cột `READY`. Một cờ để bên gọi kiểm nhanh, không phải duyệt mảng.

Mã HTTP khác `200` chỉ còn cho ca cả lời gọi hỏng: `400` sai tham số, `401` token, `403` client
team không có grant, `429` rate limit.

### Câu 2 — đường dẫn file: ghi nhận, để sau

`by-file` lùi lại cho tới khi các anh/chị dùng tới. Chúng tôi không code phần đó trong vòng này.

### Câu 3 — 100 cột: xem ⑧, giới hạn thật là độ dài

### Câu 4 — hỏi lại cùng lời gọi: **đồng ý**, và ③ đã chốt trạng thái kết thúc

---

## 5. Ba câu cần các anh/chị quyết

1. **Nhánh client team chờ được không?** (§3) Nhánh nội bộ làm được ngay; nhánh team cần grant
   xuống mức cột — một đợt riêng đổi màn phiếu và màn duyệt. Nếu luồng chạy thật của các anh/chị
   chỉ dùng client nội bộ ở giai đoạn này thì chúng tôi làm nhánh đó trước.

2. **`onMissing` mặc định `none` — đúng ý không?** Chúng tôi chọn mặc định an toàn: quên gửi tham
   số thì không âm thầm tạo YC. Nhưng nếu phần lớn lời gọi của các anh/chị là mã hoá, mặc định
   `request` sẽ tiện hơn. Các anh/chị biết tỉ lệ đó, chúng tôi không.

3. **Migration key cột bằng giá trị prefix cũ** (mục cuối §4 của các anh/chị): nạp theo lô
   **áp dụng được**, và nạp lại cùng giá trị là **idempotent** — dak từ chối ghi đè bằng giá trị
   khác, nhưng cùng giá trị thì coi như không đổi. Câu cần các anh/chị trả lời: **ai nạp?** Danh
   sách `(database, table, column, prefix cũ)` nằm bên các anh/chị; dak không suy ra được cột nào
   đã mã hoá bằng prefix nào. Gửi danh sách để chúng tôi nạp, hay các anh/chị gọi API nạp lô?

---

## 6. Bổ sung nhỏ — trả lời §4 của các anh/chị

| Mục                                                              | Trả lời                                                                                                                    |
| ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `X-Request-Id` + `Cache-Control: no-store` mọi response          | **Đã có** cho luồng máy hiện tại, áp dụng nguyên                                                                           |
| Audit ghi từng cột, `clientId`, team + grant khi là client team  | **Nhận.** Một dòng audit cho mỗi cột, không gộp một dòng cả lô — gộp là mất thông tin cột nào mở YC                        |
| Tên mã lỗi cho ca mới                                            | `invalid_tag`, `invalid_column` (gồm rỗng, trùng giới hạn, sai định dạng), `invalid_on_missing`. Ghi vào hợp đồng vòng sau |
| Key cột **không bao giờ ghi đè**, `keyVersion` là version đã đọc | **Xác nhận.** Cùng bất biến với key bảng (PHAN-HOI-DAK.md §6.B) — dak đọc đúng version đã ghim, không phải bản mới nhất    |

---

## 7. Việc tiếp theo của chúng tôi

1. Sửa hợp đồng theo §2 và §4 — gửi lại bản đầy đủ để các anh/chị đối chiếu trước khi code.
2. Trả lời ba câu §5 xong thì bắt đầu: key xuống mức cột, rồi `by-column` nhánh client nội bộ.
3. Báo các anh/chị khi nhánh nội bộ chạy được ở môi trường thử.

Grant xuống mức cột (§3) chúng tôi lên kế hoạch riêng và gửi sau — nó đổi màn hình người dùng dak
nên cần bàn trong nội bộ trước.
