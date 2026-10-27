# API lấy key — theo đường dẫn và theo cột (bản tổng hợp gửi SA)

Ngày: 2026-10-27. Gửi: đội SA. Từ: đội dak.

Tài liệu này **thay thế** mọi bản trước đã trao đổi về hai đường này (`PHAN-HOI-YC-SQL-ENGINE*`,
`PHAN-HOI-YC-DAK-VONG-2/4/6`, `YEU-CAU-TU-SA-YC.md`) — ba vòng thoả thuận đã khép, đây là một bản
duy nhất, chốt lại toàn bộ để hai bên cùng nhìn một nguồn. Các bản cũ giữ nguyên trong repo làm
lịch sử đàm phán, không sửa lại.

**Đổi tên so với mọi bản trước:** đường lấy key theo file giờ gọi là **`by-path`**, không còn
`by-file`. Lý do: dữ liệu đưa vào đường này luôn là một **đường dẫn** (người tạo key gõ vào ô
nhập ở màn quản trị dak), không phải tên file hay nội dung file — `by-path` khớp đúng cái được
truyền, `by-file` dễ hiểu lầm là khoá theo tên/nội dung file.

Mục đích chung của hai đường, theo đúng hai chiều SA dùng (xác nhận ở `TRA-LOI-DAK-BY-PATH.md`
mục 4): `by-path` để **giải mã** dữ liệu landing dak đã mã hoá; `by-column` để **mã hoá** cột
trước khi ghi bảng silver, hoặc để giải mã khi cần đọc lại.

| Loại key | Mã hoá cho | Đường SA gọi | Trạng thái |
|---|---|---|---|
| **Key theo đường dẫn** | Toàn bộ nội dung một đường dẫn (một file hoặc một thư mục) | `GET /api/v1/keys/by-path` | ⬜ chưa code — hình dạng `path` đã chốt (mục 6), sẵn sàng code |
| **Key theo cột** | Một cột cụ thể trong một bảng | `GET /api/v1/keys/by-column` | ✅ đã chạy |

---

## 1. Vì sao hai đường, không phải một

Một file mã hoá dak cấp có hai dạng khác nhau, và SA cần phân biệt vì xử lý tiếp khác nhau:

| Dạng file | Giải mã ra | SA làm tiếp |
|---|---|---|
| Mã hoá **toàn bộ** file | Dữ liệu gốc đầy đủ | Nếu cần mã hoá lại vài cột → gọi `by-column` cho các cột đó |
| Mã hoá **một số cột** trong file | File với các cột đó đã rõ, cột khác vẫn mã hoá | Gọi `by-column` cho đúng các cột còn mã hoá |

Hai loại key **lưu độc lập** ở dak: một key theo đường dẫn không bắt buộc có key cột đi kèm, và
một key cột có thể tồn tại mà không gắn đường dẫn nào. SA không suy luận được cái này từ cái kia
— gọi đúng đường cho đúng dữ liệu đang cầm.

---

## 2. `GET /api/v1/keys/by-path`

```
GET /api/v1/keys/by-path?path=<đường dẫn>&tag=internal
Authorization: Bearer <access token Keycloak>
```

| Tham số | Bắt buộc | Quy tắc |
|---|---|---|
| `path` | Có | Chuỗi đường dẫn **tương đối**, đúng y nguyên chuỗi dak lưu lúc admin tạo key — xem mục 6 (hình dạng đã chốt với SA). Cấm `/` đầu/cuối, cấm `//`, cấm `..`, cấm ký tự điều khiển, giới hạn độ dài |
| `tag` | Có (client nội bộ) | Cố định `internal` — phân biệt loại client gọi, không phải quyền |

### Trả về

**Đã có key:**

```json
{
  "path": "<đường dẫn>",
  "keyPrefix": "9f2c4b...e1",
  "keyVersion": 1
}
```

> **Đã thay đổi sau thống nhất với SA (2026-10-27, [TRA-LOI-DAK-BY-PATH.md](TRA-LOI-DAK-BY-PATH.md) mục 5):** chưa có key thì dak trả
> **`204 No Content`, không mở YC** — đường dẫn không có key nghĩa là file không mã hoá, SA đọc nguyên trạng và tác vụ thành công. Đoạn `202`
> bên dưới là bản cũ, không còn áp dụng; SA coi `202` là lỗi.

**Chưa có key** — HTTP `202` (bản cũ, xem ghi chú trên):

```json
{
  "requestCode": "YC-2026-000123",
  "state": "PENDING",
  "message": "Chưa có key cho đường dẫn này, đã gửi yêu cầu tới dak"
}
```

Gọi lại cùng `path` khi yêu cầu còn mở → nhận lại **đúng mã `requestCode`** đó, không tạo yêu cầu
thứ hai. Sau khi dak tạo key (admin xử lý yêu cầu) → gọi lại trả `200` kèm key, không cần gửi lại
`requestCode`.

SA xác nhận: `202` nghĩa là "chưa có key, tác vụ kết thúc bình thường" — SA không coi là lỗi,
tự trigger lại sau khi dak tạo key xong (`TRA-LOI-DAK-BY-PATH.md` mục 5).

### Mã lỗi

| HTTP | Khi nào |
|---|---|
| `400` | `path` sai định dạng (rỗng, `/` đầu/cuối, `//`, `..`, scheme/máy chủ (`hdfs://...`), ký tự điều khiển, quá dài) |
| `401` | Thiếu/sai token |
| `403` | Client không đúng loại được phép gọi đường này |
| `429` | Vượt rate limit |

### 2.1 Một đường dẫn là một thư mục — một key cho cả lô

SA xác nhận: một `path` có thể là một **thư mục** (một lô file), dùng **một** key để giải mã
**mọi** file trong đó. Luồng mã hoá của dak ghi file vào thư mục đó phải dùng đúng key đã cấp
cho thư mục — không cấp key riêng theo từng file trong cùng một thư mục trừ khi báo trước cho
SA để họ đổi cách gọi (`by-path` theo từng file thay vì theo thư mục).

---

## 3. `GET /api/v1/keys/by-column`

**Đã chạy**, giữ nguyên mô tả dưới đây — không đổi khi viết tài liệu này.

```
GET /api/v1/keys/by-column?database=<db>&table=<table>&columns=<c1,c2>&tag=internal&onMissing=none
Authorization: Bearer <access token Keycloak>
```

| Tham số | Bắt buộc | Quy tắc |
|---|---|---|
| `database`, `table` | Có | `[A-Za-z0-9_]{1,128}`, dak tự chuyển chữ thường |
| `columns` | Có | Danh sách cách nhau bởi dấu phẩy, tối đa 100 cột và 4096 ký tự sau khi gộp trùng. `MSISDN` và `msisdn` gộp thành một |
| `tag` | Có (client nội bộ) | Cố định `internal` |
| `onMissing` | Không | `none` (mặc định) hoặc `request`. **Mã hoá → `request`; giải mã → `none`** — xem mục 3.2 |

### Trả về — luôn `200`, một mục cho mỗi cột

```json
{
  "database": "demo_db",
  "table": "users_cdr",
  "complete": true,
  "columns": [
    { "column": "msisdn", "state": "READY", "keyPrefix": "9f2c...", "keyVersion": 1 },
    { "column": "imei", "state": "MISSING", "reason": "not_requested" }
  ]
}
```

| `state` | Có `keyPrefix` | Kèm thêm | Nghĩa |
|---|---|---|---|
| `READY` | Có | `keyVersion` | Dùng được ngay |
| `PENDING` | Không | `requestCode` | Chưa có key, đã mở yêu cầu (chỉ khi `onMissing=request`) |
| `MISSING` | Không | `reason` | Chưa có key, không mở yêu cầu — `not_requested` hoặc `yc_limit_reached` |
| `REJECTED` | Không | `requestCode` + lý do | Yêu cầu trước đó bị dak từ chối — không tự thử lại |
| `DISABLED` | Không | — | Key tồn tại nhưng đang bị khoá — cột khác trong cùng lời gọi vẫn xử lý bình thường |

`complete: true` khi mọi cột đều `READY`.

### 3.1 Mã lỗi (chỉ khi cả lời gọi hỏng)

| HTTP | Khi nào |
|---|---|
| `400` | `database`/`table`/`columns` sai định dạng, hoặc `onMissing` không phải `none`/`request` |
| `401` | Thiếu/sai token |
| `403` | Client không có quyền trên bảng này |
| `429` | Vượt rate limit |

### 3.2 `onMissing`: theo chiều SA đang gọi, không theo "mã hoá lần đầu hay không"

Xác nhận với SA (`TRA-LOI-DAK-BY-PATH.md` mục 4) — quy tắc đơn giản hơn bản trước của dak:

- **Mã hoá (ghi bảng silver) → `onMissing=request`.** Bảng silver mới thì cột chưa có key là
  chuyện bình thường, không phải lỗi cấu hình. Luồng thật của SA:
  1. Lần đầu gọi, cột chưa có key → dak mở YC, trả `PENDING`, SA dừng tác vụ.
  2. Admin dak duyệt YC, sinh key cho cột đó.
  3. SA gọi lại → cột `READY`, tác vụ mã hoá và ghi bảng tiếp tục.
- **Giải mã → `onMissing=none`.** Dữ liệu đã tồn tại và đã được mã hoá từ trước, nên thiếu key
  lúc giải mã là lỗi cấu hình thật (gõ sai tên cột, gọi nhầm bảng) — không phải ca cần xin key
  mới. Mở yêu cầu cho mỗi lần gõ sai sẽ làm hàng đợi người duyệt phình ra không cần thiết.

Bản trước của tài liệu này viết sai hướng: gắn `onMissing=none` với "chuẩn hoá" nói chung. Đúng
ra ranh giới là **mã hoá hay giải mã**, không phải "chuẩn hoá hay không" — luồng chuẩn hoá của
SA có cả hai chiều trong cùng một tác vụ (đọc landing đã mã hoá bằng `by-path` + `none`, rồi ghi
silver bằng `by-column` + `request`).

---

## 4. Yêu cầu tự mở khi chưa có key (YC)

Cả hai đường, khi chưa có key và được phép mở yêu cầu, tạo một bản ghi **YC** (khác PYC — PYC có
người duyệt trước khi cấp quyền, YC do hệ thống SA tự mở khi hỏi một key chưa tồn tại). Admin dak
xử lý YC ở màn `/sa-requests`: tạo key (sinh hoặc nhập tay) hoặc từ chối kèm lý do.

Gọi lại cùng tham số (cùng `path`, hoặc cùng cột) khi YC còn mở → nhận lại đúng mã đó, không tạo
YC thứ hai. YC từng bị từ chối thì gọi lại cũng nhận lại **đúng YC đó cùng lý do**, không tự mở
lại — tránh một client gọi liên tục để dò.

---

## 5. Việc còn lại trước khi SA gọi được `by-path`

| Việc | Ai | Trạng thái |
|---|---|---|
| Lưu key theo đường dẫn ở màn quản trị dak | dak | ✅ đã code |
| Lưu key cột ở màn quản trị dak | dak | ✅ đã code |
| `GET /api/v1/keys/by-column` | dak | ✅ đã code, đang chạy |
| `GET /api/v1/keys/by-path` | dak | ⬜ **chưa code** — thiết kế đủ, hình dạng `path` đã chốt (mục 6), không còn gì chặn |
| YC khi chưa có key theo đường dẫn | dak | ⬜ phần sau của `by-path` |
| Ô nhập path ở màn tạo key từ chối sai dạng ngay lúc nhập | dak | ⬜ cần sửa — xem mục 6 |

Không còn gì chặn phía dak (lưu trữ, hạ tầng đã có sẵn từ `by-column`) — code được ngay.

---

## 6. Hình dạng `path` — đã chốt với SA

Trả lời đủ ở `TRA-LOI-DAK-BY-PATH.md`:

| Câu hỏi | SA trả lời |
|---|---|
| Tuyệt đối hay tương đối? | **Tương đối theo một gốc** của nguồn dữ liệu (SA quản lý gốc, không gửi cho dak) |
| Ai chuẩn hoá? | **SA tự chuẩn hoá** trước khi gửi — dak chỉ lưu và so khớp nguyên chuỗi |
| Hoa thường | **Giữ nguyên** — `VTNET_cdr` và `vtnet_cdr` là hai đường dẫn khác nhau |

**Dạng chuẩn SA luôn gửi:** không `/` ở đầu, không `/` ở cuối, không `//` liên tiếp, không scheme
hay máy chủ (`hdfs://...`), không `..`, không ký tự điều khiển.

| Đường dẫn trên HDFS (SA có) | Gốc của nguồn | `path` SA gửi dak |
|---|---|---|
| `hdfs://<namenode>:8020/landing/vtnet/vtnet_cdr/20261027` | `/landing/vtnet` | `vtnet_cdr/20261027` |
| `hdfs://<namenode>:8020/landing/vtnet/vtnet_cdr/20261027/` | `/landing/vtnet` | `vtnet_cdr/20261027` |
| `/landing/vtnet//vtnet_cdr/20261027/part-00001.csv` | `/landing/vtnet` | `vtnet_cdr/20261027/part-00001.csv` |

**Việc dak phải làm theo yêu cầu của SA:** ô nhập `path` ở màn tạo key **phải từ chối** chuỗi có
`/` đầu/cuối, `//`, hoặc `://` — để sai sót lộ ra ngay lúc tạo key, không phải lúc SA gọi và tra
không ra. dak lưu **nguyên chuỗi** SA gửi làm định danh tra cứu — không tự chuẩn hoá thêm, vì SA
đã chuẩn hoá trước khi gửi và hai bên đồng ý giữ nguyên hoa/thường.
