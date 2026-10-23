# Hai luồng mới: key theo file và key theo cột (YC)

Ngày: 2026-10-21. Gửi: đội sql-engine / SparkApplication.

Tài liệu này chốt hợp đồng cho **hai đường API mới** mà các anh/chị đã mô tả, và nói rõ dak làm gì
khi chưa có key — phần đó là cơ chế mới, chưa có trong [PHAN-HOI-DAK.md](PHAN-HOI-DAK.md) §6.

Đọc xong mục này là đủ để hai bên viết code và viết test độc lập.

---

## 1. Tóm tắt một trang

|                                          |                                                                                  |
| ---------------------------------------- | -------------------------------------------------------------------------------- |
| **Hai đường mới**                        | `GET /api/v1/keys/by-file` · `GET /api/v1/keys/by-column`                        |
| **Cơ chế mới**                           | Chưa có key → dak **mở một YC** và trả mã; không trả 404                         |
| **Mã trả về**                            | `200` đủ key · `207` một phần (chỉ `by-column`) · `202` chưa có key nào          |
| **Đã chốt theo trả lời của các anh/chị** | `tag=internal` bắt buộc · mỗi cột một key · luồng YC chưa giới hạn client        |
| **Luồng cũ không đổi**                   | `GET /api/v1/keys/{database}/{table}` giữ nguyên hợp đồng §6 của PHAN-HOI-DAK.md |
| **Cần các anh/chị xác nhận**             | 4 câu ở §7                                                                       |

**Trạng thái phía dak**: đã lên kế hoạch chi tiết, **chưa code**. Gửi tài liệu này trước để nếu có
chỗ chúng tôi hiểu sai thì sửa trước khi viết, không phải sau.

---

## 2. Ba luồng, ba câu hỏi khác nhau

Sau khi thêm hai đường mới, dak có ba đường lấy key. Chúng **không thay thế nhau**:

| Đường                         | Câu hỏi nó trả lời                               | Quyền dựa trên            |
| ----------------------------- | ------------------------------------------------ | ------------------------- |
| `/{database}/{table}` (đã có) | "team này có được đọc bảng kia không?"           | phiếu đã duyệt của team   |
| `by-file` (mới)               | "key giải mã file ở đường dẫn này tồn tại chưa?" | chỉ token hợp lệ — xem §6 |
| `by-column` (mới)             | "key của cột này tồn tại chưa?"                  | chỉ token hợp lệ — xem §6 |

Đường đầu quyết định **cấp quyền**. Hai đường sau hỏi **key tồn tại chưa** — và nếu chưa, ghi lại
để người có quyền bên dak vào tạo.

Khi các anh/chị muốn khử định danh rồi khai thác dữ liệu, vẫn đi đường đầu như hiện nay.

---

## 3. YC — yêu cầu do chính lời gọi của các anh/chị sinh ra

Khi lời gọi tìm không thấy key, dak **không trả 404**. Thay vào đó dak mở một bản ghi gọi là **YC**
và trả lại mã của nó.

**Vì sao không 404:** `404` nói "không có và sẽ không bao giờ có" — bên gọi dừng lại. Thực tế là
"chưa có, đã ghi nhận, đang chờ người tạo" — bên gọi nên thử lại sau. Hai thứ dẫn tới hai hành vi
khác nhau ở phía các anh/chị, nên chúng tôi không dùng chung một mã.

**YC khác phiếu yêu cầu (PYC)** mà các anh/chị đã biết:

|                        | PYC                      | YC                                      |
| ---------------------- | ------------------------ | --------------------------------------- |
| Ai tạo                 | người, qua giao diện dak | **lời gọi API của các anh/chị**         |
| Có người duyệt không   | có                       | **không** — không duyệt gì cả           |
| Kết thúc bằng          | grant + key theo phiếu   | người có quyền bên dak tạo key, YC đóng |
| Các anh/chị cần làm gì | nộp phiếu, chờ duyệt     | **không gì cả** — chỉ gọi lại sau       |

**Gọi lại cùng tham số trả CÙNG mã YC.** dak không mở YC thứ hai cho cùng một thứ. Nhờ vậy các
anh/chị thử lại bao nhiêu lần cũng được mà không tạo ra hàng trăm bản ghi trùng.

---

## 4. `GET /api/v1/keys/by-file`

### 4.1 Request

```http
GET /api/v1/keys/by-file?path=<đường dẫn file>&tag=internal
Host: dak.<domain>
Authorization: Bearer <access_token của client>
Accept: application/json
X-Request-Id: 7f3e...          (tuỳ chọn)
```

| Mục            | Chốt                                                                                                                        |
| -------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `path`         | Query parameter, **không** phải path segment — đường dẫn chứa `/` nên nhét vào path là bài toán escape không cần thiết      |
|                | dak kiểm: không rỗng, không chứa `..`, không ký tự điều khiển, tối đa 1024 ký tự                                            |
|                | dak **không** chuẩn hoá chữ thường (khác `database`/`table`): đường dẫn file phân biệt hoa thường trên hầu hết hệ thống tệp |
| `tag`          | **Bắt buộc**, giá trị `internal`. Thiếu hoặc khác → `400`                                                                   |
| Sai định dạng  | `400 invalid_path`, dak **không gọi Keycloak** (kiểm trước khi xác thực)                                                    |
| `X-Request-Id` | Không gửi thì dak sinh. **Luôn** trả lại, cả khi lỗi                                                                        |
| Thân request   | Không có                                                                                                                    |

### 4.2 Response — có key

```http
HTTP/1.1 200 OK
Content-Type: application/json
Cache-Control: no-store
X-Request-Id: 7f3e...

{
  "filePath": "/data/landing/2026-10-21/cdr.enc",
  "keyPrefix": "9f2c4b...e1",
  "keyVersion": 3
}
```

Hình dạng theo đúng tinh thần §6.B của PHAN-HOI-DAK.md: dak **chỉ thêm** field về sau, không đổi
tên và không bỏ field cũ.

### 4.3 Response — chưa có key

```http
HTTP/1.1 202 Accepted
Content-Type: application/json
X-Request-Id: 7f3e...

{
  "requestCode": "YC-2026-000001",
  "state": "OPEN",
  "message": "Chưa có key cho đường dẫn này. Đã ghi nhận yêu cầu YC-2026-000001."
}
```

Một đường dẫn một key, nên `by-file` không có ca "một phần".

---

## 5. `GET /api/v1/keys/by-column`

### 5.1 Request

```http
GET /api/v1/keys/by-column?database=demo_db&table=users_cdr&columns=msisdn,imei&tag=internal
Authorization: Bearer <access_token của client>
```

| Mục                 | Chốt                                                                                   |
| ------------------- | -------------------------------------------------------------------------------------- |
| `database`, `table` | `[A-Za-z0-9_]{1,128}`, dak **chuẩn hoá chữ thường** — như §6.A                         |
| `columns`           | Danh sách ngăn bằng dấu phẩy. Mỗi tên theo cùng quy tắc, cũng **chuẩn hoá chữ thường** |
|                     | Tối đa 100 cột một lời gọi                                                             |
| `tag`               | **Bắt buộc**, giá trị `internal`                                                       |
| Sai định dạng       | `400 invalid_column`                                                                   |

> Chuẩn hoá chữ thường là bắt buộc chứ không tiện tay: Hive không phân biệt hoa thường, nên
> `MSISDN` và `msisdn` là **cùng một cột**. Không chuẩn hoá thì chúng thành hai key khác nhau, và
> dữ liệu mã hoá bằng key này không giải được bằng key kia.

### 5.2 Mỗi cột một key

Theo trả lời của các anh/chị: **mỗi cột có key riêng**. dak trả một mục cho mỗi cột.

### 5.3 Response — có key của MỌI cột được hỏi

```http
HTTP/1.1 200 OK

{
  "database": "demo_db",
  "table": "users_cdr",
  "columns": [
    { "column": "msisdn", "keyPrefix": "9f2c...", "keyVersion": 3 },
    { "column": "imei",   "keyPrefix": "4a71...", "keyVersion": 1 }
  ]
}
```

### 5.4 Response — có key MỘT PHẦN

Đây là ca chúng tôi nghĩ kỹ nhất, vì ba cách xử đều khác nhau về hậu quả:

| Cách                                     | Vì sao không / có                                              |
| ---------------------------------------- | -------------------------------------------------------------- |
| Trả key của 2 cột, im lặng bỏ cột thứ ba | Các anh/chị tưởng đã đủ, mã hoá thiếu một cột mà không ai biết |
| Từ chối cả lô, trả `202`                 | Phí hai key đã có, và lần gọi sau vẫn vậy                      |
| **Trả 2 key + mở YC cho cột thiếu**      | Dùng được phần có, biết rõ phần nào phải chờ                   |

Chọn cách thứ ba:

```http
HTTP/1.1 207 Multi-Status

{
  "database": "demo_db",
  "table": "users_cdr",
  "columns": [
    { "column": "msisdn", "keyPrefix": "9f2c...", "keyVersion": 3 },
    { "column": "imei",   "keyPrefix": "4a71...", "keyVersion": 1 }
  ],
  "pending": [
    { "column": "device_id", "requestCode": "YC-2026-000002", "state": "OPEN" }
  ]
}
```

**Mỗi cột thiếu key là một YC riêng.** Hỏi ba cột thiếu key thì nhận ba mã YC khác nhau — người có
quyền bên dak tạo xong key nào thì YC đó đóng, độc lập với hai YC kia. Các anh/chị gọi lại lần sau
có thể nhận `207` với ít `pending` hơn.

### 5.5 Response — không cột nào có key

`202` với một mã YC cho mỗi cột:

```json
{
  "pending": [
    { "column": "msisdn", "requestCode": "YC-2026-000003", "state": "OPEN" },
    { "column": "imei", "requestCode": "YC-2026-000004", "state": "OPEN" }
  ]
}
```

---

## 6. 🔴 Hai đường mới KHÔNG có cổng quyền như luồng theo bảng

Nói thẳng để các anh/chị biết đang dựa vào gì.

Luồng `/{database}/{table}` đi qua **bốn cổng**: client id khớp team đang hoạt động → team có phiếu
đã duyệt trên bảng đó → quyền còn hạn → key đang hoạt động. Thiếu cổng nào cũng là 403.

Hai đường mới **không có cổng nào trong bốn cổng đó**. Điều kiện duy nhất là token hợp lệ của realm
đã khai. Nghĩa là: mọi client trong realm tra được key của mọi đường dẫn file và mọi cột.

**Chấp nhận được vì** luồng YC là nội bộ giữa hai đội, và realm hiện chỉ có client của các anh/chị.
Theo trả lời của các anh/chị: _"luồng YC nội bộ nên tạm để mở, sau này sẽ giới hạn chỉ 1 client"_.

**Chúng tôi ghi đây là nợ có thời hạn.** Nó hết chấp nhận được ngay khi một trong hai điều xảy ra:

- realm `vlp-tenantw1xjixm` có thêm client thứ hai
- realm đó mở cho một hệ thống khác ngoài SA

Lúc đó dak thêm danh sách client được phép; client ngoài danh sách nhận `403`. **Việc cần từ phía
các anh/chị khi đó:** cho chúng tôi biết client id nào được giữ quyền — dak không tự biết realm có
thêm ai.

---

## 7. Bốn câu cần các anh/chị xác nhận

1. **Mã `207` có xử được không?** Nếu thư viện HTTP phía các anh/chị coi mọi mã ngoài `2xx` thông
   thường là lỗi, nói sớm — chúng tôi đổi sang `200` kèm cờ trong thân.

2. **Đường dẫn file phân biệt hoa thường — đúng không?** Chúng tôi giả định có (§4.1). Nếu hệ thống
   tệp của các anh/chị không phân biệt, dak phải chuẩn hoá, nếu không hai đường dẫn khác hoa thường
   sẽ thành hai key.

3. **Tối đa 100 cột một lời gọi — đủ chưa?** Con số này chúng tôi tự đặt để chặn lời gọi bất
   thường. Bảng thật của các anh/chị có thể nhiều cột mã hoá hơn.

4. **Các anh/chị muốn biết YC đã xong chưa bằng cách nào?** Hai lối:
   - **Hỏi lại** cùng lời gọi cũ — đơn giản nhất, dak đã hỗ trợ sẵn
   - **Một endpoint tra trạng thái YC** theo mã — dak làm thêm nếu cần

   Chúng tôi nghiêng về lối đầu: nó không thêm đường API nào, và lời gọi hỏi lại cũng chính là lời
   gọi lấy key. Nhưng nếu các anh/chị cần biết trạng thái mà chưa muốn lấy key, nói để chúng tôi
   làm thêm.

---

## 8. Việc tiếp theo của chúng tôi

| #   | Việc                                                                                               | Ghi chú                     |
| --- | -------------------------------------------------------------------------------------------------- | --------------------------- |
| 1   | Mở rộng dữ liệu key xuống mức cột và mức đường dẫn file                                            | không đụng luồng đang chạy  |
| 2   | Bảng YC + sinh mã                                                                                  |                             |
| 3   | `GET by-file`                                                                                      | làm trước vì đơn giản hơn   |
| 4   | Giao diện nhập key cho người có quyền — một màn, hai khối riêng (key theo đường dẫn, key theo cột) | để YC đóng được             |
| 5   | `GET by-column`                                                                                    |                             |
| 6   | Giao diện quản lý YC                                                                               | danh sách, tạo key, từ chối |

Chúng tôi làm theo thứ tự này và báo khi `by-file` chạy được ở môi trường thử — lúc đó hai bên gọi
thử được đầu-cuối trước khi làm nốt phần cột.

**Chưa bắt đầu code** cho tới khi các anh/chị xem qua §7. Bốn câu đó không chặn phần lớn công việc,
nhưng câu 1 và 2 đổi hình dạng API, nên biết sớm thì rẻ hơn.
