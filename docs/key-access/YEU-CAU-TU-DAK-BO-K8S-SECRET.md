# Đổi cách giao key khử định danh: bỏ K8s Secret, SA GET key của dak

Ngày: 2026-11-09. Gửi: đội SA / sql-engine. Từ: đội dak.

---

## 0. Một đoạn tóm tắt

**K8s Secret là cách giao key sai, hai bên bỏ.** Thay bằng: **SparkApplication tự gọi API của
dak để lấy key**, rồi dùng key đó mã hoá/khử định danh như hôm nay.

- Bỏ: dak tạo Secret `deid-key-<id>` trong namespace của SA, DAG mount vào `/etc/pseudonym-keys`
- Thay bằng: job gọi `GET /api/v1/keys/deid` kèm token của team → nhận `keyPrefix`
- `conf` của dag run **không còn khoá `keySecretName`**
- dak **gỡ Role/RoleBinding** (`dak-deid-secret`) khỏi namespace của SA — dak hết quyền ghi vào ns các bạn

API đã code và chạy được phía dak. **Chưa đổi gì trên đường đang chạy** — chờ các bạn xác nhận
rồi hai bên đổi cùng nhịp (mục 5).

---

## 1. Vì sao bỏ

| Lý do | Cụ thể |
|---|---|
| dak phải có quyền ghi vào namespace của SA | Role `create`/`delete` secrets trong `vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy`. Quyền này tồn tại chỉ để chuyển tay một giá trị |
| Key rơi xuống cluster | Secret nằm trong etcd từ lúc trigger tới lúc dọn, dạng base64 — không phải mã hoá |
| Đã rò thật | Một bug ở rào kiểm tên lúc xoá làm **mọi lượt chạy để lại một Secret chứa key thật** trong ns của các bạn. dak đã sửa, nhưng cơ chế mà một lỗi regex làm rò key thì cơ chế đó sai |
| Một định danh thứ ba phải đồng bộ | Tên Secret phải khớp hai bên. Đã lệch một lần: `dag_run_id` có hậu tố `-r2` mà `keySecretName` thì không |
| Hai bên quản một namespace | Ai dọn "tài nguyên lạ" trong ns đó là làm hỏng phiếu đang chạy |

Sau khi đổi: key chỉ tồn tại ở **Vault của dak** và **bộ nhớ của job**. Không có bản sao nào
trong cluster.

---

## 2. API lấy key — đã code, gọi được

```
GET /api/v1/keys/deid?database=<db>&table=<table>[&requestCode=<mã phiếu>]
Authorization: Bearer <access token Keycloak của team>
```

| Tham số | Bắt buộc | Quy tắc |
|---|---|---|
| `database` | Có | Chỉ chữ, số, gạch dưới; ≤128 ký tự. Không phân biệt hoa thường (dak hạ thường) |
| `table` | Có | Như trên |
| `requestCode` | Không | Dạng `REQ-<năm>-<6 số>` (hoặc `PYC-` với phiếu kiểu cũ). Chỉ cần khi team có **nhiều phiếu** khai thác cùng bảng — xem mục 2.2 |

### 2.1 Trả về

```json
{
  "requestCode": "REQ-2026-000011",
  "database": "demo_db",
  "table": "users_cdr_2",
  "keyPrefix": "9f2c4b...e1",
  "keyVersion": 3
}
```

| Trường | Ý nghĩa |
|---|---|
| `requestCode` | Mã phiếu dak đã chọn — **đối chiếu với `conf.requestCode`** job đang giữ. Lệch nhau là dừng job, đừng mã hoá |
| `keyPrefix` | Giá trị key: **base64 chuẩn của đúng 32 byte** (44 ký tự, kết thúc `=`) — y nguyên cái trước đây nằm trong Secret sau tiền tố `base64:` |
| `keyVersion` | Version key ở Vault. Ghi vào log/metadata của job để sau truy được lượt chạy dùng key version nào |

**Thay thế trực tiếp nội dung Secret cũ**: file `pseudonym.hmac_v1` chứa `base64:<keyPrefix>`.
Nay các bạn nhận thẳng `keyPrefix`. Nếu code job đang đọc chuỗi có tiền tố `base64:` thì tự ghép
hoặc bỏ bước ghép — tuỳ các bạn, dak không đổi định dạng giá trị.

### 2.2 Mã lỗi

| Mã | Khi nào | Job phải làm gì |
|---|---|---|
| `200` | Có đúng một phiếu khai thác APPROVED khớp bảng | Mã hoá |
| `401` | Thiếu/sai token | Dừng. Lỗi cấu hình, không retry vô hạn |
| `403` | Team không có phiếu khai thác APPROVED cho bảng này, **hoặc phiếu đã hết hạn / quyền bị thu hồi** (mục 4), hoặc key chưa tạo / đã bị khoá | **Dừng job, báo failed.** Không có đường vòng |
| `409` | Team có **nhiều** phiếu APPROVED cùng bảng | Gọi lại kèm `requestCode` lấy từ `conf` của chính dag run |
| `400` | `database`/`table`/`requestCode` sai định dạng | Dừng. Lỗi cấu hình |

Thân `409` theo đúng ba field lỗi chung của API máy dak (`error`, `message`, `requestId`) — danh
sách mã nằm **trong `message`**, không phải một mảng riêng:

```json
{
  "error": "ambiguous_request",
  "message": "Nhiều phiếu khai thác cho bảng này: REQ-2026-000011, REQ-2026-000014 — gửi lại kèm requestCode",
  "requestId": "…"
}
```

Nếu job cần đọc máy danh sách này (thay vì để người đọc), nói sớm — dak thêm được một field
`requestCodes` dạng mảng, **thêm field chứ không đổi ba field cũ**. Nhưng cách gọn hơn vẫn là
gửi kèm `requestCode` ngay từ đầu, khỏi gặp 409.

dak **cố ý không tự chọn** khi mơ hồ: hai phiếu cùng bảng là **hai key khác nhau** (key của dak
đi theo phiếu, không theo bảng). Tự chọn "mới nhất" là tự cấp quyền mà người duyệt chưa quyết.

> Vì `conf` luôn có `requestCode`, cách đơn giản nhất phía các bạn: **gửi kèm `requestCode`
> ngay từ lần gọi đầu**, khỏi gặp 409 bao giờ.

---

## 3. Token — dùng token của team

dak chốt **phương án token của team** (câu hỏi 1 của bản trao đổi trước):

- Mỗi team trong dak đã có một client Keycloak do dak tự tạo
- Job chạy cho phiếu của team nào thì dùng token của team đó
- dak lấy team **từ token**, không nhận tham số `team` trong request — nguyên tắc chung của mọi
  API máy của dak: *"tenant luôn từ token, không tin gì trong request"*

**Việc cần các bạn cho biết**: job lấy token của team bằng đường nào ở môi trường của các bạn
(Secret sẵn có trong ns, vault-agent, hay dak phải gửi gì xuống `conf`). Nếu phải gửi qua `conf`
thì nói sớm — dak **không** gửi secret qua `conf` (Airflow lưu `conf` vào DB và hiện lên UI), nên
phương án đó cần một đường riêng và phải bàn trước.

---

## 4. Thời điểm GET — job tự gọi khi chạy, và hệ quả về hạn phiếu

dak chốt: **job tự GET lúc nó chạy**, không phải dak GET lúc trigger rồi nhét vào `conf`. Lý do:
nhét vào `conf` là lặp lại đúng cái sai của Secret — giá trị key (hoặc định danh để lấy key) nằm
trong chỗ Airflow lưu DB và hiện UI.

Kéo theo một quy tắc hai bên phải chốt: **"job chỉ chạy trong hạn phiếu"**.

> ✅ **Rào này dak đã code xong 2026-11-09**, kèm test. Lúc viết bản nháp đầu thì API còn chưa
> kiểm hạn (chỉ kiểm team → phiếu `APPROVED` → key `ACTIVE`), nên một phiếu hết hạn vẫn lấy được
> key. Đã vá, các bạn không phải làm gì cho phần này.

dak đo hạn bằng **grant của phiếu** — hết hạn, hoặc quyền đã bị thu hồi, hoặc phiếu không còn
grant nào thì **403**, cùng mã với "không có phiếu" (403 ra ngoài cố ý giống nhau cho mọi nguyên
nhân; dak tra nguyên nhân thật trong audit theo `requestId`).

Hành vi job cần đúng như sau:

- Thời điểm kiểm hạn là **lúc GET**, không phải lúc trigger
- Job trigger khi phiếu còn hạn nhưng tới lúc gọi GET thì phiếu đã hết hạn → **403**
- Job phải **dừng và báo failed**. Không cache key từ lượt trước, không dùng key cũ
- Người vận hành dak gia hạn phiếu rồi bấm "Chạy lại" — dak đã có nút đó

Câu hỏi kèm cho các bạn ở mục 7 điểm 3: job có chỗ nào cache key giữa các lượt chạy không? Có
thì rào hạn phía dak vô nghĩa.

---

## 5. Nhịp đổi — ai làm gì trước

**Không bên nào đổi một mình được.** dak bỏ tạo Secret trước khi DAG của các bạn GET key thì mọi
phiếu deid chết ở bước giao key (job chờ Secret không bao giờ tới).

| Thứ tự | Ai | Việc |
|---|---|---|
| 1 | dak | Gửi doc này (xong). API `GET /api/v1/keys/deid` đã code, gọi thử được ngay |
| 2 | **SA** | Thử gọi API trên môi trường của các bạn, xác nhận nhận được `keyPrefix` đúng |
| 3 | **SA** | Sửa DAG/job: GET key thay vì mount Secret. **Giai đoạn này chạy song song được** — dak vẫn đang tạo Secret, job cứ GET, Secret thừa thì bỏ |
| 4 | **SA** | **Xác nhận bằng văn bản**: DAG không còn đọc `conf.keySecretName` |
| 5 | dak | Bỏ tạo Secret + bỏ `keySecretName` khỏi `conf` + gỡ Role/RoleBinding khỏi ns của các bạn |
| 6 | Ops hai bên | Xoá các Secret `deid-key-*` còn sót trong ns (dak làm trước khi gỡ RBAC, vì gỡ rồi là hết quyền xoá) |

Bước 3 **không phá gì** — đó là điểm quan trọng: các bạn đổi trước, chạy thử thoải mái, dak chỉ
gỡ sau khi các bạn xác nhận.

Các bạn cũng gỡ được Role `dak-mock-deid` của mock sau bước 5 (`dak-mock-k8s-secret.yaml`) — nó
tồn tại chỉ để mock giả lập quyền tạo Secret của dak.

---

## 6. Phần hợp đồng cũ thành lịch sử

Sau bước 5, các mục sau trong `API_CONTRACT.md` **không còn hiệu lực**:

| Mục | Nội dung | Thay bằng |
|---|---|---|
| §6 mục 3 | "Giao key cho job bằng tên Secret" — tên, nội dung, nhãn Secret | Mục 2 doc này |
| §6 mục 4 | dak dọn Secret khi job kết thúc | Không còn Secret để dọn |
| §7.3 bảng `conf` | Khoá `keySecretName` | Bỏ khoá này khỏi `conf` |
| §7.4 gạch đầu dòng 1–2 | "tạo SparkApplication cùng namespace với Secret key", "mount Secret vào `/etc/pseudonym-keys`" | Job GET key; không còn ràng buộc namespace nào giữa job và dak |

**Không đổi**, giữ nguyên hết:

- `GET /api/v1/keys/by-path`, `GET /api/v1/keys/by-column`, `GET /api/v1/keys/{database}/{table}`
  — ba đường này phục vụ việc khác (giải mã file/cột thường, mã hoá silver), doc này không đụng
- Cách trigger DAG, `dag_run_id`, hậu tố `-rN` cho lượt chạy thứ N
- `conf.env` và allowlist env (`OUTPUT_DB`, `OUTPUT_TABLE`, `INPUT_FROM/TO/HOURS`…)
- `pseudoColumns`, chỉ `HASH`, và quy tắc "DAG không tin `conf`"
- Cách dak theo dõi trạng thái job

---

## 7. Cần các bạn trả lời

1. **Token của team**: job lấy token bằng đường nào ở môi trường của các bạn (mục 3)? Nếu cần dak
   hỗ trợ gì để job có token thì nói rõ — nhưng không qua `conf`
2. **`keyPrefix` đủ chưa**: ngoài `keyPrefix` + `keyVersion`, job cần thêm field nào không (mục
   2.1)? dak chỉ **thêm** field, không đổi field cũ
3. **Hạn phiếu** (mục 4): job có cache key giữa các lượt chạy không? Và xác nhận job dừng khi
   nhận 403 — đúng mong đợi của các bạn
4. **Mốc thời gian** cho bước 3–4 (mục 5), để dak xếp lịch bước 5 cùng nhịp

Chi tiết việc code phía dak: `docs/39-BO-K8S-SECRET-SA-GET-KEY.md` trong repo dak.
