# Phản hồi của đội dak về `DAK_API_SPEC.md` và `DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md`

Ngày: 2026-10-06. Gửi: đội sql-engine / SparkApplication.

Cảm ơn hai tài liệu — mức chi tiết đủ để chúng tôi đối chiếu thẳng với thiết kế hiện có mà không
phải đoán chỗ nào.

Tài liệu này trả lời: **chúng tôi đồng ý những gì, khác chỗ nào, và cần các anh/chị quyết gì**.

**Trạng thái phía dak**: luồng máy (team → client Keycloak → token → lấy key) **đã code xong và
chạy thông ở môi trường local**, gồm cả Keycloak thật, Vault thật, cơ sở dữ liệu thật. Nghĩa là
phần lớn những gì spec mô tả đã có nền; phần còn lại là chỉnh cho khớp hợp đồng của các anh/chị.

---

## 1. Tóm tắt một trang

| | |
|---|---|
| **Khớp sẵn** | Client Keycloak `client_credentials` cho mỗi team; dak map token → team; phạm vi đóng trong workspace; không đọc chéo tenant/workspace; key ở Vault KV v2; quyền theo từng bảng; khoá client/khoá key có hiệu lực ngay; audit không chứa prefix |
| **Khác, cần chốt** | ① Đơn vị cấp key và **thời hạn quyền** ② Nhiều realm + introspection ③ Kiểm `aud` |
| **Khác, sửa được nhanh** | ④ Khoá mapping phải gồm `realm` ⑤ Hình dạng API và `database/table` thay cho `datasetUrn` ⑥ `keyVersion` |
| **Hợp đồng API** | §6 — chốt đủ để hai bên code và viết test độc lập |
| **dak chưa có** | Q2 (nạp key không ghi đè), Q3 (đối soát hash), Q4 (nạp hàng loạt), Q7 (rotate secret) |
| **Cần quyết trước khi code** | 6 câu ở §8 |

---

## 2. Những gì chúng tôi đồng ý, không bàn thêm

Để phần sau không bị đọc thành "hai bên khác nhau nhiều":

- Mỗi team một client Keycloak confidential, `client_credentials`, 1 engine/job = 1 team.
- dak xác định tenant/workspace/team **từ token**, API **không có** tham số chọn workspace.
- Không đọc chéo workspace hay tenant — theo thiết kế, không phải hạn chế tạm thời.
- `iss` **chỉ** dùng để tra registry, **không bao giờ** dựng URL từ nó. dak đang làm đúng điều này.
- Quyền "lấy key" là một quyền duy nhất, không tách đọc/ghi (xem thêm §5.2).
- `Cache-Control: no-store`, `X-Request-Id`, audit mỗi lần gọi, không log token/secret/prefix.
- Rate limit theo client.
- Client của team: Standard flow / Implicit / Direct access OFF, Service accounts ON.

---

## 3. Ba điểm cần quyết

### 3.1 🔴 Thời hạn của quyền — điểm quan trọng nhất

**Spec chưa nói tới việc quyền hết hạn.** Bảng `KEY_GRANT(key_id, team)` không có cột thời gian,
và trong cả hai tài liệu, "mất quyền" chỉ xảy ra khi ai đó chủ động thu hồi hoặc khoá.

Phía dak thì ngược lại: **quyền sinh ra từ một phiếu yêu cầu được duyệt, và phiếu có thời hạn**.
Đó là lý do hệ thống này tồn tại — mỗi lần cấp quyền có người yêu cầu, người duyệt, mục đích và
ngày hết hạn, để sau này trả lời được "ai cho phép, vì sao, đến bao giờ".

Hệ quả nếu ghép hai mô hình mà không chốt gì: **engine đang chạy bình thường sẽ bắt đầu nhận 403
khi phiếu hết hạn**, không cần ai làm gì cả. Với một job chạy đêm, đó là một lần hỏng không ai
hiểu vì sao.

Ba lựa chọn, chúng tôi cần các anh/chị chọn:

| | Cách làm | Đổi lại |
|---|---|---|
| **A** | Phiếu cho đường máy **không có thời hạn**, chỉ mất quyền khi bị thu hồi | Đúng như spec hiện tại. Mất tính "quyền tự hết hạn" — một quyền cấp năm ngoái vẫn còn hiệu lực hôm nay |
| **B** | Phiếu có thời hạn dài (vd 1 năm), dak **cảnh báo trước khi hết hạn** | Giữ được tính kiểm soát, nhưng ai đó phải gia hạn, và quên là engine dừng |
| **C** | Phiếu có thời hạn, và dak **tự động gia hạn** khi còn được dùng | Không ai phải nhớ, nhưng gần như quay lại (A) |

Chúng tôi nghiêng về **B**, với cảnh báo gửi trước 30 ngày và một màn hình liệt kê các quyền sắp
hết hạn. Nhưng đây là quyết định ảnh hưởng trực tiếp tới vận hành engine của các anh/chị, nên
chúng tôi không tự chọn.

### 3.2 Đơn vị cấp key: một bảng một key

Spec định danh key bằng `(tenant, workspace, database, table)` — **một bảng một key**, và
`db1.customers` với `db2.customers` phải ra hai prefix khác nhau (kịch bản 4, mục 9.5).

dak hiện định danh key bằng tên, và **một key có thể được duyệt cho nhiều bảng cùng lúc** —
phạm vi nằm ở phiếu, không nằm ở key. Nghĩa là mô hình hiện tại **không bảo đảm** hai bảng ra
hai prefix khác nhau.

**Chúng tôi sẽ đổi theo spec**: thêm `database`, `table` vào key, ràng buộc duy nhất trên
`(tenant, workspace, database, table)`. Phía các anh/chị không phải làm gì.

Phần chúng tôi giữ lại: **đường cấp quyền vẫn đi qua phiếu được duyệt**. Với API lấy key thì
không khác gì — vẫn là "có grant hay không". Chỉ khác là phía sau có hồ sơ ai duyệt.

### 3.3 Nhiều realm, và introspection

Chúng tôi **đồng ý mỗi tenant một realm** và sẽ làm registry realm đúng như mục 2.2 của spec.

Phần muốn trao đổi lại là **introspection**. dak hiện xác minh token bằng **chữ ký, với JWKS của
realm** (verify tại chỗ, không gọi Keycloak mỗi request). Spec yêu cầu introspection.

Con số từ chính spec và plan:

| | |
|---|---|
| Token lifespan | **300 giây** (spec mục 2.3) |
| Client cache prefix | **300 giây** (plan mục 5.3) |
| TTL cache introspect | 30 giây (spec mục 4.5) |

Lý do chọn introspection là thu hồi nhanh. Nhưng vì client cache prefix 300 giây, **thu hồi quyền
vẫn mất tới 5 phút mới có tác dụng thật** — đúng như kịch bản 10 và 11 ở mục 9.5 đã ghi
("sau tối đa `cacheTtlSeconds` thì query lỗi"). Lợi ích của việc introspect nhanh hơn 30 giây
bị chính tầng cache của client làm mờ đi.

Đổi lại, introspection mang hai cái giá:

- **Keycloak sập thì mọi engine/job dừng.** Với JWKS, dak đã cache khoá nên vẫn phục vụ được.
- dak phải giữ **secret của `dak-introspector` ở mỗi realm** — thêm một bí mật phải quản lý,
  nhân với số tenant.

**Đề xuất**: dùng JWKS làm mặc định, **vẫn giữ nguyên mọi kiểm tra khác** mà spec yêu cầu
(`iss` khớp registry, `exp`, `azp`, và tra `(realm, client_id)` trong cơ sở dữ liệu **ở mỗi
request, không cache** — nên khoá client vẫn có hiệu lực ngay như mục 4.4 mô tả).

Nếu đội bảo mật yêu cầu bắt buộc introspection, chúng tôi làm — chỉ cần biết để chuẩn bị
credential cho từng realm và chấp nhận phụ thuộc Keycloak.

---

## 4. Ba điểm chúng tôi sửa theo spec

Không cần các anh/chị quyết, ghi ra để biết chúng tôi hiểu đúng.

### 4.1 Khoá mapping gồm `realm`

Spec đúng: `client_id` chỉ duy nhất **trong một realm**. dak hiện ràng buộc duy nhất trên mỗi
`client_id` — với nhiều realm thì đây là lỗ hổng cấp quyền thật (đúng kịch bản 7 mục 9.5).
Chúng tôi thêm cột realm và đổi ràng buộc sang `(realm, client_id)`.

### 4.2 API `GET /api/v1/keys/{database}/{table}`

dak hiện có một endpoint khác (`POST`, nhận urn của DataHub). Chúng tôi sẽ làm **endpoint mới
đúng theo spec**: đường dẫn, mã lỗi, `Cache-Control`, `X-Request-Id`, định dạng body lỗi.

Một lưu ý nhỏ về vận hành: `database` và `table` nằm trên đường dẫn nên sẽ **xuất hiện trong
access log của ingress**. Không phải bí mật, nhưng là thông tin nghiệp vụ — nếu bên các anh/chị
có quy định về việc này thì cho chúng tôi biết (câu 6, §8).

### 4.3 `keyVersion`

Yêu cầu "đọc đúng version đã ghi" là **đúng và chúng tôi sẽ theo**. Với key dùng để mã hoá dữ
liệu, đọc nhầm version nghĩa là giải mã sai toàn bộ dữ liệu cũ — nặng hơn hẳn so với loại key
mà dak đang hình dung ban đầu (key kết nối cơ sở dữ liệu). Chúng tôi lưu version lúc ghi và đọc
đúng version đó.

---

## 5. Hai điểm chúng tôi muốn trao đổi lại

### 5.1 Q3 — xin đổi "trả hash" thành "đối soát hash"

Spec (plan mục 4.5 Q3) cần dak **trả SHA-256 của prefix đã lưu** để đối soát migration.

Trả hash của một bí mật ra ngoài là mở một đường dò: ai lấy được hash có thể thử ngược. Với
prefix ngẫu nhiên thì rủi ro thấp, nhưng endpoint đó sẽ tồn tại mãi sau khi migration xong.

**Đề xuất**: đảo chiều — bên các anh/chị **gửi hash lên**, dak trả về `khớp` / `không khớp`.
Tác dụng đối soát y hệt (vẫn khẳng định được 100% khớp trước khi cutover), nhưng dak không bao
giờ phát hash ra ngoài.

Nếu cách này vướng quy trình của các anh/chị thì chúng tôi làm theo Q3 ban đầu, chỉ thêm: endpoint
yêu cầu quyền quản trị và ghi audit mỗi lần gọi.

### 5.2 Phân biệt Đọc/Ghi không có nghĩa với key mã hoá

Spec nói rõ, và chúng tôi đồng ý: **có key là vừa mã hoá vừa giải mã được**.

Nêu ra vì giao diện dak hiện cho người duyệt chọn "chỉ cho Đọc" khi duyệt một phiếu. Với key mã
hoá thì lựa chọn đó **không chặn được gì** — người duyệt sẽ tin vào một thứ không có thật.

Chúng tôi sẽ ẩn lựa chọn đó cho loại key dùng với sql-engine. Ghi ra để các anh/chị biết: nếu
thấy quyền hiện là "Đọc và Ghi" trên mọi key loại này thì đó là **cố ý**, không phải cấp thừa.

---

## 6. Hợp đồng API — chốt

Phần này **nhận nguyên hình dạng API trong spec của các anh/chị**, và ghi rõ những chỗ spec để ngỏ
mà nếu không chốt thì hai bên sẽ code ra hai hành vi khác nhau. Đọc xong mục này là đủ để hai bên
viết code và viết test độc lập.

Mọi thứ dưới đây **dak cam kết thực hiện đúng**, trừ ba chỗ đánh dấu ❓ đang chờ câu trả lời ở §8.

### A. Request

```http
GET /api/v1/keys/{database}/{table}
Host: dak.<domain>
Authorization: Bearer <access_token của client team>
Accept: application/json
X-Request-Id: 7f3e...          (tuỳ chọn)
```

| Mục | Chốt |
|---|---|
| Phương thức, đường dẫn | `GET /api/v1/keys/{database}/{table}` — đúng spec |
| `database`, `table` | `[A-Za-z0-9_]{1,128}`; dak **chuẩn hoá chữ thường** trước khi tra |
| Sai định dạng | **400 `invalid_table`**, và dak **không gọi Keycloak** (kiểm trước khi xác thực) |
| `X-Request-Id` | Không gửi thì dak sinh. **Luôn** trả lại trong response, cả khi lỗi |
| Thân request | Không có. Mọi tham số nằm trên đường dẫn |
| Tenant/workspace | **Không** nhận từ bên gọi dưới bất kỳ hình thức nào — luôn suy từ token |

### B. Response 200

```http
HTTP/1.1 200 OK
Content-Type: application/json
Cache-Control: no-store
X-Request-Id: 7f3e...

{
  "database": "demo_db",
  "table": "users_cdr",
  "keyPrefix": "9f2c4b...e1",
  "keyVersion": 1
}
```

| Field | Kiểu | Chốt |
|---|---|---|
| `database`, `table` | string | Tên **đã chuẩn hoá chữ thường**, luôn có |
| `keyPrefix` | string | Giá trị thô, **luôn khác rỗng**. dak không bao giờ trả `null` hay `""` — thiếu dữ liệu thì trả 500 |
| `keyVersion` | number | Version KV trong Vault mà dak **đã thực sự đọc** |

**Cam kết của dak về `keyPrefix`:**
- Không bao giờ ghi đè giá trị đã lưu. Nạp lại cùng một `(workspace, database, table)` với giá trị
  khác sẽ **bị từ chối**, không âm thầm thay.
- `keyVersion` trả về là version dak đọc, không phải "version mới nhất". Ai đó ghi đè thẳng vào
  Vault thì dak **vẫn trả đúng giá trị cũ** mà dữ liệu đã mã hoá đang dùng.

⚠️ **dak không thêm field nào khác vào response 200.** Nếu sau này cần thêm, chúng tôi chỉ **thêm**
field mới, không đổi tên và không bỏ field cũ — bên các anh/chị không phải sửa code.

### C. Response lỗi

Thân lỗi, mọi mã:

```json
{
  "error": "access_denied",
  "message": "Team has no access to key demo_db.users_cdr",
  "requestId": "7f3e..."
}
```

| HTTP | `error` | dak trả khi nào | Bên gọi |
|---|---|---|---|
| 400 | `invalid_table` | `database`/`table` sai định dạng | Báo lỗi, không thử lại |
| 401 | `invalid_token` | Thiếu token; chữ ký sai; `exp` đã qua; `iss` không có trong registry; `(realm, client_id)` không có trong dak **hoặc đã bị khoá** | Xin token mới, thử lại **đúng 1 lần** |
| 403 | `access_denied` | Không có quyền — **gồm cả** bảng không tồn tại, bảng của workspace/tenant khác, và **quyền đã hết hạn** ❓ | Báo lỗi, không thử lại |
| 403 | `key_disabled` | Có quyền nhưng key đang bị khoá | Báo lỗi, không thử lại |
| 429 | `rate_limited` | Vượt giới hạn tần suất. Có header `Retry-After` (giây) | Báo lỗi, không thử lại |
| 500 | `internal_error` | Lỗi dak, hoặc dữ liệu trong Vault không hợp lệ | Báo lỗi |
| 503 | `upstream_unavailable` | Keycloak hoặc Vault không phản hồi | Báo lỗi |

🔴 **401 và 403 phân biệt rõ, và bên gọi dựa vào đó:** 401 = "danh tính có vấn đề, xin token mới
rồi thử lại"; 403 = "danh tính đúng, nhưng không có quyền — thử lại vô ích". dak **không bao giờ**
trả 403 cho một vấn đề về token, và ngược lại.

**Ba điều dak cam kết về thân lỗi:**
- `message` **không bao giờ** chứa token, secret, `keyPrefix`, hay tên bảng của workspace khác.
- 403 `access_denied` có **cùng một thân** cho mọi nguyên nhân (không có quyền / bảng không tồn
  tại / bảng của workspace khác). Phân biệt ba ca đó là cho phép dò xem workspace khác có bảng gì.
- `requestId` luôn có, và **là thứ để đối chiếu với audit log của dak** khi cần điều tra.

### D. Hành vi bên lề, chốt để không hiểu khác nhau

| Tình huống | dak làm gì |
|---|---|
| Token hợp lệ, team hợp lệ, nhưng **nhiều key cùng khớp** `(workspace, database, table)` | Không xảy ra: ràng buộc duy nhất ở tầng cơ sở dữ liệu chặn từ lúc tạo |
| Cùng một bảng, hỏi nhiều lần | Trả cùng giá trị. dak **không** cache prefix; mỗi lần đều đọc Vault |
| Team bị khoá giữa chừng | Request kế tiếp **401 ngay** — dak đọc trạng thái team ở mỗi request, không cache |
| Key bị khoá giữa chừng | Request kế tiếp **403 `key_disabled` ngay** |
| Quyền bị thu hồi giữa chừng | Request kế tiếp **403 `access_denied` ngay**. (Phía các anh/chị còn cache prefix tới `cacheTtlSeconds` — thực tế có hiệu lực sau tối đa 5 phút, đúng như kịch bản 10 mục 9.5) |
| Vault trả về `keyPrefix` rỗng hoặc thiếu | **500 `internal_error`** + cảnh báo nội bộ. Không trả chuỗi rỗng |
| Keycloak sập ❓ | Với JWKS: **vẫn phục vụ bình thường** (khoá đã cache). Với introspection: **503** |
| Vault sập | **503 `upstream_unavailable`** |
| Hai team cùng workspace, cùng được cấp một bảng | Cả hai nhận **cùng một `keyPrefix`** — một bảng một key, không phải một key mỗi team |

### E. Ba chỗ ❓ phụ thuộc câu trả lời ở §8

| Chỗ | Nếu chọn thế này | Thì hợp đồng thành |
|---|---|---|
| **Quyền hết hạn** (câu 1) | A — không thời hạn | 403 chỉ xảy ra khi bị thu hồi chủ động |
| | B hoặc C — có thời hạn | 403 `access_denied` **còn xảy ra khi phiếu hết hạn**, engine đang chạy cũng dừng. Bên gọi không phân biệt được hai nguyên nhân |
| **Introspection** (câu 2) | JWKS | Keycloak sập → dak vẫn phục vụ. Thu hồi token có hiệu lực sau tối đa 5 phút (bằng `exp`) |
| | Introspection | Keycloak sập → **503, mọi engine dừng**. Thu hồi token có hiệu lực sau tối đa 30 giây |
| **`aud`** | Chưa bật (đề xuất) | Client của team **không cần** audience mapper |
| | Bật | Mọi client **phải có** mapper `dak-api`, nếu không thì **401** |

🔴 Về `aud`: chúng tôi đã đo trên Keycloak 26.8 — token `client_credentials` mặc định mang
`aud: "account"`, **không** mang `dak-api`. Nghĩa là nếu dak bật kiểm `aud` **trước khi** mọi
client đều có audience mapper thì **toàn bộ engine nhận 401**, và thông báo lỗi không nói được lý
do. Đề xuất: giai đoạn đầu **không kiểm `aud`**, bật sau khi đã xác nhận 100% client có mapper.

### F. Môi trường dev (P0.2 của các anh/chị)

dak cung cấp, sau khi chốt câu 5 ở §8:

| Thứ | Giá trị |
|---|---|
| `dakAddr` | dak sẽ cấp |
| Realm | 2 realm: tenant T1 và T2 |
| Credential | 4 bộ: T1/W1 team A, T1/W1 team B, T1/W2 team C, T2 team D |
| Trùng `client_id` | **Team D cố ý dùng cùng `client_id` với team A** để chạy kịch bản 7 |
| Dữ liệu mẫu | Mỗi team ít nhất một bảng có key và có quyền, để chạy kịch bản 1–6 |

---

## 7. Trả lời câu hỏi mở của các anh/chị

| Câu | Trả lời |
|---|---|
| **Spec 11.1** — ai tạo `dak-api`, `dak-introspector`, thêm realm vào registry | Chúng tôi nghiêng về **người vận hành tạo, dak chỉ lưu cấu hình**. Lý do: để dak tự tạo client thì dak phải giữ một credential **tạo được client bất kỳ trong realm** — nếu dak bị xâm nhập, kẻ tấn công tạo được client cho chính mình. Một dòng cấu hình thì không có rủi ro đó. Xem câu 3, §8 |
| **Spec 11.2** — SLA, HA, TTL cache | dak hiện chạy **1 bản** (dữ liệu phiên nằm trong bộ nhớ tiến trình). Chúng tôi sẽ sửa để chạy nhiều bản **trước khi** các anh/chị phụ thuộc vào dak trong đường query. Cho chúng tôi biết SLA cần đạt (câu 5, §8) |
| **Plan 12.1** — SparkApplication có phải chuyển cùng lúc | Theo chúng tôi: **có**. Nếu job vẫn đọc Vault cũ còn engine đọc dak, mọi key tạo sau đó phải tồn tại ở hai nơi — và hai nguồn sự thật là chỗ sinh ra sai lệch không ai phát hiện |
| **Plan 12.4** — dak làm được Q1–Q8 chưa | Q5, Q6, Q8: **đã có**. Q1: xem câu 3, §8. Q2, Q3, Q4, Q7: **chưa có, sẽ làm** |
| **Plan 12.5** — rotate secret có giữ secret cũ một thời gian không | **Keycloak không hỗ trợ hai secret cùng lúc cho một client.** Nên rotate tại chỗ luôn làm engine đứt cho tới khi cập nhật cấu hình. Cách rotate không gián đoạn duy nhất: tạo client thứ hai cho cùng team, chuyển engine sang, rồi xoá client cũ. Chúng tôi sẽ làm sẵn đường này trong chức năng quản trị, trừ khi các anh/chị thấy không cần |

---

## 8. Sáu câu cần các anh/chị trả lời

Đánh số để tiện phản hồi. Ba câu đầu chặn việc code của chúng tôi.

1. **Thời hạn quyền** (§3.1): chọn A, B hay C? Nếu B thì thời hạn bao lâu là hợp lý với vận hành
   engine, và báo trước bao nhiêu ngày?
2. **Introspection** (§3.3): có bắt buộc không, hay chấp nhận JWKS với các kiểm tra còn lại giữ
   nguyên?
3. **Ai tạo client Keycloak** (§6, spec 11.1): người vận hành tạo tay, hay dak gọi Admin API?
4. **Q3 đối soát hash** (§5.1): đổi sang "gửi hash lên, trả khớp/không khớp" được không?
5. **Quy mô và SLA**: bao nhiêu tenant/realm ở giai đoạn đầu, và dak cần đạt SLA nào? (Một realm
   thì phần việc của chúng tôi nhẹ đi đáng kể.)
6. **`database`/`table` trong access log của ingress** (§4.2): có vấn đề gì không?

---

## 9. Việc tiếp theo của chúng tôi

Sau khi có trả lời câu 1–3:

1. Mô hình dữ liệu: key gắn `(database, table)` + `vault_version`; mapping theo `(realm, client_id)`;
   registry realm.
2. Endpoint `GET /api/v1/keys/{database}/{table}` đúng spec.
3. Chức năng quản trị Q2, Q3, Q4, Q7.
4. Chạy nhiều bản + metric + ngưỡng rate limit cho đường máy.
5. **Môi trường dev cho P0.2**: 2 realm, 4 bộ credential, **cố ý trùng `client_id` giữa hai realm**
   để chạy kịch bản 7. Môi trường thử của dak đã có Keycloak riêng nên phần này không lâu.

Chúng tôi có thể bắt đầu bước 1 và 2 ngay khi có câu 1–3; bước 5 thì cần biết câu 5 trước.
