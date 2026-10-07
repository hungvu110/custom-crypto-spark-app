# Phản hồi của đội sql-engine / SparkApplication về `PHAN-HOI-DAK.md`

Ngày: 2026-10-06. Gửi: đội DAK.

Cảm ơn phản hồi. Đề xuất nào phía DAK đưa ra chúng tôi đều đã cân nhắc kỹ, và gần như đều đồng ý. Tài liệu này gồm: trả lời 6 câu
ở §8 của các anh/chị, vài điểm chúng tôi bổ sung hoặc hiểu khác, những gì chúng tôi cần từ DAK, và các thay đổi đã cập nhật vào
hai tài liệu chung.

Hai tài liệu đã cập nhật theo các điểm thống nhất dưới đây:
- [DAK_API_SPEC.md](../DAK_API_SPEC.md): đổi sang JWKS, thêm thời hạn quyền, bỏ `dak-introspector`, thêm hành vi biên từ §6 của
  các anh/chị.
- [DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md](../DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md): thêm SparkApplication, fallback, trạng thái Q1–Q10.

---

## 1. Tóm tắt một trang

| | |
|---|---|
| **Hợp đồng API (§6 của DAK)** | **Đồng ý toàn bộ**, kèm vài chi tiết JWKS cần ghi rõ (mục 3.1 dưới đây) |
| **Trả lời 6 câu** | Mục 2 |
| **Bổ sung / hiểu khác** | Keycloak sập không chỉ ảnh hưởng introspection; hết hạn hàng loạt sau migration; fallback Vault cũ (mục 3) |
| **Cần từ DAK** | Mục 4: HA trước cutover, nạp/đối soát/grant theo lô, nhiều client mỗi team, cảnh báo hết hạn, mốc thời gian |
| **Việc tiếp theo** | Mục 5 |

---

## 2. Trả lời 6 câu ở §8

| # | Câu | Trả lời |
|---|---|---|
| 1 | **Thời hạn quyền** | **Phương án B** như DAK đề xuất: phiếu có thời hạn **1 năm**, cảnh báo trước **30 ngày**, có màn hình liệt kê quyền sắp hết hạn. Hết hạn trả **403 `access_denied`** như các trường hợp không có quyền khác; chúng tôi không yêu cầu mã lỗi riêng. Đề nghị thêm ở mục 3.3 và 4 |
| 2 | **Introspection** | **Chấp nhận JWKS**, giữ mọi kiểm tra khác. Các quy tắc JWKS chúng tôi cần DAK cam kết nằm ở mục 3.1 (đã đưa vào spec mục 4) |
| 3 | **Ai tạo client Keycloak** | **Vận hành tạo tay**, cả client cấp realm (`dak-api`, registry) lẫn client của từng team. DAK **không** giữ credential quản trị Keycloak nào. Vận hành tạo theo checklist ở [spec mục 2.3](../DAK_API_SPEC.md#23-yêu-cầu-với-client-của-team) rồi đăng ký `(realm, client_id)` → team vào DAK |
| 4 | **Q3 đối soát hash** | **Đồng ý** đổi chiều: chúng tôi gửi SHA-256, DAK trả khớp/không khớp. Cần làm được **theo lô** (mục 4) |
| 5 | **Quy mô và SLA** | Giai đoạn đầu **1 tenant (1 realm)**. Code và dữ liệu vẫn theo `(realm, client_id)` để thêm tenant sau không phải sửa. SLA đề xuất ở mục 4 |
| 6 | **`database`/`table` trong access log ingress** | **Chấp nhận.** Tên bảng không phải bí mật, đã có sẵn trong audit của Ranger/Hive |

Các đề xuất khác của DAK chúng tôi đồng ý:
- **Hoãn kiểm `aud`**: v1 không kiểm. Vận hành vẫn gắn mapper `dak-api` cho **mọi** client tạo mới, để bật kiểm sau mà không
  làm hỏng engine nào.
- **Rotate bằng client thứ hai** (§7, câu 12.5): đồng ý. Lưu ý: Keycloak có tính năng "client secret rotation" giữ secret cũ hợp
  lệ thêm một khoảng thời gian, nhưng đang ở dạng preview, nên phương án client thứ hai của DAK an toàn hơn cho môi trường thật.
  Dù cách nào, engine/job vẫn phải restart để nhận credential mới.
- **SparkApplication chuyển cùng đợt** với sql-engine (§7, câu 12.1): đồng ý, lý do như DAK nêu.
- **Ẩn lựa chọn "chỉ Đọc"** cho loại key này (§5.2): đồng ý.

---

## 3. Bổ sung và điểm hiểu khác

### 3.1. JWKS: các quy tắc cần ghi rõ

§3.3 của DAK nêu các kiểm tra giữ lại (`iss`, `exp`, `azp`, tra `(realm, client_id)` mỗi request). Khi bỏ introspection, phần
xác minh chữ ký trở thành tuyến bảo vệ chính, nên chúng tôi đề nghị DAK cam kết thêm (đã ghi ở
[spec mục 4](../DAK_API_SPEC.md#4-xác-minh-token-jwks)):

| Quy tắc | Lý do |
|---|---|
| Khoá công khai **chỉ** lấy từ `jwksUri` khai trong registry; **bỏ qua** header `jku`, `x5u`, `jwk`, `x5c` | Chặn kẻ tấn công tự cung cấp khoá để ký token giả |
| `alg` lấy theo danh sách cho phép (**RS256**), từ chối `none`, mọi `HS*`, và `alg` không khớp khoá | Chặn lỗi kinh điển "alg confusion" |
| Gặp `kid` lạ thì tải lại JWKS, tối đa 1 lần / 30 giây / realm | Keycloak tự đổi khoá ký; không có cơ chế này thì engine lỗi 401 hàng loạt khi đổi khoá. Giới hạn tần suất để token rác không làm DAK dội Keycloak |
| Kiểm `typ == "Bearer"`, cho lệch đồng hồ tối đa 60 giây | Không nhận refresh/ID token; tránh 401 do lệch giờ giữa các máy |
| Chưa có khoá trong cache mà Keycloak không phản hồi thì trả **503**, không phải 401 | Để bên gọi không hiểu nhầm là lỗi danh tính |

### 3.2. Keycloak sập: ảnh hưởng tới cả hai phương án

§3.3 nêu "Keycloak sập thì mọi engine/job dừng" như nhược điểm riêng của introspection. Thực tế engine/job cũng phải xin token
mới từ Keycloak mỗi ≤ 300 giây, nên dù DAK dùng JWKS, khi Keycloak sập:
- bảng đã có trong cache prefix vẫn chạy tới hết `cacheTtlSeconds`;
- bảng chưa có trong cache sẽ lỗi khi token hiện tại hết hạn.

Chúng tôi vẫn chọn JWKS vì lý do khác: bớt một bí mật cần quản lý cho mỗi realm, và việc khoá client/thu hồi quyền trên DAK vẫn
có hiệu lực ngay. Ghi ra để hai bên cùng hiểu rằng Keycloak vẫn là điểm phụ thuộc của luồng máy.

### 3.3. Hết hạn hàng loạt sau migration

Nếu migration tạo phiếu/grant hàng loạt cùng một ngày với thời hạn 1 năm, thì **toàn bộ** các quyền đó hết hạn cùng một ngày
năm sau, tức mọi engine/job có thể dừng cùng lúc. Đề nghị DAK chọn một trong hai:
- có chức năng **gia hạn theo lô**, và màn hình quyền sắp hết hạn nhóm được theo ngày hết hạn; hoặc
- rải ngày hết hạn của các grant tạo bằng migration (vd ngẫu nhiên trong khoảng 11–13 tháng).

Chúng tôi nghiêng về cách thứ nhất, vì thời hạn giữ đúng như phiếu đã duyệt.

### 3.4. Fallback: Vault cũ được giữ lâu dài

Để báo DAK biết, vì nó ảnh hưởng tới ranh giới quyền:
- Engine/job giữ sẵn cấu hình Vault cũ để **fallback thủ công** (vận hành đổi `source=vault` rồi restart) khi DAK sự cố. Không
  có fallback tự động; thư viện của chúng tôi chặn cấu hình đó.
- Token Vault cũ (đọc được mọi prefix dưới `kv/hla-datalake/datalake/spark-application/*`) được **giữ lâu dài**. Đây là rủi ro
  phía chúng tôi chấp nhận: khi fallback, phân quyền và thời hạn quyền của DAK không có hiệu lực, và audit DAK không thấy các lần
  đọc đó. Chúng tôi giám sát việc dùng token này phía Vault.
- Path cũ được **đóng băng** sau cutover: không thêm secret mới. Đề nghị DAK **không đọc, không ghi** path cũ (đã ghi ở
  [spec mục 7](../DAK_API_SPEC.md#7-vault)). Bảng tạo sau cutover chỉ có key ở DAK và không có fallback.

### 3.5. Tham số `database`/`table` phía SparkApplication

SparkApplication sẽ truyền `${DB_NAME}.${TABLE_NAME}` **chữ thường**. Phía chúng tôi sửa code job; DAK không phải làm gì thêm.

---

## 4. Những gì chúng tôi cần từ DAK

| # | Việc | Ghi chú |
|---|---|---|
| R1 | **≥ 2 replica, không giữ state trong bộ nhớ tiến trình** trước khi bất kỳ engine/job nào chuyển sang DAK | Điều kiện bắt buộc trước cutover (plan P3.4) |
| R2 | **SLA đề xuất** cho API lấy key: khả dụng ≥ 99,9%/tháng; p99 độ trễ ≤ 300 ms cho response 200 (khi Vault bình thường); deploy cuốn chiếu không gián đoạn | Mời DAK xác nhận hoặc đề xuất con số khác |
| R3 | **Q2 – nạp key**: nạp lại **cùng** giá trị cho cùng `(workspace, database, table)` thì trả thành công (idempotent); **khác** giá trị thì từ chối | Để script migration chạy lại được khi đứt giữa chừng |
| R4 | **Q3, Q4 – theo lô**: nạp key, đối soát hash, và tạo phiếu/grant cho migration đều làm được theo lô. Đề xuất định dạng ở dưới bảng | Số bảng cần migrate có thể lớn |
| R5 | **Nhiều client ACTIVE cho cùng một team** | Cần cho rotate bằng client thứ hai |
| R6 | **Cảnh báo hết hạn**: gửi tới người tạo phiếu, chủ team, và một kênh vận hành chung của engine/job. Xin DAK cho biết kênh hỗ trợ (email, chat, webhook) | Engine/job thường không có người trực theo dõi từng phiếu |
| R7 | **Gia hạn theo lô** hoặc rải ngày hết hạn (mục 3.3) | |
| R8 | **Audit có `denyReason`** (`NO_GRANT`, `GRANT_EXPIRED`, `KEY_NOT_FOUND`, `OTHER_WORKSPACE`) chỉ ở phía DAK, tra được theo `requestId` | Vì 403 trả ra ngoài cố ý giống nhau, vận hành cần chỗ này để biết nguyên nhân |
| R9 | **Đăng ký mapping client**: cho biết vận hành nhập `(realm, client_id)` → `(tenant, workspace, team)` vào DAK bằng cách nào (màn hình quản trị?) | Để viết runbook tạo client |
| R10 | **Mốc thời gian** dự kiến cho: mô hình dữ liệu + endpoint mới (§9 bước 1–2), Q2–Q4, R1 | Để chúng tôi xếp lịch Phase 2–3 |

Đề xuất định dạng lô (JSON Lines; DAK có thể đổi nếu đã có định dạng riêng):

```jsonl
{"workspace":"w1","database":"demo_db","table":"users_cdr","keyPrefix":"<giá trị cũ>"}
```

```jsonl
{"workspace":"w1","database":"demo_db","table":"users_cdr","sha256":"<sha256 hex của prefix cũ>"}
```

Kết quả đối soát chỉ cần `{"workspace","database","table","match": true|false}` cho từng dòng. File nạp key chứa bí mật: chúng
tôi sẽ gửi qua kênh DAK chỉ định, không qua email hay chat, và xoá ngay sau khi nạp.

---

## 5. Việc tiếp theo

**Phía DAK** (theo §9 của các anh/chị, kèm mục 4 ở trên):
1. Mô hình dữ liệu và endpoint `GET /api/v1/keys/{database}/{table}` theo [spec](../DAK_API_SPEC.md), xác minh JWKS theo mục 3.1.
2. Q2–Q4 theo lô, R3–R8.
3. Chạy nhiều bản (R1).
4. Môi trường dev: **1 realm** là đủ cho giai đoạn này, gồm W1 (team A, team B) và W2 (team C), mỗi team ít nhất một bảng có key
   và grant. Realm thứ hai và team D trùng `client_id` để dành cho trước khi onboard tenant thứ hai.

**Phía chúng tôi**:
1. Thư viện `key-prefix-lib` nguồn `dak` (test với stub theo đúng hợp đồng §6).
2. Sửa SparkApplication truyền `database.table`, manifest dùng `DAK_*`.
3. Kiểm kê bảng, view, job cần migrate; gửi DAK danh sách để ước lượng khối lượng nạp.

Nếu các anh/chị đồng ý với các điểm trên, chúng tôi coi [DAK_API_SPEC.md](../DAK_API_SPEC.md) bản hiện tại là hợp đồng chốt cho
v1, và hai bên bắt đầu code độc lập.
