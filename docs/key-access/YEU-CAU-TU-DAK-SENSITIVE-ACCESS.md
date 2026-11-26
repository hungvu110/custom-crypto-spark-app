# Thông báo hợp đồng mới: service account tạm cho phiếu khai thác dữ liệu nhạy cảm

Ngày: 2026-11-09. Gửi: đội SA / sql-engine.

Tài liệu này mô tả một **luồng hợp đồng mới**, tách hẳn khỏi `/api/machine/keys/fetch` và
`/api/v1/keys/**` (docs/18, đã sync trước đây). Không đụng tới hai đường đó — luồng dưới đây chỉ
áp dụng cho loại phiếu mới "khai thác dữ liệu nhạy cảm".

---

## 1. Tóm tắt một trang

Phiếu dak có thêm loại thứ ba **SENSITIVE_ACCESS** — khai thác dữ liệu nhạy cảm **ở dạng gốc**,
không qua khử định danh, không có vùng dữ liệu riêng. Khác hẳn hai loại phiếu cũ:

| | Phiếu thường / khai thác dữ liệu (cũ) | Phiếu nhạy cảm (mới) |
|---|---|---|
| dak cấp gì | key mã hoá (đọc qua `/api/v1/keys`) | **1 service account Keycloak tạm**, SA tự dùng để truy cập nguồn dữ liệu |
| Ai giữ dữ liệu | dak quản lý key, SA không chạm hạ tầng dữ liệu | **dak không biết SA đọc gì ở đâu** — ngoài phạm vi dak, chỉ cấp danh tính |
| Thời hạn | theo grant, key có thể rotate | account sống đúng bằng thời hạn phiếu, **tự động bị khoá khi phiếu hết hạn** |
| Dùng lại | — | Idempotent: gọi lại nhận cùng client, không sinh secret mới |

dak **không** có API trả dữ liệu hay trả key cho loại phiếu này. Việc dak làm chỉ dừng ở: tạo
một client Keycloak service-account, ghi secret vào Vault, và tự khoá client đó khi phiếu hết
hạn. **Sau khi có credential, SA tự biết cách dùng nó để lấy dữ liệu** — phần đó nằm ngoài hợp
đồng với dak.

---

## 2. Luồng

```
   Người dùng dak                 dak                      VLP Keycloak / Vault
   ──────────────                 ───                      ─────────────────────
   1. Tạo phiếu SENSITIVE_ACCESS
   2. Người có quyền duyệt phiếu
          │
   3. Bấm "Lấy service account"
      (hoặc SA tự gọi API dưới)
          │
          └──────────────────────>│
                                  │ 4. Tạo client Keycloak service-account
                                  │    clientId = svc-dak-req-<id phiếu>
                                  │<───────────────────────────────────> (admin API)
                                  │ 5. Ghi secret vào Vault
                                  │<───────────────────────────────────> (KV v2)
          │<─────────────────────┘
   6. Nhận lại clientId + realm + đường dẫn Vault (KHÔNG có secret)
          │
   7. SA đọc secret ở Vault theo đường dẫn trên
          │
   8. SA tự gọi Keycloak: grant_type=client_credentials → access_token
          │
   9. SA tự dùng token đó để truy cập nguồn dữ liệu nhạy cảm — ngoài phạm vi dak
```

Bước 7-9 giống đúng mô hình team-clients SA đã dùng ở docs/18 §2.4 (đọc id/secret ở Vault, tự
gọi Keycloak lấy token) — khác ở chỗ: client này **không gắn vào team sống lâu**, nó gắn vào
**một phiếu**, và dak **tự khoá nó khi phiếu hết hạn** (xem §4).

---

## 3. API — ai gọi, lấy gì

```
POST /api/requests/{requestId}/service-account
Authorization: cookie phiên dak (người dùng), KHÔNG dùng cho máy
```

Đây là API **nội bộ dak**, người dùng bấm nút trên UI, không phải API SA gọi trực tiếp — SA
không có phiên đăng nhập dak. Response:

```json
{
  "clientId": "svc-dak-req-123",
  "realm": "<realm Keycloak của tenant>",
  "credentialPath": "<mount>/<base>/sensitive-accounts/123",
  "expiresAt": "2026-12-09T08:00:00Z",
  "disabled": false
}
```

**Không có trường secret.** SA đọc secret trực tiếp ở Vault theo `credentialPath` trả về — cùng
cách đọc credential team-clients đã làm (docs/18 §8), khác nhánh path (`sensitive-accounts/`
thay vì `team-clients/`).

Idempotent: gọi lại API này (nhiều lần, nhiều người có quyền đọc phiếu) luôn trả về cùng
`clientId`/`credentialPath`, không sinh secret mới.

---

## 4. Thời hạn và thu hồi

- Account sống đúng bằng thời hạn phiếu (grant). Hết hạn → **job nội bộ dak** (chạy hằng ngày)
  tự gọi Keycloak khoá (`disable`) client, **không cần SA báo**, **không cần SA tự gỡ**.
- Client bị khoá vẫn còn tồn tại trên Keycloak (không xoá) — gọi `client_credentials` sau khi
  khoá sẽ thất bại ở phía Keycloak như mọi client bị disable khác.
- Nếu job khoá thất bại (Keycloak tạm lỗi), dak tự thử lại ở lượt chạy tiếp theo — không cần SA
  làm gì thêm.

**Chưa có cơ chế gia hạn** account khi phiếu được gia hạn/tái duyệt — nếu SA cần job dài hơn một
phiếu, báo lại để dak thiết kế thêm (hiện ngoài phạm vi tài liệu này).

---

## 5. Việc cần làm phía SA

1. Xác nhận: **không cần sửa client hiện có** cho hai đường `/api/machine/keys/fetch` và
   `/api/v1/keys/**` — luồng này hoàn toàn tách biệt, dùng credential khác, mục đích khác.
2. Nếu SA sẽ là bên **tiêu thụ** credential loại này (thay vì chỉ người dùng dak bấm nút xem):
   xác nhận cách SA muốn nhận `credentialPath`/`clientId` — qua API dak gọi lại SA, hay SA tự
   poll, hay người vận hành dak gửi tay. Hiện tại dak chỉ hiển thị trên UI cho người dùng, **chưa
   có đường đẩy tự động sang SA** — cần biết SA cần gì trước khi code thêm.
3. Xác nhận: sau khi có token Keycloak từ client này, SA tự biết truy cập nguồn dữ liệu nhạy cảm
   ở đâu (ngoài dak) — hay cần dak cung cấp thêm thông tin định tuyến (host, port, schema)?
   **Đây là câu hỏi mở quan trọng nhất** — tài liệu này chỉ mô tả phần dak đã làm, chưa mô tả
   phần SA cần làm với credential sau khi nhận.

---

## 6. Thời điểm áp dụng

Đã code và test xong ở môi trường local (migration, service, API, e2e UI). **Chưa phát hành lên
staging/production.** Sẽ báo lại sau khi SA xác nhận §5, đặc biệt câu hỏi ở mục 3 — không muốn
phát hành một hợp đồng rồi phải đổi ngay vì thiếu thông tin định tuyến dữ liệu.
