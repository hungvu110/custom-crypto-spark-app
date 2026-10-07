# dak-mock — Mock DAK cho use case lấy key

Service Spring Boot giả lập DAK, **chỉ** phục vụ API mà sql-engine và SparkApplication gọi để lấy `keyPrefix`:

```
GET /api/v1/keys/{database}/{table}      Authorization: Bearer <access token Keycloak của team>
```

Hợp đồng API đầy đủ: [docs/DAK_API_SPEC.md](../docs/DAK_API_SPEC.md). Phía client (`key-prefix-lib`, nguồn `source=dak`):
[docs/DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md](../docs/DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md).

## 1. Mock làm gì và không làm gì

| Có                                                                                   | Không (cố ý bỏ qua)                                                        |
| ------------------------------------------------------------------------------------ | -------------------------------------------------------------------------- |
| Xác minh access token bằng **JWKS của realm Keycloak thật** (RS256, `iss`, `exp`, `typ`, `azp`) | **Phân quyền**: grant theo bảng, thời hạn quyền, phiếu duyệt         |
| Ánh xạ `(realm, client_id)` → (tenant, workspace, team) từ file cấu hình            | Chức năng quản trị (tạo client, tạo/nạp key, cấp quyền, khoá key)          |
| Đọc `keyPrefix` từ **Vault KV v2 bằng token tĩnh**, cùng path với luồng column/cdr cũ | Đọc đúng `vault_version` đã lưu (mock đọc version mới nhất)               |
| Mã lỗi, body lỗi, `X-Request-Id`, `Cache-Control: no-store` đúng spec                | Rate limit, kiểm `aud`, chạy nhiều replica, lưu audit vào DB (chỉ ghi log) |

Vì không có phân quyền: **mọi team có token hợp lệ đều lấy được mọi key có trong Vault**. Chỉ dùng cho dev/test.

## 2. Cấu trúc code

```
dak-mock/
├── pom.xml                                  Spring Boot 3.4, Java 17 — project độc lập, KHÔNG thuộc reactor Spark
└── src/main/java/vai/lakehouse/dakmock/
    ├── DakMockApplication.java              điểm vào
    ├── config/DakProperties.java            cấu hình `dak.*` (realm, client, Vault), kiểm tra ngay lúc khởi động
    ├── web/RequestIdFilter.java             gắn X-Request-Id + Cache-Control: no-store cho MỌI response
    ├── web/KeyController.java               GET /api/v1/keys/{database}/{table}: kiểm tên → xác minh token → lấy key
    ├── auth/TokenVerifier.java              xác minh JWT bằng JWKS của realm, tra client đã đăng ký
    ├── key/KeyService.java                  lấy key cho team + ghi log audit (không ghi prefix)
    ├── key/VaultKeyReader.java              GET {vault}/v1/{kvMount}/data/{path} với X-Vault-Token
    ├── web/DakApiException.java             lỗi theo bảng mã lỗi của spec (400/401/403/500/503)
    └── web/ApiExceptionHandler.java         chuyển lỗi thành {"error","message","requestId"}
```

Luồng một request:

```mermaid
sequenceDiagram
    autonumber
    participant C as key-prefix-lib (driver)
    participant F as RequestIdFilter
    participant K as KeyController
    participant T as TokenVerifier
    participant J as Keycloak JWKS
    participant S as KeyService
    participant V as Vault KV v2

    C->>F: GET /api/v1/keys/demo_db/users_cdr (Bearer)
    Note over F: X-Request-Id (giữ của bên gọi nếu hợp lệ, không thì sinh mới)<br/>Cache-Control: no-store
    F->>K: chuyển tiếp
    Note over K: database/table khớp [A-Za-z0-9_]{1,128}? Sai → 400<br/>Chuẩn hoá chữ thường
    K->>T: verify(Authorization)
    Note over T: iss (chưa tin) → tra realm trong cấu hình; không có → 401
    T->>J: tải JWKS từ jwks-uri của cấu hình (có cache)
    Note over T: Chữ ký RS256, exp/iat, iss, typ=Bearer, azp<br/>(realm, azp) đã đăng ký và enabled? Không → 401
    K->>S: fetch(team, db, table)
    S->>V: GET /v1/kv/data/hla-datalake/datalake/spark-application/demo_db.users_cdr
    alt Có secret
        V-->>S: data.data.keyPrefix, metadata.version
        S-->>C: 200 {database, table, keyPrefix, keyVersion}
    else 404
        S-->>C: 403 access_denied
    else Vault từ chối token / không phản hồi
        S-->>C: 503 upstream_unavailable
    end
```

### Các điểm bảo mật trong code

| Điểm                                          | Ở đâu                         | Vì sao                                                                                       |
| --------------------------------------------- | ----------------------------- | -------------------------------------------------------------------------------------------- |
| Không dựng URL từ `iss`; khoá chỉ lấy từ `jwks-uri` cấu hình | `TokenVerifier`     | Kẻ tấn công không thể tự cung cấp khoá ký (header `jku`/`x5u` bị bỏ qua — có test)           |
| Chỉ RS256                                     | `TokenVerifier.decoder`       | Chặn token `alg: none` và token ký HMAC (có test)                                             |
| Khoá đăng ký client là `(realm, client_id)`   | `DakProperties`, `TokenVerifier` | `client_id` chỉ duy nhất trong một realm                                                  |
| Kiểm tên bảng trước khi xác minh token        | `KeyController`               | Spec: tên sai trả 400, không cần gọi Keycloak                                                |
| Bảng không có key trả **403**, không phải 404 | `KeyService`                  | Không dò được bảng nào tồn tại                                                               |
| JWKS không tải được trả **503**, không phải 401 | `TokenVerifier`             | Để bên gọi không hiểu nhầm là lỗi danh tính                                                  |
| Không log token, secret, prefix               | `KeyService`, `ApiExceptionHandler`, `DakProperties.Vault.toString` |                                                           |

## 3. Cấu hình

[`src/main/resources/application.yml`](src/main/resources/application.yml):

```yaml
dak:
  clock-skew: 60s
  realms:                                   # registry realm: mỗi tenant một realm
    - realm: tenant-a
      tenant: tenant-a
      issuer: https://keycloak.example/realms/tenant-a      # phải khớp CHÍNH XÁC claim iss của token
      jwks-uri: https://keycloak.example/realms/tenant-a/protocol/openid-connect/certs
  clients:                                  # client Keycloak của team
    - realm: tenant-a
      client-id: dak.w1.team-a
      workspace: w1
      team: team-a
      # enabled: false                      # khoá client -> token của client này bị 401
  vault:
    addr: ${VAULT_ADDR:http://vault.cyberspace.vn}
    token: ${VAULT_TOKEN:}                  # token tĩnh, LUÔN lấy từ biến môi trường
    kv-mount: kv
    path-template: hla-datalake/datalake/spark-application/{database}.{table}
    key-field: keyPrefix
```

- `issuer` lấy từ `https://<keycloak>/realms/<realm>/.well-known/openid-configuration` (field `issuer`) để chắc chắn khớp
  từng ký tự với `iss` trong token.
- `path-template` nhận `{tenant}`, `{workspace}`, `{database}`, `{table}`. Mặc định trùng path của luồng column/cdr cũ, với
  **tên key hai phần** `<database>.<table>` (chữ thường) — đúng các secret tạo ở bước migration 5 của plan.
- Ứng dụng **không khởi động** nếu thiếu `VAULT_TOKEN`, `path-template` thiếu `{database}`/`{table}`, client trỏ tới realm không
  có, hoặc trùng `(realm, client-id)`.

Muốn đổi cấu hình mà không sửa file trong repo, đặt file riêng rồi trỏ tới khi chạy:
`--spring.config.additional-location=file:/path/dak-mock-local.yml`.

## 4. Chuẩn bị secret trong Vault

Mỗi bảng một secret, field `keyPrefix`:

```bash
# Ví dụ cho bảng demo_db.users_cdr (dùng token có quyền ghi, KHÔNG phải token chỉ đọc của mock)
vault kv put kv/hla-datalake/datalake/spark-application/demo_db.users_cdr keyPrefix="<giá trị prefix>"
```

Token mà mock dùng (`VAULT_TOKEN`) chỉ cần quyền **đọc** path này:

```hcl
path "kv/data/hla-datalake/datalake/spark-application/*" {
  capabilities = ["read"]
}
```

## 5. Chạy

Yêu cầu: JDK 17+ và Maven.

```bash
read -rs -p "Vault token (chỉ đọc): " VAULT_TOKEN; echo; export VAULT_TOKEN
export VAULT_ADDR=http://vault.cyberspace.vn

mvn -f dak-mock/pom.xml spring-boot:run
# hoặc build jar rồi chạy
mvn -f dak-mock/pom.xml package
java -jar dak-mock/target/dak-mock-0.1.0-SNAPSHOT.jar --spring.config.additional-location=file:./dak-mock-local.yml
```

Cổng mặc định `8085` (đổi bằng biến `PORT`).

## 6. Gọi thử

```bash
# 1. Xin access token của team từ Keycloak (realm của tenant)
read -rs -p "Client secret: " CS; echo
TOKEN=$(curl -s -X POST "https://<keycloak>/realms/tenant-a/protocol/openid-connect/token" \
  -d grant_type=client_credentials -d client_id=dak.w1.team-a --data-urlencode "client_secret=$CS" \
  | jq -r .access_token); unset CS

# 2. Gọi mock
curl -s -i http://localhost:8085/api/v1/keys/demo_db/users_cdr -H "Authorization: Bearer $TOKEN"
```

Kết quả mong đợi:

| Tình huống                                    | Response                                                                    |
| --------------------------------------------- | --------------------------------------------------------------------------- |
| Token hợp lệ, Vault có secret                 | `200 {"database":"demo_db","table":"users_cdr","keyPrefix":"...","keyVersion":1}` |
| Tên bảng sai (`demo-db`)                      | `400 {"error":"invalid_table",...}`                                         |
| Thiếu token, token hết hạn, sai chữ ký, realm lạ, client chưa đăng ký/bị khoá | `401 {"error":"invalid_token",...}`            |
| Vault không có secret cho bảng                | `403 {"error":"access_denied",...}`                                         |
| Secret không có field `keyPrefix` hoặc rỗng   | `500 {"error":"internal_error",...}`                                        |
| Vault từ chối token của mock / không phản hồi, hoặc không tải được JWKS | `503 {"error":"upstream_unavailable",...}`       |

Lý do chi tiết của từng lỗi (vd "client dak.w9.ghost is not registered") chỉ nằm trong **log** của mock, kèm `requestId`;
body trả về luôn là thông điệp chung. Mỗi lần lấy key ghi một dòng log `AUDIT`:

```
KEY_FETCH result=ALLOWED httpStatus=200 error=- denyReason=- realm=tenant-a tenant=tenant-a workspace=w1 clientId=dak.w1.team-a team=team-a database=demo_db table=users_cdr keyVersion=1
```

## 7. Nối với sql-engine / SparkApplication

Mock chạy **HTTP**, mà `key-prefix-lib` mặc định bắt buộc `https://`. Khi thử với mock phải bật `allowInsecureHttp`
(chỉ cho môi trường test):

```properties
# sql-engine
spark.columncrypto.source=dak
spark.columncrypto.dak.addr=http://<host-mock>:8085
spark.columncrypto.dak.tokenUrl=https://<keycloak>/realms/tenant-a/protocol/openid-connect/token
spark.columncrypto.dak.clientId=dak.w1.team-a
spark.columncrypto.dak.clientSecret=<client secret>
spark.columncrypto.dak.allowInsecureHttp=true
```

```yaml
# SparkApplication (env của driver)
- name: CRYPTO_PREFIX_SOURCE
  value: "dak"
- name: DAK_ADDR
  value: "http://<host-mock>:8085"
- name: DAK_ALLOW_INSECURE_HTTP
  value: "true"
# DAK_TOKEN_URL, DAK_CLIENT_ID, DAK_CLIENT_SECRET như manifest k8s/spark-application-*.yaml
```

Rồi chạy thử `SELECT cdr_decrypt('demo_db.users_cdr', name, created_at) FROM demo_db.users_cdr` trên engine.

## 8. Test

```bash
mvn -f dak-mock/pom.xml test
```

`KeyApiTest` chạy mock thật (cổng ngẫu nhiên), dùng MockWebServer đóng vai Keycloak (JWKS) và Vault, token ký bằng khoá RSA
sinh trong test. 16 kịch bản: 200 và chuẩn hoá chữ thường; 400 trước khi xác minh token; 401 cho thiếu token, sai chữ ký cùng
`kid`, realm lạ, hết hạn, `typ` sai, `alg: none`/HS256, client chưa đăng ký/bị khoá/`client_id` lệch `azp`, header `jku` trỏ ra
ngoài (và xác nhận mock không gọi tới đó); 403 khi Vault không có secret; 500 khi secret rỗng; 503 khi Vault từ chối token và khi
JWKS không tải được; thay `X-Request-Id` không hợp lệ.

## 9. Lưu ý khi mở bằng IDE

`dak-mock` là project Maven riêng (Java 17). Nếu VSCode/IntelliJ đang mở thư mục gốc như project Spark (Java 11), IDE sẽ báo
lỗi cú pháp giả ở các `record`. Import `dak-mock/pom.xml` như một project riêng để IDE dùng đúng JDK.
