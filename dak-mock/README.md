# dak-mock — Mock DAK cho use case lấy key (chạy trên Kubernetes nội bộ)

Service Spring Boot giả lập DAK, **chỉ** phục vụ API mà sql-engine và SparkApplication gọi để lấy `keyPrefix`:

```
GET /api/v1/keys/{database}/{table}      Authorization: Bearer <access token Keycloak của team>
```

Hợp đồng API đầy đủ: [docs/DAK_API_SPEC.md](../docs/DAK_API_SPEC.md). Phía client (`key-prefix-lib`, nguồn `source=dak`):
[docs/DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md](../docs/DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md).

## 1. Môi trường triển khai

| Thành phần | Giá trị |
| --- | --- |
| Keycloak | `https://sso-lakehouse.cyberspace.vn` |
| Realm (tenant) | `vlp-tenantw1xjixm` |
| Issuer | `https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm` |
| JWKS | `https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/certs` |
| Token endpoint (bên gọi dùng) | `https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/token` |
| Namespace | `vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy` (workspace `wsw7vtwvi`, team `teamtscauiy`; cùng namespace với sql-engine và SparkApplication) |
| Service | `dak-mock:8085` (HTTP, ClusterIP) |
| Vault | `http://vault.cyberspace.vn`, KV v2 mount `kv` |

Tất cả chạy trong cluster: build image → push registry nội bộ → `kubectl apply`. Không có bước chạy local.

## 2. Mock làm gì và không làm gì

| Có                                                                                   | Không (cố ý bỏ qua)                                                        |
| ------------------------------------------------------------------------------------ | -------------------------------------------------------------------------- |
| Xác minh access token bằng **JWKS của realm Keycloak** (RS256, `iss`, `exp`, `typ`, `azp`) | **Phân quyền**: grant theo bảng, thời hạn quyền, phiếu duyệt         |
| Ánh xạ `(realm, client_id)` → (tenant, workspace, team) từ ConfigMap                | Chức năng quản trị (tạo client, tạo/nạp key, cấp quyền, khoá key)          |
| Đọc `keyPrefix` từ **Vault KV v2 bằng token tĩnh**, cùng path với luồng column/cdr cũ | Database: mock không có DB, mọi cấu hình nằm trong ConfigMap              |
| Mã lỗi, body lỗi, `X-Request-Id`, `Cache-Control: no-store` đúng spec                | Đọc đúng `vault_version` đã lưu (mock đọc version mới nhất), rate limit, kiểm `aud` |

Vì không có phân quyền: **mọi team có token hợp lệ đều lấy được mọi key có trong Vault**. Chỉ dùng cho môi trường test.

## 3. Cấu trúc code

```
dak-mock/
├── Dockerfile                               build 2 bước, chạy user 1000:1000
├── k8s/dak-mock.yaml                        ConfigMap + Deployment + Service
├── pom.xml                                  Spring Boot 3.4, Java 17 — project độc lập, KHÔNG thuộc reactor Spark
└── src/main/java/vai/lakehouse/dakmock/
    ├── config/DakProperties.java            cấu hình `dak.*` (realm, client, Vault), kiểm tra ngay lúc khởi động
    ├── web/RequestIdFilter.java             X-Request-Id + Cache-Control: no-store cho MỌI response
    ├── web/KeyController.java               GET /api/v1/keys/{database}/{table}: kiểm tên → xác minh token → lấy key
    ├── auth/TokenVerifier.java              xác minh JWT bằng JWKS của realm, tra client đã đăng ký
    ├── key/KeyService.java                  lấy key cho team + ghi log audit (không ghi prefix)
    ├── key/VaultKeyReader.java              GET {vault}/v1/{kvMount}/data/{path} với X-Vault-Token
    ├── web/DakApiException.java             lỗi theo bảng mã lỗi của spec (400/401/403/500/503)
    └── web/ApiExceptionHandler.java         chuyển lỗi thành {"error","message","requestId"}
```

```mermaid
sequenceDiagram
    autonumber
    participant C as Spark driver (key-prefix-lib)
    participant M as dak-mock (pod)
    participant J as sso-lakehouse (JWKS)
    participant V as Vault KV v2

    C->>M: GET http://dak-mock:8085/api/v1/keys/demo_db/users_cdr (Bearer)
    Note over M: Kiểm tên database/table, chuẩn hoá chữ thường — sai → 400
    Note over M: iss (chưa tin) → realm vlp-tenantw1xjixm trong ConfigMap; không có → 401
    M->>J: tải JWKS (có cache)
    Note over M: Chữ ký RS256, exp/iat, iss, typ=Bearer, azp<br/>(realm, azp) có trong ConfigMap và enabled? Không → 401
    M->>V: GET /v1/kv/data/hla-datalake/datalake/spark-application/demo_db.users_cdr
    alt Có secret
        V-->>M: data.data.keyPrefix, metadata.version
        M-->>C: 200 {database, table, keyPrefix, keyVersion}
    else 404
        M-->>C: 403 access_denied
    else Vault từ chối token / không phản hồi, hoặc không tải được JWKS
        M-->>C: 503 upstream_unavailable
    end
```

Các điểm bảo mật trong code: khoá ký chỉ lấy từ `jwks-uri` của ConfigMap (header `jku`/`x5u` của token bị bỏ qua); chỉ
nhận RS256 (từ chối `alg: none`, HMAC); đăng ký client khoá theo `(realm, client_id)`; bảng không có key trả 403 (không dò
được bảng); không log token, secret, prefix.

## 4. Cấu hình (ConfigMap `dak-mock-config`)

Trong [`k8s/dak-mock.yaml`](k8s/dak-mock.yaml). ConfigMap được mount vào `/config` và ghi đè `application.yml` trong jar;
danh sách `realms`/`clients` trong ConfigMap **thay thế** toàn bộ danh sách mặc định.

```yaml
dak:
  realms:
    - realm: vlp-tenantw1xjixm
      tenant: vlp-tenantw1xjixm
      issuer: https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm
      jwks-uri: https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/certs
  clients:
    - realm: vlp-tenantw1xjixm
      client-id: <client-id-cua-team>      # PHẢI SỬA: client vận hành tạo cho team
      workspace: wsw7vtwvi
      team: teamtscauiy
      # enabled: false                     # khoá client -> token của client này bị 401
  vault:
    addr: http://vault.cyberspace.vn
    kv-mount: kv
    path-template: hla-datalake/datalake/spark-application/{database}.{table}
    key-field: keyPrefix
```

- Mỗi client của team là một phần tử trong `clients`. Client phải được tạo trong realm `vlp-tenantw1xjixm` theo checklist
  [docs/DAK_API_SPEC.md mục 2.3](../docs/DAK_API_SPEC.md#23-yêu-cầu-với-client-của-team) (confidential, bật Service accounts,
  tắt Standard flow/Direct access).
- `path-template` trùng path của luồng column/cdr cũ với **tên key hai phần** `<database>.<table>` (chữ thường).
- Pod không khởi động nếu thiếu Secret token Vault, `path-template` thiếu `{database}`/`{table}`, client trỏ tới realm không có,
  hoặc trùng `(realm, client-id)`.

### 4.1. Kiểm `issuer` trước khi deploy

`issuer` phải khớp **từng ký tự** với claim `iss` trong token, nếu không mọi request bị 401. Kiểm từ trong cluster bằng pod
tạm (mục 7) — nếu bản Keycloak dùng context path `/auth` thì `issuer` sẽ có `/auth` và phải sửa cả `jwks-uri`:

```sh
curl -s https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/.well-known/openid-configuration \
  | grep -o '"issuer":"[^"]*"\|"jwks_uri":"[^"]*"'
```

## 5. Chuẩn bị

### 5.1. Secret trong Vault cho từng bảng

```bash
# Dùng token có quyền GHI (không phải token chỉ đọc của mock)
vault kv put kv/hla-datalake/datalake/spark-application/demo_db.users_cdr keyPrefix="<giá trị prefix>"
```

Token mà mock dùng chỉ cần quyền **đọc**:

```hcl
path "kv/data/hla-datalake/datalake/spark-application/*" {
  capabilities = ["read"]
}
```

### 5.2. Secret token Vault trên k8s

```bash
NS=vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy
read -rs -p "Vault token (chỉ đọc): " T; echo
kubectl create secret generic dak-mock-vault-token -n "$NS" --from-literal=token="$T"; unset T
```

## 6. Build và deploy

### 6.1. Test và build image

```bash
mvn -f dak-mock/pom.xml test                                   # 16 test, không cần Keycloak/Vault thật
docker build -t hub.vtcc.vn:8989/dak-mock:0.1.0 dak-mock       # build context là thư mục dak-mock
docker push hub.vtcc.vn:8989/dak-mock:0.1.0
```

Image chạy bằng **user 1000:1000**, jar thuộc root (chỉ đọc). `.dockerignore` loại `target/` và `.env` khỏi build context.
Bước build tải dependency từ Maven Central; nếu máy build chỉ đi qua mirror nội bộ thì thêm `settings.xml` vào bước build.

### 6.2. Deploy

```bash
# Sửa client-id/team trong ConfigMap trước (mục 4)
kubectl apply -f dak-mock/k8s/dak-mock.yaml
kubectl rollout status -n "$NS" deploy/dak-mock
kubectl logs -n "$NS" deploy/dak-mock | grep "Started DakMockApplication"
```

Sửa ConfigMap sau khi đã deploy: `kubectl apply` lại rồi `kubectl rollout restart -n "$NS" deploy/dak-mock`.

| Mục trong manifest | Giá trị |
| --- | --- |
| `resources` | requests `250m`/`512Mi`, limits `1` CPU/`2Gi`; JVM lấy 75% memory limit làm heap |
| Pod `securityContext` | `runAsNonRoot`, `runAsUser/runAsGroup/fsGroup: 1000`, `seccompProfile: RuntimeDefault` |
| Container `securityContext` | `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, `runAsNonRoot`, `runAsUser: 1000`, `readOnlyRootFilesystem: true` (`/tmp` là emptyDir) |
| `automountServiceAccountToken` | `false` |
| Probe | `tcpSocket` cổng `http` |

Pod phải gọi được `sso-lakehouse.cyberspace.vn` (JWKS) và `vault.cyberspace.vn`. Nếu namespace có NetworkPolicy chặn egress
thì mở 2 đích này.

## 7. Kiểm tra trong cluster

Mở pod tạm có `curl`, cùng namespace (đặt securityContext để qua được Pod Security "restricted"; nếu cluster không kéo được
image từ Docker Hub thì thay bằng image có curl ở registry nội bộ):

```bash
kubectl run dak-check -n "$NS" --rm -it --restart=Never --image=curlimages/curl:8.10.1 --overrides='{
  "spec": {
    "securityContext": {"runAsNonRoot": true, "runAsUser": 1000, "seccompProfile": {"type": "RuntimeDefault"}},
    "containers": [{"name": "dak-check", "image": "curlimages/curl:8.10.1", "command": ["sh"], "stdin": true, "tty": true,
      "securityContext": {"allowPrivilegeEscalation": false, "capabilities": {"drop": ["ALL"]}}}]}}'
```

Trong pod:

```sh
# 1. Xin token của team (secret đọc từ bàn phím, đưa vào curl qua stdin)
printf 'Client secret: '; stty -echo; read CS; stty echo; echo
TOKEN=$(printf '%s' "$CS" | curl -s \
  https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=<client-id-cua-team> --data-urlencode client_secret@- \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p'); unset CS

# 2. Gọi mock
curl -s -i http://dak-mock:8085/api/v1/keys/demo_db/users_cdr -H "Authorization: Bearer $TOKEN"
```

Pod tạm tự xoá khi thoát (`--rm`).

| Tình huống                                    | Response                                                                    |
| --------------------------------------------- | --------------------------------------------------------------------------- |
| Token hợp lệ, Vault có secret                 | `200 {"database":"demo_db","table":"users_cdr","keyPrefix":"...","keyVersion":1}` |
| Tên bảng sai (`demo-db`)                      | `400 invalid_table`                                                         |
| Thiếu token, hết hạn, sai chữ ký, `iss` không khớp ConfigMap, client chưa khai/bị khoá | `401 invalid_token`                    |
| Vault không có secret cho bảng                | `403 access_denied`                                                         |
| Secret không có field `keyPrefix` hoặc rỗng   | `500 internal_error`                                                        |
| Vault từ chối token / không phản hồi, hoặc không tải được JWKS | `503 upstream_unavailable`                                 |

Lý do chi tiết chỉ nằm trong log, kèm `requestId`:

```bash
kubectl logs -n "$NS" deploy/dak-mock | grep <requestId>
kubectl logs -n "$NS" deploy/dak-mock | grep KEY_FETCH          # dòng audit, không có prefix
```

Lỗi hay gặp:

| Log | Nguyên nhân |
| --- | --- |
| `401 invalid_token: issuer is not in the realm registry` | `issuer` trong ConfigMap không khớp `iss` của token (mục 4.1) |
| `401 invalid_token: client ... is not registered or is disabled` | `client-id` của team chưa khai trong ConfigMap |
| `503 upstream_unavailable: cannot load signing keys ... PKIX path building failed` | JVM không tin cert của `sso-lakehouse` — xem mục 8 |
| `503 upstream_unavailable: Vault rejected the DAK token` | Token Vault sai, hết hạn hoặc thiếu quyền đọc path |

## 8. Cert nội bộ (chỉ khi cần)

Nếu `sso-lakehouse.cyberspace.vn` dùng cert do CA nội bộ cấp, JVM trong image không tin nên không tải được JWKS (503,
log "PKIX path building failed"). Tạo truststore gồm CA mặc định của JDK **và** CA nội bộ, đưa vào Secret:

```bash
# internal-ca.pem: CA nội bộ (lấy từ team hạ tầng). Bắt đầu từ cacerts của JDK để vẫn tin các CA công khai.
cp "$JAVA_HOME/lib/security/cacerts" truststore.p12
keytool -importcert -noprompt -alias internal-ca -file internal-ca.pem \
  -keystore truststore.p12 -storetype PKCS12 -storepass changeit
kubectl create secret generic dak-mock-truststore -n "$NS" --from-file=truststore.p12
```

Rồi bỏ comment 3 khối `truststore` / `JAVA_TOOL_OPTIONS` trong `k8s/dak-mock.yaml`, apply và restart.

## 9. Nối với Spark

Spark chạy cùng namespace nên dùng `http://dak-mock:8085`. Mock chạy HTTP nên phải bật `allowInsecureHttp`.

SparkApplication: [k8s/spark-application-cdr.yaml](../k8s/spark-application-cdr.yaml) và
[k8s/spark-application-column.yaml](../k8s/spark-application-column.yaml) đã trỏ sẵn tới mock:

```yaml
- name: CRYPTO_PREFIX_SOURCE
  value: "dak"
- name: DAK_ADDR
  value: "http://dak-mock:8085"
- name: DAK_ALLOW_INSECURE_HTTP
  value: "true"
- name: DAK_TOKEN_URL
  value: "https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/token"
- name: DAK_CLIENT_ID
  value: "<client_id của team>"            # PHẢI SỬA, trùng client-id trong ConfigMap của mock
# DAK_CLIENT_SECRET lấy từ Secret spark-dak-client (key clientSecret)
```

sql-engine (Spark conf trên màn hình tạo engine):

```properties
spark.columncrypto.source=dak
spark.columncrypto.dak.addr=http://dak-mock:8085
spark.columncrypto.dak.tokenUrl=https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/token
spark.columncrypto.dak.clientId=<client_id của team>
spark.columncrypto.dak.clientSecret=<client secret>
spark.columncrypto.dak.allowInsecureHttp=true
```

(sql-engine cùng namespace `vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy` nên dùng `http://dak-mock:8085`; nếu engine nằm ở
namespace khác thì dùng `http://dak-mock.vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy.svc.cluster.local:8085`.)

Thử: `SELECT cdr_decrypt('demo_db.users_cdr', name, created_at) FROM demo_db.users_cdr LIMIT 5`. Luồng lấy key chỉ chạy khi
câu SQL có hàm crypto được phân tích, không chạy lúc engine khởi động.

## 10. Lưu ý khi mở bằng IDE

`dak-mock` là project Maven riêng (Java 17). Nếu IDE mở thư mục gốc như project Spark (Java 11) sẽ báo lỗi cú pháp giả ở các
`record`. Import `dak-mock/pom.xml` như một project riêng.
