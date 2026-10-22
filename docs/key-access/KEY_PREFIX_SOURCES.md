# Nguồn keyPrefix: file / Vault / DAK, và cách nạp lib crypto (bài toán 2 + 3)

> Tách ra từ README gốc. Hạ tầng chung `key-prefix-lib` (`PrefixSourceFactory`) cũng cấp khoá cho `pseudonym-udf`
> (bài toán 4; tên khoá `pseudonym.hmac_v<N>` / `pseudonym.aes_siv_v<N>`, xem `pseudonymize-table-app/README.md` mục 4).

## Mã hoá cột — plugin, không nhúng vào jar app

Jar của `spark-app` **không chứa lib crypto nào**. Mã hoá là plugin nạp lúc deploy, gồm 3 mảnh khớp nhau:

| Mảnh              | Ở đâu                                                                                                    |
| ----------------- | -------------------------------------------------------------------------------------------------------- |
| Jar lib crypto    | `Dockerfile` COPY vào `/opt/app/` theo target (`column-crypto` hoặc `cdr-crypto`, xem BUILD_AND_TEST.md) |
| Nạp + đăng ký hàm | `sparkConf` trong manifest: `spark.jars` (đường dẫn jar) + `spark.sql.extensions` (class extension)      |
| App gọi hàm nào   | env `CRYPTO_ENCRYPT_FUNCTION`/`CRYPTO_DECRYPT_FUNCTION`/`CRYPTO_ENCRYPTED_COLUMNS`/`CRYPTO_KEY_FIELD`    |

App (`CryptoStep`) chỉ gọi `fn('<tên bảng>', <cột>, <cột keyField>)` theo TÊN hàm SQL — hàm nào có mặt phụ thuộc
extension nào được nạp; app không biết và không biên dịch với lib nào. (Bản thân các lib vẫn hỗ trợ thêm DataFrame API —
`ColumnCrypto`, `CdrCrypto` — cho code khác dùng, `spark-app` không bắt buộc dùng.)
Muốn đổi loại crypto: đổi target image + manifest tương ứng. Không nạp extension
mà vẫn đặt tên hàm thì job fail sớm với `AnalysisException: Undefined function`.

| Biến thể      | Image target    | Manifest                            | Jar nạp (`spark.jars`)                                       | Hàm SQL app gọi                         |
| ------------- | --------------- | ----------------------------------- | ------------------------------------------------------------ | --------------------------------------- |
| Không mã hoá  | `no-crypto`     | (bỏ các biến `CRYPTO_*_FUNCTION`)   | không                                                        | —                                       |
| column-crypto | `column-crypto` | `k8s/spark-application-column.yaml` | `key-prefix-lib` + `column-crypto-lib`                       | SQL `column_encrypt` / `column_decrypt` |
| cdr-crypto    | `cdr-crypto`    | `k8s/spark-application-cdr.yaml`    | `key-prefix-lib` + `cdr-crypto-udf` + `DataLakeSecurity_jv8` | SQL `cdr_encrypt` / `cdr_decrypt`       |

Cả hai hàm cùng hợp đồng 3 tham số `fn(tableName, value, keyValue)`: `tableName` là hằng chuỗi (nơi extension tra keyPrefix),
`value` là cột cần xử lý, `keyValue` là giá trị cột `CRYPTO_KEY_FIELD` của chính dòng đó.

**`column_encrypt`/`column_decrypt`** (module `column-crypto-lib`): AES-256-GCM qua hàm built-in `aes_encrypt`/`aes_decrypt` của
Spark SQL (có từ Spark 3.3) — không tự viết crypto tay. Key sinh **riêng cho từng dòng**:

```
key = SHA-256(keyPrefix || giá trị cột keyField của chính dòng đó)
```

Công thức nằm ở `CryptoExpressions`, dùng chung cho **DataFrame API** (`ColumnCrypto.encryptColumns`, thư viện vẫn giữ và có test) lẫn
**SQL function** `column_encrypt`/`column_decrypt` (dùng trên query console của sql-engine, xem
`docs/column-crypto/COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md`), nên dữ liệu ghi bằng đường này giải mã được bằng đường kia.

**`cdr_encrypt`/`cdr_decrypt` và `CdrCrypto`** (module `cdr-crypto-udf`): thuật toán do đối tác quy định (AES-128-ECB, xem mục 1.3 và
`docs/cdr-crypto/PARTNER_CDR_CRYPTO_UDF_PLAN.md`), là UDF thật nên **executor cũng cần đủ 3 jar**. Hỗ trợ cả 2 cách dùng, cùng 1 hàm biến đổi
(`CdrCrypto.transform`) nên kết quả giống hệt nhau:

```scala
// DataFrame API (mỗi cột một key, xin qua DAK by-column theo tên cột của DataFrame)
val enc = CdrCrypto.encryptColumns(df, "demo_db.sub_rel_product", keyField = "start_datetime", Seq("isdn"))
val dec = CdrCrypto.decryptColumns(enc, "demo_db.sub_rel_product", keyField = "start_datetime", Seq("isdn"))
// SQL:  SELECT cdr_encrypt('demo_db.sub_rel_product', 'isdn', isdn, start_datetime) FROM t
```

## keyPrefix — hạ tầng dùng chung `key-prefix-lib`

Cả 2 lib lấy keyPrefix qua `key-prefix-lib`, đọc cấu hình theo thứ tự ưu tiên: `spark.<lib>.*` (Spark conf — cách sql-engine
cấu hình) rồi tới **cùng một bộ biến môi trường** `CRYPTO_PREFIX_SOURCE`, `CRYPTO_KEY_PREFIX_DIR`, `VAULT_*`... (cách SparkApplication
cấu hình). Vì vậy 1 khối env trong manifest cấu hình cho cả `column_*` lẫn `cdr_*`, và app không cần biết gì về nguồn prefix.
Mỗi bảng có 1 prefix — 2 hàm dùng chung sẽ lấy cùng giá trị từ cùng path/field Vault (tách riêng bằng `spark.cdrcrypto.*` nếu sau này cần).

```yaml
# CRYPTO_KEY_PREFIX_DIR — K8s Secret "key-prefix" (NHẠY CẢM, KHÔNG commit giá trị thật).
# data của Secret là <tên bảng>: <keyPrefix của bảng đó>; mount thành thư mục, mỗi key 1 file.
apiVersion: v1
kind: Secret
metadata:
  name: key-prefix
stringData:
  demo_db.sample_table: "sample_table_dev_prefix" # -> file /etc/key-prefix/demo_db.sample_table (tên key = DB_NAME.TABLE_NAME)
```

Ký tự xuống dòng ở cuối file được bỏ khi đọc. Tên bảng chỉ được gồm `[A-Za-z0-9_.-]` và không
bắt đầu bằng `.` (chặn path traversal vì tên bảng đi vào đường dẫn file). Thông báo lỗi không bao giờ in
giá trị `keyPrefix`.

- `keyPrefix` là bí mật thật sự (gần tương đương credential) — `keyField` chỉ là tên cột
  (config), nhưng giá trị của nó vốn công khai với bất kỳ ai đọc được bảng, nên toàn bộ độ an
  toàn của cơ chế này quy về việc giữ kín `keyPrefix`. Không commit giá trị thật của `keyPrefix` vào git.

  Manifest mẫu lấy `keyPrefix` qua DAK (`CRYPTO_PREFIX_SOURCE=dak`, chỉ driver cần; không còn env `VAULT_*`). Cột mã hoá khai
  thẳng bằng env (`CRYPTO_ENCRYPTED_COLUMNS`, `CRYPTO_KEY_FIELD`) — **không còn** ConfigMap
  `column-crypto-settings` / `CRYPTO_CONFIG_PATH`.

  **Nguồn `file` (K8s Secret)** — dùng khi không có Vault: đặt `CRYPTO_PREFIX_SOURCE=file`,
  `CRYPTO_KEY_PREFIX_DIR=/etc/key-prefix` và mount Secret `key-prefix` (`k8s/key-prefix-secret.yaml`,
  mỗi key = tên bảng, giá trị mẫu — thay bằng giá trị thật, không commit) vào thư mục đó cho driver.
  Lưu ý K8s Secret **chỉ base64-encode chứ không tự mã hoá** trong `etcd` (trừ khi cluster đã bật
  riêng encryption-at-rest cho Secret), và không chặn được ai đã `kubectl exec` vào pod đang chạy —
  nó chỉ tách quyền đọc `keyPrefix` khỏi code/image, không phải kiểm soát truy cập theo từng lần
  decrypt như Ranger KMS.

  **Che khỏi plan (`column_*`):** `keyPrefix` là literal trong biểu thức SQL nên mặc định sẽ hiện trong
  `explain`/`queryExecution.toString` (tab SQL của Spark UI, event log). `ColumnCryptoExtension` tự đặt
  `spark.sql.redaction.string.regex` ngay khi resolve hàm, nên các chỗ đó chỉ hiện `*********(redacted)`.
  (`cdr_*` để prefix trong closure của UDF chứ không phải literal của plan, nên không cần bước này.)

## Chọn nguồn keyPrefix (`CRYPTO_PREFIX_SOURCE`)

| Giá trị      | Hành vi                                                                                                          |
| ------------ | ---------------------------------------------------------------------------------------------------------------- |
| `dak`        | **(đang triển khai)** lấy key qua DAK: token Keycloak `client_credentials` (`DAK_*`), tên key `'database.table'` |
| `file`       | (mặc định) đọc `<CRYPTO_KEY_PREFIX_DIR>/<tên bảng>` — K8s Secret mount                                           |
| `vault`      | gọi Vault KV v2 (`VAULT_*`); giữ lại làm **fallback thủ công** khi DAK sự cố                                     |
| `file,vault` | thử `file` trước, không có/lỗi thì fallback sang `vault`; lỗi gộp lý do của cả hai nguồn                         |

`dak` **không được ghép** với nguồn khác (`dak,vault` bị từ chối): fallback tự động chạy cả khi DAK trả 403, tức là
bỏ qua phân quyền. Khi DAK sự cố, vận hành đổi `CRYPTO_PREFIX_SOURCE=vault`, thêm lại các env `VAULT_*` rồi chạy lại job
(mục "Lấy keyPrefix từ DAK").

Kết quả được cache trong bộ nhớ theo bảng, `CRYPTO_CACHE_TTL_SECONDS` (mặc định 300, `0` = tắt cache).
Cùng các key logic này, sql-engine cấu hình qua Spark conf `spark.columncrypto.<key>` (hoặc `spark.cdrcrypto.<key>`) thay vì env
(`source`, `file.dir`, `vault.addr`, `vault.role`, `vault.kvBasePath`, `vault.authMount`,
`vault.kvMount`, `vault.keyField`, `vault.jwtPath`, `vault.authMethod`, `vault.token`, `cacheTtlSeconds`,
`dak.addr`, `dak.tokenUrl`, `dak.clientId`, `dak.clientSecret`, `dak.allowInsecureHttp`, `dak.tag`, `dak.caller`).

## Lấy keyPrefix từ DAK (`CRYPTO_PREFIX_SOURCE=dak`)

`DakPrefixSource` (module `key-prefix-lib`) chạy trên driver: xin access token `client_credentials` tại `DAK_TOKEN_URL`
(Keycloak, realm của tenant), cache token tới gần hết hạn, rồi gọi `GET <DAK_ADDR>/api/v1/keys/<database>/<table>`.
DAK trả 401 thì xin token mới và thử lại đúng 1 lần; 403 và mọi lỗi khác báo ngay, kèm `requestId` để tra audit của DAK.

DAK kiểm tra quyền ở mỗi request, nhưng prefix đã cache vẫn được dùng tới hết `CRYPTO_CACHE_TTL_SECONDS`, nên thu hồi quyền chỉ có
hiệu lực sau tối đa TTL đó. Đặt `0` để lần lấy prefix kế tiếp lỗi ngay. Với SparkApplication, prefix được lấy một lần lúc job phân tích
câu lệnh, nên job đang chạy không bị dừng khi quyền bị thu hồi. Chi tiết và đánh đổi trên sql-engine:
[`CDR_CRYPTO_SQL_ENGINE_GUIDE.md`](../cdr-crypto/CDR_CRYPTO_SQL_ENGINE_GUIDE.md#26-thu-hồi-quyền-trên-dak-và-cachettlseconds) mục 2.6.

| Biến                       | Bắt buộc | Ghi chú                                                                                 |
| -------------------------- | -------- | --------------------------------------------------------------------------------------- |
| `DAK_ADDR`                 | Có       | `https://` (http chỉ khi `DAK_ALLOW_INSECURE_HTTP=true`, dùng cho test local)           |
| `DAK_TOKEN_URL`            | Có       | `https://<keycloak>/realms/<realm của tenant>/protocol/openid-connect/token`            |
| `DAK_CLIENT_ID`            | Có       | Client Keycloak của team, vận hành cấp                                                  |
| `DAK_CLIENT_SECRET`        | Có       | Lấy từ K8s Secret `spark-dak-client` (key `clientSecret`) qua `secretKeyRef`            |
| `DAK_ALLOW_INSECURE_HTTP`  | Không    | `true` cho phép `http://` — chỉ khi test với dak-mock (`DAK_ADDR=http://dak-mock:8085`) |
| `CRYPTO_CACHE_TTL_SECONDS` | Không    | Cache keyPrefix (mặc định 300 s); token Keycloak được cache riêng tới gần hết hạn       |

Keycloak nội bộ (`sso-lakehouse.cyberspace.vn`) dùng cert tự ký, nên pod driver cần thêm `hostAliases` (IP `10.221.148.42`),
initContainer `build-truststore` (ConfigMap `keycloak-ca`, key `ca.pem`) và `-Djavax.net.ssl.trustStore=…` trong
`spark.driver.extraJavaOptions`. Hai manifest trong `k8s/` đã có sẵn; initContainer phải khai `resources.limits` vì
ResourceQuota của namespace bắt buộc, và `keycloak-ca` phải được mount cả ở container chính để webhook của Spark Operator
đưa volume vào pod ([`COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md`](../column-crypto/COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md) mục 4.7).

Tên key gửi lên DAK là **`database.table`**, chữ thường (`spark-app` tự ghép `DB_NAME.TABLE_NAME`; trên sql-engine là tham
số đầu của hàm, vd `cdr_decrypt('demo_db.users_cdr', ...)`). Tên một phần (`'users_cdr'`) bị từ chối khi `source=dak`.

**Fallback thủ công** khi DAK sự cố: đổi `CRYPTO_PREFIX_SOURCE=vault`, thêm lại các env `VAULT_*` (manifest mẫu đã bỏ chúng,
xem [`SPARK_APP_REFERENCE.md`](../project/SPARK_APP_REFERENCE.md)) và Secret token Vault, rồi chạy lại job. Fallback đọc secret `<VAULT_KV_PATH>/<database>.<table>` ở Vault cũ và **bỏ qua** phân quyền, thời hạn quyền của
DAK — chỉ dùng khi DAK sự cố và quay lại `dak` ngay khi DAK phục hồi. Thiết kế và quy trình: `docs/key-access/DAK_KEY_ACCESS_SQL_ENGINE_PLAN.md`,
hợp đồng API: `docs/key-access/DAK_API_SPEC.md`.

## Key theo cột qua DAK `by-column` (`ColumnKeySource`)

Ngoài key cấp **bảng** ở trên, `key-prefix-lib` có nguồn key cấp **cột**: mỗi cột của bảng một key, lấy qua
`GET <DAK_ADDR>/api/v1/keys/by-column`. Thiết kế: [`DAK_BY_COLUMN_CLIENT_PLAN.md`](DAK_BY_COLUMN_CLIENT_PLAN.md); mock để test:
[`dak-mock/README.md`](../../dak-mock/README.md) mục 2.1.

> **Bên dùng:** `cdr_encrypt`/`cdr_decrypt` (4 tham số) và `CdrCrypto` DataFrame API của `cdr-crypto-udf` đọc nguồn này
> ([`CDR_CRYPTO_SQL_ENGINE_GUIDE.md`](../cdr-crypto/CDR_CRYPTO_SQL_ENGINE_GUIDE.md)). `column_*` vẫn dùng key cấp bảng ở trên.

Khác với key cấp bảng:

| Điểm               | Key theo cột                                                                                                         |
| ------------------ | -------------------------------------------------------------------------------------------------------------------- |
| Nguồn              | **Chỉ `dak`**. `source` khác (`file`, `vault`, hoặc không đặt) bị từ chối: không có key cột nào ở file hay Vault cũ   |
| Client             | **Client nội bộ** DAK cấp cho luồng này, và phải đặt `dak.tag=internal` (`DAK_TAG=internal`)                         |
| Mã hoá / giải mã   | Mã hoá gửi `onMissing=request`: cột chưa có key thì DAK mở một yêu cầu (YC). Giải mã gửi `onMissing=none`           |
| Kết quả            | Chỉ dùng được khi **mọi** cột được hỏi đều có key; một cột chưa có thì cả lời gọi lỗi, nêu từng cột và mã YC         |
| Cache              | Theo `(bảng, cột)`, cùng `cacheTtlSeconds` (mặc định 300). Key cột không bao giờ đổi, TTL chỉ giới hạn thời gian một key đã bị khoá trên DAK còn dùng được |

Hai cấu hình mới (đọc chung bởi nguồn `dak` của cả key bảng lẫn key cột):

| Key logic    | sql-engine                  | SparkApplication | Ý nghĩa                                                                                       |
| ------------ | --------------------------- | ---------------- | --------------------------------------------------------------------------------------------- |
| `dak.tag`    | `spark.cdrcrypto.dak.tag`    | `DAK_TAG`        | Client nội bộ: `internal`. Client team: để trống. Chỉ chữ thường, số, `_`, `-`               |
| `dak.caller` | `spark.cdrcrypto.dak.caller` | `DAK_CALLER`     | Tuỳ chọn. Gửi làm header `X-Dak-Caller` (tên engine/job), DAK chỉ dùng để ghi audit. `[A-Za-z0-9._-]{1,128}` |

Giá trị sai định dạng làm lib báo lỗi cấu hình ngay khi dựng nguồn (thông báo nêu tên key, không in lại giá trị).

**Một client cho một job SparkApplication.** Client nội bộ không thuộc team nào nên gọi đường theo bảng `/{database}/{table}` sẽ nhận
`403`. Trên SparkApplication, `column_*` và `cdr_*` đọc **chung** bộ `DAK_*`, nên một job không dùng được cả `column_encrypt` (cần
client team) lẫn `cdr_encrypt` theo cột (cần client nội bộ). Trên sql-engine không vướng vì hai bộ hàm có namespace riêng
(`spark.columncrypto.*` / `spark.cdrcrypto.*`).

Lỗi thường gặp:

| Thông báo chứa                                   | Nguyên nhân / cách xử lý                                                                                         |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `Column keys are only available from DAK`        | `source` chưa đặt `dak`                                                                                          |
| `HTTP 400 invalid_tag` … `check the dak.tag`     | Client nội bộ chưa đặt `dak.tag=internal`, hoặc client team lại đặt `dak.tag`                                    |
| `HTTP 403 access_denied` … `grant`               | Client team thiếu grant còn hạn trên ít nhất một cột                                                             |
| `'<cột>' PENDING (requestCode=YC-…)`             | Cột chưa có key, DAK đã mở YC. Người có quyền bên DAK tạo key rồi chạy lại (dak-mock: `POST /mock-admin/yc/{code}/fulfill`) |
| `'<cột>' MISSING (reason=not_requested)`         | Khi giải mã: sai tên bảng/cột, hoặc cột chưa từng được mã hoá                                                    |
| `'<cột>' MISSING (reason=yc_limit_reached)`      | Bảng đã có quá nhiều YC đang mở; chờ DAK xử lý rồi thử lại                                                       |
| `'<cột>' REJECTED`                               | YC bị DAK từ chối; không thử lại, liên hệ DAK                                                                    |
| `'<cột>' DISABLED`                               | Key của cột đang bị khoá trên DAK                                                                                |
| `invalid by-column response`                     | DAK trả sai hợp đồng (thiếu/thừa cột, lệch bảng, trạng thái lạ); gửi `requestId` cho DAK                         |

Thông báo lỗi không bao giờ chứa token, secret hay key, chỉ mã lỗi, `requestId`, tên bảng/cột và mã YC.

## Lấy keyPrefix từ HashiCorp Vault (`CRYPTO_PREFIX_SOURCE=vault`)

`VaultPrefixSource` (module `key-prefix-lib`, chỉ dùng JDK `HttpClient` + Jackson có sẵn của Spark) làm 3 bước trên driver:
đăng nhập Kubernetes auth bằng JWT của service account (`POST /v1/auth/<auth-mount>/login`),
đọc KV v2 (`GET /v1/<kv-mount>/data/<VAULT_KV_PATH>/<tên bảng>`, lấy field `VAULT_KEY_FIELD`), rồi thu
hồi token (`POST /v1/auth/token/revoke-self`). Lỗi không bao giờ in JWT, token hay giá trị prefix.

Với path hiện tại `kv/hla-datalake/datalake/spark-application/<tên bảng>` (KV v2 — không có đoạn
`key-prefix` trong path), mỗi secret phải có field `keyPrefix` (đổi bằng `VAULT_KEY_FIELD`).
Cấu hình phía Vault (làm 1 lần, chỉ cần khi dùng Kubernetes auth — xem "Xác thực bằng token Vault
tĩnh" bên dưới nếu Vault dùng token tĩnh như hạ tầng hiện tại):

```bash
vault auth enable kubernetes
vault write auth/kubernetes/config kubernetes_host="https://<K8S_API_SERVER>:443"   # + token_reviewer_jwt / kubernetes_ca_cert nếu Vault chạy ngoài cluster
vault policy write spark-key-prefix - <<'EOF'
path "kv/data/hla-datalake/datalake/spark-application/*" { capabilities = ["read"] }
EOF
vault write auth/kubernetes/role/spark-application \
  bound_service_account_names=spark-application-sa \
  bound_service_account_namespaces=vlp-tenantw1xjixm-wsytjjtr0-ingestion \
  policies=spark-key-prefix ttl=5m
vault kv put kv/hla-datalake/datalake/spark-application/sample_table keyPrefix='<giá trị thật>'
```

### Xác thực bằng token Vault tĩnh (`VAULT_AUTH_METHOD=token`)

Mặc định `VaultPrefixSource` dùng Kubernetes auth (ở trên). Khi Vault không bật Kubernetes auth cho
app — ví dụ ExternalSecrets `ClusterSecretStore` đang dùng `tokenSecretRef` — có thể dùng thẳng một
**token Vault tĩnh**:

| Env (SparkApplication)    | Spark conf (sql-engine)               | Ý nghĩa                                                   |
| ------------------------- | ------------------------------------- | --------------------------------------------------------- |
| `VAULT_AUTH_METHOD=token` | `spark.columncrypto.vault.authMethod` | `kubernetes` (mặc định) hoặc `token`                      |
| `VAULT_TOKEN`             | `spark.columncrypto.vault.token`      | Token Vault; bắt buộc khi `token`. Không cần `VAULT_ROLE` |

Ở chế độ này lib **không login và không `revoke-self`** (revoke một token dùng chung sẽ làm hỏng mọi
dịch vụ đang dùng nó, kể cả ExternalSecrets). Token không bao giờ xuất hiện trong thông báo lỗi và
`VaultConfig.toString` che nó. Spark tự che conf có chứa `token` trong tab Environment và log
(`spark.redaction.regex`), nhưng giá trị vẫn nằm plaintext trong cấu hình engine/manifest — nên đưa
vào `SparkApplication` bằng `env.valueFrom.secretKeyRef` thay vì ghi thẳng.

**Không nên dùng lại token của ExternalSecrets** (`datalake-vault-token`): store báo `ReadWrite` nên
token đó có quyền ghi, rộng hơn nhiều so với việc chỉ đọc `keyPrefix`. Tạo token riêng, chỉ đọc:

```bash
vault policy write spark-key-prefix - <<'POLICY'
path "kv/data/hla-datalake/datalake/spark-application/*" { capabilities = ["read"] }
POLICY
# Token định kỳ: sống thêm mỗi lần renew; lib KHÔNG tự renew nên phải renew định kỳ hoặc đặt period đủ dài rồi rotate
vault token create -policy=spark-key-prefix -period=768h -orphan -display-name=spark-column-crypto
```

Token có TTL sẽ hết hạn → mọi query mã hoá/giải mã lỗi `HTTP 403` sau TTL cache; cần quy trình
rotate (hoặc chuyển sang Kubernetes auth/AppRole để không phải giữ credential dài hạn).

`VAULT_ADDR` hiện là HTTP nên token và prefix đi trên mạng không mã hoá — app log cảnh báo; nên
chuyển sang HTTPS (JDK dùng truststore mặc định, nếu Vault dùng CA riêng thì cần bổ sung truststore).

- `keyField` bắt buộc không nằm trong `encryptedColumns` (validate lúc chạy, fail-fast) —
  nó phải giữ plaintext để dùng lại làm nguyên liệu sinh key lúc giải mã.
- Cột đã mã hoá lưu dạng `STRING` (Base64 của ciphertext, IV do `aes_encrypt` tự sinh và gắn
  kèm) — DDL hiện tại (`name`/`city`) đã sẵn `STRING` nên không cần đổi; nếu sau này mã hoá
  cột kiểu khác (số, ngày...), DDL phải khai `STRING` cho cột đó.
