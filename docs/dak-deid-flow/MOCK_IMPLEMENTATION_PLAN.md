# Plan: hiện thực flow phiếu khử định danh (PYC) trong `dak-mock`

Ngày: 2026-10-23 (sửa lần 4: `dagId` do người dùng chọn, DAK đọc key bảng từ Vault rồi giao cho job, mã phiếu `PYC-`). Phạm vi: module
[`dak-mock/`](../../dak-mock/README.md) (Spring Boot 3.4, Java 17). Hợp đồng bên ngoài ở [API_CONTRACT.md](./API_CONTRACT.md); quyết định đã chốt và việc còn mở ở
[README.md](./README.md) mục 4–5.

Bám khuôn của luồng `by-column` đã có: [DAK_MOCK_BY_COLUMN_PLAN.md](../dak-mock-api/DAK_MOCK_BY_COLUMN_PLAN.md) (cấu hình `dak.*` kiểm lúc khởi động,
registry trong bộ nhớ, endpoint `/mock-admin/**`, test bằng MockWebServer).

## 1. Nguyên tắc

- **Mock đóng vai portal bằng HTTP.** DAK thật có giao diện đăng nhập + upload; mock không dựng giao diện mà mở `/mock-portal/**` (upload file, xem, huỷ)
  và `/mock-admin/deid-requests/**` (duyệt) để chạy thử bằng `curl`. Mọi hành vi nghiệp vụ nằm ở `DeidRequestService`, nên lớp HTTP chỉ là vỏ mỏng.
- **Phiếu thuộc team, không theo user** (Q3). Tài khoản giả chỉ để biết phiếu thuộc team nào và ghi audit `username`.
- **Không đụng key cột; chỉ đọc key theo bảng ở bước duyệt** (Q7, Q13), bằng `VaultKeyReader` có sẵn. Upload không gọi Vault. Job chạy thẳng trên bảng như nó đang có (Q1).
- **Key đi tới job bằng tham chiếu, không bằng giá trị.** Mock tạo Secret riêng cho phiếu và chỉ truyền **tên Secret** cho job; key không vào phiếu, `conf`, env, log, audit hay response.
- **Không dùng Keycloak để xác thực người dùng phiếu.** Người dùng là tài khoản nội bộ DAK. Keycloak chỉ được dùng ở một chỗ mới: lấy token `client_credentials` để gọi Airflow (mục 4.7).
  `TokenVerifier`, `dak.clients`, `dak.grants` không đụng tới.
- **Không đụng hành vi cũ.** `KeyController`, `ColumnKeyController`, `YcRegistry`, `VaultKeyReader`, `/mock-admin/yc/**` giữ nguyên. Code mới nằm trong package `vai.lakehouse.dakmock.deid`.
- **Bất biến.** Phiếu là `record`; mỗi chuyển trạng thái trả bản **mới**, registry thay bản cũ. Không `setter`.
- **Gửi job nằm sau một giao diện** `JobSubmitter` với 3 hiện thực (`noop`, `kubernetes`, `airflow`), chọn bằng cấu hình. `noop` làm trước nên
  toàn bộ luồng phiếu test được mà không cần cluster hay Vault.
- **Người dùng chỉ điều khiển được** `database`, `table`, cột, `mode` (đã qua bộ ký tự) và `dagId` (nằm trong danh sách cho phép); tên job, namespace, nhãn do mock đặt đè lên template.
  `purpose` và tên file không bao giờ vào manifest.
- Mỗi file < 400 dòng, mỗi hàm < 50 dòng, không nesting > 4 tầng.

## 2. Cấu hình (`dak.deid`)

```yaml
dak:
  deid:
    enabled: true # false: mọi endpoint /mock-portal/deid-* và /mock-admin/deid-requests trả 404
    allowed-modes: [HASH] # Q5. Bản đầu chỉ chấp nhận HASH (ENCRYPT/MAPPING cần thêm vật liệu key, README O2)
    max-active-per-team: 20
    max-upload-bytes: 65536 # 64 KB, khớp spring.servlet.multipart.max-file-size / max-request-size
    output-database: "" # Q6. Trống = cùng database nguồn
    output-table-template: "{table}_deid_{yyyyMMdd}_{seq}" # phải chứa {table} và {seq}; {seq} = số cuối của mã phiếu, không đệm 0
    submitter: noop # noop | kubernetes | airflow
    kubernetes: # bắt buộc khi submitter=kubernetes HOẶC airflow (cả hai đều tạo Secret key)
      api-server: https://kubernetes.default.svc
      namespace: vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy # nơi tạo Secret key (và SparkApplication khi submitter=kubernetes)
      template-file: /deid/sparkapplication.yaml # chỉ cần khi submitter=kubernetes; mount từ ConfigMap dak-mock-deid-template
    airflow:
      base-url: http://airflow-webserver.airflow.svc.cluster.local:8080 # CHƯA CÓ GIÁ TRỊ THẬT — điền khi chạy thật (README O1)
      allowed-dag-ids: [deid_pseudonymize_table] # Q12/O3: dagId người dùng chọn PHẢI nằm trong danh sách này; bắt buộc không rỗng khi submitter=airflow
      auth: client-credentials # client-credentials (Q9, mặc định) | bearer (token tĩnh, chỉ để thử nhanh)
      token-url: https://sso-lakehouse.cyberspace.vn/realms/vlp-tenantw1xjixm/protocol/openid-connect/token # vận hành sẽ truyền giá trị thật (README O1)
      client-id: <client-airflow> # vận hành sẽ truyền giá trị thật; client client_credentials tạo riêng cho luồng này
      scope: "" # tuỳ chọn; vận hành sẽ truyền nếu cần
      # client secret KHÔNG nằm ở đây: env AIRFLOW_CLIENT_SECRET (Secret dak-mock-airflow); với auth=bearer: env AIRFLOW_TOKEN
    # Tài khoản giả của portal. NHẠY CẢM (có token) nên KHÔNG nằm trong ConfigMap: đặt trong Secret dak-mock-portal-users (mục 6).
    portal-users:
      - username: hungvt
        team: teamtscauiy
        # tenant / workspace: chỉ cần nếu dak.vault.path-template có {tenant}/{workspace} (mặc định không có)
        token: <chuoi-ngau-nhien>
```

Key bảng được đọc bằng cấu hình `dak.vault.*` **có sẵn** (`addr`, `token`, `kv-mount`, `path-template`, `key-field`), cùng path với luồng `GET /api/v1/keys/{database}/{table}`
(mặc định `hla-datalake/datalake/spark-application/{database}.{table}`, field `keyPrefix`). Token Vault của mock đã có quyền đọc path này.

`DakProperties` kiểm thêm **lúc khởi động** (sai thì ứng dụng không chạy, như các khối khác):

- `allowed-modes` không rỗng; **chỉ giá trị `HASH` được chấp nhận** (`ENCRYPT`/`MAPPING` làm ứng dụng không khởi động, kèm lý do). `output-table-template` chứa `{table}` và `{seq}`;
  `max-active-per-team > 0`; `max-upload-bytes` trong 1 KB..1 MB.
- `portal-users`: `username` khớp `[a-z0-9][a-z0-9_.-]{0,62}` và không trùng; `team` khớp `[a-z0-9][a-z0-9_-]{0,62}`; `token` dài **24–256 ký tự ASCII hiển thị, không có khoảng trắng** và không trùng
  giữa các user. Nếu `dak.vault.path-template` chứa `{tenant}`/`{workspace}` thì mọi user phải có `tenant`/`workspace` hợp lệ. Danh sách rỗng = portal giả tắt (mọi lời gọi `404`), giống `dak.admin.token` rỗng.
- `submitter=kubernetes` hoặc `airflow`: `kubernetes.namespace` không rỗng (nơi tạo Secret key). `api-server` là URL http(s) (mặc định `https://kubernetes.default.svc`), `namespace` là nhãn DNS-1123. Riêng `kubernetes`: `template-file` không rỗng (D1); việc **đọc, parse và kiểm placeholder** của file làm ở D6 (mục 4.5).
- `submitter=airflow`: `base-url` không rỗng, **`allowed-dag-ids` không rỗng** và mỗi phần tử khớp `[A-Za-z0-9_][A-Za-z0-9_.-]{0,249}` (không bắt đầu bằng `.` hay `-`, nên loại `.` và `..`), không trùng; `base-url` là URL http(s). `auth=client-credentials` cần thêm `token-url`, `client-id` và env
  `AIRFLOW_CLIENT_SECRET` không rỗng; `auth=bearer` cần env `AIRFLOW_TOKEN`. Thiếu thì từ chối khởi động.
- `toString()` của các khối có secret in `***`, giống `Vault` và `Admin`.

## 3. Lớp mới và lớp sửa

| Lớp                                        | Việc                                                                                                                                                                         |
| ------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `deid/DeidRequest` (record)                | Phiếu bất biến + các hàm `with…` cho từng chuyển trạng thái; `State` enum và tập trạng thái cuối                                                                             |
| `deid/DeidRequestRegistry`                 | Lưu phiếu trong bộ nhớ, thread-safe. `create`, `transition(code, nguồn, hàm)` là **compare-and-set** nguyên tử; đếm phiếu hoạt động theo team; chống trùng                     |
| `deid/DeidUploadParser`                    | Parse file upload thành `DeidUpload`: kiểm kích thước, JSON nghiêm ngặt, không trường lạ; kiểm `dagId` với allowlist; **gom mọi lỗi** thành `problems` (mục 4.1). Dùng lại `ColumnKeyRequest.tableName/columnName` (chỉ để kiểm tên) |
| `deid/DeidRequestService`                  | Nghiệp vụ: upload, huỷ (người dùng), duyệt/từ chối/retry (admin), làm mới trạng thái job, audit. **Mọi quy tắc nằm ở đây**                                                     |
| `deid/PortalUsers`                         | Tra tài khoản giả theo token, so sánh hằng thời gian; trả `PortalUser(username, team, tenant, workspace)`                                                                    |
| `deid/KeyDelivery` (interface)             | `KeyHandle deliver(DeidRequest)` trả **tên Secret và `keyVersion`** (`keyVersion` là `null` với `noop`, nên phiếu không hiện trường này) và `void release(DeidRequest)`; lỗi ném `KeyDeliveryException` với thông điệp cố định (mục 4.6b)                             |
| `deid/NoopKeyDelivery`                     | Dùng khi `submitter=noop`: không đọc Vault, trả tên Secret giả `deid-key-<mã>` để manifest log ra đủ tham số                                                                  |
| `deid/KubernetesSecretKeyDelivery`         | Đọc key bảng bằng `VaultKeyReader`, kiểm định dạng, tạo/xoá Secret qua Kubernetes API                                                                                         |
| `deid/ManifestRenderer`                    | Đọc template YAML, thay placeholder trong **giá trị scalar** đã parse, đè `metadata.name/namespace/labels`                                                                   |
| `deid/JobSubmitter` (interface)            | `JobRef submit(DeidRequest, String keySecretName)` và `JobState status(JobRef)`; lỗi ném `JobSubmitException` (thông điệp đã lọc secret)                                     |
| `deid/NoopJobSubmitter`                    | Render manifest, ghi log, trả `JobRef` giả; `status` luôn `SUCCEEDED`. Dùng để chạy thử không cluster                                                                        |
| `deid/KubernetesJobSubmitter`              | `POST` SparkApplication qua Kubernetes API bằng ServiceAccount token; `GET` để lấy trạng thái                                                                                |
| `deid/AirflowJobSubmitter`                 | Lấy token Keycloak (`client_credentials`, có cache), `POST` DAG run với `dagId` của phiếu qua Airflow REST API; `GET` để lấy trạng thái                                       |
| `web/MockPortalDeidController`             | `/mock-portal/deid-requests/**` và `/mock-portal/deid-dags`: xác thực token portal → `PortalUsers` → service. Upload nhận `multipart/form-data` field `file`                  |
| `web/MockDeidAdminController`              | `/mock-admin/deid-requests/**`: dùng lại việc kiểm token của `MockAdminController` (rút thành bean dùng chung `MockAdminAuth`, **không đổi hành vi** endpoint cũ)            |
| `config/DakProperties`                     | Thêm khối `Deid` (mục 2)                                                                                                                                                     |
| `web/DakApiException`                      | Thêm `invalidFile(problems)`, `invalidCredentials`, `invalidState`, `duplicateRequest`, `tooManyActiveRequests`; `ApiExceptionHandler` thêm trường `problems` khi có          |
| `application.yml`                          | `spring.servlet.multipart.max-file-size`/`max-request-size` = 64KB; chỉ nhận `multipart` cho endpoint upload                                                                 |
| `key/VaultKeyReader`                       | **Không đổi**, chỉ gọi `read(tenant, workspace, database, table)`. Lưu ý `KeyMaterial` là record nên `toString()` in ra `keyPrefix`: **không bao giờ log hay nối chuỗi đối tượng này** |
| `auth/TokenVerifier`, `column/*`           | **Không đổi, không dùng.** Luồng phiếu không chạm key cột hay Keycloak của người dùng                                                                                        |

## 4. Thiết kế chi tiết

### 4.1 Upload phiếu (thay cho "tạo nháp + submit")

Thứ tự cố định, **dừng ở bước đầu có lỗi** (trừ bước 3 gom lỗi):

```text
upload(user, file):
  1. xác thực token portal → PortalUser; sai → 401 invalid_credentials
  2. kiểm file: đủ nhỏ (≤ max-upload-bytes), là JSON object, không trường lạ, không khoá JSON trùng
       hỏng cả file → 400 invalid_file
  3. kiểm nội dung, GOM MỌI LỖI: database/table, columns, mode, dagId, purpose   → 400 invalid_file + problems[]
       dagId: bắt buộc khi submitter=airflow; phải khớp [A-Za-z0-9_][A-Za-z0-9_.-]{0,249} và nằm trong allowed-dag-ids;
              mọi lý do sai dùng CÙNG thông điệp (không lộ DAG nào tồn tại)
              khi submitter khác airflow: tuỳ chọn, được lưu nhưng không dùng
  4. kiểm giới hạn: phiếu hoạt động của team (429), trùng (409 duplicate_request)
  5. tạo phiếu PENDING_APPROVAL: team = team của user, createdBy = username,
       sourceFile = {tên đã làm sạch, sha256 của đúng các byte đã đọc}; audit
```

Không bước nào gọi Vault, Keycloak, Kubernetes hay Airflow: upload chỉ là kiểm và ghi vào bộ nhớ, nên nhanh và test được thuần đơn vị. Lỗi bước 3 trả `problems[]` có `field`
(vd `columns[1].column`), `code`, `message` để portal thật hiển thị cạnh ô nhập; mock giữ cùng dạng này để team DAK có chuẩn.

`DeidUploadParser` dùng Jackson với `FAIL_ON_UNKNOWN_PROPERTIES`, `STRICT_DUPLICATE_DETECTION`, `FAIL_ON_TRAILING_TOKENS`; đọc tối đa `max-upload-bytes + 1` byte
để biết file quá khổ mà không nạp cả file vào bộ nhớ. **File không bao giờ được ghi xuống đĩa**, và nội dung gốc không được lưu lại (chỉ nội dung đã parse + SHA-256).

### 4.2 Phiếu và registry

Mã phiếu `PYC-<năm>-<6 số>` từ một bộ đếm tăng dần trong tiến trình (như `YcRegistry`). `outputTable` suy ra từ `output-table-template`
(vd `PYC-2026-000007` tạo ngày 2026-10-23 → `users_cdr_deid_20261023_7`); tên này chỉ được **chốt lúc duyệt** và lưu vào phiếu, không tính lại, rồi **truyền vào job như tham số khi trigger** (Q6, mục 4.5 và 4.7).
Có ngày trong tên để khi mock restart (bộ đếm về 1) phiếu mới của ngày khác không trùng bảng đích của phiếu cũ.

`transition(code, tập trạng thái nguồn, hàm)` giữ khoá registry, kiểm trạng thái hiện tại thuộc tập nguồn rồi mới áp hàm; sai thì ném
`409 invalid_state`. Đây là chỗ đảm bảo **chỉ một lời gọi `approve` thắng** và **huỷ với duyệt không thể cùng thắng**: người thua đọc lại phiếu.

Tra cứu theo team: mọi hàm đọc/huỷ của người dùng nhận `(team, code)`; phiếu không thuộc team trả cùng lỗi `not_found` như phiếu không tồn tại.

Chống trùng: khoá `(team, database, table, tập cột đã sắp xếp)` trong các phiếu chưa ở trạng thái cuối.

### 4.3 Duyệt, giao key và gửi job (thứ tự cố định)

```text
approve(code):
  1. CAS PENDING_APPROVAL → APPROVED   (chốt outputDatabase/outputTable, decidedAt)
       thua CAS (đã APPROVED/JOB_SUBMITTED/…): trả phiếu hiện tại, không làm gì thêm
       phiếu CANCELLED/REJECTED/…: 409 invalid_state
  2. secret = keyDelivery.deliver(phiếu)   (ngoài khoá registry — gọi mạng)
       đọc key bảng ở Vault → kiểm 32 byte base64 → tạo Secret deid-key-<mã>
       lỗi → CAS APPROVED → SUBMIT_FAILED, submitError cố định (mục 4.6b)
  3. submitter.submit(phiếu, secret)       (ngoài khoá registry — gọi mạng)
       OK  → CAS APPROVED → JOB_SUBMITTED  (ghi jobRef, keyVersion)
       lỗi → keyDelivery.release(phiếu) rồi CAS APPROVED → SUBMIT_FAILED (ghi submitError đã lọc)
```

Duyệt **không** từ chối phiếu vì thiếu key: bảng chưa có key thì phiếu vào `SUBMIT_FAILED` (`table key not found`); sau khi tạo key, admin `retry-submit`.

`retry-submit`: CAS `SUBMIT_FAILED → APPROVED` rồi lặp bước 2 và 3. Vì tên job và tên Secret suy ra từ mã phiếu, và `409 already exists` của job được coi là đã gửi
(hợp đồng mục 7.1), retry an toàn kể cả khi lần trước gửi được nhưng mock chưa kịp ghi nhận.

### 4.4 Làm mới trạng thái job

Không có tiến trình nền. Xem phiếu (người dùng hoặc admin) ở trạng thái `JOB_SUBMITTED`/`RUNNING` gọi `submitter.status(jobRef)` rồi ghi `jobState`,
chuyển `RUNNING`/`SUCCEEDED`/`FAILED`. Quy tắc:

- Lỗi khi hỏi trạng thái (mạng, 5xx): **giữ nguyên** trạng thái cũ, ghi log `WARN`, vẫn trả `200`. Xem phiếu không được hỏng vì Kubernetes/Airflow chập chờn.
- Trạng thái cuối của job được ghi vào phiếu **một lần**; các lần xem sau không hỏi lại. Cùng lúc đó gọi `keyDelivery.release(phiếu)` để **xoá Secret key**; xoá lỗi thì `WARN` và để lại
  (Secret có nhãn `dak.vai/request-code` để quét dọn, mục 6).
- `SparkApplication` đã bị xoá (mẫu job có `timeToLiveSeconds: 86400`) khi phiếu chưa thấy trạng thái cuối: đặt `jobState=UNKNOWN`, xoá Secret key, phiếu giữ nguyên.
  Cách phòng: đặt `timeToLiveSeconds` trong template **≥ 604800** (7 ngày), và ghi rõ trong tài liệu vận hành.

Hệ quả: Secret key **sống đến khi có người xem phiếu sau khi job xong** (không có tiến trình nền). Khoảng thời gian này là rủi ro đã biết của mock; DAK thật nên có poller nền xoá ngay khi job xong.

### 4.5 Render manifest (`ManifestRenderer`)

Template là file `sparkapplication.yaml` trong ConfigMap `dak-mock-deid-template`, **suy ra từ**
[pseudonymize-table.yaml](../../pseudonymize-table-app/k8s/pseudonymize-table.yaml). Placeholder, chỉ nhận trong **giá trị chuỗi**:

| Placeholder              | Thay bằng                | Bộ ký tự bắt buộc                                             |
| ------------------------ | ------------------------ | ------------------------------------------------------------- |
| `${DEID_DATABASE}`       | `database`               | `[a-z0-9_]{1,128}`                                            |
| `${DEID_TABLE}`          | `table`                  | `[a-z0-9_]{1,128}`                                            |
| `${DEID_PSEUDO_COLUMNS}` | `msisdn:HASH,imei:HASH`  | mỗi phần `[a-z0-9_]+:HASH`, nối bằng `,`                      |
| `${DEID_OUTPUT_DB}`      | `outputDatabase`         | `[a-z0-9_]{1,128}`                                            |
| `${DEID_OUTPUT_TABLE}`   | `outputTable`            | `[a-z0-9_]{1,200}`                                            |
| `${DEID_KEY_SECRET}`     | `deid-key-2026-000007`   | `deid-key-\d{4}-\d{6}`                                        |
| `${DEID_REQUEST_CODE}`   | `PYC-2026-000007`        | `PYC-\d{4}-\d{6}`                                             |

Cách làm an toàn (chống chèn YAML): **parse template thành cây trước** (SnakeYAML `SafeConstructor`, đã là phụ thuộc bắc cầu của Spring Boot),
rồi thay placeholder **trong từng scalar chuỗi**, cuối cùng serialize sang JSON để `POST`. Không thay trên text thô nên một giá trị chứa xuống dòng
hay `: ` không thể tạo field mới. Mỗi giá trị vẫn bị kiểm bộ ký tự ở bảng trên và `ManifestRenderer` ném lỗi nếu sai (phòng thủ lớp hai, vì đầu vào đã kiểm ở bước upload).

Sau khi thay, renderer **đè**: `metadata.name = deid-<năm>-<6 số>`, `metadata.namespace = dak.deid.kubernetes.namespace`,
`metadata.labels["dak.vai/request-code"] = <mã phiếu>`, và xoá `metadata.generateName`/`resourceVersion`/`uid` nếu template lỡ có.
Khởi động từ chối template thiếu `${DEID_DATABASE}`, `${DEID_TABLE}`, `${DEID_PSEUDO_COLUMNS}`, `${DEID_OUTPUT_DB}`, `${DEID_OUTPUT_TABLE}`, `${DEID_KEY_SECRET}`
(thiếu thì job chạy trên bảng cố định, hoặc không nhận key của phiếu).

Template phải có `OUTPUT_MODE=overwrite`, `VERIFY=true` (hợp đồng mục 7.3) và **không** chứa giá trị secret. Phần nhận key theo mẫu của job (nguồn `file`):

```yaml
driver:
  env:
    - { name: CRYPTO_PREFIX_SOURCE, value: file }
    - { name: CRYPTO_KEY_PREFIX_DIR, value: /etc/pseudonym-keys }
  volumeMounts:
    - { name: pseudonym-keys, mountPath: /etc/pseudonym-keys, readOnly: true }
volumes:
  - name: pseudonym-keys
    secret:
      secretName: ${DEID_KEY_SECRET} # Secret riêng của phiếu, do mock tạo (mục 4.6b)
```

### 4.6 `KubernetesJobSubmitter`

| Việc                 | Chi tiết                                                                                                                                                                                                                         |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tạo                  | `POST {api-server}/apis/sparkoperator.k8s.io/v1beta2/namespaces/{ns}/sparkapplications`, body JSON đã render, `Authorization: Bearer <SA token>`                                                                                  |
| Đã tồn tại           | `409` → `GET` đúng tên; có nhãn `dak.vai/request-code` khớp phiếu thì coi là đã gửi; khác nhãn → `JobSubmitException` (tên bị chiếm bởi thứ khác)                                                                                  |
| Lỗi quyền / lỗi khác | `401/403` → `JobSubmitException("kubernetes denied the request")`; `5xx`/mạng → `JobSubmitException("kubernetes unavailable")`. Thân lỗi của API server **không** đưa vào phiếu                                                    |
| Trạng thái           | `GET …/sparkapplications/{name}` → `status.applicationState.state`; ánh xạ ở [API_CONTRACT.md](./API_CONTRACT.md) mục 7.5; chưa có `status` = `RUNNING`; `404` → `UNKNOWN` (mục 4.4)                                              |
| Token SA             | Đọc `/var/run/secrets/kubernetes.io/serviceaccount/token` **ở mỗi lần gọi** (token được xoay)                                                                                                                                    |
| TLS                  | Dựng `SSLContext` riêng từ `…/serviceaccount/ca.crt` cho client này. **Không** thêm vào truststore chung: JVM của mock đang dùng truststore tự dựng (cacerts + CA Keycloak, xem `dak-mock.yaml`), CA của API server không có trong đó |
| HTTP client          | `RestClient` + `JdkClientHttpRequestFactory` với timeout kết nối 2s, đọc 5s — cùng khuôn `VaultKeyReader`                                                                                                                         |

`KubernetesJobSubmitter` và `KubernetesSecretKeyDelivery` dùng chung một `KubernetesApiClient` nhỏ (token, TLS, timeout) để không lặp code.

### 4.6b `KubernetesSecretKeyDelivery`

`deliver(phiếu)`:

1. **Đọc:** `VaultKeyReader.read(tenant, workspace, database, table)` với tenant/workspace của team. Vault `404` → `KeyDeliveryException("table key not found")`; lỗi khác (mạng, 5xx, 403) → `KeyDeliveryException("vault unavailable")`.
2. **Kiểm định dạng:** `keyPrefix` phải là base64 hợp lệ của đúng **32 byte**; không thì `KeyDeliveryException("table key has unsupported format")`. Thông điệp **không chứa** giá trị key hay độ dài.
3. **Tạo Secret:** `POST {api-server}/api/v1/namespaces/{ns}/secrets`:

```json
{
  "apiVersion": "v1",
  "kind": "Secret",
  "type": "Opaque",
  "metadata": {
    "name": "deid-key-2026-000007",
    "labels": { "dak.vai/request-code": "PYC-2026-000007", "dak.vai/kind": "deid-key" }
  },
  "data": { "pseudonym.hmac_v1": "<base64 của chuỗi 'base64:<keyPrefix>'>" }
}
```

   `409` (còn sót từ lần trước) → `DELETE` Secret đó rồi `POST` lại **đúng một lần**. `401/403` → `KeyDeliveryException("kubernetes denied the request")`; `5xx`/mạng → `KeyDeliveryException("kubernetes unavailable")`.
4. Trả **tên Secret**; ghi `keyVersion` (`KeyMaterial.version()`) vào phiếu. Giá trị key chỉ tồn tại trong biến cục bộ của hàm, không được gán vào field, log hay exception.

`release(phiếu)`: `DELETE …/secrets/deid-key-<mã>`; `404` coi như đã xoá. **Chỉ xoá tên khớp `deid-key-\d{4}-\d{6}`** (kiểm trong code trước khi gọi), để một lỗi logic không xoá được Secret khác.

Lỗi trong `deliver` luôn quy về `KeyDeliveryException` với **một trong 5 thông điệp cố định** ở trên; `submitError` của phiếu chỉ nhận đúng các thông điệp này.

### 4.7 `AirflowJobSubmitter`

**Cập nhật 2026-10-26 (đã kiểm trên Airflow thật):** `airflow config get-value api auth_backends` trả `basic_auth,session`, nên REST API **KHÔNG nhận Bearer token Keycloak**; giả định ở đoạn dưới là sai cho môi trường này. Mock thêm `auth=basic` (HTTP Basic với một user Airflow riêng cho DAK, `username` + env `AIRFLOW_PASSWORD`) và đó là cách dùng hiện tại. `client-credentials` chỉ dùng được khi Airflow bật thêm backend nhận Bearer Keycloak.

**Xác thực (Q9, Q11), phương án `client-credentials`.** Webserver Airflow tích hợp Keycloak và (giả định) Airflow REST API chấp nhận Bearer token Keycloak. Mock dùng `client_id`/`client_secret` (`auth=client-credentials`):

1. `POST {token-url}` với `grant_type=client_credentials` (kèm `scope` nếu cấu hình), xác thực client bằng HTTP Basic `client-id:AIRFLOW_CLIENT_SECRET`.
2. Giữ `access_token` trong bộ nhớ đến `expires_in − 30s` rồi lấy lại. Gọi Airflow gặp `401` thì bỏ token đang giữ, lấy token mới và **thử lại đúng một lần**; vẫn `401` thì `JobSubmitException("airflow denied the request")`.
3. Gọi Airflow với `Authorization: Bearer <access_token>`. Token và secret **không bao giờ** vào log, phiếu hay response.

Mock đã tin CA Keycloak nội bộ qua truststore dựng sẵn và đã có `hostAliases` cho `sso-lakehouse.cyberspace.vn` trong `dak-mock.yaml`, nên không phải làm thêm.
`auth=bearer` (token tĩnh từ env `AIRFLOW_TOKEN`) chỉ để thử nhanh khi chưa có client.

**Trigger.** `POST {base-url}/api/v1/dags/{dagId}/dagRuns` (Airflow **2.8**, REST `/api/v1`, README Q18; không hỗ trợ `/api/v2`) với `dagId` **lấy từ phiếu** (người dùng chọn, Q12), và **kiểm lại với `allowed-dag-ids` ngay trước khi gọi**
(danh sách có thể đã đổi từ lúc upload); không còn trong danh sách thì `JobSubmitException("airflow dag not allowed")`. Body:

```json
{
  "dag_run_id": "deid-2026-000007",
  "conf": {
    "requestCode": "PYC-2026-000007",
    "database": "demo_db",
    "table": "users_cdr",
    "pseudoColumns": "msisdn:HASH,imei:HASH",
    "outputDb": "demo_db",
    "outputTable": "users_cdr_deid_20261023_7",
    "keySecretName": "deid-key-2026-000007"
  }
}
```

`outputDb`/`outputTable` là tham số khi trigger do DAK quyết (Q6). `keySecretName` là **tên** Secret, không phải key; `conf` không bao giờ chứa key (Airflow lưu `conf` trong DB và hiện trên giao diện).
`dagId` không tồn tại trên Airflow (`404`) → `JobSubmitException("airflow dag not found")`. `409` (đã có `dag_run_id`) = đã gửi → `GET …/dagRuns/{dag_run_id}` để lấy `state`.
Trạng thái: `queued`/`running` → `RUNNING`, `success` → `SUCCEEDED`, `failed` → `FAILED`.

**DAG phía Airflow không thuộc mock.** Mock chỉ cam kết các key trong `conf` ở trên; DAG đọc `conf` rồi tạo SparkApplication bằng `SparkKubernetesOperator`, mount Secret có tên `conf.keySecretName`
(xem [RANGER_AIRFLOW_MONITORING_GUIDE.md](../operations/RANGER_AIRFLOW_MONITORING_GUIDE.md) mục 3). Vì mọi DAG trong `allowed-dag-ids` đều nhận tên Secret chứa key, **allowlist là điều kiện bảo mật**, không phải tiện ích (README O3). Một DAG mẫu là việc tuỳ chọn ở bước D11.

### 4.8 Bảo mật

- **Xác thực portal giả:** token trong `portal-users` ≥ 24 ký tự, so sánh hằng thời gian (`MessageDigest.isEqual`), sai hoặc thiếu → `401 invalid_credentials` với
  cùng thông điệp cho mọi nguyên nhân. Token chỉ nằm trong Secret, không bao giờ vào ConfigMap, log hay response. Đây **chỉ là mock**: DAK thật dùng phiên đăng nhập của portal.
- **Cách ly team:** mọi truy vấn của người dùng đi qua `(team, code)`; phiếu team khác trả đúng lỗi `not_found` như phiếu không tồn tại, **cùng thân và không tra job/Airflow trước khi kiểm team**.
  Có test so sánh hai phản hồi.
- **`dagId` do người dùng chọn:** kiểm bộ ký tự **và** allowlist ở upload, kiểm lại allowlist lúc trigger, thông điệp lỗi giống nhau cho mọi lý do (không dò được tên DAG). Admin thấy `dagId` khi duyệt.
  `dagId` đi vào URL nên bộ ký tự `[A-Za-z0-9_][A-Za-z0-9_.-]{0,249}` ngăn ký tự `/`, `?`, `#`, khoảng trắng và `.`/`..` (path traversal).
- **Key khử định danh:** chỉ tồn tại (a) trong Vault, (b) trong biến cục bộ của `deliver`, (c) trong Secret của phiếu. Không vào phiếu, `conf`, env của SparkApplication, log, audit, `submitError` hay response.
  Không in `KeyMaterial` (record tự in `keyPrefix`). Test kiểm không dòng log hay response nào chứa giá trị key. Secret xoá khi job xong (mục 4.4) và khi gửi job lỗi.
- **File upload:** giới hạn kích thước ở hai tầng (multipart của Spring và `DeidUploadParser`), không ghi đĩa, không lưu nội dung gốc, JSON nghiêm ngặt (mục 4.1),
  tên file làm sạch trước khi hiển thị (bỏ đường dẫn, ký tự điều khiển, cắt 255). `Content-Type` của phần `file` không tin: chỉ dựa vào việc parse được.
- **Quyền k8s** (mục 6) trong đúng một namespace: `sparkapplications` `create`+`get`; `secrets` `create`+`delete` (không `get`/`list`/`update`). `delete` secrets **không giới hạn theo tên được** (`create` không hỗ trợ `resourceNames`),
  nên rủi ro là một pod `dak-mock` bị chiếm xoá được Secret bất kỳ trong namespace đó (kể cả keytab). Giảm rủi ro: (1) code chỉ xoá tên khớp `deid-key-…`; (2) namespace riêng sẽ giảm rủi ro nhiều nhất, nhưng **đã chốt dùng namespace hiện tại** (README Q17) nên rủi ro này được chấp nhận;
  (3) NetworkPolicy chặn truy cập tới pod; (4) DAK thật nên thêm admission policy (Kyverno/OPA). Quyền `create` SparkApplication cũng không giới hạn theo field nên ai chiếm được pod có thể tạo SparkApplication tuỳ ý dưới ServiceAccount của Spark.
- **Secret của Airflow client** chỉ ở Secret `dak-mock-airflow`, nạp bằng `secretKeyRef`; client này chỉ nên có quyền gọi API DAG run (hạn chế ở phía Keycloak/Airflow, không phải mock).
- `purpose` do người dùng nhập là **văn bản tự do**: chỉ lưu và trả lại, **không** đi vào manifest và **không** vào audit.
- Không response, log hay `submitError` nào chứa token portal/admin/SA/Airflow/Keycloak, client secret, `keyPrefix` hay giá trị key. Có một ca test xuyên suốt kiểm điều này như `by-column`.

### 4.9 Audit

Logger `AUDIT`, một dòng cho **mỗi chuyển trạng thái**, kể cả chuyển do làm mới trạng thái job
(mẫu ở [API_CONTRACT.md](./API_CONTRACT.md) mục 9). Upload ghi `actor=<username>` và `fileSha256`. Lấy key ghi `KEY_DELIVERY requestCode=… keyVersion=<n> result=OK|ERROR` (không có giá trị key).
Lời gọi bị từ chối (`400/401/404/409/429`) ghi `result=ERROR error=<mã>`; mọi giá trị do người dùng nhập được thay `\r\n` bằng khoảng trắng trước khi ghi.

## 5. Các bước (TDD)

Mỗi bước: viết test (RED) → code tối thiểu (GREEN) → dọn (IMPROVE). Test API theo khuôn `ColumnKeyApiTest`, nhưng luồng phiếu với `submitter=noop` **không cần giả Vault hay Keycloak**
(D6b giả Vault + Kubernetes API, D7 giả Kubernetes API, D8 giả token endpoint + Airflow). Mục tiêu độ phủ ≥ 80% cho package `deid`.

| #   | Việc                                                         | Test chính                                                                                                                                                                                                                                                         |
| --- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D1  | `DakProperties.Deid` + `portal-users` + kiểm lúc khởi động   | **Đã làm xong (2026-10-23, 53 test).** Mỗi cấu hình sai ở mục 2 làm ứng dụng không khởi động (token ngắn, user trùng/token trùng, `ENCRYPT` trong `allowed-modes`, `allowed-dag-ids` rỗng khi airflow, thiếu `kubernetes.namespace`, `client-credentials` thiếu `token-url`/secret, path-template có `{tenant}` mà user thiếu `tenant`…); mặc định `submitter=noop`, `allowed-modes=[HASH]`, portal tắt khi không có user |
| D2  | `DeidRequest` + `DeidRequestRegistry`                        | **Đã làm xong (2026-10-26, 118 test).** Mọi cạnh của máy trạng thái ([README](./README.md) mục 2) và mọi cạnh **không** hợp lệ → `invalid_state`; CAS nguyên tử: 2 luồng cùng `approve`, đúng 1 thắng; `approve` với `cancel` cùng lúc, đúng 1 thắng; chống trùng; giới hạn 20; tra theo `(team, code)`   |
| D3  | `DeidUploadParser`                                           | **Đã làm xong (2026-10-26, 44 test gồm cả test body lỗi `problems`).** Hợp lệ; JSON hỏng; không phải object; trường lạ; khoá trùng; dữ liệu thừa sau JSON; quá 64 KB (không nạp hết vào bộ nhớ); từng lỗi `invalid_table/column/mode/purpose`; **`dagId`**: thiếu khi airflow, ký tự lạ (`../x`, `a/b`, `a?b`), ngoài allowlist (cùng thông điệp với DAG không tồn tại), hợp lệ, và bị bỏ qua khi `submitter` khác airflow; **gom nhiều lỗi một lượt** với `field` đúng; chuẩn hoá chữ thường; gộp cột trùng |
| D4  | `DeidRequestService` + `MockPortalDeidController`, `noop`    | **Đã làm xong (2026-10-26, 48 test: `PortalUsersTest`, `DeidRequestServiceTest`, `DeidPortalApiTest`, `DeidPortalDisabledApiTest`).** `DeidPortalApiTest`: upload → phiếu `PENDING_APPROVAL` có `team`/`createdBy`/`sourceFile.sha256` đúng; mã `PYC-…`; token sai/thiếu `401`; upload lặp `409`; huỷ rồi upload lại được; huỷ lặp `200`; vượt 20 → `429`; user thứ hai **cùng team** thấy và huỷ được phiếu; user team B xem/huỷ phiếu team A → `404` giống hệt phiếu không tồn tại; `GET /mock-portal/deid-dags` trả allowlist |
| D5  | `MockDeidAdminController`                                    | **Đã làm xong (2026-10-26, 58 test: `DeidAdminServiceTest`, `NoopJobAndKeyDeliveryTest`, `MockAdminAuthTest`, `MockDeidAdminApiTest`, `MockDeidAdminFailureApiTest` và các ca thêm vào test cũ).** list lọc theo state; `approve` → `JOB_SUBMITTED` với `noop`; `approve` lặp lại không giao key và không gửi job lần hai (đếm lời gọi `deliver`/`submit` của đối tượng giả); `approve` phiếu `CANCELLED` → `409`; `reject` bắt buộc `reasonText`; thiếu/sai token → `404`; mọi test cũ của `/mock-admin/yc/**` vẫn qua |
| D6  | `ManifestRenderer`                                           | **Đã làm xong (2026-10-26, 59 test).** Chỉ còn dùng để `noop` log manifest mẫu; KHÔNG cần cho đường Airflow (DAG của bạn tự dựng SparkApplication). Template: `dak-mock/k8s/deid/sparkapplication.yaml` |
| D6b | `KubernetesSecretKeyDelivery` + `KubernetesApiClient`        | **Đã làm xong (2026-10-26, 24 + 22 test).** Mọi ca ở mô tả đã được test: Secret đúng tên/nhãn/`data`; 5 thông điệp cố định (`table key not found`, `...unsupported format`, `vault unavailable`, `kubernetes denied the request`, `kubernetes unavailable`); `409` → `DELETE` rồi `POST` đúng một lần; `release` chỉ xoá tên `deid-key-\d{4}-\d{6}`; key không lọt vào exception. Client: token SA đọc lại mỗi lời gọi, CA cluster riêng (test TLS bằng `okhttp-tls`), từ chối path lạ |
| D7  | ~~`KubernetesJobSubmitter`~~                                 | **Bỏ (2026-10-26):** đã có Airflow + DAG, mock chỉ trigger DAG, không `kubectl apply` SparkApplication. `submitter=kubernetes` bị gỡ khỏi phạm vi |
| D8  | `AirflowJobSubmitter` + `AirflowTokens`                      | **Đã làm xong (2026-10-26, 29 test).** Token `client_credentials` (Basic, `scope`), cache đến `expires_in−30s`, `401` → token mới thử lại một lần; `dagId` kiểm lại allowlist trước khi gọi mạng; `conf` chỉ có `keySecretName`; `404`/`403`/`409`/`5xx` ánh xạ đủ; `bearer` tĩnh; `JobSubmitter.status` nay nhận `(request, ref)` vì Airflow cần `dagId` |
| D9  | `JobProgressRefresher` (làm mới trạng thái khi xem phiếu)    | **Đã làm xong (2026-10-26, 9 test).** `RUNNING`/`SUCCEEDED`/`FAILED`, `release` đúng một lần (kể cả khi hai lần xem đua nhau), `UNKNOWN` giữ trạng thái + `release`, lỗi hỏi trạng thái/`release` không làm hỏng việc xem. Với `noop`, lần xem đầu sau khi duyệt thấy `SUCCEEDED` |
| D10 | Tài liệu + RBAC tối thiểu (mục 6)                            | **Đã làm xong (2026-10-26).** `dak-mock/k8s/dak-mock-deid-rbac.yaml` (ServiceAccount + Role `secrets create/delete` + RoleBinding), `dak-mock.yaml` (khối `dak.deid`, env `AIRFLOW_CLIENT_SECRET`, mount `/portal`, `serviceAccountName`), [dak-mock/README.md](../../dak-mock/README.md) mục 11. Chưa `kubectl apply --dry-run=server` (cần cluster) |
| D11 | DAG khử định danh (của bạn)                                  | **Đã có mẫu:** [sample_dag_deid_pseudonymize_table.py](./sample_dag_deid_pseudonymize_table.py) (kiểm `conf`, env + volume Secret). Bạn thay `base_spec` bằng spec đang chạy và đặt `dag_id` vào `allowed-dag-ids`; chưa chạy thử trên Airflow thật |

Ca test xuyên suốt: không response hay dòng log nào chứa giá trị key, `keyPrefix`, token portal/admin/SA/Airflow, client secret Keycloak; phiếu team A không bao giờ xuất hiện trong danh sách của team B.

Thứ tự đã làm: D1 → D2 → D3 → D4 → D5 → D6 → D6b → D8 → D9 → D10 → D11 (D7 bỏ). **Lưu ý namespace:** Secret key phải nằm cùng namespace với SparkApplication mà DAG tạo (`vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy`), nên `dak.deid.kubernetes.namespace` và `NAMESPACE` trong DAG phải trùng.

## 6. Triển khai trên cluster

| Việc                                              | Chi tiết                                                                                                                                                                                                                              |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ConfigMap `dak-mock-config`                       | Thêm khối `dak.deid` (mục 2) **không có** `portal-users`. Bước đầu để `submitter: noop`                                                                                                                                                |
| Secret `dak-mock-portal-users` (mới)              | Key `application.yml` chứa `dak.deid.portal-users` (có token). Mount `/portal` chỉ đọc; đổi `SPRING_CONFIG_ADDITIONAL_LOCATION` từ `file:/config/` thành `file:/config/,file:/portal/`. Tạo từ file tạm bằng `kubectl create secret generic … --from-file=application.yml=<file>`, **không commit**. Các khoá `dak.deid.*` còn lại ở ConfigMap gộp với khoá này theo từng property; riêng danh sách `portal-users` chỉ khai ở Secret |
| ConfigMap `dak-mock-deid-template` (mới)          | Key `sparkapplication.yaml` = template mục 4.5; mount `/deid` chỉ đọc vào container `dak-mock` (chỉ khi `submitter=kubernetes`)                                                                                                       |
| ServiceAccount `dak-mock` (mới)                   | Gắn vào Deployment. Deployment đang đặt `automountServiceAccountToken: false`; đổi thành `true` khi `submitter=kubernetes` **hoặc** `airflow` (cả hai tạo Secret key); với `noop` giữ `false`                                          |
| Role + RoleBinding (mới)                          | Namespace chạy job: `sparkapplications` `[create, get]` (chỉ khi `submitter=kubernetes`) và `secrets` `[create, delete]`. **Đã chốt (README Q17): dùng namespace hiện tại của job**, không tách namespace riêng; rủi ro ở mục 4.8                                             |
| Vault                                             | Không đổi: token `dak-mock-vault-token` đã có quyền đọc `kv/data/hla-datalake/datalake/spark-application/*` (xem comment đầu `dak-mock.yaml`). Key bảng phải được tạo sẵn (xem mục 7)                                                  |
| Secret `dak-mock-airflow` (chỉ khi dùng Airflow)  | Key `clientSecret` (client secret của client Airflow trên Keycloak; hoặc key `token` khi `auth=bearer`). Nạp vào env `AIRFLOW_CLIENT_SECRET`/`AIRFLOW_TOKEN` bằng `secretKeyRef` với `optional: true`, giống `MOCK_ADMIN_TOKEN`        |
| Image                                             | Không cần `kubectl`. Image `dak-mock` không đổi cách build                                                                                                                                                                             |
| Pod Security                                      | Giữ nguyên `securityContext` hiện tại; không cần thêm quyền. `/deid` và `/portal` là mount chỉ đọc                                                                                                                                     |
| Quét dọn Secret sót                               | `kubectl get secret -n <ns> -l dak.vai/kind=deid-key` liệt kê Secret key còn sót (mock restart, hoặc phiếu chưa được xem sau khi job xong); xoá bằng `kubectl delete secret -l dak.vai/kind=deid-key`                                  |

Role mẫu (đặt trong file mới `dak-mock/k8s/dak-mock-deid-rbac.yaml`; `dak-mock.yaml` chỉ sửa các điểm ở bảng trên):

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: dak-mock-deid
  namespace: vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy
rules:
  - apiGroups: ["sparkoperator.k8s.io"]
    resources: ["sparkapplications"]
    verbs: ["create", "get"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["create", "delete"] # không get/list/update; delete không giới hạn theo tên được
```

Tạo tài khoản giả (token sinh ngẫu nhiên, không gõ vào lịch sử shell):

```bash
T=$(openssl rand -hex 24)
cat > /tmp/portal-users.yml <<EOF
dak:
  deid:
    portal-users:
      - username: hungvt
        team: teamtscauiy
        token: $T
EOF
kubectl create secret generic dak-mock-portal-users -n "$NS" --from-file=application.yml=/tmp/portal-users.yml
shred -u /tmp/portal-users.yml; echo "token của hungvt: $T"; unset T
```

## 7. Kịch bản chạy thử đầu-cuối

Tiền đề: key theo bảng đã có trong Vault. Tạo bằng pod `vault-cli` ([k8s/vault-cli-pod.yaml](../../k8s/vault-cli-pod.yaml)), **32 byte ngẫu nhiên mã hoá base64**:
`vault kv put -mount=kv -cas=0 hla-datalake/datalake/spark-application/demo_db.users_cdr keyPrefix="$(openssl rand -base64 32)"`. File mẫu `phieu.json`:
`{"database":"demo_db","table":"users_cdr","columns":[{"column":"msisdn"},{"column":"imei"}],"dagId":"deid_pseudonymize_table","purpose":"thu nghiem"}`.

| #   | Bước                                                                                                  | Kết quả mong đợi                                                                                                                  |
| --- | ----------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `submitter: noop`. Người dùng `curl -F file=@phieu.json /mock-portal/deid-requests` (token portal)      | `201`, phiếu `PYC-…` ở `PENDING_APPROVAL`, `team=teamtscauiy`, `createdBy=hungvt`                                                  |
| 2   | Upload lại đúng file đó                                                                               | `409 duplicate_request` kèm mã phiếu ở bước 1 (không có phiếu thứ hai)                                                            |
| 3   | Upload file có `purpose` rỗng, một tên cột sai ký tự và `dagId` ngoài danh sách (khi `airflow`)        | `400 invalid_file` với `problems` liệt kê **đủ các lỗi**; không có phiếu mới                                                       |
| 4   | Tài khoản thứ hai **cùng team** xem và huỷ phiếu ở bước 1, rồi upload lại                            | Xem được, huỷ được (`CANCELLED`); upload lại ra phiếu mới                                                                          |
| 5   | Admin `GET /mock-admin/deid-requests?state=PENDING_APPROVAL`, rồi `approve`                           | `JOB_SUBMITTED`; log mock có manifest đã render (đúng `DB_NAME`, `PSEUDO_COLUMNS`, `OUTPUT_TABLE`, tên Secret key); **không có key** |
| 6   | Người dùng xem phiếu                                                                                  | `SUCCEEDED` (noop), có `outputDatabase`, `outputTable`                                                                             |
| 7   | Đổi `submitter: kubernetes`, áp RBAC + template, restart mock; upload phiếu mới và duyệt              | `kubectl get secret -l dak.vai/request-code=PYC-… -n …` thấy `deid-key-…`; SparkApplication `deid-…` và driver pod chạy, mount Secret đó |
| 8   | Chờ job xong, người dùng xem phiếu                                                                    | `RUNNING` rồi `SUCCEEDED`; **Secret `deid-key-…` đã bị xoá**; bảng đích có cột đã thành `k1_…` (kiểm bằng `pseudonym-verify-utils`) |
| 9   | Admin `approve` lần hai trên phiếu đó                                                                 | `200`, **không** có SparkApplication hay Secret thứ hai                                                                           |
| 10  | Upload phiếu cho bảng **chưa có key** rồi `approve`                                                    | `SUBMIT_FAILED`, `submitError="table key not found"`; tạo key bằng `vault-cli`, rồi `retry-submit` → `JOB_SUBMITTED`              |
| 11  | Tạo key bảng sai định dạng (vd `keyPrefix="chuoi-ngan"`) rồi duyệt một phiếu                           | `SUBMIT_FAILED`, `submitError="table key has unsupported format"`, **không** có Secret nào được tạo                              |
| 12  | Xoá RoleBinding rồi duyệt một phiếu mới                                                               | `SUBMIT_FAILED` với `submitError` gọn, không lộ chi tiết API server; áp lại RoleBinding, `retry-submit` → `JOB_SUBMITTED`          |
| 13  | Upload một phiếu, người dùng huỷ, rồi admin `approve`                                                 | `approve` trả `409 invalid_state`; không có Secret, không có job                                                                  |
| 14  | `reject` một phiếu với `reasonText`                                                                   | Người dùng thấy `REJECTED` + `reasonText`; upload phiếu mới cho cùng bảng được ngay                                               |
| 15  | Người dùng team B xem phiếu của team A                                                                | `404`, giống hệt phản hồi cho một mã không tồn tại                                                                                 |
| 16  | (Khi có thông tin Airflow) `submitter: airflow`, upload với `dagId` trong allowlist, duyệt             | Mock lấy token Keycloak bằng client credentials; DAG run `deid-…` của **đúng `dagId` đã chọn** xuất hiện trên Airflow, `conf` có `keySecretName` và **không có key** |

## 8. Rủi ro và việc còn mở

| Việc                                                                                         | Ảnh hưởng                                                                                                                                                                                         |
| -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Hệ quả của Q1:** job băm thẳng trên bảng đã mã hoá                                          | Nếu mã hoá cột không tất định (vd key theo dòng) thì cùng giá trị gốc ra nhiều định danh giả, không liên kết được chủ thể. Đã chốt chấp nhận; ghi trong hợp đồng mục 6                                  |
| **O1:** giá trị cấu hình Airflow chưa có (`base-url`, `token-url`, `client_id`, scope)        | Phiên bản (2.8, `/api/v1`) và cơ chế xác thực đã chốt. Chỉ cần điền giá trị khi chạy thật D8/D11; test D8 dùng MockWebServer nên không phụ thuộc                                                      |
| **Q15 (O2): cách giao key và định dạng key bảng** (đã duyệt)                                | Đề xuất Secret theo phiếu + file `pseudonym.hmac_v1`, key bảng phải là **32 byte base64**. Key bảng hiện có trong Vault có thể không đúng định dạng này (vd chuỗi tuỳ ý dùng cho `column_encrypt`): khi đó phiếu `SUBMIT_FAILED`. Cần kiểm key bảng thật trước khi chạy thật |
| **Chỉ `HASH`**                                                                               | `ENCRYPT` (khoá 64 byte) và `MAPPING` (bảng ánh xạ) cần thêm vật liệu key nên bị từ chối ở upload và ở cấu hình. Muốn mở rộng thì phải thiết kế thêm cách giao key                                   |
| **Q16 (O3):** allowlist `dagId` (đã duyệt)                                                    | Là điều kiện bảo mật vì mọi DAG trong danh sách đều nhận tên Secret chứa key. Bỏ allowlist = người dùng chọn DAG bất kỳ, chỉ còn bước duyệt chặn                                                      |
| **Q17 (O4):** quyền `secrets` `create`+`delete` không giới hạn theo tên (đã chốt dùng namespace hiện tại)                             | Pod `dak-mock` bị chiếm có thể xoá Secret bất kỳ trong namespace. Chấp nhận rủi ro; giảm bằng code chỉ xoá `deid-key-…` và NetworkPolicy (namespace riêng bị bỏ)                                                       |
| Secret key sống đến khi có người xem phiếu sau khi job xong                                   | Mock không có tiến trình nền nên không xoá ngay được. Có quét dọn theo nhãn; DAK thật nên có poller nền                                                                                             |
| Phiếu và tài khoản giả chỉ ở mock (Q10 chấp nhận)                                             | Không có đăng nhập thật; token tĩnh trong Secret. Restart mất phiếu (job đã gửi vẫn chạy; Secret key sót, truy ngược bằng nhãn `dak.vai/request-code`)                                                 |
| Mock không giữ file gốc đã upload                                                            | Chỉ có nội dung đã parse + SHA-256. DAK thật nên lưu file gốc phục vụ đối soát/kiểm toán                                                                                                          |
| Định dạng file phiếu thật (Q2 giả định JSON)                                                  | `DeidUploadParser` chỉ là ví dụ; hợp đồng giữ nguyên vì chỉ ràng buộc các trường logic. Có mẫu thật thì thêm bộ parse tương ứng                                                                     |
| Một replica, xem phiếu làm mới trạng thái đồng bộ                                             | Nhiều người xem cùng lúc gây nhiều lời gọi tới Kubernetes/Airflow. Chấp nhận ở mock; DAK thật nên có poller nền hoặc cache vài giây                                                                 |
| Quyền `create sparkapplications` không giới hạn theo field                                    | Mục 4.8. Mock chấp nhận; DAK thật cần admission policy                                                                                                                                             |
