# Kịch bản test: nạp dữ liệu VTNET `ebs_3g_4g` vào silver (success flow)

Mục tiêu: một batch của bảng `ebs_3g_4g` đi hết luồng **phiếu → admin duyệt → DAK trigger DAG → job Spark → bảng silver có cột `imsi` đã mã hoá**.
Thiết kế: [PLAN.md](PLAN.md). Cách dùng job: [silver-ingest-app/README.md](../../silver-ingest-app/README.md). Cách dùng mock: [dak-mock/README.md](../../dak-mock/README.md) mục 2.3, 2.4.

Ký hiệu: `<…>` là giá trị bạn phải điền (tên cột thật, token, client id…). Ví dụ dùng cột `imsi` cần mã hoá; `<KEY_FIELD>` là một cột khác giữ plaintext.

## 0. Dữ liệu dùng để test

| | |
| --- | --- |
| File | `/prod8/landing/vtnet/ebs_3g_4g/dt=20261012/batch=1520/ebs_3g_4g-143450.csv` (~187 MB) |
| `path` trong phiếu | `ebs_3g_4g/dt=20261012/batch=1520` (tương đối theo gốc `/prod8/landing/vtnet`) |
| `batchId` | `202610121520` (ngày 2026-10-12, batch 15:20) |
| Bảng đích | `silver.vtnet_ebs_3g_4g`, partition `batch_id=202610121520` |
| Cột mã hoá / cột giữ plaintext | `imsi` / `<KEY_FIELD>` (một cột khác, luôn có giá trị, vd cột thời gian của bản ghi) |

Kiểm file có đúng và xem header (cần để biết tên cột thật):

```bash
hdfs dfs -ls -h /prod8/landing/vtnet/ebs_3g_4g/dt=20261012/batch=1520
hdfs dfs -cat /prod8/landing/vtnet/ebs_3g_4g/dt=20261012/batch=1520/ebs_3g_4g-143450.csv | head -3
```

- Dòng đầu là header (có tên cột, trong đó có `imsi`) → giữ nguyên cấu hình DAG (`INPUT_OPTION_header=true`).
- Không có header → trong `airflow/dags/silver_ingest_vtnet.py` thêm `"INPUT_SCHEMA": "<c1 STRING, imsi STRING, …>"` và bỏ `INPUT_OPTION_header`. Nếu không, tên cột là `_c0, _c1…` và job báo cột `imsi` không có trong file.
- Delimiter không phải dấu phẩy → thêm `"INPUT_OPTION_delimiter": "<ký tự>"`.

Hai kịch bản tuỳ file có mã hoá cả file hay không:

| | Kịch bản A (mặc định) | Kịch bản B |
| --- | --- | --- |
| File landing | **Không** mã hoá cả file (csv thường, như file ở trên) | Mã hoá cả file (AES-256-GCM) |
| Key theo đường dẫn (`by-path`) | **Không tạo** → DAK trả `204` → job đọc nguyên trạng | **Tạo** → job giải mã trước |
| Key theo cột `imsi` (`by-column`) | Cần (mục 3.2) | Cần (mục 3.2) |

Các mục 1–2 và 4–8 dùng chung; mục 3.1 chỉ làm với kịch bản B. **Kịch bản B làm trọn từng bước (kèm file mã hoá mẫu 50 dòng) ở mục 13.**

## 1. Những gì phải có sẵn (checklist)

| # | Cần | Kiểm bằng |
| --- | --- | --- |
| 1 | Image `silver-ingest-app` đã build và push, tag khớp `IMAGE` trong `airflow/dags/silver_ingest_common.py` | `docker pull <registry>/silver-ingest-app:<tag>` |
| 2 | 3 file DAG đã chép **cùng thư mục dags** của Airflow: `silver_ingest_common.py`, `silver_ingest_vtt.py`, `silver_ingest_vtnet.py` | DAG `silver_ingest_vtnet` hiện trong UI, không có import error |
| 3 | Airflow có user riêng cho DAK (vd `dak-trigger`, role đọc DAG + tạo dag run) | `curl -u dak-trigger:… <airflow>/api/v1/dags/silver_ingest_vtnet` trả `200` |
| 4 | dak-mock đang chạy với `dak.deid.submitter: airflow`, `dak.deid.airflow.*`, `AIRFLOW_PASSWORD` | README dak-mock mục 11 |
| 5 | Tài khoản portal giả (`dak.deid.portal-users`, có token) và token admin (Secret `dak-mock-admin-token`) | `/mock-admin/silver-requests` trả `200` với token admin |
| 6 | **Client nội bộ** của DAK: khai trong ConfigMap dak-mock với `kind: internal`, và có client Keycloak tương ứng | README dak-mock mục 4 |
| 7 | Secret `spark-dak-client` (key `clientSecret`) trong **namespace chạy job** | `kubectl get secret spark-dak-client -n <ns>` |
| 8 | `DAK_CLIENT_ID` trong `silver_ingest_vtnet.py` đã thay `<client-id-noi-bo>` bằng client id nội bộ thật | đọc file |
| 8b | ConfigMap `keycloak-ca` (key `ca.pem`, đã có sẵn trong cụm hiện tại) trong namespace chạy job; `KEYCLOAK_IP`/`KEYCLOAK_HOST` trong `silver_ingest_common.py` đúng với cụm | `kubectl get configmap keycloak-ca -n <ns>` |
| 9 | Spark Operator và ServiceAccount `spark-application-sa` trong namespace; Kerberos/HDFS như các job khác | job khác chạy được |
| 10 | Thư mục landing đọc được bằng principal `presto` (file `rw-r--r--`, đọc được) | `hdfs dfs -cat … \| head` |

Mock cần token Vault đủ quyền (mục 2).

## 2. Vault: những key và path (mount `kv`, KV v2)

dak-mock đọc và tạo key ở Vault bằng token tĩnh trong Secret `dak-mock-vault-token`. Token cần quyền `read` **và** `create` trên:

```hcl
path "kv/data/hla-datalake/datalake/column-keys/*" { capabilities = ["create", "read"] }
path "kv/data/hla-datalake/datalake/path-keys/*"   { capabilities = ["create", "read"] }
```

(Mock ghi bằng `cas=0`: chỉ tạo mới, không bao giờ ghi đè nên không cần `update`.)

| Key | Dùng khi | Path trong Vault | Field | Giá trị |
| --- | --- | --- | --- | --- |
| **Key cột `imsi`** | luôn (mã hoá `imsi`) | `hla-datalake/datalake/column-keys/silver/vtnet_ebs_3g_4g/imsi` | `keyPrefix` | một chuỗi bí mật bất kỳ, nên `openssl rand -base64 32` |
| **Key đường dẫn** | chỉ kịch bản B | `hla-datalake/datalake/path-keys/<SHA256 hex của "ebs_3g_4g/dt=20261012/batch=1520">` | `keyPrefix` | **base64 chuẩn của đúng 32 byte** (AES-256), `openssl rand -base64 32` |

Quy tắc đặt tên path:
- Key cột: `column-keys/<database>/<table>/<column>`, cả ba phần **chữ thường**. `database` = `silver`, `table` = đúng giá trị `table` trong phiếu (`vtnet_ebs_3g_4g`), `column` = `imsi`.
- Key đường dẫn: `<pathHash>` là SHA-256 dạng hex của **đúng chuỗi** `path` (không có `/` đầu hoặc cuối, giữ hoa thường). Tính bằng:

```bash
printf '%s' 'ebs_3g_4g/dt=20261012/batch=1520' | sha256sum | cut -d' ' -f1
```

Hai template path trên là mặc định của mock (`dak.vault.column-path-template`, `dak.vault.path-key-template`). DAK thật có kho và path riêng, mock chỉ giả lập.

## 3. Tạo key

Có hai cách, chọn một. **Cách 1** (qua mock) đơn giản hơn, mock tự ghi Vault đúng path ở trên. **Cách 2** ghi thẳng Vault bằng pod `vault-cli` (`k8s/vault-cli-pod.yaml`).

Chuẩn bị (port-forward tới dak-mock, đọc token từ bàn phím):

```bash
NS=<namespace>
kubectl -n $NS port-forward deploy/dak-mock 8085 &
read -rs -p "Admin token: " AT; echo
```

### 3.1 Key đường dẫn (chỉ kịch bản B, file mã hoá)

Key này phải **trùng đúng key đã dùng để mã hoá file**. Cách 1:

```bash
curl -s -X POST localhost:8085/mock-admin/path-keys -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' \
  -d '{"path":"ebs_3g_4g/dt=20261012/batch=1520","keyValue":"<base64 của đúng 32 byte>"}'
```

Cách 2 (thẳng Vault):

```bash
H=$(printf '%s' 'ebs_3g_4g/dt=20261012/batch=1520' | sha256sum | cut -d' ' -f1)
kubectl exec vault-cli -n $NS -- sh -c \
  "vault kv put -mount=kv -cas=0 hla-datalake/datalake/path-keys/$H keyPrefix=\"\$(head -c 32 /dev/urandom | base64)\""
```

Kịch bản A: **bỏ qua mục này**. Không có key nghĩa là DAK trả `204` và job đọc file nguyên trạng.

### 3.2 Key cột `imsi` (cả hai kịch bản)

Cách 1:

```bash
curl -s -X POST localhost:8085/mock-admin/column-keys -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' \
  -d '{"database":"silver","table":"vtnet_ebs_3g_4g","column":"imsi"}'
```

(Không đưa `keyValue` thì mock sinh 32 byte ngẫu nhiên, base64.)

Cách 2:

```bash
kubectl exec vault-cli -n $NS -- sh -c \
  'vault kv put -mount=kv -cas=0 hla-datalake/datalake/column-keys/silver/vtnet_ebs_3g_4g/imsi keyPrefix="$(head -c 32 /dev/urandom | base64)"'
```

Kiểm: `kubectl exec vault-cli -n $NS -- vault kv get -mount=kv hla-datalake/datalake/column-keys/silver/vtnet_ebs_3g_4g/imsi` (đừng dán giá trị `keyPrefix` ra nơi khác).

> **Key cột không bao giờ đổi.** Bảng silver đã có dữ liệu mã hoá bằng key nào thì chỉ giải mã được bằng đúng key đó; `cas=0` chặn ghi đè. Đừng xoá/tạo lại key sau khi đã ghi dữ liệu.

Nếu **không** tạo key cột trước, luồng vẫn chạy nhưng dài hơn: xem biến thể ở mục 9.

## 4. Gửi phiếu

```bash
read -rs -p "Portal token: " PT; echo
cat > /tmp/ebs-request.json <<'EOF'
{
  "source": "VTNET",
  "path": "ebs_3g_4g/dt=20261012/batch=1520",
  "batchId": "202610121520",
  "table": "vtnet_ebs_3g_4g",
  "encryptColumns": ["imsi"],
  "keyField": "<KEY_FIELD>",
  "purpose": "Test: load ebs_3g_4g batch 15:20 of 2026-10-12 into silver, encrypt imsi"
}
EOF
curl -s -X POST localhost:8085/mock-portal/silver-requests -H "X-Mock-Portal-Token: $PT" \
  -H 'Content-Type: application/json' --data @/tmp/ebs-request.json
```

Kỳ vọng: `201`, `"state": "PENDING_APPROVAL"`, `"code": "SLV-<năm>-000001"`, `"dagId": "silver_ingest_vtnet"`. Ghi lại `code` (dưới đây `SLV-2026-000001`).
Sai thì `400 invalid_request` với `problems[]` liệt kê mọi lỗi (vd `table` không bắt đầu bằng `vtnet_`, `batchId` thiếu giờ).

## 5. Admin duyệt (kích hoạt DAG)

```bash
CODE=SLV-2026-000001
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/approve -H "X-Mock-Admin-Token: $AT"
```

Kỳ vọng: `"state": "JOB_SUBMITTED"`, `"dagRunId": "slv-2026-000001"`. Nếu `SUBMIT_FAILED` thì đọc `submitError` (`airflow dag not found` = DAG chưa vào Airflow; `airflow denied the request` = sai user/mật khẩu Airflow; `airflow unavailable` = sai địa chỉ), sửa rồi `POST …/retry-submit`.

## 6. Theo dõi job

```bash
curl -s localhost:8085/mock-portal/silver-requests/$CODE -H "X-Mock-Portal-Token: $PT"    # state: RUNNING -> SUCCEEDED
kubectl get sparkapplication -n $NS | grep silver-vtnet
kubectl logs -n $NS <silver-vtnet-xxxxxxxxxxxx>-driver | grep -E "SILVER|PATH KEY|COLUMN KEYS|WRITE|JOB"
```

Log driver đúng theo thứ tự:

| Mục log | Kịch bản A | Kịch bản B |
| --- | --- | --- |
| `3. PATH KEY (DAK by-path)` | "DAK has no key for this path: files are not encrypted, reading them as they are" | "path key received …: files are encrypted, decrypting with aes-256-gcm" |
| `4. COLUMN KEYS` | `keys received for imsi` (nếu đã tạo ở mục 3.2) | giống A |
| `5. READ, ENCRYPT COLUMNS, WRITE` | số file, danh sách cột, `written silver.vtnet_ebs_3g_4g` | giống A (kèm "decrypting in memory") |
| cuối | `JOB SUCCEEDED` | `JOB SUCCEEDED … decrypted and written` |

Kỳ vọng cuối: dag run `success`, phiếu `SUCCEEDED`, driver exit code `0`.

## 7. Kiểm kết quả trong bảng silver

```sql
SELECT batch_id, count(*) FROM silver.vtnet_ebs_3g_4g GROUP BY batch_id;     -- 1 dòng: 202610121520 | <số dòng của file>
SELECT imsi, <KEY_FIELD> FROM silver.vtnet_ebs_3g_4g LIMIT 5;                 -- imsi là chuỗi mã hoá (base64), KHÔNG phải số IMSI gốc
DESCRIBE DETAIL silver.vtnet_ebs_3g_4g;                                       -- partitionColumns = [batch_id]
```

Kiểm:
1. Số dòng khớp số dòng file nguồn (trừ dòng header): `hdfs dfs -cat <file> | wc -l`.
2. Cột `imsi` không còn giá trị gốc; mọi cột khác (kể cả `<KEY_FIELD>`) giữ nguyên.
3. Giải mã lại được (cần engine nạp `cdr-crypto-udf`, key theo cột lấy từ DAK):

```sql
SELECT cdr_decrypt('silver.vtnet_ebs_3g_4g', 'imsi', imsi, <KEY_FIELD>) FROM silver.vtnet_ebs_3g_4g LIMIT 5;   -- ra IMSI gốc
```

## 8. Chạy lại cùng lô (không ghi trùng)

Gửi lại **phiếu mới** với đúng `path`/`batchId`/`table` (phiếu cũ đã `SUCCEEDED` nên không bị chống trùng). Sau khi `SUCCEEDED`:

```sql
SELECT count(*) FROM silver.vtnet_ebs_3g_4g WHERE batch_id = '202610121520';   -- vẫn bằng số dòng của file, không gấp đôi
```

Gửi batch kế (`path` `…/batch=1540`, `batchId` `202610121540`): bảng có thêm một partition, partition `…1520` giữ nguyên.

## 9. Biến thể: chưa tạo key cột (DAK mở yêu cầu, job chờ duyệt)

Bỏ qua mục 3.2, làm tiếp từ mục 4. Sau khi duyệt phiếu (mục 5):

1. Job gọi `by-column` với `onMissing=request`; DAK mở yêu cầu cho cột `imsi`. Log: `Waiting for DAK to approve the key requests of 'silver.vtnet_ebs_3g_4g' (imsi=YC-…)`, kiểm lại sau 30 s, 60 s, 120 s… (tối đa 600 s một lần, bỏ cuộc sau 7200 s). Phiếu vẫn `RUNNING`.
2. Xem và duyệt yêu cầu (đóng vai admin DAK):

```bash
curl -s localhost:8085/mock-admin/yc -H "X-Mock-Admin-Token: $AT"                              # tìm requestCode của cột imsi
curl -s -X POST localhost:8085/mock-admin/yc/<requestCode>/fulfill -H "X-Mock-Admin-Token: $AT"   # mock tạo key ở Vault
```

3. Lần kiểm kế tiếp của job thấy cột `READY`, mã hoá và ghi bảng; phiếu `SUCCEEDED`. Key vừa tạo nằm ở đúng path ở mục 2.

## 10. Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
| --- | --- | --- |
| Job exit 1, "no such column imsi" / cột `_c0` | Không có header, hoặc sai tên cột | Mục 0: đặt `INPUT_SCHEMA` |
| Job exit 1, `FileDecryptionException … wrong key or corrupted file` | Kịch bản B nhưng key đường dẫn khác key đã mã hoá file | Tạo đúng key (mục 3.1) |
| Job đọc ra dữ liệu rác, hoặc cột `imsi` lạ | Kịch bản B nhưng **chưa tạo** key đường dẫn → DAK trả `204` → job đọc ciphertext | Tạo key đường dẫn **trước** khi nạp |
| Job exit 1, `403 key_disabled` | Key đường dẫn đang bị khoá | Admin mở khoá: `POST /mock-admin/path-keys/enable` `{"path":"…"}` |
| Job exit 5 | Cột `imsi` hết thời gian chờ duyệt, hoặc bị từ chối / khoá | Duyệt YC (mục 9) rồi gửi phiếu mới |
| Job exit 1, "must be strictly under KEY_PATH_ROOT" | `INPUT_ROOT` trong DAG không phải tiền tố của đường dẫn thật | Gốc VTNET phải là `<hdfs>/prod8/landing/vtnet/` |
| `400 invalid_request`, `table` | Bảng không bắt đầu bằng `vtnet_` hoặc có chữ hoa/`-` | Dùng `vtnet_ebs_3g_4g` |
| Phiếu `409 duplicate_request` | Còn phiếu cùng `path`/`batchId` chưa kết thúc | Đợi phiếu cũ kết thúc hoặc huỷ |
| Driver không lấy được token DAK | Sai `DAK_TOKEN_URL`/`DAK_CLIENT_ID`, thiếu Secret `spark-dak-client` | Mục 1, dòng 6–8 |
| Driver lỗi `PKIX path building failed` / `UnknownHostException sso-lakehouse…` | Thiếu hoặc sai ConfigMap `keycloak-ca`, hoặc sai IP host alias | Mục 1, dòng 8b |
| Pod bị từ chối: `initContainers[0].volumeMounts[0].name: Not found: keycloak-ca` | Mutating webhook của Spark Operator tắt, hoặc ConfigMap chưa tạo | Bật webhook, tạo `keycloak-ca` |

## 11. Xoá phiếu cũ

Mock DAK **không có API xoá phiếu**: phiếu nằm trong bộ nhớ của pod `dak-mock` (mất khi pod khởi động lại). Phiếu đã kết thúc (`SUCCEEDED`, `FAILED`, `REJECTED`, `CANCELLED`) **không chặn** phiếu mới cùng `path`/`batchId`, nên thường không cần xoá, chỉ gửi phiếu mới. Chỉ phiếu chưa kết thúc mới chặn (`409 duplicate_request`).

| Phiếu đang ở trạng thái | Cách xử lý |
| --- | --- |
| `PENDING_APPROVAL` | Người gửi huỷ: `curl -s -X POST localhost:8085/mock-portal/silver-requests/$CODE/cancel -H "X-Mock-Portal-Token: $PT"` (thành `CANCELLED`); hoặc admin từ chối: `curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/reject -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d '{"reasonText":"test lai"}'` (thành `REJECTED`) |
| `SUBMIT_FAILED` | Sửa nguyên nhân (DAG chưa có, Airflow lỗi…) rồi `POST /mock-admin/silver-requests/$CODE/retry-submit`. Không có API huỷ |
| `JOB_SUBMITTED` / `RUNNING` | Chờ job kết thúc (phiếu tự sang `SUCCEEDED`/`FAILED`). Muốn dừng sớm thì xoá SparkApplication (`kubectl -n <ns> delete sparkapplication <tên>`) hoặc đánh failed DAG run trong Airflow |
| Mọi trạng thái, muốn xoá sạch | Khởi động lại pod: `kubectl -n <ns> rollout restart deploy/dak-mock`. **Xoá hết** phiếu, YC và token phiên; key đã lưu trong Vault không mất |

Xem danh sách phiếu để tìm mã: `curl -s localhost:8085/mock-admin/silver-requests -H "X-Mock-Admin-Token: $AT"`.

## 12. Dọn dẹp sau khi test

- Xoá bảng thử nếu cần: `DROP TABLE silver.vtnet_ebs_3g_4g` (xoá luôn partition). **Giữ** key cột `imsi` trong Vault nếu bảng thật sẽ dùng lại tên này; xoá key rồi tạo lại sẽ làm dữ liệu cũ không giải mã được.
- Thu hồi token đã dùng: `unset PT AT`.
- SparkApplication tự xoá sau 7 ngày (`timeToLiveSeconds`).

## 13. Kịch bản B từng bước: file mã hoá cả file (AES-256-GCM), key đường dẫn trên DAK, ghi bảng mã hoá cột

Mục tiêu: chứng minh job **giải mã được file mã hoá cả file bằng key `by-path` lấy từ DAK**, rồi mã hoá cột `imsi` và ghi bảng. Dùng file mẫu nhỏ (50 dòng) tự tạo, không đụng file thật ở mục 0. Điều kiện có sẵn: checklist mục 1, quyền Vault mục 2.

| | |
| --- | --- |
| `path` trong phiếu | `ebs_3g_4g/dt=20261012/batch=1600` |
| `batchId` | `202610121600` |
| Bảng đích | `silver.vtnet_ebs_3g_4g`, partition `batch_id=202610121600` (bảng chung với kịch bản A, partition `…1520` nếu đã chạy A không bị đụng) |
| Cột mã hoá / cột giữ plaintext | `imsi` / `event_time` |
| Schema file | `imsi,lac,sac,eci,event_time,event_ids,event_result,date_hour` (có header, phân tách `,`) |
| `<pathHash>` | `c4a515164c6bf88067c45d1232a3a62a511b41907011f2c825ab727f1603ed58` (SHA-256 của `path` ở trên) |

> **Key đường dẫn là bí mật:** chỉ giữ trong terminal của bạn, không dán vào chat, ticket hay commit. Dùng xong thì xoá file mẫu và `unset` biến.

### 13.1 Tạo file mẫu và mã hoá cả file

Script [tools/make_encrypted_sample.py](tools/make_encrypted_sample.py) (cần `pip install cryptography`) sinh 50 dòng giả, rồi mã hoá cả file theo đúng định dạng job giải mã: `IV 12 byte ‖ ciphertext ‖ tag 16 byte`, AES-256-GCM, key 32 byte.

```bash
python3 docs/silver-ingest/tools/make_encrypted_sample.py ./ebs-b
# rows: 50
# key (base64, 32 bytes): <PATH_KEY_B64>        <- giữ lại, dùng ở 13.3
```

Ra hai file trong `./ebs-b/`: `ebs_3g_4g-160000.csv` (**đã mã hoá**, file đưa lên HDFS) và `ebs_3g_4g-160000.csv.plain` (csv gốc, chỉ để đối chiếu ở 13.6, **không** đưa lên HDFS). Muốn dùng key có sẵn: thêm `--key <base64 32 byte>`.

### 13.2 Đưa file mã hoá lên landing

```bash
D=/prod8/landing/vtnet/ebs_3g_4g/dt=20261012/batch=1600
hdfs dfs -mkdir -p $D
hdfs dfs -put ./ebs-b/ebs_3g_4g-160000.csv $D/
hdfs dfs -ls -h $D
hdfs dfs -cat $D/ebs_3g_4g-160000.csv | head -c 64 | xxd | head -2     # byte rác, KHÔNG đọc được chữ "imsi,lac,…"
```

File phải đọc được bằng principal `presto` (mục 1 dòng 10).

### 13.3 Tạo key đường dẫn trên DAK (phải làm TRƯỚC khi gửi phiếu)

Key này là chính `<PATH_KEY_B64>` ở 13.1, đúng 32 byte. Chuẩn bị port-forward như mục 3, rồi:

```bash
read -rs -p "Path key (base64): " PK; echo
curl -s -X POST localhost:8085/mock-admin/path-keys -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' \
  -d "{\"path\":\"ebs_3g_4g/dt=20261012/batch=1600\",\"keyValue\":\"$PK\"}"
unset PK
```

Cách 2 (thẳng Vault, pod `vault-cli`): `vault kv put -mount=kv -cas=0 hla-datalake/datalake/path-keys/c4a515164c6bf88067c45d1232a3a62a511b41907011f2c825ab727f1603ed58 keyPrefix='<PATH_KEY_B64>'`.

Kiểm key đã có (chỉ xem metadata, không in giá trị): `vault kv metadata get -mount=kv hla-datalake/datalake/path-keys/c4a515164c6bf88067c45d1232a3a62a511b41907011f2c825ab727f1603ed58`.

> Chưa tạo key này mà nạp file mã hoá thì DAK trả `204`, job coi file là plaintext và đọc ciphertext ra dữ liệu rác (mục 10). Tạo key trước.

### 13.4 Tạo key cột `imsi` (bỏ qua nếu đã làm ở kịch bản A)

Bảng `silver.vtnet_ebs_3g_4g` dùng chung một key cột `imsi`. Đã tạo ở mục 3.2 thì **bỏ qua** (key không đổi, `cas=0` chặn ghi đè). Chưa có thì tạo:

```bash
curl -s -X POST localhost:8085/mock-admin/column-keys -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' \
  -d '{"database":"silver","table":"vtnet_ebs_3g_4g","column":"imsi"}'
```

Không tạo cũng được, nhưng job sẽ chờ duyệt YC: xem mục 9.

### 13.5 Gửi phiếu, duyệt, theo dõi

```bash
read -rs -p "Portal token: " PT; echo
cat > /tmp/ebs-b-request.json <<'JSON'
{
  "source": "VTNET",
  "path": "ebs_3g_4g/dt=20261012/batch=1600",
  "batchId": "202610121600",
  "table": "vtnet_ebs_3g_4g",
  "encryptColumns": ["imsi"],
  "keyField": "event_time",
  "purpose": "Test B: encrypted-file ebs_3g_4g batch 16:00 of 2026-10-12, decrypt then encrypt imsi"
}
JSON
curl -s -X POST localhost:8085/mock-portal/silver-requests -H "X-Mock-Portal-Token: $PT" \
  -H 'Content-Type: application/json' --data @/tmp/ebs-b-request.json        # 201, PENDING_APPROVAL, ghi lại "code"
CODE=<code vừa nhận>
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/approve -H "X-Mock-Admin-Token: $AT"   # JOB_SUBMITTED
curl -s localhost:8085/mock-portal/silver-requests/$CODE -H "X-Mock-Portal-Token: $PT"                  # RUNNING -> SUCCEEDED
kubectl get sparkapplication -n $NS | grep silver-vtnet
kubectl logs -n $NS <silver-vtnet-xxxxxxxxxxxx>-driver | grep -E "SILVER|PATH KEY|COLUMN KEYS|decrypt|WRITE|JOB"
```

Log driver kỳ vọng, đúng thứ tự (đây là điểm khác kịch bản A):

| Mục log | Nội dung |
| --- | --- |
| `3. PATH KEY (DAK by-path)` | `path key received (keyVersion=…): files are encrypted, decrypting with aes-256-gcm` |
| `4. COLUMN KEYS` | `keys received for imsi (keyField=event_time)` |
| `5. READ, ENCRYPT COLUMNS, WRITE` | `files = 1`, `decrypting in memory (text format)`, danh sách cột `imsi, lac, sac, eci, event_time, event_ids, event_result, date_hour, batch_id`, `written silver.vtnet_ebs_3g_4g` |
| cuối | `JOB SUCCEEDED — 1 file(s) decrypted and written to silver.vtnet_ebs_3g_4g`, exit code `0` |

Log **không** được chứa giá trị key hay dòng dữ liệu giải mã.

### 13.6 Kiểm bảng silver

```sql
SELECT batch_id, count(*) FROM silver.vtnet_ebs_3g_4g WHERE batch_id = '202610121600' GROUP BY batch_id;   -- 202610121600 | 50
SELECT imsi, event_time, event_ids, event_result FROM silver.vtnet_ebs_3g_4g WHERE batch_id = '202610121600' LIMIT 5;
```

Kiểm:
1. **50 dòng**, khớp `wc -l ./ebs-b/ebs_3g_4g-160000.csv.plain` trừ header (51 dòng, trừ 1).
2. Cột `imsi` là chuỗi mã hoá, **khác** giá trị 32 ký tự hex trong file `.plain`. Các cột khác (`lac`, `sac`, `eci`, `event_time`, `event_ids`, `event_result`, `date_hour`) bằng đúng file `.plain`; ô trống (`lac`, `sac`) vẫn là null.
3. Giải mã cột lại ra imsi gốc và khớp file `.plain`:

```sql
SELECT cdr_decrypt('silver.vtnet_ebs_3g_4g', 'imsi', imsi, event_time) AS imsi_plain, event_time
FROM silver.vtnet_ebs_3g_4g WHERE batch_id = '202610121600' ORDER BY event_time LIMIT 5;   -- so với 5 dòng đầu (theo event_time) của .plain
```

4. Không còn thư mục staging (định dạng text giải mã trong bộ nhớ, `DECRYPT_STAGING_PATH` không đặt).

### 13.7 Ca lỗi nên thử (mỗi ca một phiếu mới, `batchId`/`path` mới nếu cần)

| Ca | Cách làm | Kỳ vọng |
| --- | --- | --- |
| Sai key | Tạo key đường dẫn khác `<PATH_KEY_B64>` cho một `path` khác rồi đặt file mã hoá bằng key đúng vào path đó | Job exit 1, `FileDecryptionException … wrong key or corrupted file`, **chưa ghi bảng** |
| Quên tạo key đường dẫn | Bỏ 13.3 cho một `path` khác | DAK `204`, job đọc ciphertext, cột lạ hoặc lỗi parse: chứng minh phải tạo key trước |
| Key sai độ dài | Tạo key đường dẫn không phải base64 của 32 byte | Job exit 1, `The file key from DAK has an unsupported format` |
| File bị sửa | Sửa 1 byte của file đã mã hoá rồi `hdfs dfs -put -f` | Job exit 1, GCM từ chối (sai tag), **không ra dữ liệu rác** |

### 13.8 Dọn dẹp

```bash
hdfs dfs -rm -r /prod8/landing/vtnet/ebs_3g_4g/dt=20261012/batch=1600     # file mã hoá thử
rm -rf ./ebs-b /tmp/ebs-b-request.json                                    # có cả file .plain
unset PT AT PK
```

Xoá partition thử nếu cần: `DELETE FROM silver.vtnet_ebs_3g_4g WHERE batch_id = '202610121600'`. Key đường dẫn của `batch=1600` chỉ dùng cho thử nghiệm này; **giữ** key cột `imsi` (mục 12).
