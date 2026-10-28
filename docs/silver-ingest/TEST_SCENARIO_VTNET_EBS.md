# Kịch bản test: nạp dữ liệu VTT và VTNET vào silver theo dataset, chạy mỗi ngày

Mục tiêu: một dataset đi hết luồng **phiếu (path = dataset) → admin duyệt MỘT lần → DAK đặt lịch ở Airflow → DAG của dataset chạy mỗi ngày → bảng trong `demo_db`**, với từng cách xử lý.
Thiết kế: [PLAN.md](PLAN.md). Cách dùng job: [silver-ingest-app/README.md](../../silver-ingest-app/README.md). Cách dùng mock: [dak-mock/README.md](../../dak-mock/README.md) mục 2.3, 2.4.
(Tên file giữ `…_EBS.md` để các link cũ không gãy; kịch bản này bao cả VTNET (`nims`, `ebs_3g_4g`) lẫn VTT (`f001-mpre-call`).)

Ký hiệu: `<…>` là giá trị bạn phải điền (namespace, token, địa chỉ Airflow…). Mọi lệnh `curl` tới dak-mock chạy sau khi `kubectl port-forward deploy/dak-mock 8085`.

## 0. Dataset và kịch bản

Mỗi dataset (của VTT lẫn VTNET) có một DAG định kỳ (`silver_ingest_<nguồn>_<dataset>`), một mode của Spark app, và **một phiếu** cho cả dataset. Mỗi lần chạy nạp thư mục của
**ngày hôm trước** (mọi `batch=...`/`hour=...` bên trong) vào partition `batch_id = <ngày>`:

| Dataset | DAG | Mode | Thư mục một ngày | `batch_id` |
| --- | --- | --- | --- | --- |
| `nims` | `silver_ingest_vtnet_nims` | `decrypt-encrypt` | `nims/dt=YYYYMMDD/batch=N/…` | `YYYYMMDD` |
| `ebs_3g_4g` | `silver_ingest_vtnet_ebs_3g_4g` | `passthrough` | `ebs_3g_4g/dt=YYYYMMDD/batch=HHmm/…` | `YYYYMMDD` |
| `f001-mpre-call` (VTT) | `silver_ingest_vtt_f001_mpre_call` | `passthrough` | `f001-mpre-call/date=YYYYMMDD/hour=HH/…` | `YYYYMMDD` |

| Kịch bản | Dataset | File landing | Key `by-path` của dataset | Cột mã hoá | Mục |
| --- | --- | --- | --- | --- | --- |
| **N1** (thực tế của `nims`) | `nims` | mã hoá cả file | **có** | không (`encryptColumns` rỗng): chỉ giải mã | 5 |
| **N2** | `nims` | mã hoá cả file | có | có (`lng`, `lat`, `keyField=eci`) | 6 |
| **N3** | `nims` | không mã hoá | **chưa có** (DAK trả `204`) | không | 7 |
| **E1** | `ebs_3g_4g` | thường | không dùng | không: ghi nguyên trạng | 8 |
| **V1** | `f001-mpre-call` (VTT) | thường (cột đã được VTT mã hoá sẵn) | không dùng | không: ghi nguyên trạng | 8.1 |

**Một dataset chỉ có một phiếu chưa kết thúc.** `nims` có ba kịch bản (N1, N2, N3) nên chạy **lần lượt**: xong một kịch bản thì `stop` phiếu (mục 4) rồi mới gửi phiếu kế.

Dữ liệu thử là file giả sinh bằng [tools/make_encrypted_sample.py](tools/make_encrypted_sample.py), cùng schema với file thật của `nims`
(`tac,eci,lng,lat,city,district,cell_status`, có header). Xem file thật (cần biết tên cột, header, delimiter):

```bash
hdfs dfs -ls -h /landing/nims/dt=20261013
hdfs dfs -cat /landing/nims/dt=20261013/batch=1/infra_cell_4g-130012.csv | head -n 5
```

- Header có tên cột → giữ nguyên cấu hình DAG (`INPUT_OPTION_header=true`). Không có header → thêm `"INPUT_SCHEMA": "<c1 STRING, …>"` vào `BASE_ENV` trong `airflow/dags/silver_ingest_vtnet.py` và bỏ `INPUT_OPTION_header`.
- Delimiter không phải dấu phẩy → thêm `"INPUT_OPTION_delimiter": "<ký tự>"`.
- Gốc landing là `INPUT_ROOT` trong `silver_ingest_vtnet.py` (đang đặt `<hdfs>/prod8/landing/vtnet/`, **giả định**); thay `ROOT` ở các lệnh dưới cho khớp.
- Thư mục của ngày có nhiều bảng khác schema (vd `nims` có thêm file loại khác ngoài `infra_cell_4g-*.csv`) thì job nạp tất cả vào một bảng: **chưa hỗ trợ lọc theo tên file**.

## 1. Những gì phải có sẵn (checklist)

| # | Cần | Kiểm bằng |
| --- | --- | --- |
| 1 | Image `silver-ingest-app` đã build và push, tag khớp `IMAGE` trong `airflow/dags/silver_ingest_common.py` (image có **cả hai** entry point `PassthroughSilverApp`, `DecryptEncryptSilverApp`) | `docker pull <registry>/silver-ingest-app:<tag>` |
| 2 | 4 file DAG chép **cùng thư mục dags** của Airflow: `silver_ingest_common.py`, `silver_ingest_factory.py`, `silver_ingest_vtt.py`, `silver_ingest_vtnet.py` | DAG `silver_ingest_vtnet_nims`, `silver_ingest_vtnet_ebs_3g_4g`, `silver_ingest_vtt_f001_mpre_call` hiện trong UI, không có import error |
| 3 | User Airflow riêng cho DAK (vd `dak-trigger`) có quyền: **đọc/tạo/sửa/xoá Variables**, **sửa DAGs** (bật/tắt), đọc DAG runs (và tạo dag run khi chạy tay) | `curl -u dak-trigger:… $AF/api/v1/variables?limit=1` và `$AF/api/v1/dags/silver_ingest_vtnet_nims` đều trả `200` |
| 4 | dak-mock đang chạy với `dak.deid.submitter: airflow`, `dak.deid.airflow.*`, `AIRFLOW_PASSWORD` | README dak-mock mục 11 |
| 5 | Tài khoản portal giả (`dak.deid.portal-users`, có token) và token admin (Secret `dak-mock-admin-token`) | `/mock-admin/silver-requests` trả `200` với token admin |
| 6 | **Client nội bộ** của DAK: khai trong ConfigMap dak-mock với `kind: internal`, và có client Keycloak tương ứng (chỉ cần cho `nims`) | README dak-mock mục 4 |
| 7 | Secret `spark-dak-client` (key `clientSecret`) trong **namespace chạy job** (chỉ cần cho `nims`) | `kubectl get secret spark-dak-client -n <ns>` |
| 8 | `DAK_CLIENT_ID` trong `dak_env` của `silver_ingest_vtnet.py` đã thay `<client-id-noi-bo>` bằng client id nội bộ thật | đọc file |
| 8b | ConfigMap `keycloak-ca` (key `ca.pem`) trong namespace chạy job; `KEYCLOAK_IP`/`KEYCLOAK_HOST` trong `silver_ingest_common.py` đúng với cụm | `kubectl get configmap keycloak-ca -n <ns>` |
| 9 | Spark Operator và ServiceAccount `spark-application-sa` trong namespace; Kerberos/HDFS như các job khác | job khác chạy được |
| 10 | Thư mục landing đọc được bằng principal `presto` (file `rw-r--r--`, đọc được) | `hdfs dfs -cat … \| head` |
| 11 | `pip install cryptography` (chỉ để chạy script tạo file mã hoá) | `python3 -c "import cryptography"` |

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
| **Key của dataset `nims`** (`by-path`) | N1, N2 (file mã hoá) | `hla-datalake/datalake/path-keys/212b79ed7b6510ef4a1834f695ee8d754b8304d4ed6ece0f2fa88e6151519ebe` | `keyPrefix` | **base64 chuẩn của đúng 32 byte** (AES-256): chính key mà script in ra |
| **Key cột** | chỉ N2 | `hla-datalake/datalake/column-keys/demo_db/vtnet_nims_infra_cell_4g_enc/lng` và `…/lat` | `keyPrefix` | một chuỗi bí mật bất kỳ, nên `openssl rand -base64 32` |

Quy tắc đặt tên path:
- Key `by-path`: **một key cho cả dataset**, đường dẫn gửi DAK là **tên dataset** (`nims`), không phải đường dẫn của từng ngày hay lô. `<pathHash>` là SHA-256 dạng hex của **đúng chuỗi** đó (giữ hoa thường). Tính bằng:

```bash
printf '%s' 'nims' | sha256sum | cut -d' ' -f1     # 212b79ed7b6510ef4a1834f695ee8d754b8304d4ed6ece0f2fa88e6151519ebe
```

- Key cột: `column-keys/<database>/<table>/<column>`, cả ba phần **chữ thường**. `database` = `demo_db`, `table` = đúng giá trị `table` trong phiếu, `column` = tên cột.

Hai template path trên là mặc định của mock (`dak.vault.column-path-template`, `dak.vault.path-key-template`). DAK thật có kho và path riêng, mock chỉ giả lập.

## 3. Chuẩn bị chung

### 3.1 Port-forward, token, địa chỉ Airflow

```bash
NS=<namespace>
AF=http://<airflow>                       # gốc Airflow; lệnh dưới dùng user dak-trigger
kubectl -n $NS port-forward deploy/dak-mock 8085 &
read -rs -p "Admin token: " AT; echo
read -rs -p "Portal token: " PT; echo
read -rs -p "Airflow password: " AP; echo
```

### 3.2 Tạo file mẫu và đưa lên landing

```bash
# N1, N2: file mã hoá cả file. In ra key (base64) — giữ lại cho mục 3.3, không dán vào chat, ticket hay commit.
python3 docs/silver-ingest/tools/make_encrypted_sample.py ./nims-enc --dataset nims
# N3: file không mã hoá.
python3 docs/silver-ingest/tools/make_encrypted_sample.py ./nims-plain --dataset nims --plain
# E1: file ebs không mã hoá.
python3 docs/silver-ingest/tools/make_encrypted_sample.py ./ebs-plain --dataset ebs_3g_4g --plain
# V1: file VTT (cột đã được VTT mã hoá sẵn, nên chỉ là csv thường với giá trị trông như đã mã hoá).
python3 docs/silver-ingest/tools/make_encrypted_sample.py ./vtt-plain --dataset f001-mpre-call --plain
```

`./nims-enc/` có hai file: `infra_cell_4g-130012.csv` (**đã mã hoá**, đưa lên HDFS) và `infra_cell_4g-130012.csv.plain` (csv gốc, chỉ để đối chiếu, **không** đưa lên HDFS).

Đặt file vào thư mục của **một ngày thử** (`dt=20261013`), mỗi ngày hai lô để thấy job nạp cả ngày:

```bash
ROOT=/prod8/landing/vtnet        # khớp INPUT_ROOT trong silver_ingest_vtnet.py
put() { hdfs dfs -mkdir -p "$ROOT/$1" && hdfs dfs -put -f "$2" "$ROOT/$1/" && hdfs dfs -ls -h "$ROOT/$1"; }

put nims/dt=20261013/batch=1 ./nims-enc/infra_cell_4g-130012.csv       # N1, N2 (file mã hoá)
put nims/dt=20261013/batch=2 ./nims-enc/infra_cell_4g-130012.csv
put nims/dt=20261014/batch=1 ./nims-plain/infra_cell_4g-130012.csv     # N3 (file không mã hoá), ngày khác để không lẫn với N1/N2
put ebs_3g_4g/dt=20261012/batch=1520 ./ebs-plain/ebs_3g_4g-160000.csv  # E1
put ebs_3g_4g/dt=20261012/batch=1540 ./ebs-plain/ebs_3g_4g-160000.csv

ROOT_VTT=/landing/vtt/lakehouse/vtt-core     # khớp INPUT_ROOT trong silver_ingest_vtt.py
hdfs dfs -mkdir -p $ROOT_VTT/f001-mpre-call/date=20261012/hour=12 $ROOT_VTT/f001-mpre-call/date=20261012/hour=13
hdfs dfs -put -f ./vtt-plain/part-0001.csv $ROOT_VTT/f001-mpre-call/date=20261012/hour=12/       # V1: hai giờ của ngày
hdfs dfs -put -f ./vtt-plain/part-0001.csv $ROOT_VTT/f001-mpre-call/date=20261012/hour=13/

hdfs dfs -cat $ROOT/nims/dt=20261013/batch=1/infra_cell_4g-130012.csv | head -c 64 | xxd | head -2   # byte rác, KHÔNG đọc được chữ "tac,eci,…"
```

File phải đọc được bằng principal `presto` (mục 1 dòng 10).

### 3.3 Key của dataset `nims` (làm **trước** khi file mã hoá vào landing, trước N1 và N2)

Key này là chính key script đã in ở 3.2 (đúng 32 byte, base64). Cách 1 (qua mock):

```bash
read -rs -p "Dataset key (base64): " PK; echo
curl -s -X POST localhost:8085/mock-admin/path-keys -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' \
  -d "{\"path\":\"nims\",\"keyValue\":\"$PK\"}"
unset PK
```

Cách 2 (thẳng Vault, pod `vault-cli` của `k8s/vault-cli-pod.yaml`):

```bash
kubectl exec vault-cli -n $NS -- sh -c \
  "vault kv put -mount=kv -cas=0 hla-datalake/datalake/path-keys/212b79ed7b6510ef4a1834f695ee8d754b8304d4ed6ece0f2fa88e6151519ebe keyPrefix='<key base64>'"
```

Kiểm key đã có (chỉ xem metadata, không in giá trị): `kubectl exec vault-cli -n $NS -- vault kv metadata get -mount=kv hla-datalake/datalake/path-keys/212b79ed7b6510ef4a1834f695ee8d754b8304d4ed6ece0f2fa88e6151519ebe`.

> **Key tạo một lần cho cả dataset** (`cas=0` chặn ghi đè), mọi ngày sau dùng chung.
> **Tạo key trước khi file mã hoá vào landing.** DAK chưa có key thì trả `204`, job coi file là plaintext và đọc ciphertext. Có cột mã hoá thì job báo cột không có trong file rồi dừng; **không có cột mã hoá (N1) thì không có gì chặn**: job ghi byte rác vào bảng mà dag run vẫn `success`. Luôn kiểm bảng sau mỗi ngày chạy đầu (mục 5.5).

Kịch bản N3 (DAK chưa có key) phải chạy **trước** bước này, hoặc dùng dataset chưa có key.

## 4. Khung phiếu, theo dõi và dừng (dùng cho mọi kịch bản)

```bash
submit()  { curl -s -X POST localhost:8085/mock-portal/silver-requests -H "X-Mock-Portal-Token: $PT" \
              -H 'Content-Type: application/json' --data @"$1"; }                      # in ra phiếu, ghi lại "code"
approve() { curl -s -X POST localhost:8085/mock-admin/silver-requests/$1/approve -H "X-Mock-Admin-Token: $AT"; }
state()   { curl -s localhost:8085/mock-portal/silver-requests/$1 -H "X-Mock-Portal-Token: $PT"; }
stop()    { curl -s -X POST localhost:8085/mock-admin/silver-requests/$1/stop -H "X-Mock-Admin-Token: $AT"; }
af()      { curl -s -u dak-trigger:"$AP" -H 'Content-Type: application/json' "$@"; }
run_day() { af -X POST $AF/api/v1/dags/$1/dagRuns -d "{\"dag_run_id\":\"test-$2-$(date +%s)\",\"conf\":{\"date\":\"$2\"}}"; }  # chạy tay một ngày
```

Phiếu đúng: `201`, `"state": "PENDING_APPROVAL"`, `"code": "SLV-<năm>-000001"`, `"dagId"` là DAG của dataset, `"scheduleTime"` là giờ chạy, **không có** `batchId`.
Sai thì `400 invalid_request` với `problems[]` liệt kê mọi lỗi (vd `path` là một ngày/lô thay vì tên dataset, có `batchId`, thiếu `scheduleTime`, `table` không bắt đầu bằng `vtnet_`).
Dataset đang có phiếu chưa kết thúc thì `409 duplicate_request`.

**Duyệt một lần** → `"state": "SCHEDULED"` (không có `dagRunId`). Nếu `SUBMIT_FAILED` thì đọc `submitError` (`airflow dag not found` = DAG chưa vào Airflow; `airflow denied the request` = sai user/mật khẩu hoặc thiếu quyền Variables/DAGs;
`airflow unavailable` = sai địa chỉ), sửa rồi `POST …/retry-submit`. Kiểm DAK đã đặt lịch ở Airflow (sau khoảng 30 giây để Airflow parse lại file DAG):

```bash
af $AF/api/v1/variables/silver_ingest_vtnet_nims          # value = JSON: ticket, table, cron "0 3 * * *", encryptColumns…
af $AF/api/v1/dags/silver_ingest_vtnet_nims               # is_paused=false, schedule_interval có cron của phiếu
```

**Chạy thử không chờ tới giờ:** `run_day silver_ingest_vtnet_nims 20261013` (dag run tay, `conf.date` chọn ngày có dữ liệu). Lịch thật chạy lúc `scheduleTime` cho **ngày hôm trước**: muốn kiểm lịch,
đặt `scheduleTime` vài phút sau giờ hiện tại và để dữ liệu ở thư mục `dt=<hôm qua>`.

Theo dõi job:

```bash
state $CODE                                    # state = SCHEDULED, lastRun = RUNNING -> SUCCEEDED
kubectl get sparkapplication -n $NS | grep silver-vtnet
kubectl logs -n $NS <silver-vtnet-…-xxxxxxxxxxxx>-driver | grep -E "SILVER|PATH KEY|COLUMN KEYS|decrypt|WRITE|READ|JOB"
```

**Dừng** (xong kịch bản, hoặc muốn đổi bảng/cột/giờ chạy): `stop $CODE` → `STOPPED`, DAK tắt DAG và xoá Variable. Muốn đổi cấu hình thì `stop` rồi gửi phiếu mới.

## 5. Kịch bản N1: `nims`, mã hoá cả file, không mã hoá cột nào

Mục tiêu: job lấy key của dataset ở DAK, giải mã file, ghi bảng nguyên giá trị, **không gọi `by-column`**.

### 5.1 Gửi phiếu

```bash
cat > /tmp/nims-n1.json <<'JSON'
{
  "source": "VTNET",
  "path": "nims",
  "scheduleTime": "03:00",
  "table": "vtnet_nims_infra_cell_4g",
  "encryptColumns": [],
  "purpose": "Test N1: daily load of the nims dataset at 03:00, decrypt the files only, no column to encrypt"
}
JSON
submit /tmp/nims-n1.json          # ghi lại code, vd CODE=SLV-2026-000001
```

(`encryptColumns` có thể bỏ hẳn; `keyField` không cần. `path` là `nims`, không có ngày, không có `batchId`.)

### 5.2 Duyệt một lần và kiểm lịch

```bash
CODE=SLV-2026-000001
approve $CODE                      # SCHEDULED
af $AF/api/v1/variables/silver_ingest_vtnet_nims
af $AF/api/v1/dags/silver_ingest_vtnet_nims
```

### 5.3 Chạy một ngày

```bash
run_day silver_ingest_vtnet_nims 20261013
```

### 5.4 Log driver đúng theo thứ tự

| Mục log | Nội dung |
| --- | --- |
| `1. CONFIG` | `MODE: decrypt-encrypt`, `KEY_PATH: nims`, `CDR_ENCRYPT_COLUMNS: (none)`, `INPUT_PATH: …/nims/dt=20261013`, `BATCH: batch_id = 20261013` |
| `3. PATH KEY (DAK by-path)` | `path: nims`; `path key received (keyVersion=…): files are encrypted, decrypting with aes-256-gcm` |
| `4. COLUMN KEYS` | `no column to encrypt: DAK by-column is not called, columns are written as they are` |
| `5. READ, ENCRYPT COLUMNS, WRITE` | `files = 2` (hai lô của ngày), `decrypting in memory (text format)`, cột `tac, eci, lng, lat, city, district, cell_status, batch_id`, `written demo_db.vtnet_nims_infra_cell_4g` |
| cuối | `JOB SUCCEEDED — 2 file(s) decrypted and written to demo_db.vtnet_nims_infra_cell_4g`, exit code `0` |

Log **không** được chứa giá trị key hay dòng dữ liệu giải mã. Kỳ vọng cuối: dag run `success`, `state $CODE` có `"lastRun": "SUCCEEDED"` (phiếu vẫn `SCHEDULED`), driver exit code `0`.

### 5.5 Kiểm bảng

```sql
SELECT batch_id, count(*) FROM demo_db.vtnet_nims_infra_cell_4g GROUP BY batch_id;   -- 1 dòng: 20261013 | 100 (hai lô × 50 dòng)
SELECT * FROM demo_db.vtnet_nims_infra_cell_4g ORDER BY eci LIMIT 5;                 -- đọc được như file .plain, KHÔNG phải byte rác
DESCRIBE DETAIL demo_db.vtnet_nims_infra_cell_4g;                                    -- partitionColumns = [batch_id]
```

Kiểm:
1. 100 dòng (hai file × 50 dòng): `batch=1` và `batch=2` đều được nạp, và bảng **không** có cột `batch` (cột partition của thư mục không lọt vào bảng).
2. **Mọi cột bằng đúng file `.plain`** (tên cột đúng `tac, eci, lng, lat, city, district, cell_status`, không phải `_c0…` hay chuỗi rác, `city`/`district` đúng tiếng Việt có dấu).
3. `by-column` không được gọi: không có dòng `COLUMN KEYS … keys received`, và không có yêu cầu key (YC) mới ở `GET /mock-admin/yc`.

### 5.6 Chạy lại một ngày và ngày kế tiếp

- Chạy lại **cùng ngày**: `run_day silver_ingest_vtnet_nims 20261013` lần nữa → partition `20261013` bị thay, vẫn 100 dòng, không gấp đôi.
- **Ngày kế**: đặt file vào `nims/dt=20261015/batch=1`, `run_day silver_ingest_vtnet_nims 20261015` → bảng có thêm partition `20261015`, partition `20261013` giữ nguyên.
- Ngày **không có** thư mục: `run_day silver_ingest_vtnet_nims 20260101` → job exit 1 (`INPUT_PATH does not exist`), dag run `failed`, `lastRun = FAILED`, phiếu vẫn `SCHEDULED`.

Xong thì `stop $CODE`.

## 6. Kịch bản N2: `nims`, mã hoá cả file và mã hoá cột

Khác N1: phiếu chọn cột. Dùng **bảng khác** để không trộn cột đã mã hoá và chưa mã hoá trong một bảng. Phải `stop` phiếu N1 trước (một dataset một phiếu).

```bash
cat > /tmp/nims-n2.json <<'JSON'
{
  "source": "VTNET",
  "path": "nims",
  "scheduleTime": "03:00",
  "table": "vtnet_nims_infra_cell_4g_enc",
  "encryptColumns": ["lng", "lat"],
  "keyField": "eci",
  "purpose": "Test N2: daily load of the nims dataset, decrypt the files then encrypt lng and lat"
}
JSON
```

Tạo key cột trước (nếu không, xem mục 10):

```bash
for c in lng lat; do
  curl -s -X POST localhost:8085/mock-admin/column-keys -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' \
    -d "{\"database\":\"demo_db\",\"table\":\"vtnet_nims_infra_cell_4g_enc\",\"column\":\"$c\"}"   # không đưa keyValue: mock sinh 32 byte ngẫu nhiên
done
```

Rồi `submit /tmp/nims-n2.json`, `approve`, `run_day silver_ingest_vtnet_nims 20261013`. Variable lúc này có `"encryptColumns": ["lng","lat"], "keyField": "eci"`. Log khác N1 ở mục `4. COLUMN KEYS`: `keys received for lng, lat (keyField=eci)`. Kiểm bảng:

```sql
SELECT batch_id, count(*) FROM demo_db.vtnet_nims_infra_cell_4g_enc GROUP BY batch_id;      -- 20261013 | 100
SELECT eci, lng, lat, city FROM demo_db.vtnet_nims_infra_cell_4g_enc ORDER BY eci LIMIT 5;  -- lng, lat là chuỗi mã hoá, các cột khác như file .plain
SELECT cdr_decrypt('demo_db.vtnet_nims_infra_cell_4g_enc', 'lng', lng, eci) AS lng_plain
FROM demo_db.vtnet_nims_infra_cell_4g_enc ORDER BY eci LIMIT 5;                              -- ra lng gốc, khớp file .plain (cần engine nạp cdr-crypto-udf)
```

> **Key cột không bao giờ đổi.** Bảng đã có dữ liệu mã hoá bằng key nào thì chỉ giải mã được bằng đúng key đó; `cas=0` chặn ghi đè. Đừng xoá/tạo lại key cột sau khi đã ghi dữ liệu.

Xong thì `stop`.

## 7. Kịch bản N3: `nims`, DAK chưa có key của dataset (file không mã hoá)

Chạy khi DAK **chưa có** key của `nims` (làm trước mục 3.3), và `stop` phiếu trước đó. Ngày thử `20261014` chứa csv thường.

```bash
cat > /tmp/nims-n3.json <<'JSON'
{
  "source": "VTNET",
  "path": "nims",
  "scheduleTime": "03:00",
  "table": "vtnet_nims_infra_cell_4g_plain",
  "encryptColumns": [],
  "purpose": "Test N3: the nims dataset has no key at DAK so the files are read as they are"
}
JSON
```

`submit`, `approve`, `run_day silver_ingest_vtnet_nims 20261014`. Log `3. PATH KEY`: `DAK has no key for this path: files are not encrypted, reading them as they are`; `5.`: không có dòng `decrypting`;
`JOB SUCCEEDED — 1 file(s) written` (không có chữ `decrypted`). Bảng `demo_db.vtnet_nims_infra_cell_4g_plain`, partition `20261014`, 50 dòng như file `.plain`. Xong thì `stop`.

## 8. Kịch bản E1: `ebs_3g_4g`, ghi nguyên trạng (passthrough)

```bash
cat > /tmp/ebs-e1.json <<'JSON'
{
  "source": "VTNET",
  "path": "ebs_3g_4g",
  "scheduleTime": "02:30",
  "table": "vtnet_ebs_3g_4g",
  "purpose": "Test E1: daily load of the ebs_3g_4g dataset at 02:30, write as it is"
}
JSON
```

Phiếu **không có** `encryptColumns`/`keyField` (có thì `400`). Dataset khác `nims` nên chạy song song với phiếu `nims` được. DAG `silver_ingest_vtnet_ebs_3g_4g` chạy `PassthroughSilverApp`: không gọi DAK,
không cần Secret `spark-dak-client` hay `keycloak-ca`. `submit`, `approve`, `run_day silver_ingest_vtnet_ebs_3g_4g 20261012`.
Log: `3. READ SOURCE` (`files = 2`: hai lô `batch=1520`, `batch=1540`), `4. WRITE TARGET`, `JOB SUCCEEDED — 2 file(s) written`; **không có** mục `PATH KEY`/`COLUMN KEYS`.
Bảng `demo_db.vtnet_ebs_3g_4g`, partition `batch_id=20261012`, 100 dòng, cột đúng như file (`imsi, lac, sac, eci, event_time, event_ids, event_result, date_hour`). Xong thì `stop`.

### 8.1 Kịch bản V1: dataset VTT `f001-mpre-call`, ghi nguyên trạng

Phiếu VTT cũng là phiếu định kỳ cho cả dataset (`path` là tên bảng dưới `vtt-core`, không phải một giờ). Các cột đã được VTT mã hoá sẵn nên job chỉ đọc và ghi.

```bash
cat > /tmp/vtt-v1.json <<'JSON'
{
  "source": "VTT",
  "path": "f001-mpre-call",
  "scheduleTime": "01:30",
  "table": "vtt_f001_mpre_call",
  "purpose": "Test V1: daily load of the VTT table f001-mpre-call at 01:30, write as it is"
}
JSON
```

`submit /tmp/vtt-v1.json`, `approve` (→ `SCHEDULED`, Variable `silver_ingest_vtt_f001_mpre_call`), `run_day silver_ingest_vtt_f001_mpre_call 20261012`. Log: `3. READ SOURCE` (`files = 2`: hai giờ của ngày),
`4. WRITE TARGET`, `JOB SUCCEEDED — 2 file(s) written`; không có `PATH KEY`/`COLUMN KEYS`, không cần Secret `spark-dak-client`. Bảng `demo_db.vtt_f001_mpre_call`, partition `batch_id=20261012`, 100 dòng,
giá trị các cột **giống file** (không bị mã hoá thêm, không bị đổi); bảng không có cột `hour`. Phiếu VTT có bảng bắt đầu bằng `vtt_` (`vtnet_` bị từ chối). Một ngày VTT thật có khoảng 24 giờ × 98 file × 70 MB: cần executor đủ lớn. Xong thì `stop`.

## 9. Kiểm lịch thật

Phiếu `SCHEDULED` tự chạy mỗi ngày. Để thấy một lần chạy theo lịch: gửi phiếu với `scheduleTime` vài phút sau giờ hiện tại, đặt file vào thư mục của **hôm qua** (`dt=<hôm qua>`), duyệt, rồi đợi. Kiểm:
- `af "$AF/api/v1/dags/silver_ingest_vtnet_nims/dagRuns?order_by=-start_date&limit=1"` có dag run `scheduled__…`; `state $CODE` có `lastRun`.
- Bảng có partition `batch_id = <hôm qua>`.
- Không có dag run nào cho các ngày trước lúc duyệt (`catchup=False`).

## 10. Biến thể: chưa tạo key cột (DAK mở yêu cầu, job chờ duyệt)

Chỉ với phiếu có `encryptColumns` (N2). Bỏ qua bước tạo key cột ở mục 6, duyệt phiếu rồi chạy một ngày:

1. Job gọi `by-column` với `onMissing=request`; DAK mở yêu cầu cho từng cột. Log: `Waiting for DAK to approve the key requests of 'demo_db.vtnet_nims_infra_cell_4g_enc' (lng=YC-…, lat=YC-…)`, kiểm lại sau 30 s, 60 s, 120 s… (tối đa 600 s một lần, bỏ cuộc sau 7200 s). Dag run vẫn chạy; `lastRun = RUNNING`.
2. Xem và duyệt yêu cầu (đóng vai admin DAK):

```bash
curl -s localhost:8085/mock-admin/yc -H "X-Mock-Admin-Token: $AT"                              # tìm requestCode của từng cột
curl -s -X POST localhost:8085/mock-admin/yc/<requestCode>/fulfill -H "X-Mock-Admin-Token: $AT"   # mock tạo key ở Vault
```

3. Lần kiểm kế tiếp của job thấy cột `READY`, mã hoá và ghi bảng; dag run `success`. Key vừa tạo nằm ở đúng path ở mục 2. Hết thời gian chờ mà chưa duyệt: job exit 5, dag run của ngày đó `failed` (các ngày sau vẫn chạy theo lịch).

## 11. Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
| --- | --- | --- |
| `400 invalid_request`, `path` | `path` không đúng là tên dataset của nguồn (`nims`, `ebs_3g_4g` của VTNET; `f001-mpre-call` của VTT): có ngày/lô/giờ, sai chữ hoa/thường, hoặc dataset của nguồn khác | Chỉ gửi tên dataset |
| `400 invalid_request`, `batchId` / `scheduleTime` | Phiếu có `batchId` (không được có), hoặc thiếu/sai `scheduleTime` (`HH:mm`) | Bỏ `batchId`, đặt `scheduleTime` |
| `400 invalid_request`, `encryptColumns`/`keyField` | Dataset passthrough (`ebs_3g_4g`, `f001-mpre-call`) có hai trường này; hoặc có cột mà thiếu `keyField` | Bỏ hai trường, hoặc thêm `keyField` |
| `400 invalid_request`, `table` | Bảng không bắt đầu bằng tiền tố của nguồn (`vtt_`, `vtnet_`) hoặc có chữ hoa/`-` | Dùng `vtnet_nims_…`, `vtt_f001_mpre_call` |
| Phiếu `409 duplicate_request` | Dataset đang có phiếu chưa kết thúc (kể cả `SCHEDULED`, của team nào cũng vậy) | `stop` hoặc huỷ phiếu cũ (mục 12) |
| `SUBMIT_FAILED`, `airflow denied the request` | User Airflow thiếu quyền Variables hoặc sửa DAG | Cấp quyền (mục 1 dòng 3), `retry-submit` |
| `SUBMIT_FAILED`, `airflow dag not found` | DAG của dataset chưa vào Airflow (chưa chép file DAG, hoặc lỗi import) | Chép đủ 3 file DAG, xem import error |
| Phiếu `SCHEDULED` mà DAG không chạy theo lịch | Chưa tới 30 giây để Airflow parse lại; DAG bị tạm dừng (`is_paused`); Variable sai (xem mô tả DAG: "The settings in the Airflow Variable are not used…"); chưa tới giờ | Đợi, `af $AF/api/v1/dags/<dag>`, sửa phiếu (`stop` rồi gửi lại) |
| Dag run `failed`, "no approved request is scheduled for this dataset" | Chạy tay khi dataset chưa có phiếu đã duyệt (không có Variable) | Duyệt phiếu trước |
| Job exit 1, `INPUT_PATH does not exist` / `No file under INPUT_PATH` | Thư mục `dt=<ngày>` của ngày đó chưa có hoặc rỗng (dữ liệu chưa về trước giờ chạy) | Đặt giờ chạy muộn hơn; nạp bù bằng `run_day` |
| Job exit 1, "no such column …" / cột `_c0` | Không có header, hoặc sai tên cột | Mục 0: đặt `INPUT_SCHEMA` |
| Job exit 1, `FileDecryptionException … wrong key or corrupted file` | Key của dataset khác key đã mã hoá file | Tạo đúng key (mục 3.3) |
| Bảng ghi **byte rác** mà dag run `success` (N1, không có cột mã hoá) | File mã hoá nhưng DAK **chưa có** key của dataset → `204` → job đọc ciphertext, không có cột nào để báo lỗi | Tạo key dataset **trước** khi file vào landing; luôn kiểm bảng (mục 5.5); xoá partition lỗi (`DELETE FROM … WHERE batch_id = …`) rồi chạy lại ngày đó |
| Job exit 1, `403 key_disabled` | Key của dataset đang bị khoá | Admin mở khoá: `POST /mock-admin/path-keys/enable` `{"path":"nims"}` |
| Job exit 5 | Cột mã hoá hết thời gian chờ duyệt, hoặc bị từ chối / khoá | Duyệt YC (mục 10) rồi chạy lại ngày đó |
| Driver không lấy được token DAK | Sai `DAK_TOKEN_URL`/`DAK_CLIENT_ID`, thiếu Secret `spark-dak-client` | Mục 1, dòng 6–8 |
| Driver lỗi `PKIX path building failed` / `UnknownHostException sso-lakehouse…` | Thiếu hoặc sai ConfigMap `keycloak-ca`, hoặc sai IP host alias | Mục 1, dòng 8b |
| Pod bị từ chối: `initContainers[0].volumeMounts[0].name: Not found: keycloak-ca` | Mutating webhook của Spark Operator tắt, hoặc ConfigMap chưa tạo | Bật webhook, tạo `keycloak-ca` |
| Job báo `ClassNotFoundException … DecryptEncryptSilverApp` hoặc `PassthroughSilverApp` | Image cũ, chưa có hai entry point mới | Build và push lại image, cập nhật `IMAGE` |

## 12. Xoá, huỷ và dừng phiếu

Mock DAK **không có API xoá phiếu**: phiếu nằm trong bộ nhớ của pod `dak-mock` (mất khi pod khởi động lại). Phiếu đã kết thúc (`SUCCEEDED`, `FAILED`, `REJECTED`, `CANCELLED`, `STOPPED`) **không chặn** phiếu mới của dataset. Chỉ phiếu chưa kết thúc mới chặn (`409 duplicate_request`).

| Phiếu đang ở trạng thái | Cách xử lý |
| --- | --- |
| `PENDING_APPROVAL` | Người gửi huỷ: `curl -s -X POST localhost:8085/mock-portal/silver-requests/$CODE/cancel -H "X-Mock-Portal-Token: $PT"` (thành `CANCELLED`); hoặc admin từ chối: `curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/reject -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d '{"reasonText":"test lai"}'` (thành `REJECTED`) |
| `SUBMIT_FAILED` | Sửa nguyên nhân (DAG chưa có, Airflow lỗi, thiếu quyền…) rồi `POST /mock-admin/silver-requests/$CODE/retry-submit`. Không có API huỷ |
| `SCHEDULED` | `stop $CODE` (`STOPPED`): DAK tắt DAG và xoá Variable. Một dag run đang chạy vẫn chạy nốt; muốn dừng ngay thì xoá SparkApplication (`kubectl -n <ns> delete sparkapplication <tên>`) hoặc đánh failed dag run trong Airflow |
| `JOB_SUBMITTED` / `RUNNING` (phiếu VTT) | Chờ job kết thúc (phiếu tự sang `SUCCEEDED`/`FAILED`) |
| Mọi trạng thái, muốn xoá sạch | Khởi động lại pod: `kubectl -n <ns> rollout restart deploy/dak-mock`. **Xoá hết** phiếu, YC và token phiên; key đã lưu trong Vault **và Variable/lịch ở Airflow không mất**: nếu còn phiếu `SCHEDULED`, xoá Variable và tắt DAG bằng tay (`af -X DELETE $AF/api/v1/variables/silver_ingest_vtnet_nims`, `af -X PATCH $AF/api/v1/dags/silver_ingest_vtnet_nims -d '{"is_paused": true}'`) |

Xem danh sách phiếu để tìm mã: `curl -s localhost:8085/mock-admin/silver-requests -H "X-Mock-Admin-Token: $AT"`.

## 13. Dọn dẹp sau khi test

```bash
for c in <các mã phiếu còn SCHEDULED>; do stop $c; done                                      # tắt lịch và xoá Variable
hdfs dfs -rm -r $ROOT/nims/dt=20261013 $ROOT/nims/dt=20261014 $ROOT/ebs_3g_4g/dt=20261012      # file thử (N1–N3, E1)
hdfs dfs -rm -r $ROOT_VTT/f001-mpre-call/date=20261012                                          # file thử (V1)
rm -rf ./nims-enc ./nims-plain ./ebs-plain ./vtt-plain /tmp/nims-n*.json /tmp/ebs-e1.json /tmp/vtt-v1.json                  # có cả file .plain
unset PT AT AP PK
```

- Xoá bảng thử nếu cần: `DROP TABLE demo_db.vtnet_nims_infra_cell_4g` (và `_enc`, `_plain`, `vtnet_ebs_3g_4g`, `vtt_f001_mpre_call`), xoá luôn partition. Xoá một ngày: `DELETE FROM demo_db.<bảng> WHERE batch_id = '<ngày>'`.
- **Giữ** key cột nếu bảng thật sẽ dùng lại tên bảng; xoá key rồi tạo lại sẽ làm dữ liệu cũ không giải mã được. Key `by-path` của dataset `nims` là key thử: thay bằng key thật của VTNET khi chạy thật.
- SparkApplication tự xoá sau 7 ngày (`timeToLiveSeconds`).
