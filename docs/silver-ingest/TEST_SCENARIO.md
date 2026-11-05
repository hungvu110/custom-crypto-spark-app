# Kịch bản test: nạp dữ liệu VTT và VTNET vào silver theo dataset, chạy mỗi ngày

Mục tiêu: một dataset đi hết luồng **phiếu (path = dataset) → admin duyệt MỘT lần → DAK đặt lịch ở Airflow → DAG của dataset chạy mỗi ngày → bảng trong `demo_db`**.
Thiết kế: [PLAN.md](PLAN.md). Cách dùng job: [silver-ingest-app/README.md](../../silver-ingest-app/README.md). Cách dùng mock: [dak-mock/README.md](../../dak-mock/README.md) mục 2.3, 2.4.
(Tên file giữ `…_EBS.md` để các link cũ không gãy; kịch bản này bao cả VTNET (`nims`, `ebs_3g_4g`) lẫn VTT (`f001-mpre-call`).)

Cách đọc: mỗi bước là **một lệnh**, chạy lần lượt từ trên xuống. Chỗ có `<…>` là giá trị bạn phải điền. Các lệnh dùng chung các biến shell đặt ở mục 3.1
(`NS`, `AF`, `AT`, `PT`, `AP`) và `CODE` (mã phiếu) đặt sau khi gửi phiếu; mở **một terminal** cho cả kịch bản để các biến còn nguyên.

## 0. Dataset và kịch bản

Mỗi dataset (của VTT lẫn VTNET) có một DAG định kỳ (`silver_ingest_<nguồn>_<dataset>`), một mode của Spark app, và **một phiếu** cho cả dataset. Mỗi lần chạy nạp **một ngày**: thư mục của
**ngày hôm trước** (mọi `batch=...`/`hour=...` bên trong) vào các dòng `dt = <ngày>` (mỗi dòng có thêm cột `hour`), không bao giờ cả bảng:

| Dataset | DAG | Mode | Thư mục một ngày | `dt` | Partition bảng |
| --- | --- | --- | --- | --- | --- |
| `nims` | `silver_ingest_vtnet_nims` | `decrypt-encrypt` | `nims/dt=YYYYMMDD/batch=N/…` | `YYYYMMDD` | `dt` |
| `ebs_3g_4g` | `silver_ingest_vtnet_ebs_3g_4g` | `passthrough` | `ebs_3g_4g/dt=YYYYMMDD/batch=HHmm/…` | `YYYYMMDD` | `dt, hour` |
| `f001-mpre-call` (VTT) | `silver_ingest_vtt_f001_mpre_call` | `passthrough` | `f001-mpre-call/date=YYYYMMDD/hour=HH/…` | `YYYYMMDD` | `dt, hour` |

| Kịch bản | Dataset | File landing | Key `by-path` của dataset | Cột mã hoá | Mục |
| --- | --- | --- | --- | --- | --- |
| **N1** | `nims` | **mã hoá cả file** | có | **không** (`encryptColumns` rỗng): chỉ giải mã | 5 |
| **E1** | `ebs_3g_4g` | thường | không dùng | không: ghi nguyên trạng | 6 |
| **V1** | `f001-mpre-call` (VTT) | thường (cột đã được VTT mã hoá sẵn) | không dùng | không: ghi nguyên trạng | 6.1 |

Với `nims`, kịch bản chỉ kiểm trường hợp thực tế: file mã hoá cả file và không mã hoá cột nào. (Phiếu và job vẫn hỗ trợ cột mã hoá, nhưng kịch bản này không kiểm.)

Dữ liệu thử là file giả sinh bằng [tools/make_encrypted_sample.py](tools/make_encrypted_sample.py), cùng schema với file thật của `nims`
(`tac,eci,lng,lat,city,district,cell_status`, có header). Xem file thật (cần biết tên cột, header, delimiter):

```bash
hdfs dfs -ls -h /landing/nims/dt=20261013
```

```bash
hdfs dfs -cat /landing/nims/dt=20261013/batch=1/infra_cell_4g-130012.csv | head -n 5
```

- Header có tên cột → giữ nguyên cấu hình DAG (`INPUT_OPTION_header=true`). Không có header → thêm `"INPUT_SCHEMA": "<c1 STRING, …>"` vào `BASE_ENV` trong `airflow/dags/silver_ingest_vtnet.py` và bỏ `INPUT_OPTION_header`.
- Delimiter không phải dấu phẩy → thêm `"INPUT_OPTION_delimiter": "<ký tự>"`.
- Gốc landing là `INPUT_ROOT` trong `silver_ingest_vtnet.py` / `silver_ingest_vtt.py` (đang đặt `<hdfs>/prod8/landing/vtnet/` và `<hdfs>/landing/vtt/lakehouse/vtt-core/`, **giả định**); đổi các đường dẫn `/prod8/landing/vtnet` và `/landing/vtt/lakehouse/vtt-core` ở các lệnh dưới cho khớp.
- Thư mục của ngày có nhiều bảng khác schema (vd `nims` có thêm file loại khác ngoài `infra_cell_4g-*.csv`) thì job nạp tất cả vào một bảng: **chưa hỗ trợ lọc theo tên file**.

## 1. Những gì phải có sẵn (checklist)

| # | Cần | Kiểm bằng |
| --- | --- | --- |
| 1 | Image `silver-ingest-app` đã build và push, tag khớp `IMAGE` trong `airflow/dags/silver_ingest_common.py` (image có **cả hai** entry point `PassthroughSilverApp`, `DecryptEncryptSilverApp`) | `docker pull <registry>/silver-ingest-app:<tag>` |
| 2 | 4 file DAG chép **cùng thư mục dags** của Airflow: `silver_ingest_common.py`, `silver_ingest_factory.py`, `silver_ingest_vtt.py`, `silver_ingest_vtnet.py` | DAG `silver_ingest_vtnet_nims`, `silver_ingest_vtnet_ebs_3g_4g`, `silver_ingest_vtt_f001_mpre_call` hiện trong UI, không có import error |
| 3 | User Airflow riêng cho DAK (vd `dak-trigger`) có quyền: **đọc/tạo/sửa/xoá Variables**, **sửa DAGs** (bật/tắt), đọc DAG runs và **tạo dag run** (DAK tạo khi admin chạy bù một ngày) | `curl -s -u dak-trigger:"$AP" "$AF/api/v1/variables?limit=1"` và `curl -s -u dak-trigger:"$AP" $AF/api/v1/dags/silver_ingest_vtnet_nims` đều trả JSON, không phải lỗi `403` |
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

## 2. Vault: key của dataset `nims` (mount `kv`, KV v2)

dak-mock đọc và tạo key ở Vault bằng token tĩnh trong Secret `dak-mock-vault-token`. Token cần quyền `read` **và** `create` trên:

```hcl
path "kv/data/hla-datalake/datalake/path-keys/*" { capabilities = ["create", "read"] }
```

(Mock ghi bằng `cas=0`: chỉ tạo mới, không bao giờ ghi đè nên không cần `update`.)

| Key | Dùng khi | Path trong Vault | Field | Giá trị |
| --- | --- | --- | --- | --- |
| **Key của dataset `nims`** (`by-path`) | N1 (file mã hoá) | `hla-datalake/datalake/path-keys/212b79ed7b6510ef4a1834f695ee8d754b8304d4ed6ece0f2fa88e6151519ebe` | `keyPrefix` | **base64 chuẩn của đúng 32 byte** (AES-256): chính key mà script in ra |

**Một key cho cả dataset**: đường dẫn gửi DAK là **tên dataset** (`nims`), không phải đường dẫn của từng ngày hay lô. `<pathHash>` là SHA-256 dạng hex của **đúng chuỗi** đó (giữ hoa thường). Tính bằng:

```bash
printf '%s' 'nims' | sha256sum | cut -d' ' -f1
```

Kết quả phải là `212b79ed7b6510ef4a1834f695ee8d754b8304d4ed6ece0f2fa88e6151519ebe`. Template path trên là mặc định của mock (`dak.vault.path-key-template`). DAK thật có kho và path riêng, mock chỉ giả lập.

## 3. Chuẩn bị chung

### 3.1 Namespace, địa chỉ Airflow, port-forward và token

Đặt namespace chạy job:

```bash
NS=<namespace>
```

Đặt gốc Airflow (các lệnh dưới dùng user `dak-trigger`):

```bash
AF=http://<airflow>
```

Port-forward tới dak-mock (để chạy nền, giữ terminal này hoặc mở terminal khác cho các lệnh sau, nhưng nhớ đặt lại các biến):

```bash
kubectl -n $NS port-forward deploy/dak-mock 8085 &
```

Đọc token admin của dak-mock (gõ vào, không hiện lên màn hình):

```bash
read -rs -p "Admin token: " AT; echo
```

Đọc token portal:

```bash
read -rs -p "Portal token: " PT; echo
```

Đọc mật khẩu Airflow của user `dak-trigger`:

```bash
read -rs -p "Airflow password: " AP; echo
```

### 3.2 Tạo file mẫu

Tạo file `nims` **mã hoá cả file** (cho N1). Lệnh in ra `key (base64, 32 bytes): …`: **giữ lại** key này cho mục 3.4, không dán vào chat, ticket hay commit:

```bash
python3 docs/silver-ingest/tools/make_encrypted_sample.py ./nims-enc --dataset nims
```

Tạo file `ebs_3g_4g` không mã hoá (cho E1):

```bash
python3 docs/silver-ingest/tools/make_encrypted_sample.py ./ebs-plain --dataset ebs_3g_4g --plain
```

Tạo file VTT (cho V1; cột đã được VTT mã hoá sẵn, nên chỉ là csv thường với giá trị trông như đã mã hoá):

```bash
python3 docs/silver-ingest/tools/make_encrypted_sample.py ./vtt-plain --dataset f001-mpre-call --plain
```

`./nims-enc/` có hai file: `infra_cell_4g-130012.csv` (**đã mã hoá**, đưa lên HDFS) và `infra_cell_4g-130012.csv.plain` (csv gốc, chỉ để đối chiếu, **không** đưa lên HDFS). Mỗi file chỉ 50 dòng nên mỗi lần chạy thử rất nhanh.

### 3.3 Đưa file mẫu lên landing

Mỗi dataset có một **ngày thử** gồm hai lô (hoặc hai giờ) để thấy job nạp cả ngày. File phải đọc được bằng principal `presto` (mục 1 dòng 10).

`nims`, ngày `20261013`, lô 1:

```bash
hdfs dfs -mkdir -p /prod8/landing/vtnet/nims/dt=20261013/batch=1
```

```bash
hdfs dfs -put -f ./nims-enc/infra_cell_4g-130012.csv /prod8/landing/vtnet/nims/dt=20261013/batch=1/
```

`nims`, ngày `20261013`, lô 2:

```bash
hdfs dfs -mkdir -p /prod8/landing/vtnet/nims/dt=20261013/batch=2
```

```bash
hdfs dfs -put -f ./nims-enc/infra_cell_4g-130012.csv /prod8/landing/vtnet/nims/dt=20261013/batch=2/
```

Kiểm file `nims` đã được mã hoá (kết quả là byte rác, **không** đọc được chữ `tac,eci,…`):

```bash
hdfs dfs -cat /prod8/landing/vtnet/nims/dt=20261013/batch=1/infra_cell_4g-130012.csv | head -c 64 | xxd | head -2
```

`ebs_3g_4g`, ngày `20261012`, lô `1520`:

```bash
hdfs dfs -mkdir -p /prod8/landing/vtnet/ebs_3g_4g/dt=20261012/batch=1520
```

```bash
hdfs dfs -put -f ./ebs-plain/ebs_3g_4g-160000.csv /prod8/landing/vtnet/ebs_3g_4g/dt=20261012/batch=1520/
```

`ebs_3g_4g`, ngày `20261012`, lô `1540`:

```bash
hdfs dfs -mkdir -p /prod8/landing/vtnet/ebs_3g_4g/dt=20261012/batch=1540
```

```bash
hdfs dfs -put -f ./ebs-plain/ebs_3g_4g-160000.csv /prod8/landing/vtnet/ebs_3g_4g/dt=20261012/batch=1540/
```

VTT `f001-mpre-call`, ngày `20261012`, giờ 12:

```bash
hdfs dfs -mkdir -p /landing/vtt/lakehouse/vtt-core/f001-mpre-call/date=20261012/hour=12
```

```bash
hdfs dfs -put -f ./vtt-plain/part-0001.csv /landing/vtt/lakehouse/vtt-core/f001-mpre-call/date=20261012/hour=12/
```

VTT `f001-mpre-call`, ngày `20261012`, giờ 13:

```bash
hdfs dfs -mkdir -p /landing/vtt/lakehouse/vtt-core/f001-mpre-call/date=20261012/hour=13
```

```bash
hdfs dfs -put -f ./vtt-plain/part-0001.csv /landing/vtt/lakehouse/vtt-core/f001-mpre-call/date=20261012/hour=13/
```

### 3.4 Tạo key của dataset `nims` (làm **trước** khi file mã hoá được nạp, trước N1)

Key là chính key script đã in ở 3.2 (đúng 32 byte, base64). Đọc key vào biến (gõ vào, không hiện lên màn hình):

```bash
read -rs -p "Dataset key (base64): " PK; echo
```

Cách 1, qua mock (lệnh này tạo key ở Vault đúng path ở mục 2):

```bash
curl -s -X POST localhost:8085/mock-admin/path-keys -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d "{\"path\":\"nims\",\"keyValue\":\"$PK\"}"
```

Xoá biến chứa key:

```bash
unset PK
```

Cách 2 (thay cho cách 1), ghi thẳng Vault qua pod `vault-cli` của `k8s/vault-cli-pod.yaml` (thay `<key base64>` bằng key):

```bash
kubectl exec vault-cli -n $NS -- sh -c "vault kv put -mount=kv -cas=0 hla-datalake/datalake/path-keys/212b79ed7b6510ef4a1834f695ee8d754b8304d4ed6ece0f2fa88e6151519ebe keyPrefix='<key base64>'"
```

Kiểm key đã có (chỉ xem metadata, không in giá trị):

```bash
kubectl exec vault-cli -n $NS -- vault kv metadata get -mount=kv hla-datalake/datalake/path-keys/212b79ed7b6510ef4a1834f695ee8d754b8304d4ed6ece0f2fa88e6151519ebe
```

> **Key tạo một lần cho cả dataset** (`cas=0` chặn ghi đè), mọi ngày sau dùng chung.
> **Tạo key trước khi file mã hoá được nạp.** DAK chưa có key thì trả `204`, job coi file là plaintext và đọc ciphertext. `nims` không có cột mã hoá nên **không có gì chặn**: job ghi byte rác vào bảng mà dag run vẫn `success`. Luôn kiểm bảng sau lần chạy đầu (mục 5.5).

## 4. Cách gửi phiếu, chạy thử nhanh và dừng phiếu

Mục này giải thích các lệnh sẽ dùng ở các kịch bản; mỗi kịch bản có lệnh đầy đủ của nó.

- **Gửi phiếu:** `curl -s -X POST localhost:8085/mock-portal/silver-requests -H "X-Mock-Portal-Token: $PT" -H 'Content-Type: application/json' --data @<file phiếu>.json`.
  Phiếu đúng: `201`, `"state": "PENDING_APPROVAL"`, `"code": "SLV-<năm>-000001"`, `"dagId"` là DAG của dataset, `"cron"` là lịch chạy, **không có** `batchId`.
  Sai thì `400 invalid_request` với `problems[]` liệt kê mọi lỗi (vd `path` là một ngày/lô thay vì tên dataset, có `batchId`, thiếu hoặc sai `cron`, `table` không bắt đầu bằng tiền tố của nguồn).
  Dataset đang có phiếu chưa kết thúc thì `409 duplicate_request`. **Ghi lại `code`** rồi đặt `CODE=<code>` cho các lệnh sau.
- **Duyệt một lần:** `curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/approve -H "X-Mock-Admin-Token: $AT"` → `"state": "SCHEDULED"`. Nếu `SUBMIT_FAILED` thì đọc `submitError`
  (`airflow dag not found` = DAG chưa vào Airflow; `airflow denied the request` = sai user/mật khẩu hoặc thiếu quyền Variables/DAGs; `airflow unavailable` = sai địa chỉ), sửa rồi
  `curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/retry-submit -H "X-Mock-Admin-Token: $AT"`.
- **Xem phiếu:** `curl -s localhost:8085/mock-portal/silver-requests/$CODE -H "X-Mock-Portal-Token: $PT"` (`state` = `SCHEDULED`, `lastRun` = `RUNNING` rồi `SUCCEEDED`).
- **Chạy bù một ngày** (không đổi lịch, chỉ với phiếu `SCHEDULED`): `curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/run -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d '{"date":"<yyyyMMdd>"}'`. Kết quả `{"requestCode", "dagId", "dagRunId", "date"}`; `date` sai (không phải `yyyyMMdd` có thật) thì `400`, phiếu chưa `SCHEDULED` thì `409`.
- **Dừng phiếu:** `curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/stop -H "X-Mock-Admin-Token: $AT"` → `STOPPED`, DAK tắt DAG và xoá Variable. Muốn đổi bảng/giờ chạy thì dừng rồi gửi phiếu mới.

### Chạy thử nhanh: chọn ngày bằng thao tác `run` của admin, KHÔNG đưa `dt` vào `path` của phiếu

- Phiếu được duyệt một lần cho **cả dataset** và DAG chạy mỗi ngày, nên `path` luôn là tên dataset (`nims`). Đưa `dt=...` vào `path` bị từ chối (`400`): nếu phiếu gắn với một ngày cố định thì lịch hằng ngày vô nghĩa.
- Mỗi lần chạy chỉ nạp **một ngày**, không phải cả bảng. Muốn chạy nhanh hoặc nạp bù một ngày thì admin gọi **`POST /mock-admin/silver-requests/{code}/run`** với `{"date": "yyyyMMdd"}` (lệnh ở mục "Chạy bù một ngày" ngay trên):
  DAK trigger đúng một dag run `conf.date` cho DAG của dataset, **lịch và phiếu không đổi**. Gọi lại cùng ngày là một lần chạy mới (partition của ngày đó bị thay, không ghi trùng). Thư mục thử ở mục 3.3 chỉ 50 dòng mỗi file.
- Với dữ liệu thật, chọn một ngày ít dữ liệu (hoặc tạo một thư mục ngày thử nhỏ) làm `date`. Lịch thật chỉ nạp ngày hôm trước, nên lần chạy bù không làm lệch lịch.
- Vẫn có thể trigger thẳng ở Airflow (`POST $AF/api/v1/dags/<dag>/dagRuns` với `{"conf": {"date": "<yyyyMMdd>"}}`), không qua DAK.
- Muốn nhỏ hơn một ngày (chỉ một `batch=…`) thì cần thêm tuỳ chọn cho lần chạy bù: **chưa có**.

## 5. Kịch bản N1: `nims`, mã hoá cả file, không mã hoá cột nào

Mục tiêu: job lấy key của dataset ở DAK, giải mã file, ghi bảng nguyên giá trị, **không gọi `by-column`**. Cần đã làm 3.1 đến 3.4.

### 5.1 Gửi phiếu

Tạo file phiếu (`path` là `nims`, không có ngày, không có `batchId`; `encryptColumns` rỗng, có thể bỏ hẳn; `keyField` không cần):

```bash
cat > /tmp/nims-n1.json <<'JSON'
{
  "source": "VTNET",
  "path": "nims",
  "cron": "0 3 * * *",
  "table": "vtnet_nims_infra_cell_4g",
  "encryptColumns": [],
  "purpose": "Test N1: daily load of the nims dataset at 03:00, decrypt the files only, no column to encrypt"
}
JSON
```

Gửi phiếu và ghi lại `code` trong kết quả:

```bash
curl -s -X POST localhost:8085/mock-portal/silver-requests -H "X-Mock-Portal-Token: $PT" -H 'Content-Type: application/json' --data @/tmp/nims-n1.json
```

Đặt mã phiếu (thay bằng mã nhận được, vd `SLV-2026-000001`):

```bash
CODE=<code>
```

### 5.2 Duyệt một lần và kiểm lịch

Duyệt phiếu (kết quả `"state": "SCHEDULED"`):

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/approve -H "X-Mock-Admin-Token: $AT"
```

Kiểm Variable DAK đã ghi ở Airflow (sau khoảng 30 giây để Airflow parse lại file DAG; `value` là JSON có `ticket`, `table`, `cron` là `0 3 * * *`, `encryptColumns` là `[]`):

```bash
curl -s -u dak-trigger:"$AP" $AF/api/v1/variables/silver_ingest_vtnet_nims
```

Kiểm DAG đã bật và có lịch (`is_paused` là `false`, `schedule_interval` có cron của phiếu):

```bash
curl -s -u dak-trigger:"$AP" $AF/api/v1/dags/silver_ingest_vtnet_nims
```

### 5.3 Chạy một ngày

Chạy bù ngày thử `20261013` qua DAK (DAK trigger đúng một dag run `conf.date` cho DAG của dataset; kết quả có `dagRunId`, mỗi lần gọi là một lần chạy mới):

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/run -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d '{"date":"20261013"}'
```

Xem SparkApplication vừa tạo (tên dạng `silver-vtnet-nims-<12 ký tự>`):

```bash
kubectl get sparkapplication -n $NS | grep silver-vtnet-nims
```

Xem log driver (thay `<tên>` bằng tên SparkApplication, thêm `-driver`):

```bash
kubectl logs -n $NS <tên>-driver | grep -E "SILVER|PATH KEY|COLUMN KEYS|decrypt|WRITE|READ|JOB"
```

Xem phiếu (`state` vẫn `SCHEDULED`, `lastRun` là `RUNNING` rồi `SUCCEEDED`):

```bash
curl -s localhost:8085/mock-portal/silver-requests/$CODE -H "X-Mock-Portal-Token: $PT"
```

### 5.4 Log driver đúng theo thứ tự

| Mục log | Nội dung |
| --- | --- |
| `1. CONFIG` | `MODE: decrypt-encrypt`, `KEY_PATH: nims`, `CDR_ENCRYPT_COLUMNS: (none)`, `INPUT_PATH: …/nims/dt=20261013`, `BATCH: dt = 20261013`, `HOURS: hour from /batch=[^/]*/infra_cell_4g…`, `OUTPUT_PARTITION_BY: dt` |
| `3. PATH KEY (DAK by-path)` | `path: nims`; `path key received (keyVersion=…): files are encrypted, decrypting with aes-256-gcm` |
| `4. COLUMN KEYS` | `no column to encrypt: DAK by-column is not called, columns are written as they are` |
| `5. READ, ENCRYPT COLUMNS, WRITE` | `files = 2` (hai lô của ngày), `decrypting in memory (text format)`, cột `tac, eci, lng, lat, city, district, cell_status, hour, dt`, `written demo_db.vtnet_nims_infra_cell_4g` |
| cuối | `JOB SUCCEEDED — 2 file(s) decrypted and written to demo_db.vtnet_nims_infra_cell_4g`, exit code `0` |

Log **không** được chứa giá trị key hay dòng dữ liệu giải mã. Kỳ vọng cuối: dag run `success`, `lastRun` là `SUCCEEDED` (phiếu vẫn `SCHEDULED`), driver exit code `0`.

### 5.5 Kiểm bảng

Số dòng của ngày vừa nạp theo giờ (giờ lấy từ tên file `infra_cell_4g-HHmmss.csv`; tổng 100 dòng, tức hai lô × 50 dòng):

```sql
SELECT dt, hour, count(*) FROM demo_db.vtnet_nims_infra_cell_4g GROUP BY dt, hour ORDER BY hour;
```

Đọc vài dòng (phải đọc được như file `.plain`, **không** phải byte rác):

```sql
SELECT * FROM demo_db.vtnet_nims_infra_cell_4g ORDER BY eci LIMIT 5;
```

Kiểm partition (`partitionColumns = [dt]`: nims chỉ partition theo ngày, `hour` là cột thường):

```sql
DESCRIBE DETAIL demo_db.vtnet_nims_infra_cell_4g;
```

Kiểm:
1. 100 dòng (hai file × 50 dòng): `batch=1` và `batch=2` đều được nạp, và bảng **không** có cột `batch` (cột partition của thư mục không lọt vào bảng).
2. **Mọi cột bằng đúng file `.plain`** (tên cột đúng `tac, eci, lng, lat, city, district, cell_status`, không phải `_c0…` hay chuỗi rác, `city`/`district` đúng tiếng Việt có dấu).
3. `by-column` không được gọi: không có dòng `COLUMN KEYS … keys received`, và không có yêu cầu key (YC) mới:

```bash
curl -s localhost:8085/mock-admin/yc -H "X-Mock-Admin-Token: $AT"
```

### 5.6 Chạy lại một ngày và ngày kế tiếp

Chạy lại **cùng ngày** (chỉ cần gọi lại lệnh): partition `20261013` bị thay, vẫn 100 dòng, không gấp đôi.

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/run -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d '{"date":"20261013"}'
```

**Ngày kế**: đặt file vào `nims/dt=20261014/batch=1`:

```bash
hdfs dfs -mkdir -p /prod8/landing/vtnet/nims/dt=20261014/batch=1
```

```bash
hdfs dfs -put -f ./nims-enc/infra_cell_4g-130012.csv /prod8/landing/vtnet/nims/dt=20261014/batch=1/
```

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/run -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d '{"date":"20261014"}'
```

Kết quả: bảng có thêm partition `20261014` (50 dòng), partition `20261013` giữ nguyên.

Ngày **không có** thư mục: job exit 1 (`INPUT_PATH does not exist`), dag run `failed`, `lastRun` là `FAILED`, phiếu vẫn `SCHEDULED`.

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/run -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d '{"date":"20260101"}'
```

Xong N1 thì dừng phiếu:

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/stop -H "X-Mock-Admin-Token: $AT"
```

## 6. Kịch bản E1: `ebs_3g_4g`, ghi nguyên trạng (passthrough)

DAG `silver_ingest_vtnet_ebs_3g_4g` chạy `PassthroughSilverApp`: không gọi DAK, không cần Secret `spark-dak-client` hay `keycloak-ca`. Dataset khác `nims` nên chạy song song với phiếu `nims` được.
Phiếu **không có** `encryptColumns`/`keyField` (có thì `400`).

Tạo file phiếu:

```bash
cat > /tmp/ebs-e1.json <<'JSON'
{
  "source": "VTNET",
  "path": "ebs_3g_4g",
  "cron": "30 2 * * *",
  "table": "vtnet_ebs_3g_4g",
  "purpose": "Test E1: daily load of the ebs_3g_4g dataset at 02:30, write as it is"
}
JSON
```

Gửi phiếu, ghi lại `code`:

```bash
curl -s -X POST localhost:8085/mock-portal/silver-requests -H "X-Mock-Portal-Token: $PT" -H 'Content-Type: application/json' --data @/tmp/ebs-e1.json
```

Đặt mã phiếu:

```bash
CODE=<code>
```

Duyệt một lần (`SCHEDULED`):

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/approve -H "X-Mock-Admin-Token: $AT"
```

Chạy ngày `20261012`:

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/run -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d '{"date":"20261012"}'
```

Xem SparkApplication và log driver như ở 5.3 (thay `nims` bằng `ebs-3g-4g`):

```bash
kubectl get sparkapplication -n $NS | grep silver-vtnet-ebs-3g-4g
```

```bash
kubectl logs -n $NS <tên>-driver | grep -E "SILVER|READ|WRITE|JOB"
```

Log: `3. READ SOURCE` (`files = 2`: hai lô `batch=1520`, `batch=1540`), `4. WRITE TARGET`, `JOB SUCCEEDED — 2 file(s) written`; **không có** mục `PATH KEY`/`COLUMN KEYS`.

Kiểm bảng `demo_db.vtnet_ebs_3g_4g`, `dt=20261012`, 100 dòng, cột đúng như file (`imsi, lac, sac, eci, event_time, event_ids, event_result, date_hour`) cộng `hour` (từ tên thư mục `batch=HHmm`) và `dt`; `partitionColumns = [dt, hour]`:

```sql
SELECT dt, hour, count(*) FROM demo_db.vtnet_ebs_3g_4g GROUP BY dt, hour ORDER BY hour;
```

Dừng phiếu:

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/stop -H "X-Mock-Admin-Token: $AT"
```

### 6.1 Kịch bản V1: dataset VTT `f001-mpre-call`, ghi nguyên trạng

Phiếu VTT cũng là phiếu định kỳ cho cả dataset (`path` là tên bảng dưới `vtt-core`, không phải một giờ). Các cột đã được VTT mã hoá sẵn nên job chỉ đọc và ghi.

Tạo file phiếu (bảng VTT phải bắt đầu bằng `vtt_`, `vtnet_` bị từ chối):

```bash
cat > /tmp/vtt-v1.json <<'JSON'
{
  "source": "VTT",
  "path": "f001-mpre-call",
  "cron": "30 1 * * *",
  "table": "vtt_f001_mpre_call",
  "purpose": "Test V1: daily load of the VTT table f001-mpre-call at 01:30, write as it is"
}
JSON
```

Gửi phiếu, ghi lại `code`:

```bash
curl -s -X POST localhost:8085/mock-portal/silver-requests -H "X-Mock-Portal-Token: $PT" -H 'Content-Type: application/json' --data @/tmp/vtt-v1.json
```

Đặt mã phiếu:

```bash
CODE=<code>
```

Duyệt một lần (`SCHEDULED`, Variable `silver_ingest_vtt_f001_mpre_call`):

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/approve -H "X-Mock-Admin-Token: $AT"
```

Chạy ngày `20261012`:

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/run -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d '{"date":"20261012"}'
```

Log: `3. READ SOURCE` (`files = 2`: hai giờ của ngày), `4. WRITE TARGET`, `JOB SUCCEEDED — 2 file(s) written`; không có `PATH KEY`/`COLUMN KEYS`, không cần Secret `spark-dak-client`.
Bảng `demo_db.vtt_f001_mpre_call`, `dt=20261012`, 100 dòng, giá trị các cột **giống file** (không bị mã hoá thêm, không bị đổi), cộng cột `hour` (từ thư mục `hour=HH`) và `dt`; `partitionColumns = [dt, hour]`:

```sql
SELECT dt, hour, count(*) FROM demo_db.vtt_f001_mpre_call GROUP BY dt, hour ORDER BY hour;
```

Một ngày VTT thật có khoảng 24 giờ × 98 file × 70 MB: cần executor đủ lớn. Dừng phiếu:

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/stop -H "X-Mock-Admin-Token: $AT"
```

## 7. Kiểm lịch thật

Phiếu `SCHEDULED` tự chạy mỗi ngày. Để thấy một lần chạy theo lịch: gửi phiếu với `cron` chạy vài phút sau giờ hiện tại (vd bây giờ 14:20 thì `"cron": "25 14 * * *"`), đặt file vào thư mục của **hôm qua** (`dt=<hôm qua>`), duyệt, rồi đợi. Kiểm dag run mới nhất
(phải có dag run `scheduled__…`):

```bash
curl -s -u dak-trigger:"$AP" "$AF/api/v1/dags/silver_ingest_vtnet_nims/dagRuns?order_by=-start_date&limit=1"
```

Xem `lastRun` của phiếu:

```bash
curl -s localhost:8085/mock-portal/silver-requests/$CODE -H "X-Mock-Portal-Token: $PT"
```

Kỳ vọng: bảng có các dòng `dt = <hôm qua>`; không có dag run nào cho các ngày trước lúc duyệt (`catchup=False`).

## 8. Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
| --- | --- | --- |
| `400 invalid_request`, `path` | `path` không đúng là tên dataset của nguồn (`nims`, `ebs_3g_4g` của VTNET; `f001-mpre-call` của VTT): có ngày/lô/giờ (`nims/dt=…`), sai chữ hoa/thường, hoặc dataset của nguồn khác | Chỉ gửi tên dataset; chạy bù một ngày bằng `/run` (mục 4) |
| `400 invalid_request`, `batchId` / `cron` | Phiếu có `batchId` (không được có), hoặc thiếu/sai `cron` (chỉ `<phút> <giờ> * * *` hoặc `<phút> * * * *`) | Bỏ `batchId`, đặt `cron` đúng dạng |
| `400 invalid_request`, `encryptColumns`/`keyField` | Dataset passthrough (`ebs_3g_4g`, `f001-mpre-call`) có hai trường này | Bỏ hai trường |
| `400 invalid_request`, `table` | Bảng không bắt đầu bằng tiền tố của nguồn (`vtt_`, `vtnet_`) hoặc có chữ hoa/`-` | Dùng `vtnet_nims_…`, `vtt_f001_mpre_call` |
| Phiếu `409 duplicate_request` | Dataset đang có phiếu chưa kết thúc (kể cả `SCHEDULED`, của team nào cũng vậy) | Dừng hoặc huỷ phiếu cũ (mục 9) |
| `SUBMIT_FAILED`, `airflow denied the request` | User Airflow thiếu quyền Variables hoặc sửa DAG | Cấp quyền (mục 1 dòng 3), `retry-submit` |
| `SUBMIT_FAILED`, `airflow dag not found` | DAG của dataset chưa vào Airflow (chưa chép file DAG, hoặc lỗi import) | Chép đủ 4 file DAG, xem import error |
| Phiếu `SCHEDULED` mà DAG không chạy theo lịch | Chưa tới 30 giây để Airflow parse lại; DAG bị tạm dừng (`is_paused`); Variable sai (xem mô tả DAG: "The settings in the Airflow Variable are not used…"); chưa tới giờ | Đợi, xem DAG ở Airflow, sửa phiếu (dừng rồi gửi lại) |
| Dag run `failed`, "no approved request is scheduled for this dataset" | Chạy tay khi dataset chưa có phiếu đã duyệt (không có Variable) | Duyệt phiếu trước |
| Job exit 1, `INPUT_PATH does not exist` / `No file under INPUT_PATH` | Thư mục `dt=<ngày>` của ngày đó chưa có hoặc rỗng (dữ liệu chưa về trước giờ chạy) | Đặt giờ chạy muộn hơn; nạp bù bằng `/run` với `date` |
| Job exit 1, "no such column …" / cột `_c0` | Không có header, hoặc sai tên cột | Mục 0: đặt `INPUT_SCHEMA` |
| Job exit 1, `FileDecryptionException … wrong key or corrupted file` | Key của dataset khác key đã mã hoá file | Tạo đúng key (mục 3.4) |
| Bảng ghi **byte rác** mà dag run `success` (`nims`) | File mã hoá nhưng DAK **chưa có** key của dataset → `204` → job đọc ciphertext, không có cột nào để báo lỗi | Tạo key dataset **trước** khi file được nạp; luôn kiểm bảng (mục 5.5); xoá partition lỗi (`DELETE FROM … WHERE dt = …`) rồi chạy lại ngày đó |
| Job exit 1, `403 key_disabled` | Key của dataset đang bị khoá | Admin mở khoá: `curl -s -X POST localhost:8085/mock-admin/path-keys/enable -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d '{"path":"nims"}'` |
| Driver không lấy được token DAK | Sai `DAK_TOKEN_URL`/`DAK_CLIENT_ID`, thiếu Secret `spark-dak-client` | Mục 1, dòng 6–8 |
| Driver lỗi `PKIX path building failed` / `UnknownHostException sso-lakehouse…` | Thiếu hoặc sai ConfigMap `keycloak-ca`, hoặc sai IP host alias | Mục 1, dòng 8b |
| Pod bị từ chối: `initContainers[0].volumeMounts[0].name: Not found: keycloak-ca` | Mutating webhook của Spark Operator tắt, hoặc ConfigMap chưa tạo | Bật webhook, tạo `keycloak-ca` |
| Job báo `ClassNotFoundException … DecryptEncryptSilverApp` hoặc `PassthroughSilverApp` | Image cũ, chưa có hai entry point mới | Build và push lại image, cập nhật `IMAGE` |

## 9. Xoá, huỷ và dừng phiếu

Mock DAK **không có API xoá phiếu**: phiếu nằm trong bộ nhớ của pod `dak-mock` (mất khi pod khởi động lại). Phiếu đã kết thúc (`REJECTED`, `CANCELLED`, `STOPPED`) **không chặn** phiếu mới của dataset. Chỉ phiếu chưa kết thúc mới chặn (`409 duplicate_request`).

Xem danh sách phiếu để tìm mã:

```bash
curl -s localhost:8085/mock-admin/silver-requests -H "X-Mock-Admin-Token: $AT"
```

Theo trạng thái của phiếu:

- **`PENDING_APPROVAL`**: người gửi huỷ (thành `CANCELLED`):

  ```bash
  curl -s -X POST localhost:8085/mock-portal/silver-requests/$CODE/cancel -H "X-Mock-Portal-Token: $PT"
  ```

  hoặc admin từ chối (thành `REJECTED`):

  ```bash
  curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/reject -H "X-Mock-Admin-Token: $AT" -H 'Content-Type: application/json' -d '{"reasonText":"test lai"}'
  ```

- **`SUBMIT_FAILED`**: sửa nguyên nhân (DAG chưa có, Airflow lỗi, thiếu quyền…) rồi gửi lại. Không có API huỷ:

  ```bash
  curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/retry-submit -H "X-Mock-Admin-Token: $AT"
  ```

- **`SCHEDULED`**: dừng phiếu (`STOPPED`), DAK tắt DAG và xoá Variable. Một dag run đang chạy vẫn chạy nốt; muốn dừng ngay thì xoá SparkApplication (`kubectl -n $NS delete sparkapplication <tên>`) hoặc đánh failed dag run trong Airflow:

  ```bash
  curl -s -X POST localhost:8085/mock-admin/silver-requests/$CODE/stop -H "X-Mock-Admin-Token: $AT"
  ```

- **Muốn xoá sạch mọi phiếu**: khởi động lại pod dak-mock. Việc này **xoá hết** phiếu, YC và token phiên; key đã lưu trong Vault **và Variable/lịch ở Airflow không mất**:

  ```bash
  kubectl -n $NS rollout restart deploy/dak-mock
  ```

  Nếu còn phiếu `SCHEDULED`, xoá Variable và tắt DAG bằng tay:

  ```bash
  curl -s -u dak-trigger:"$AP" -X DELETE $AF/api/v1/variables/silver_ingest_vtnet_nims
  ```

  ```bash
  curl -s -u dak-trigger:"$AP" -H 'Content-Type: application/json' -X PATCH $AF/api/v1/dags/silver_ingest_vtnet_nims -d '{"is_paused": true}'
  ```

## 10. Dọn dẹp sau khi test

Dừng mọi phiếu còn `SCHEDULED` (một lệnh cho mỗi phiếu, thay `<mã phiếu>`):

```bash
curl -s -X POST localhost:8085/mock-admin/silver-requests/<mã phiếu>/stop -H "X-Mock-Admin-Token: $AT"
```

Xoá file thử trên HDFS, từng dataset:

```bash
hdfs dfs -rm -r /prod8/landing/vtnet/nims/dt=20261013 /prod8/landing/vtnet/nims/dt=20261014
```

```bash
hdfs dfs -rm -r /prod8/landing/vtnet/ebs_3g_4g/dt=20261012
```

```bash
hdfs dfs -rm -r /landing/vtt/lakehouse/vtt-core/f001-mpre-call/date=20261012
```

Xoá file thử trên máy (có cả file `.plain`):

```bash
rm -rf ./nims-enc ./ebs-plain ./vtt-plain /tmp/nims-n1.json /tmp/ebs-e1.json /tmp/vtt-v1.json
```

Bỏ các biến chứa bí mật:

```bash
unset PT AT AP PK
```

- Xoá bảng thử nếu cần: `DROP TABLE demo_db.vtnet_nims_infra_cell_4g` (và `vtnet_ebs_3g_4g`, `vtt_f001_mpre_call`), xoá luôn partition. Xoá một ngày: `DELETE FROM demo_db.<bảng> WHERE dt = '<ngày>'`.
- Key `by-path` của dataset `nims` là key thử: thay bằng key thật của VTNET khi chạy thật.
- SparkApplication tự xoá sau 7 ngày (`timeToLiveSeconds`).
