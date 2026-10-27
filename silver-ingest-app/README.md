# silver-ingest-app

Spark job chuẩn hoá một lô file nguồn vào bảng silver. Team gửi phiếu chuẩn hoá ở DAK, admin duyệt, DAK trigger DAG Airflow của nguồn
(dak-mock: [README mục 2.4](../dak-mock/README.md)); mỗi nguồn một entry point, một DAG.
Thiết kế và quyết định: [docs/silver-ingest/PLAN.md](../docs/silver-ingest/PLAN.md). Hợp đồng API lấy key của DAK (`by-path`, `by-column`):
[docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md](../docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md).

| Nguồn | Entry point | DAG | Job làm gì |
| --- | --- | --- | --- |
| VTT | `vai.lakehouse.silveringest.app.VttSilverApp` | `silver_ingest_vtt` | File đã được VTT mã hoá từng cột: đọc → ghi bảng |
| VTNET | `vai.lakehouse.silveringest.app.VtnetSilverApp` | `silver_ingest_vtnet` | DAK `by-path`: có key thì giải mã cả file, không có thì đọc nguyên trạng → mã hoá các cột phiếu chọn bằng cdr-crypto (DAK `by-column`) → ghi bảng |

Kịch bản test từng bước (key, path Vault, phiếu): [docs/silver-ingest/TEST_SCENARIO_VTNET_EBS.md](../docs/silver-ingest/TEST_SCENARIO_VTNET_EBS.md).

## 1. Luồng VTNET

1. Hỏi DAK key theo đường dẫn (`GET /api/v1/keys/by-path`, một key cho cả file hoặc cả thư mục). Đường dẫn gửi đi **tương đối theo gốc**
   `KEY_PATH_ROOT` của nguồn (theo bảng, theo lô, vd `ebs_3g_4g/dt=20261012/batch=1520`), ở dạng chuẩn: không `/` đầu/cuối, không `//`, giữ hoa thường —
   như SA đã trả lời DAK ([TRA-LOI-DAK-BY-PATH.md](../docs/key-access/TRA-LOI-DAK-BY-PATH.md)). DAK so khớp nguyên chuỗi; admin DAK tạo key
   phải nhập đúng dạng này.
   - **Có key**: file bị mã hoá cả file, giải mã ở bước 3.
   - **Không có key** (DAK trả rỗng `204`, không mở yêu cầu — đã thống nhất với DAK): file không mã hoá, đọc **nguyên trạng** ở bước 3.
   - DAK trả `202` (mở yêu cầu key, cách cũ đã bỏ) hoặc `403 key_disabled` (key của đường dẫn đang bị khoá): job dừng với lỗi (exit 1), không
     đoán là file không mã hoá và không chờ.
   Cả hai trường hợp job chạy tiếp và thành công.
2. Xin key các cột `CDR_ENCRYPT_COLUMNS` (`by-column`, dataset = `OUTPUT_DB.OUTPUT_TABLE`, `onMissing=request`) **trước** khi giải mã. Lần
   đầu cột chưa có key: DAK mở yêu cầu và trả `PENDING`; job **chờ** và gọi lại theo exponential backoff (`DAK_PENDING_WAIT_*`) cho tới khi
   admin DAK duyệt, rồi chạy tiếp — luồng đã thống nhất với DAK (hợp đồng mục 3.2). Hết thời gian chờ, hoặc yêu cầu bị từ chối/khoá: **exit 5**
   (chưa đụng tới plaintext, chưa ghi gì).
3. Đọc file. Không có key: Spark đọc thẳng (mọi định dạng). Có key: giải mã từng file trên executor:
   - định dạng text (`csv`, `tsv`, `txt`, `text`, `json`): trong bộ nhớ, plaintext không chạm đĩa;
   - định dạng nhị phân (`parquet`, `orc`, `avro`): ra thư mục tạm riêng của lần chạy dưới `DECRYPT_STAGING_PATH`, **luôn xoá** sau khi chạy
     (kể cả khi lỗi; không xoá được thì log `FAIL` kèm đường dẫn để xoá tay).
4. Mã hoá các cột bằng `CdrCrypto.encryptColumns` (cùng thuật toán với SQL `cdr_encrypt`), `CDR_KEY_FIELD` giữ plaintext. Ghi bảng.

Dữ liệu đã ghi giải mã được bằng SQL: `cdr_decrypt('<OUTPUT_DB>.<OUTPUT_TABLE>', '<cột>', <cột>, <CDR_KEY_FIELD>)`.

**Thuật toán giải mã cả file** (chỉ dùng khi `by-path` có key) chọn bằng `FILE_CIPHER`: hiện có `aes-256-gcm` (file = IV 12 byte ‖ ciphertext ‖ tag 16 byte, key =
`keyPrefix` của `by-path`, base64 chuẩn của đúng 32 byte) và đây là **giả định** cho tới khi VTNET xác nhận định dạng thật. Thêm thuật
toán: cài `FileCipher` (`crypto/FileCipher.scala`), thêm tên vào `FileCipher.forName` và `IngestConfig.FileCiphers`.

## 2. Cấu hình (env của driver)

| Env | Nguồn | Mặc định | Ý nghĩa |
| --- | --- | --- | --- |
| `INPUT_PATH` | cả hai | bắt buộc | File hoặc thư mục cần nạp (đường dẫn tuyệt đối hoặc URI; DAG ghép gốc landing + `conf.path`). VTNET liệt kê mọi file trong cây, bỏ file ẩn `_*`, `.*` |
| `INPUT_FORMAT` | cả hai | bắt buộc | `csv`, `tsv`, `txt` (= csv), `text`, `json`, `parquet`, `orc`, `avro` |
| `INPUT_SCHEMA` | cả hai | suy từ dữ liệu | Schema DDL, vd `msisdn STRING, amount INT`. Nên đặt cho VTNET để không phải đọc (giải mã) thêm một lượt để suy schema |
| `INPUT_OPTION_<tên>` | cả hai | — | Option đọc của Spark, vd `INPUT_OPTION_header=true`, `INPUT_OPTION_recursiveFileLookup=true` (VTT, thư mục lồng nhau) |
| `OUTPUT_DB`, `OUTPUT_TABLE` | cả hai | bắt buộc | Bảng đích `[A-Za-z0-9_]{1,128}`. Database được tạo nếu chưa có |
| `OUTPUT_FORMAT` | cả hai | `delta` | `delta` hoặc `hive` (Parquet) |
| `BATCH_VALUE` | cả hai | không | Mã lô (DAG: `conf.batchId`, `yyyyMMddHH` hoặc `yyyyMMddHHmm`), 1–64 ký tự `[A-Za-z0-9_.:-]`. Job thêm cột `BATCH_COLUMN` = giá trị này vào mọi dòng, làm partition **đầu tiên** của bảng đích |
| `BATCH_COLUMN` | cả hai | `batch_date` (DAG đặt `batch_id`) | Tên cột lô. Không được trùng cột có sẵn trong dữ liệu, cột cần mã hoá hay `CDR_KEY_FIELD` |
| `OUTPUT_MODE` | cả hai | `overwrite-partitions` nếu có `BATCH_VALUE`, ngược lại `overwrite` | `overwrite-partitions`: chỉ thay partition của lô (chạy lại cùng lô không ghi trùng, lô khác giữ nguyên; cần `BATCH_VALUE`). `overwrite`: thay **cả bảng**. `append`: ghi nối (chạy lại cùng lô là ghi trùng) |
| `OUTPUT_PARTITION_BY` | cả hai | không | Cột partition thêm sau cột lô, phân cách bằng dấu phẩy. Không được là cột cần mã hoá |
| `WAREHOUSE_PATH`, `ENABLE_HIVE_SUPPORT`, `APP_NAME` | cả hai | | Như pseudonymize-table-app |
| `FILE_CIPHER` | VTNET | `aes-256-gcm` | Thuật toán giải mã cả file, dùng khi `by-path` có key |
| `KEY_PATH_ROOT` | VTNET | — | Gốc của nguồn; đường dẫn gửi `by-path` là phần của `INPUT_PATH` sau gốc này (`INPUT_PATH` phải nằm hẳn dưới gốc). Bắt buộc, trừ khi đặt `KEY_PATH` |
| `KEY_PATH` | VTNET | phần của `INPUT_PATH` sau `KEY_PATH_ROOT` | Đặt thẳng đường dẫn tương đối gửi `by-path` (không có `hdfs://`; luôn đổi về dạng chuẩn) |
| `CDR_ENCRYPT_COLUMNS` | VTNET | bắt buộc | Cột mã hoá bằng cdr-crypto (DAG: từ `conf.encryptColumns` của phiếu) |
| `CDR_KEY_FIELD` | VTNET | bắt buộc | Cột giữ plaintext, ghép sau key cột thành key của từng dòng (DAG: `conf.keyField`) |
| `CDR_ON_MISSING` | VTNET | `request` | `onMissing` gửi `by-column`. `request`: cột chưa có key thì DAK mở yêu cầu và job chờ duyệt. `none`: không mở yêu cầu (cột thiếu key → exit 5 ngay) |
| `DAK_PENDING_WAIT_TIMEOUT_SECONDS` | VTNET | `0` (không chờ); DAG đặt `7200` | Chờ admin DAK duyệt yêu cầu key cột tối đa bao lâu. Trong lúc chờ, driver và executor vẫn giữ tài nguyên |
| `DAK_PENDING_WAIT_INITIAL_SECONDS`, `DAK_PENDING_WAIT_MULTIPLIER`, `DAK_PENDING_WAIT_MAX_INTERVAL_SECONDS` | VTNET | `30`, `2`, `300` (DAG: `30`, `2`, `600`) | Exponential backoff: lần gọi lại đầu sau 30 s, mỗi lần sau nhân 2, tối đa 600 s một lần |
| `DECRYPT_STAGING_PATH` | VTNET | bắt buộc với `parquet`/`orc`/`avro` | Thư mục tạm cho file giải mã bằng key `by-path`; không được nằm trong `INPUT_PATH`. Không dùng khi DAK không có key |
| `CRYPTO_PREFIX_SOURCE=dak`, `DAK_ADDR`, `DAK_TOKEN_URL`, `DAK_CLIENT_ID`, `DAK_CLIENT_SECRET`, `DAK_TAG=internal`, `DAK_CALLER` | VTNET | | Client **nội bộ** của DAK, dùng cho cả `by-path` và `by-column` (key-prefix-lib) |

Env của VTNET đặt cho VTT là lỗi cấu hình. Giới hạn của VTNET (vì file có thể phải giải mã): mỗi file phải nhỏ hơn 2 GB (giải mã nguyên khối); text là UTF-8 và đọc
theo dòng, nên `INPUT_OPTION_multiLine=true` bị từ chối và `json` phải là JSON Lines.

**Ghi theo lô** (`overwrite-partitions`): lần đầu tạo bảng partition theo `BATCH_COLUMN` (rồi `OUTPUT_PARTITION_BY`). Các lần sau, Delta dùng
`replaceWhere "<BATCH_COLUMN> = '<BATCH_VALUE>'"` (thay đúng mọi dòng của lô), `hive` dùng dynamic partition overwrite (thay các partition có
trong dữ liệu lần này). Bảng đã có mà partition khác cấu hình thì bị từ chối trước khi ghi (exit 1). Một mã lô (`batchId`) là **một** lô: chạy
lại với cùng mã lô thay dữ liệu đã ghi của lô đó; hai lô khác nhau của cùng một bảng phải có mã lô khác nhau.

## 3. Exit code

| Code | Ý nghĩa | Task Airflow |
| --- | --- | --- |
| 0 | Xong (VTNET: có key `by-path` thì đã giải mã, không có thì đọc nguyên trạng) | success |
| 1 | Lỗi khi chạy (đường dẫn không có, file không giải mã được, cột không có trong file...) | failed |
| 2 | Sai cấu hình (chưa dựng Spark) | failed |
| 5 | VTNET: không có key dùng được cho một số cột — hết thời gian chờ mà yêu cầu vẫn `PENDING`, hoặc `REJECTED`/`DISABLED`, hoặc `MISSING` với `onMissing=none` — chưa ghi gì | failed |

## 4. DAG

[airflow/dags/](../airflow/dags/) (gốc repo): `silver_ingest_vtt.py`, `silver_ingest_vtnet.py` và phần dùng chung `silver_ingest_common.py`
(chép **cả ba** vào thư mục dags của Airflow). Cấu hình cố định theo nguồn là hằng `SOURCE`/`INPUT_ROOT` trong file DAG (gốc landing, định
dạng, schema, bảng đích — hiện là **giả định**, sửa theo dữ liệu thật). `conf` do DAK gửi khi phiếu được duyệt:

| Khoá `conf` | VTT | VTNET | Ý nghĩa |
| --- | --- | --- | --- |
| `path` | bắt buộc | bắt buộc | Tương đối theo gốc landing của nguồn, dạng chuẩn (VTNET: `ebs_3g_4g/dt=20261012/batch=1520`; VTT: `f001-mpre-call/date=20261024/hour=12`). DAG đặt `INPUT_PATH` = gốc + `path`; VTNET gửi đúng `path` này cho `by-path` |
| `batchId` | bắt buộc | bắt buộc | `yyyyMMddHH` hoặc `yyyyMMddHHmm` (ngày giờ có thật) → partition `batch_id`. VTNET có batch 20 phút (`batch=1520`) nên dùng `202610121520`, không thì ba batch cùng giờ ghi đè nhau |
| `table` | bắt buộc | bắt buộc | Bảng đích trong `silver`, người gửi chọn; bắt đầu bằng `vtt_` / `vtnet_` |
| `outputMode` | tuỳ chọn | tuỳ chọn | `overwrite-partitions` (mặc định) hoặc `append`. Không nhận `overwrite` (thay cả bảng) |
| `encryptColumns` | không nhận | bắt buộc | Danh sách tên cột cần mã hoá (người gửi phiếu chọn) → `CDR_ENCRYPT_COLUMNS` |
| `keyField` | không nhận | bắt buộc | Cột giữ plaintext → `CDR_KEY_FIELD` |

Khoá khác bị từ chối. Task đầu `validate_conf` kiểm `conf` trước khi tạo SparkApplication. Tên SparkApplication là
`silver-<nguồn>-<12 ký tự băm của dag_run_id>` (duy nhất theo dag run). Ví dụ trigger tay (bình thường dak-mock trigger khi duyệt phiếu):

```bash
curl -u dak-trigger:"$AIRFLOW_PASSWORD" -H 'Content-Type: application/json' \
  -X POST http://<airflow>/api/v1/dags/silver_ingest_vtnet/dagRuns \
  -d '{"dag_run_id": "slv-2026-000001", "conf": {"path": "ebs_3g_4g/dt=20261012/batch=1520", "batchId": "202610121520", "table": "vtnet_ebs_3g_4g",
       "encryptColumns": ["msisdn", "imei"], "keyField": "start_time"}}'
```

Job VTNET cần, cùng namespace với SparkApplication (như `k8s/spark-application-cdr.yaml`):
- Secret `spark-dak-client` (key `clientSecret`): secret của client nội bộ DAK.
- ConfigMap `keycloak-ca` (key `ca.pem`, có thể chứa nhiều cert): CA nội bộ của Keycloak.
  `kubectl create configmap keycloak-ca --from-file=ca.pem=<file CA> -n <namespace>`

Driver VTNET gọi Keycloak qua `https://sso-lakehouse.cyberspace.vn` nên DAG thêm vào **driver** (executor không gọi Keycloak/DAK; DAG VTT không có
các phần này): `hostAliases` (`KEYCLOAK_IP` → `KEYCLOAK_HOST`), initContainer `build-truststore` (truststore = `cacerts` của JDK + CA nội bộ,
chạy bằng chính image của app), mount `keycloak-ca` và `truststore`, và `-Djavax.net.ssl.trustStore=…` trong `spark.driver.extraJavaOptions`.
Cấu hình giống `k8s/spark-application-cdr.yaml` và initContainer của `dak-mock/k8s/dak-mock.yaml` (cùng `securityContext`, có `runAsUser: 185`, không đặt `readOnlyRootFilesystem`); `hostAliases`/`initContainers`/`volumeMounts` cần mutating webhook của Spark Operator bật.
Sửa IP/host/tên ConfigMap ở hằng `KEYCLOAK_*` trong `airflow/dags/silver_ingest_common.py`.

## 5. Build, test, image

Module nằm trong profile Maven `cdr-crypto` (cần jar đối tác đã cài vào `~/.m2`, xem
[BUILD_AND_TEST.md](../docs/project/BUILD_AND_TEST.md)). Chạy ở gốc repo:

```bash
mvn -B -Pcdr-crypto -pl silver-ingest-app -am test                     # test (Spark cục bộ, nguồn key giả)
python3 -B -m unittest discover -s airflow/tests  # test DAG, cần jinja2
mvn -B -Pcdr-crypto -pl silver-ingest-app -am clean package -DskipTests
docker build -f silver-ingest-app/Dockerfile -t <registry>/silver-ingest-app:<tag> .
```

Image chứa fat jar (app + Delta + spark-avro + patch block token) và ba jar rời nạp qua `spark.jars`: `cdr-crypto-udf`, `key-prefix-lib`,
`DataLakeSecurity_jv8.jar`. Đổi tag thì sửa `IMAGE` trong ``.

Chạy thử đầu-cuối với dak-mock ([dak-mock/README.md](../dak-mock/README.md) mục 2.3, 2.4):

1. File thử có mã hoá: tạo key cho đường dẫn bằng `POST /mock-admin/path-keys` với `{"path": "ebs_3g_4g/dt=20261012/batch=1520", "keyValue": "<base64 32
   byte>"}` và mã hoá file bằng đúng key đó theo định dạng `aes-256-gcm` ở mục 1 (`AesGcmFileCipher.encrypt` trong test là ví dụ). File thử
   không mã hoá: bỏ qua bước này.
2. Gửi phiếu `POST /mock-portal/silver-requests`, admin `POST /mock-admin/silver-requests/<mã>/approve` → mock trigger DAG.
3. Lần đầu các cột chưa có key: DAK mở YC cột, job chờ (phiếu vẫn `RUNNING`) → admin `POST /mock-admin/yc/<mã YC>/fulfill` → job gọi lại,
   cột `READY`, ghi bảng → phiếu `SUCCEEDED`.

## 6. Cấu trúc code

```text
src/main/scala/vai/lakehouse/silveringest/
├── app/      SilverApp (cấu hình -> session -> job -> exit code), VttSilverApp, VtnetSilverApp, SparkSessions
├── config/   IngestConfig: env của hai nguồn, gom mọi lỗi một lượt
├── crypto/   FileCipher, AesGcmFileCipher
├── job/      VttJob, VtnetJob, SourceFiles (liệt kê file), SourceReader, EncryptedSource (giải mã), TableWriter
└── logging/  Log (logger "VLP")
```
