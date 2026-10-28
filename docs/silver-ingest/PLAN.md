# Plan: chuẩn hoá dữ liệu nguồn VTT và VTNET vào silver

Ngày: 2026-10-27. Trạng thái: K1, M1, M2, S1–S5, A1 đã làm; build và đẩy image làm tay.

Kịch bản test từng bước (key, path Vault, phiếu, kiểm kết quả) cho các dataset VTNET: [TEST_SCENARIO_VTNET_EBS.md](TEST_SCENARIO_VTNET_EBS.md).

## 1. Bài toán

Team gửi phiếu chuẩn hoá ở DAK, admin duyệt, DAK đưa phiếu sang Airflow. DAG tạo SparkApplication đọc file, ghi thành bảng ở database `demo_db`.
**Mỗi dataset một DAG** (hiện có VTT và các dataset VTNET); mỗi DAG chọn một trong hai **mode** của Spark app (hai entry point), cả hai nhận **file**.
**Mọi phiếu (VTT lẫn VTNET) là phiếu định kỳ**: **duyệt một lần cho cả dataset**, DAG của dataset chạy **mỗi ngày** một lần theo giờ trên phiếu (quyết định 2026-10-28).

| Mode | Dữ liệu nhận được | Job làm gì | Dataset hiện có |
| --- | --- | --- | --- |
| `passthrough` | File đã ở dạng dùng được (VTT mã hoá **từng cột** sẵn, hoặc không cần mã hoá) | Đọc file → ghi bảng. Không giải mã, không mã hoá thêm, không gọi DAK | VTT, VTNET `ebs_3g_4g` |
| `decrypt-encrypt` | File có thể bị mã hoá **toàn bộ** | Hỏi DAK key của dataset (`by-path`). Có key → giải mã file; chưa có key (DAK trả rỗng) → đọc nguyên trạng. Rồi mã hoá các cột người gửi phiếu chọn bằng **cdr-crypto** (key theo cột qua `by-column`); **không chọn cột nào thì chỉ giải mã** → ghi bảng. Mọi trường hợp đều thành công | VTNET `nims` |

Dữ liệu lưu theo ngày, bên trong ngày có nhiều lô (VTNET `dt=YYYYMMDD/batch=...`) hoặc nhiều giờ (VTT `date=YYYYMMDD/hour=HH`), nên phiếu chỉ trỏ tới **dataset** (`path` = `nims`, `ebs_3g_4g`,
`f001-mpre-call`), không trỏ tới ngày, lô hay giờ. Mỗi lần chạy nạp cả thư mục của **ngày hôm trước** vào partition `batch_id = <ngày>` (`yyyyMMdd`):

| Dataset | Thư mục một ngày | `batch_id` |
| --- | --- | --- |
| `ebs_3g_4g` | `ebs_3g_4g/dt=YYYYMMDD/` (mọi `batch=HHmm`) | `YYYYMMDD` |
| `nims` | `nims/dt=YYYYMMDD/` (mọi `batch=N`) | `YYYYMMDD` |
| VTT `f001-mpre-call` | `f001-mpre-call/date=YYYYMMDD/` (mọi `hour=HH`) | `YYYYMMDD` |

## 2. Quyết định đã chốt

| Câu hỏi | Quyết định |
| --- | --- |
| File ở mode `decrypt-encrypt` có mã hoá không | **Do DAK quyết định lúc chạy**: job luôn gọi `by-path`; có key thì giải mã, chưa có key (DAK trả rỗng `204`, không mở YC) thì đọc nguyên trạng. **DAK đã đồng ý** ([TRA-LOI-DAK-BY-PATH.md](../key-access/TRA-LOI-DAK-BY-PATH.md) mục 5); `202` bị coi là lỗi |
| Phiếu chuẩn hoá | Team gửi, **admin duyệt**. **Mọi phiếu là phiếu định kỳ** (quyết định 2026-10-28; VTT, `ebs_3g_4g` và `nims` đều chạy một ngày một lần): `path` = **tên dataset**, không có `batchId`, thêm `scheduleTime` (`HH:mm`, giờ chạy hằng ngày, **người gửi chọn**, admin duyệt cả giờ chạy); `table` (người gửi chọn), `outputMode` (tuỳ chọn), `purpose`; dataset `decrypt-encrypt` thêm `encryptColumns` (**có thể rỗng**: chỉ giải mã file), `keyField` (bắt buộc khi có cột). **Duyệt MỘT lần**: DAK ghi cấu hình vào Airflow Variable của dataset và bật DAG, phiếu ở `SCHEDULED` cho tới khi admin `stop`. Cả dataset chỉ có một phiếu chưa kết thúc (của mọi team). dak-mock: [README mục 2.4](../../dak-mock/README.md) |
| Cách ghi bảng đích | **Ghi đè theo partition của lô.** Job thêm cột partition `batch_id` và mặc định `OUTPUT_MODE=overwrite-partitions`: chỉ thay partition của lô, chạy lại cùng lô không ghi trùng, lô khác giữ nguyên. `batch_id` = **ngày** nạp (`yyyyMMdd`) do DAG tự tính, nên chạy lại một ngày chỉ thay ngày đó. DAG chỉ nhận `overwrite-partitions` hoặc `append`, không nhận `overwrite` (thay cả bảng) |
| Lịch chạy (DAK → Airflow) | Airflow REST không đổi được `schedule` của DAG đã có. **DAG factory đọc Airflow Variable** (quyết định 2026-10-28): duyệt phiếu thì DAK ghi cấu hình (`ticket`, `table`, `cron`, `encryptColumns`, `keyField`, `outputMode`) vào Variable `silver_ingest_vtnet_<dataset>` rồi bật DAG; factory dựng DAG với `schedule` = cron của phiếu. Dừng phiếu: tắt DAG và xoá Variable. Giờ chạy do người gửi phiếu chọn |
| Ngày mỗi lần chạy nạp | **Ngày hôm trước** (đầu khoảng dữ liệu của lần chạy, đổi sang giờ Việt Nam): lần chạy 03:00 ngày 28 nạp `dt=<ngày 27>`. Không nạp bù khi duyệt (`catchup=False`); nạp bù hoặc chạy thử một ngày bằng thao tác admin `run` (`POST /mock-admin/silver-requests/{code}/run`, body `{"date":"yyyyMMdd"}`): DAK trigger một dag run `conf.date` ngoài lịch, phiếu và lịch không đổi. DAK không còn "trigger khi đủ dữ liệu": giờ chạy trên phiếu phải đặt sau lúc dữ liệu của ngày trước đã đủ |
| Cột mã hoá | **Tuỳ chọn** (quyết định 2026-10-28): dataset như `nims` hiện không cần mã hoá cột nào nên phiếu để `encryptColumns` rỗng; job chỉ giải mã file, không gọi `by-column` và không cần `keyField`. Spark app, DAG và dak-mock đều hỗ trợ |
| Key `by-path` | **Một key cho cả dataset** (quyết định 2026-10-28, mọi dataset): DAG đặt `KEY_PATH` = tên dataset (vd `nims`), mọi lô dùng chung, không phải tạo key mỗi ngày/lô |
| DAG ở đâu | Thư mục `airflow/` ở gốc repo (cùng DAG khử định danh, chuyển ra từ `dak-mock/k8s/airflow`) |
| Thuật toán mã hoá cả file của VTNET (khi có) | Chưa rõ → **pluggable** (`FILE_CIPHER`). Mặc định `aes-256-gcm`: file = IV 12 byte ‖ ciphertext ‖ tag 16 byte, key = `keyPrefix` của `by-path` (base64 chuẩn của đúng 32 byte). Đổi thuật toán chỉ thêm một class |
| Định dạng file sau giải mã / file VTT | Cấu hình được: `INPUT_FORMAT` = `csv`, `tsv`, `txt`, `text`, `json`, `parquet`, `orc`, `avro`; `INPUT_SCHEMA`, `INPUT_OPTION_*` |
| Hợp đồng key của DAK | [`docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md`](../key-access/API-LAY-KEY-DUONG-DAN-COT.md) (2026-10-27), thay mọi bản trước. Đường key theo file đổi tên `by-file` → **`by-path`** (`200 {path, keyPrefix, keyVersion}`) |
| `by-path` với thư mục | **Một key cho cả thư mục**: gọi `by-path` một lần với đường dẫn đầu vào, key đó giải mã mọi file trong thư mục |
| Hình dạng `path` (câu hỏi mục 6 của DAK) | **Đã trả lời DAK** ([TRA-LOI-DAK-BY-PATH.md](../key-access/TRA-LOI-DAK-BY-PATH.md)): **tương đối theo một gốc** của nguồn, SA tự chuẩn hoá (không `/` đầu/cuối, không `//`, không scheme), giữ nguyên hoa thường. Bản trả lời đó nêu đường dẫn theo bảng/ngày/lô (vd `vtnet_cdr/20261027`); **quyết định mới nhất là key theo cả dataset** (xem dòng "Key `by-path`"), tức đường dẫn gửi DAK là tên dataset. Job vẫn hỗ trợ cả hai cách: `KEY_PATH` đặt thẳng, hoặc `KEY_PATH_ROOT` = gốc landing và đường dẫn = phần của `INPUT_PATH` sau gốc. Cần xác nhận lại với DAK nếu họ đã code theo cách cũ |
| `onMissing` của `by-column` khi chuẩn hoá | **`request`**, DAK đã xác nhận (hợp đồng mục 3.2: mã hoá → `request`, giải mã → `none`). Cột chưa có key: DAK mở yêu cầu (`PENDING`), **job chờ và gọi lại theo exponential backoff** (`DAK_PENDING_WAIT_*`, DAG: timeout 2 giờ, 30 s × 2, tối đa 600 s) tới khi admin duyệt rồi chạy tiếp; hết timeout thì exit 5 |
| `by-path` ở dak-mock | Theo hợp đồng mục 2 và thoả thuận mục 5: chưa có key → `204`, không YC. Key theo đường dẫn tạo bằng `POST /mock-admin/path-keys` (vai trò luồng mã hoá file) |
| Thuật toán mã hoá cột | cdr-crypto (`cdr-crypto-udf`, `CdrCrypto.encryptColumns`), mỗi cột một key, dataset = bảng đích `OUTPUT_DB.OUTPUT_TABLE` |

## 3. Thiết kế

### 3.1 Module mới `silver-ingest-app`

Một jar, hai entry point (mỗi mode một class; mỗi dataset một DAG chọn mode của nó):

| Class | Mode |
| --- | --- |
| `vai.lakehouse.silveringest.app.PassthroughSilverApp` | `passthrough` |
| `vai.lakehouse.silveringest.app.DecryptEncryptSilverApp` | `decrypt-encrypt` |

Module phụ thuộc `cdr-crypto-udf` nên nằm trong profile Maven `cdr-crypto` (cần jar đối tác như `cdr-crypto-udf`). `cdr-crypto-udf`,
`key-prefix-lib` và jar đối tác là jar rời trong image, nạp qua `spark.jars` (UDF thật, executor cũng cần).

### 3.2 Giải mã file (decrypt-encrypt)

- File văn bản (`csv`, `tsv`, `txt`, `text`, `json`): giải mã **trong bộ nhớ** của executor rồi đọc thẳng, plaintext không bao giờ chạm đĩa.
- File nhị phân (`parquet`, `orc`, `avro`): Spark chỉ đọc được từ file, nên giải mã ra **thư mục tạm** `DECRYPT_STAGING_PATH` (bắt buộc với
  định dạng này), đọc, ghi bảng, rồi **xoá thư mục tạm** trong `finally` (kể cả khi lỗi).
- Mỗi file được giải mã nguyên khối (GCM cần cả file để kiểm tag), nên một file phải nhỏ hơn 2 GB.
- Tag sai (file hỏng hoặc sai key) → job lỗi, nêu tên file, không ghi bảng.

### 3.3 Exit code (cả hai job)

| Code | Ý nghĩa |
| --- | --- |
| 0 | Xong (decrypt-encrypt: có key `by-path` thì giải mã, không có thì đọc nguyên trạng) |
| 1 | Lỗi khi chạy |
| 2 | Sai cấu hình (chưa dựng Spark) |
| 5 | decrypt-encrypt: DAK chưa có key cho một số cột cần mã hoá (`PENDING` kèm mã YC) — chưa ghi gì, chạy lại sau khi DAK tạo key |

### 3.4 DAG

Một DAG **định kỳ** cho mỗi dataset, `silver_ingest_<nguồn>_<dataset>` (thư mục `airflow/dags/`): `silver_ingest_vtt.py` và `silver_ingest_vtnet.py` khai `DATASETS`
(`mode`, tuỳ chọn `key_path`) và gọi DAG factory chung `silver_ingest_factory.py`. Cấu hình chung (gốc landing, định dạng file, schema, database đích `demo_db`) là
**hằng trong DAG**. Cấu hình của phiếu đã duyệt đọc từ **Airflow Variable** `silver_ingest_vtnet_<dataset>` (JSON do DAK ghi):

| Khoá của Variable | passthrough | decrypt-encrypt | Ý nghĩa |
| --- | --- | --- | --- |
| `ticket` | bắt buộc | bắt buộc | Mã phiếu đã duyệt, để tham chiếu |
| `table` | bắt buộc | bắt buộc | Bảng đích trong `demo_db`, **người gửi phiếu chọn**, bắt đầu bằng `vtt_` (VTT) hoặc `vtnet_` (VTNET) |
| `cron` | bắt buộc | bắt buộc | Cron hằng ngày `<phút> <giờ> * * *` (từ `scheduleTime` của phiếu) → `schedule` của DAG |
| `outputMode` | tuỳ chọn | tuỳ chọn | `overwrite-partitions` (mặc định) hoặc `append` |
| `encryptColumns` | không nhận | tuỳ chọn (có thể rỗng) | Cột cần mã hoá, người gửi phiếu chọn; rỗng = chỉ giải mã file |
| `keyField` | không nhận | bắt buộc khi có cột | Cột giữ plaintext làm nguyên liệu key |

Cấu hình sai (không phải JSON, cron không phải cron hằng ngày, bảng sai tiền tố...) bị bỏ qua: DAG không có schedule và `validate_conf` báo lý do. `conf` của một dag run chỉ nhận
`date` (`yyyyMMdd`) khi chạy tay.

DAG kiểm `conf` ở task đầu `validate_conf` trước khi tạo job, như DAG khử định danh.

## 4. Việc cần làm

| # | Việc | Kiểm |
| --- | --- | --- |
| K1 | `key-prefix-lib`: `PathKeySource` (+ chuẩn hoá đường dẫn), `DakPathKeySource` (`by-path`: 200 → key, 204 → không có key, khác kể cả 202 và `403 key_disabled` → lỗi), `PathKeySourceFactory` | Test với HTTP stub |
| M1 | dak-mock: `GET /api/v1/keys/by-path` (kiểm `path`, `tag=internal`, client nội bộ), key ở Vault theo SHA-256 của đường dẫn, chưa có key → `204` | Test API |
| M2 | dak-mock: admin tạo key trực tiếp `POST /mock-admin/path-keys`, `POST /mock-admin/column-keys` | Test API |
| M3 | dak-mock: phiếu chuẩn hoá silver (gửi, duyệt, trigger DAG qua Airflow REST, theo dõi dag run, chống trùng) | Test parser, service, Airflow giả, API |
| M5 | dak-mock: phiếu định kỳ (mọi nguồn): `scheduleTime`, duyệt một lần (`SCHEDULED`), ghi Variable + bật DAG qua Airflow REST, `stop`, `lastRun`, chặn trùng theo dataset | Test parser, service, client Airflow (MockWebServer), API |
| S1 | `silver-ingest-app`: cấu hình theo env cho hai nguồn, gom mọi lỗi một lượt | Test cấu hình |
| S2 | `FileCipher` + `AesGcmFileCipher`, giải mã trong bộ nhớ và ra thư mục tạm | Test mã hoá/giải mã, tag sai |
| S3 | Job `passthrough`: đọc file → ghi bảng | Test với Spark cục bộ |
| S4 | Job `decrypt-encrypt`: `by-path` → giải mã (có key) hoặc đọc nguyên trạng (không có key) → `by-column` (`onMissing=request`) + cdr-crypto → ghi bảng; không có cột mã hoá thì chỉ giải mã; cột chưa có key thì exit 5 | Test với Spark cục bộ, nguồn key giả |
| S5 | Dockerfile, README | Đọc lại |
| A1 | Một DAG định kỳ mỗi dataset của VTT và VTNET (DAG factory `silver_ingest_factory.py`, `DATASETS` đọc Variable) + test Python |
| M4 | dak-mock: định tuyến phiếu VTNET theo dataset (`SilverDataset`), `path` là dataset, `encryptColumns` rỗng; test chống lệch với `DATASETS` của DAG | `SilverDatasetDagDriftTest`, test parser/service/API | `python3 -B -m unittest discover -s airflow/tests` |

## 5. Rủi ro và giới hạn đã biết

- **Thuật toán mã hoá file của VTNET chưa được xác nhận.** Mặc định `aes-256-gcm` là giả định, chỉ dùng khi `by-path` có key. Khi chốt định
  dạng thật: thêm một `FileCipher` nếu khác AES-256-GCM, đặt `FILE_CIPHER` trong DAG.
- `by-path` không có key → `204` (DAK đã đồng ý). Nếu file thật sự mã hoá mà DAK chưa có key cho đường dẫn, job sẽ đọc ciphertext như
  plaintext: luồng mã hoá file của DAK phải tạo key **trước** khi file được đưa vào landing.
- Hai lô khác nhau của cùng một bảng phải có `batchId` khác nhau: cùng `batchId` thì lô sau **thay** dữ liệu lô trước. VTNET có batch 20 phút
  (`dt=20261012/batch=1520`) nên dùng `yyyyMMddHHmm` (`202610121520`); dùng `yyyyMMddHH` thì ba batch cùng giờ ghi đè nhau. Với dataset VTNET, mã lô là ngày nạp (`yyyyMMdd`) do DAG tự tính nên không thể gửi lệch. Job không tự biết thư mục của ngày đã ghi xong chưa: giờ chạy trên phiếu phải đặt sau lúc dữ liệu của ngày hôm trước đã đủ (ảnh HDFS cho thấy `ebs_3g_4g/dt=<ngày>` còn được ghi tới khoảng 02:20 sáng hôm sau). Một dataset có nhiều bảng trong cùng thư mục ngày (vd `nims` có `infra_cell_4g-*.csv`) cần lọc theo tên file: **chưa hỗ trợ**, hiện cả thư mục ngày vào một bảng.
- **VTT nạp cả ngày một lần.** Thư mục `date=<ngày>` có khoảng 24 giờ × 98 file × 70 MB (~165 GB) cho mỗi lần chạy: cần cấu hình executor đủ cho khối lượng này, hoặc đổi sang chu kỳ theo giờ nếu một job một ngày quá nặng. Hiện chỉ có dataset `f001-mpre-call`; thêm bảng khác của `vtt-core` vào `DATASETS` (và `SilverDataset`).
- **Gốc landing cần xác nhận.** DAG đang đặt chung `/prod8/landing/vtnet/` cho mọi dataset VTNET (giả định, tất cả dataset cùng một cụm HDFS). Màn hình `hdfs-kerberos-client-hkh` lại cho thấy `/landing/ebs_3g_4g/dt=...` và `/landing/nims/dt=.../batch=1/infra_cell_4g-<giờ>.csv`: nếu đó là gốc thật thì sửa `INPUT_ROOT` trong `silver_ingest_vtnet.py`.
- Dữ liệu thật (HDFS): VTNET `/prod8/landing/vtnet/<bảng>/dt=<ngày>/batch=<HHmm>/<file>.csv`; VTT `/landing/vtt/lakehouse/vtt-core/<bảng>/date=<ngày>/hour=<giờ>/`
  (~98 file × 70 MB mỗi giờ). Gốc landing của hai DAG đã đặt theo đây; schema và delimiter từng bảng **chưa khai** (`INPUT_SCHEMA` để trống).
- Bảng đích đã tồn tại mà không partition theo `batch_id` bị từ chối (exit 1): tạo bảng mới hoặc đổi `OUTPUT_TABLE`.
- DAK thật **chưa code** `by-path` (tài liệu mới mục 5): họ chờ câu trả lời mục 6 — đã soạn ở `TRA-LOI-DAK-BY-PATH.md`, cần gửi DAK. Job VTNET chạy được với
  dak-mock; với DAK thật cần DAK làm xong `by-path`.
- Lần đầu ghi một bảng silver mới: job chờ admin DAK duyệt yêu cầu key cột (tối đa `DAK_PENDING_WAIT_TIMEOUT_SECONDS`). Trong lúc chờ, driver
  và executor vẫn giữ tài nguyên; quá timeout thì job exit 5 và phải gửi lại phiếu.
