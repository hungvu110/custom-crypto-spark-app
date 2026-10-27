# Plan: chuẩn hoá dữ liệu nguồn VTT và VTNET vào silver

Ngày: 2026-10-27. Trạng thái: K1, M1, M2, S1–S5, A1 đã làm; build và đẩy image làm tay.

Kịch bản test từng bước cho bảng `ebs_3g_4g` (key, path Vault, phiếu, kiểm kết quả): [TEST_SCENARIO_VTNET_EBS.md](TEST_SCENARIO_VTNET_EBS.md).

## 1. Bài toán

Team gửi phiếu chuẩn hoá một lô file ở DAK, admin duyệt, DAK trigger DAG Airflow của nguồn. DAG tạo SparkApplication đọc file, ghi thành bảng ở vùng silver.
Mỗi nguồn một job riêng (case by case), hiện có hai nguồn, cả hai gửi **file**:

| Nguồn | Dữ liệu nhận được | Job làm gì |
| --- | --- | --- |
| **VTT** | File đã được VTT mã hoá **từng cột** | Đọc file → ghi bảng. Không giải mã, không mã hoá thêm |
| **VTNET** | File có thể bị mã hoá **toàn bộ** | Hỏi DAK key theo đường dẫn (`by-path`). Có key → giải mã file; chưa có key (DAK trả rỗng) → đọc nguyên trạng. Sau đó mã hoá các cột người gửi phiếu chọn bằng **cdr-crypto** (key theo cột qua `by-column`) → ghi bảng. Cả hai trường hợp đều thành công |

## 2. Quyết định đã chốt

| Câu hỏi | Quyết định |
| --- | --- |
| File VTNET có mã hoá không | **Do DAK quyết định lúc chạy**: job luôn gọi `by-path`; có key thì giải mã, chưa có key (DAK trả rỗng `204`, không mở YC) thì đọc nguyên trạng. **DAK đã đồng ý** ([TRA-LOI-DAK-BY-PATH.md](../key-access/TRA-LOI-DAK-BY-PATH.md) mục 5); `202` bị coi là lỗi |
| Phiếu chuẩn hoá | Team gửi, **admin duyệt**, DAK trigger DAG của nguồn. Phiếu: `source`, `path` (tương đối), `batchId`, `table` (người gửi chọn), `outputMode` (tuỳ chọn), `purpose`; VTNET thêm `encryptColumns`, `keyField` do **người gửi chọn**. dak-mock: [README mục 2.4](../../dak-mock/README.md) |
| Cách ghi bảng đích | **Ghi đè theo partition của lô.** DAK gửi `conf.batchId` (`yyyyMMddHH` hoặc `yyyyMMddHHmm`, bắt buộc); job thêm cột partition `batch_id` = giá trị đó và mặc định `OUTPUT_MODE=overwrite-partitions`: chỉ thay partition của lô, chạy lại cùng lô không ghi trùng, lô khác giữ nguyên. DAG chỉ nhận `overwrite-partitions` hoặc `append`, không nhận `overwrite` (thay cả bảng) |
| DAG ở đâu | Thư mục `airflow/` ở gốc repo (cùng DAG khử định danh, chuyển ra từ `dak-mock/k8s/airflow`) |
| Thuật toán mã hoá cả file của VTNET (khi có) | Chưa rõ → **pluggable** (`FILE_CIPHER`). Mặc định `aes-256-gcm`: file = IV 12 byte ‖ ciphertext ‖ tag 16 byte, key = `keyPrefix` của `by-path` (base64 chuẩn của đúng 32 byte). Đổi thuật toán chỉ thêm một class |
| Định dạng file sau giải mã / file VTT | Cấu hình được: `INPUT_FORMAT` = `csv`, `tsv`, `txt`, `text`, `json`, `parquet`, `orc`, `avro`; `INPUT_SCHEMA`, `INPUT_OPTION_*` |
| Hợp đồng key của DAK | [`docs/key-access/API-LAY-KEY-DUONG-DAN-COT.md`](../key-access/API-LAY-KEY-DUONG-DAN-COT.md) (2026-10-27), thay mọi bản trước. Đường key theo file đổi tên `by-file` → **`by-path`** (`200 {path, keyPrefix, keyVersion}`) |
| `by-path` với thư mục | **Một key cho cả thư mục**: gọi `by-path` một lần với đường dẫn đầu vào, key đó giải mã mọi file trong thư mục |
| Hình dạng `path` (câu hỏi mục 6 của DAK) | **Đã trả lời DAK** ([TRA-LOI-DAK-BY-PATH.md](../key-access/TRA-LOI-DAK-BY-PATH.md)): **tương đối theo một gốc** của nguồn (theo bảng, theo ngày, vd `vtnet_cdr/20261027`), SA tự chuẩn hoá (không `/` đầu/cuối, không `//`, không scheme), giữ nguyên hoa thường. Job: `KEY_PATH_ROOT` = gốc landing của nguồn, đường dẫn = phần của `INPUT_PATH` sau gốc |
| `onMissing` của `by-column` khi chuẩn hoá | **`request`**, DAK đã xác nhận (hợp đồng mục 3.2: mã hoá → `request`, giải mã → `none`). Cột chưa có key: DAK mở yêu cầu (`PENDING`), **job chờ và gọi lại theo exponential backoff** (`DAK_PENDING_WAIT_*`, DAG: timeout 2 giờ, 30 s × 2, tối đa 600 s) tới khi admin duyệt rồi chạy tiếp; hết timeout thì exit 5 |
| `by-path` ở dak-mock | Theo hợp đồng mục 2 và thoả thuận mục 5: chưa có key → `204`, không YC. Key theo đường dẫn tạo bằng `POST /mock-admin/path-keys` (vai trò luồng mã hoá file) |
| Thuật toán mã hoá cột | cdr-crypto (`cdr-crypto-udf`, `CdrCrypto.encryptColumns`), mỗi cột một key, dataset = bảng đích `OUTPUT_DB.OUTPUT_TABLE` |

## 3. Thiết kế

### 3.1 Module mới `silver-ingest-app`

Một jar, hai entry point (mỗi nguồn một SparkApplication, một DAG):

| Class | Nguồn |
| --- | --- |
| `vai.lakehouse.silveringest.app.VttSilverApp` | VTT |
| `vai.lakehouse.silveringest.app.VtnetSilverApp` | VTNET |

Module phụ thuộc `cdr-crypto-udf` nên nằm trong profile Maven `cdr-crypto` (cần jar đối tác như `cdr-crypto-udf`). `cdr-crypto-udf`,
`key-prefix-lib` và jar đối tác là jar rời trong image, nạp qua `spark.jars` (UDF thật, executor cũng cần).

### 3.2 Giải mã file (VTNET)

- File văn bản (`csv`, `tsv`, `txt`, `text`, `json`): giải mã **trong bộ nhớ** của executor rồi đọc thẳng, plaintext không bao giờ chạm đĩa.
- File nhị phân (`parquet`, `orc`, `avro`): Spark chỉ đọc được từ file, nên giải mã ra **thư mục tạm** `DECRYPT_STAGING_PATH` (bắt buộc với
  định dạng này), đọc, ghi bảng, rồi **xoá thư mục tạm** trong `finally` (kể cả khi lỗi).
- Mỗi file được giải mã nguyên khối (GCM cần cả file để kiểm tag), nên một file phải nhỏ hơn 2 GB.
- Tag sai (file hỏng hoặc sai key) → job lỗi, nêu tên file, không ghi bảng.

### 3.3 Exit code (cả hai job)

| Code | Ý nghĩa |
| --- | --- |
| 0 | Xong (VTNET: có key `by-path` thì giải mã, không có thì đọc nguyên trạng) |
| 1 | Lỗi khi chạy |
| 2 | Sai cấu hình (chưa dựng Spark) |
| 5 | VTNET: DAK chưa có key cho một số cột cần mã hoá (`PENDING` kèm mã YC) — chưa ghi gì, chạy lại sau khi DAK tạo key |

### 3.4 DAG

Hai DAG mẫu `silver_ingest_vtt`, `silver_ingest_vtnet` (thư mục `airflow/dags/`). Cấu hình cố định theo nguồn (gốc landing, định dạng file,
schema, bảng đích) là **hằng trong DAG**; `conf` do DAK gửi từ phiếu đã duyệt:

| Khoá `conf` | VTT | VTNET | Ý nghĩa |
| --- | --- | --- | --- |
| `path` | bắt buộc | bắt buộc | Tương đối theo gốc landing của nguồn, dạng chuẩn. `INPUT_PATH` = gốc + `path`; VTNET gửi đúng `path` cho `by-path` |
| `batchId` | bắt buộc | bắt buộc | Mã lô `yyyyMMddHH` hoặc `yyyyMMddHHmm` (ngày giờ có thật) → giá trị cột partition `batch_id` |
| `table` | bắt buộc | bắt buộc | Bảng đích trong `silver`, **người gửi phiếu chọn**, bắt đầu bằng `vtt_` / `vtnet_` |
| `outputMode` | không | không | `overwrite-partitions` (mặc định) hoặc `append` |
| `encryptColumns`, `keyField` | không nhận | bắt buộc | Cột cần mã hoá và cột keyField, người gửi phiếu chọn |

DAG kiểm `conf` ở task đầu `validate_conf` trước khi tạo job, như DAG khử định danh.

## 4. Việc cần làm

| # | Việc | Kiểm |
| --- | --- | --- |
| K1 | `key-prefix-lib`: `PathKeySource` (+ chuẩn hoá đường dẫn), `DakPathKeySource` (`by-path`: 200 → key, 204 → không có key, khác kể cả 202 và `403 key_disabled` → lỗi), `PathKeySourceFactory` | Test với HTTP stub |
| M1 | dak-mock: `GET /api/v1/keys/by-path` (kiểm `path`, `tag=internal`, client nội bộ), key ở Vault theo SHA-256 của đường dẫn, chưa có key → `204` | Test API |
| M2 | dak-mock: admin tạo key trực tiếp `POST /mock-admin/path-keys`, `POST /mock-admin/column-keys` | Test API |
| M3 | dak-mock: phiếu chuẩn hoá silver (gửi, duyệt, trigger DAG qua Airflow REST, theo dõi dag run, chống trùng) | Test parser, service, Airflow giả, API |
| S1 | `silver-ingest-app`: cấu hình theo env cho hai nguồn, gom mọi lỗi một lượt | Test cấu hình |
| S2 | `FileCipher` + `AesGcmFileCipher`, giải mã trong bộ nhớ và ra thư mục tạm | Test mã hoá/giải mã, tag sai |
| S3 | Job VTT: đọc file → ghi bảng | Test với Spark cục bộ |
| S4 | Job VTNET: `by-path` → giải mã (có key) hoặc đọc nguyên trạng (không có key) → `by-column` (`onMissing=request`) + cdr-crypto → ghi bảng; cột chưa có key thì exit 5 | Test với Spark cục bộ, nguồn key giả |
| S5 | Dockerfile, README | Đọc lại |
| A1 | Hai DAG + test Python | `python3 -B -m unittest discover -s airflow/tests` |

## 5. Rủi ro và giới hạn đã biết

- **Thuật toán mã hoá file của VTNET chưa được xác nhận.** Mặc định `aes-256-gcm` là giả định, chỉ dùng khi `by-path` có key. Khi chốt định
  dạng thật: thêm một `FileCipher` nếu khác AES-256-GCM, đặt `FILE_CIPHER` trong DAG.
- `by-path` không có key → `204` (DAK đã đồng ý). Nếu file thật sự mã hoá mà DAK chưa có key cho đường dẫn, job sẽ đọc ciphertext như
  plaintext: luồng mã hoá file của DAK phải tạo key **trước** khi file được đưa vào landing.
- Hai lô khác nhau của cùng một bảng phải có `batchId` khác nhau: cùng `batchId` thì lô sau **thay** dữ liệu lô trước. VTNET có batch 20 phút
  (`dt=20261012/batch=1520`) nên dùng `yyyyMMddHHmm` (`202610121520`); dùng `yyyyMMddHH` thì ba batch cùng giờ ghi đè nhau.
- Dữ liệu thật (HDFS): VTNET `/prod8/landing/vtnet/<bảng>/dt=<ngày>/batch=<HHmm>/<file>.csv`; VTT `/landing/vtt/lakehouse/vtt-core/<bảng>/date=<ngày>/hour=<giờ>/`
  (~98 file × 70 MB mỗi giờ). Gốc landing của hai DAG đã đặt theo đây; schema và delimiter từng bảng **chưa khai** (`INPUT_SCHEMA` để trống).
- Bảng đích đã tồn tại mà không partition theo `batch_id` bị từ chối (exit 1): tạo bảng mới hoặc đổi `OUTPUT_TABLE`.
- DAK thật **chưa code** `by-path` (tài liệu mới mục 5): họ chờ câu trả lời mục 6 — đã soạn ở `TRA-LOI-DAK-BY-PATH.md`, cần gửi DAK. Job VTNET chạy được với
  dak-mock; với DAK thật cần DAK làm xong `by-path`.
- Lần đầu ghi một bảng silver mới: job chờ admin DAK duyệt yêu cầu key cột (tối đa `DAK_PENDING_WAIT_TIMEOUT_SECONDS`). Trong lúc chờ, driver
  và executor vẫn giữ tài nguyên; quá timeout thì job exit 5 và phải gửi lại phiếu.
