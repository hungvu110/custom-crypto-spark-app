# Kế hoạch: cho người gửi phiếu nhập env của job khử định danh

Trạng thái: **đã làm xong** (2026-10-26): E1 đến E11 và A1 đến A4. Mọi quyết định ở mục 9. Còn việc kiểm trên Airflow thật (mục 8).

## 1. Tóm tắt

- Trên phiếu có thêm một mục **env**: người gửi phiếu nhập các cặp `TÊN = giá trị` để cấu hình job khử định danh.
- Bảng kết quả (`OUTPUT_DB`, `OUTPUT_TABLE`) và cách ghi (`OUTPUT_MODE`) cũng nhập trong mục env này. **Phiếu không còn ô riêng cho bảng kết quả** (phần code cũ đã được gỡ).
- Người gửi phiếu chỉ nhập được những env nằm trong **danh sách env được phép** (mục 3). Env khác bị từ chối ngay khi gửi phiếu.
- Admin chỉ **duyệt hoặc từ chối**, không sửa env.
- Env chỉ áp dụng cho **driver** của job. Executor không nhận env nào do người gửi nhập.
- Không làm phần tài nguyên (cores, memory, số executor) và `sparkConf`.

## 2. Ví dụ cho dễ hình dung

Người gửi phiếu nhập:

```json
{
  "database": "demo_db",
  "table": "users_cdr_2",
  "columns": [{ "column": "name", "mode": "HASH" }],
  "dagId": "deid_pseudonymize_table",
  "env": {
    "OUTPUT_DB": "demo_db",
    "OUTPUT_TABLE": "users_cdr_2_pseudonymized",
    "OUTPUT_MODE": "overwrite"
  },
  "purpose": "Share data with partner X for churn analysis"
}
```

Phiếu đi qua các bước:

1. **Gửi phiếu:** mock kiểm từng env (mục 5). Sai thì báo lỗi, không tạo phiếu.
2. **Admin xem phiếu:** thấy đủ env, rồi duyệt hoặc từ chối.
3. **Lúc duyệt:** với env người gửi **không nhập**, mock tự điền giá trị mặc định. Ví dụ không nhập `OUTPUT_TABLE` thì mock đặt tên theo mẫu cấu hình như hiện nay (`users_cdr_2_deid_20261026_1`).
4. **Gửi cho DAG:** mock gửi một danh sách env đã đầy đủ. DAG kiểm lại một lần nữa rồi mới đưa vào driver của job.

## 3. Env người gửi phiếu được nhập

Danh sách ban đầu (đặt trong cấu hình `dak.deid.job-env`, thêm env mới chỉ cần sửa cấu hình và DAG, không phải viết lại code). Mỗi env trong cấu hình có `name`, `pattern` (quy tắc giá trị, phải khớp toàn bộ) và `default-value` (giá trị mặc định, có thể bỏ trống). `OUTPUT_DB` và `OUTPUT_TABLE` không có `default-value` vì mock tính khi duyệt:

| Tên | Ý nghĩa | Giá trị hợp lệ | Nếu không nhập |
| --- | --- | --- | --- |
| `OUTPUT_DB` | Database chứa bảng kết quả | Chữ thường, số, dấu gạch dưới, 1 đến 128 ký tự | Database của bảng nguồn (hoặc `dak.deid.output-database` nếu bạn đã đặt) |
| `OUTPUT_TABLE` | Tên bảng kết quả | Như trên | Mock tự đặt theo mẫu `{table}_deid_{yyyyMMdd}_{seq}` |
| `OUTPUT_MODE` | Cách ghi vào bảng kết quả | `overwrite` hoặc `append` | `overwrite` |
| `OUTPUT_FORMAT` | Định dạng bảng kết quả | `delta`, `hive` hoặc `iceberg` | `delta` |
| `ENABLE_HIVE_SUPPORT` | Bật hỗ trợ Hive | `true` hoặc `false` | `true` |

Quy ước chung: giá trị là chuỗi, tối đa 256 ký tự, không có ký tự xuống dòng. Giá trị phải **đúng ngay từ đầu** (ví dụ `OUTPUT_DB` viết hoa bị từ chối), mock không tự sửa giá trị.

## 4. Env không bao giờ được nhập

Các env này quyết định **job xử lý bảng nào và lấy key ở đâu**. Nếu người gửi phiếu sửa được, họ có thể làm job làm việc khác với những gì admin đã duyệt. Mock luôn tự đặt giá trị cho chúng:

| Env | Vì sao không cho nhập |
| --- | --- |
| `DB_NAME`, `TABLE_NAME`, `PSEUDO_COLUMNS` | Chính là bảng và cột trên phiếu mà admin đã duyệt. Giá trị lấy từ phiếu |
| `PSEUDO_MODE`, `VERIFY` | Cách khử định danh (chỉ `HASH`) và bước đối chiếu sau khi ghi. Cố định |
| `WAREHOUSE_PATH` | Nơi lưu bảng của môi trường. Cố định |
| `INPUT_*`, `MAPPING_TABLE_PATH` | Cho job đọc file hoặc thư mục bất kỳ trên HDFS |
| `CRYPTO_*`, `VAULT_*`, `DAK_*` | Quyết định job lấy key từ đâu |
| `JAVA_*`, `LD_*`, `SPARK_*`, `HADOOP_*`, `KRB5*` | Cấu hình bên trong container, nơi có keytab |
| Tên có chứa `SECRET`, `TOKEN`, `PASSWORD`, `PASSWD`, `KEY` | Tránh để bí mật nằm trong phiếu, trong `conf` của Airflow và trong log |

Nếu ai đó thêm nhầm một env thuộc nhóm này vào danh sách được phép (mục 3), mock **không khởi động** và nói rõ tên env bị cấm. Đây là lớp bảo vệ thêm, xem mục 10 (bạn chưa quyết có giữ không).

Lưu ý: so với bản plan trước, `OUTPUT_DB`, `OUTPUT_TABLE` và `OUTPUT_MODE` **đã được chuyển sang nhóm được nhập**, theo quyết định của bạn.

## 5. Cách kiểm tra

**Lúc gửi phiếu (mock):**
- `env` phải là một object, tối đa 10 mục.
- Mỗi tên phải nằm trong danh sách ở mục 3, giá trị phải khớp quy tắc của tên đó.
- Lỗi được gom cùng các lỗi khác của phiếu trong một lần trả lời. Mỗi lỗi có `field` dạng `env.TÊN` và `code` là `invalid_env`. Thông điệp lỗi **không lặp lại** giá trị người dùng đã nhập.
- Quy tắc riêng: **bảng kết quả không được trùng bảng nguồn**. Mock lấy cặp (`OUTPUT_DB`, `OUTPUT_TABLE`), nơi thiếu thì dùng giá trị mặc định, rồi so với (`database`, `table`) của phiếu. Trùng thì từ chối, vì job sẽ ghi đè hoặc ghi thêm vào đúng bảng dữ liệu gốc.

**Lúc duyệt (mock):** điền giá trị mặc định cho env còn thiếu, kiểm lại quy tắc "không trùng bảng nguồn" (phòng trường hợp cấu hình đã đổi từ lúc gửi phiếu), rồi lưu danh sách env cuối cùng vào phiếu. Nếu bị trùng thì duyệt trả `400` và phiếu vẫn chờ duyệt.

**Ở DAG:** kiểm lại toàn bộ như trên. DAG có thể bị người khác chạy tay, nên không được tin dữ liệu gửi tới.

## 6. Hai điều cần biết về rủi ro

Vì bạn chọn cho người gửi phiếu nhập `OUTPUT_TABLE` và `OUTPUT_MODE`, có hai rủi ro bạn cần chấp nhận hoặc xử lý:

1. **Ghi đè bảng của người khác.** Nếu người gửi nhập `OUTPUT_TABLE` là tên một bảng đã có (của người khác) cùng `OUTPUT_MODE=overwrite`, job sẽ xoá và thay toàn bộ nội dung bảng đó. Bạn đã chốt không giới hạn database đích. Mock cũng không biết bảng đã tồn tại hay chưa (nó không kết nối tới Hive metastore). Cách giảm rủi ro: admin đọc kỹ `OUTPUT_DB` và `OUTPUT_TABLE` trên phiếu trước khi duyệt. Nếu sau này muốn chặt hơn, có thể thêm danh sách các database được phép làm đích.
2. **`OUTPUT_MODE=append` có thể ghi trùng dữ liệu.** Nếu job lỗi giữa chừng rồi được chạy lại, phần đã ghi lần trước vẫn còn và bị ghi thêm một lần nữa. Với `overwrite`, chạy lại bao nhiêu lần cũng ra cùng một kết quả. Đề xuất: vẫn cho phép `append` nhưng ghi rõ cảnh báo này trong tài liệu cho người dùng.

## 7. Việc cần làm

### 7.1 Ở dak-mock

| # | Việc | Cách kiểm (viết test trước) | Trạng thái |
| --- | --- | --- | --- |
| E1 | Thêm cấu hình danh sách env được phép `dak.deid.job-env` (5 env ở mục 3) và kiểm khi khởi động: tên đúng dạng, không trùng, không thuộc nhóm bị cấm, quy tắc giá trị hợp lệ | Cấu hình sai thì mock không khởi động; thêm `INPUT_PATH`, `CRYPTO_X` hoặc `MY_TOKEN` vào danh sách thì bị từ chối | Đã làm xong (2026-10-26) |
| E2 | Một bộ kiểm env dùng chung cho cả lúc gửi phiếu và lúc gửi cho Airflow | Tên lạ, giá trị sai, quá 10 mục, không phải chuỗi, `null`, có ký tự xuống dòng | Đã làm xong (2026-10-26) |
| E3 | Parser của file phiếu nhận trường `env` và báo lỗi `invalid_env` | Phiếu hợp lệ, `env` rỗng, `env` thiếu; từng loại lỗi; lỗi gom chung với lỗi khác; thông điệp không lặp lại giá trị | Đã làm xong (2026-10-26) |
| E4 | Thêm `env` vào phiếu: lưu khi gửi, trả về khi xem, không thay đổi sau khi tạo | Phiếu giữ nguyên `env` qua các bước duyệt, gửi lại, huỷ | Đã làm xong (2026-10-26) |
| E5 | Lúc duyệt: điền mặc định, kiểm "không trùng bảng nguồn", lưu env cuối cùng. Phiếu vẫn trả `outputDatabase`/`outputTable` sau khi duyệt, nhưng chỉ để xem, giá trị lấy từ `OUTPUT_DB`/`OUTPUT_TABLE` cuối cùng | Không nhập gì thì ra tên mặc định như hiện nay; nhập cả hai thì giữ nguyên; trùng bảng nguồn thì `400` và phiếu vẫn chờ duyệt | Đã làm xong (2026-10-26) |
| E6 | Endpoint cho giao diện: `GET /mock-portal/deid-job-env` trả danh sách env được nhập, giá trị hợp lệ, giá trị mặc định | Thiếu token thì `401`; trả đúng danh sách; không lộ env bị cấm | Đã làm xong (2026-10-26) |
| E7 | Gửi cho Airflow: `conf.env` chứa env cuối cùng (kể cả `OUTPUT_*`). **Bỏ** hai khoá `outputDb` và `outputTable` cũ khỏi `conf`, DAG đọc từ `env`. Kiểm lại trước khi gọi mạng | `conf.env` đúng; vẫn chỉ có tên Secret, không có key; danh sách được phép đổi sau lúc gửi phiếu thì từ chối trước khi gọi mạng | Đã làm xong (2026-10-26) |
| E8 | Log và audit ghi **tên** env và số lượng, không ghi giá trị (tiếng Anh như mọi log khác) | Log có tên env, không có giá trị | Đã làm xong (2026-10-26) |
| E9 | File mẫu mới: `valid-request-with-env.json`, `invalid-env.json`, cập nhật `DeidSampleFilesTest` | Mẫu hợp lệ được nhận, mẫu lỗi bị từ chối đúng `field` và `code` | Đã làm xong (2026-10-26) |
| E10 | Một test phát hiện lệch giữa mock và DAG: đọc danh sách env trong file DAG, so với danh sách trong cấu hình mặc định của mock | Sửa một bên mà quên bên kia thì test đỏ | Đã làm xong (2026-10-26) |
| E11 | Cập nhật tài liệu: API_CONTRACT, plan triển khai, README `dak-mock`, `k8s/dak-mock.yaml` | Đọc lại cho khớp | Đã làm xong (2026-10-26) |

Ghi chú triển khai E1 đến E5: tên cấu hình của giá trị mặc định là `default-value` (không dùng `default` vì trùng từ khoá Java). Trong code, `DeidProperties.JobEnv` giữ danh sách,
`JobEnvPolicy` là bộ kiểm dùng chung, phiếu có thêm trường `env` (đứng sau `purpose` trong JSON, bỏ qua khi rỗng). Lúc gửi phiếu `env` chỉ chứa những gì người gửi nhập; lúc duyệt
mock thay bằng env cuối cùng (đã điền mặc định). Test hiện có: 609, tất cả qua.

### 7.2 Ở DAG mẫu (`dak-mock/k8s/airflow/dags/sample_dag_deid_pseudonymize_table.py`)

| # | Việc | Cách kiểm | Trạng thái |
| --- | --- | --- | --- |
| A1 | Thêm một bảng `JOB_ENV` trong DAG, mỗi env có giá trị mặc định (nếu có) và quy tắc giá trị, khớp với danh sách ở mock | Test pytest cho từng trường hợp ở A2 | Đã làm xong (2026-10-26) |
| A2 | `validate_conf` kiểm `conf.env`: phải là object, mọi tên nằm trong `JOB_ENV`, mọi giá trị khớp quy tắc, `OUTPUT_DB`/`OUTPUT_TABLE` không trùng `database`/`table` của phiếu | Hợp lệ; thiếu `env`; tên lạ; giá trị sai; `env` không phải object; đích trùng bảng nguồn | Đã làm xong (2026-10-26) |
| A3 | `build_spec`: với mỗi env trong `JOB_ENV`, luôn sinh một dòng env cho driver, giá trị lấy từ `conf.env`, thiếu thì dùng mặc định. Bỏ `OUTPUT_DB`, `OUTPUT_TABLE`, `OUTPUT_MODE`, `ENABLE_HIVE_SUPPORT`, `OUTPUT_FORMAT` cố định cũ. Giữ `render_template_as_native_obj=False` | Chạy thử phần tạo spec mà không cần Airflow: mọi giá trị env là chuỗi, đủ mọi env | Đã làm xong (2026-10-26) |
| A4 | Thêm `dak-mock/k8s/airflow/tests/test_deid_dag.py` (unittest của Python, giả lập module `airflow`, chạy được cả bằng pytest) để chạy tự động A2 và A3 | `python3 -B -m unittest discover -s dak-mock/k8s/airflow/tests` | Đã làm xong (2026-10-26) |

Chạy tay DAG (không qua DAK) vẫn dùng được: không có `conf` thì DAG dùng các giá trị mặc định có sẵn trong file, như hiện nay.

## 8. Thứ tự làm và cách triển khai

Thứ tự: E1 → E2 → E3 → E4 → E5 → E6 → E7 → E8 → E9 → A1 → A2 → A3 → A4 → E10 → E11. Mỗi bước viết test trước, chạy lại toàn bộ test sau mỗi bước.

**Triển khai thật:** mock mới và DAG mới phải lên **cùng lúc**, vì `conf` đổi cách gửi bảng kết quả (từ hai khoá riêng sang `env`). DAG cũ không hiểu `conf.env`. Trong lúc chuyển, nên tạm dừng nhận phiếu mới.

## 9. Các quyết định bạn đã chốt

| Câu hỏi | Quyết định |
| --- | --- |
| Bảng kết quả nhập ở đâu | Trong mục env (`OUTPUT_DB`, `OUTPUT_TABLE`), phiếu bỏ ô riêng; code cũ đã được gỡ |
| Env cho phép ban đầu | `OUTPUT_DB`, `OUTPUT_TABLE`, `OUTPUT_MODE`, thêm `OUTPUT_FORMAT` và `ENABLE_HIVE_SUPPORT` |
| Driver hay executor | Chỉ driver |
| Tài nguyên và `sparkConf` | Không làm |
| Admin có sửa env không | Không, chỉ duyệt hoặc từ chối |
| Giới hạn database đích | Không giới hạn |
| Chống trùng phiếu có tính env không | **Không tính**. Hai phiếu đang xử lý của cùng team, cùng bảng nguồn, cùng tập cột vẫn bị coi là trùng, dù `env` khác nhau. Một team phải đợi phiếu đầu kết thúc mới gửi được phiếu thứ hai cho cùng bảng và cột |
| Giữ lớp bảo vệ "env bị cấm" (mục 4) | **Có giữ**: mock không khởi động nếu danh sách được phép chứa env bị cấm |
