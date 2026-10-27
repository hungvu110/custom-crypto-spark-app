# Trả lời đội DAK — về `PHAN-HOI-API-CONTRACT-DEID.md`

Ngày: 2026-10-27. Gửi: đội DAK. Từ: đội SA.

Cảm ơn đã đọc kỹ. File này trả lời đủ **sáu câu** của các bạn. Đọc xong file này là bắt đầu làm được phần gọi ra Vault,
Kubernetes và Airflow; không cần hỏi thêm. Hợp đồng `API_CONTRACT.md` đã được sửa theo các quyết định dưới đây (mục 2.2, 7.3 và 11).

## 1. Tóm tắt

| Câu | Quyết định của SA | DAK cần làm |
| --- | --- | --- |
| ① Vault | **DAK đọc kho riêng của DAK.** Path `spark-application/*` không dùng cho luồng này | Xác nhận path thật (mục 2) |
| ② Airflow | User Airflow riêng cho DAK, Basic. **Mật khẩu DAK nạp qua env `AIRFLOW_PASSWORD`**, lấy từ Secret Kubernetes tĩnh | Nạp env này vào pod (mục 3) |
| ③ Kubernetes | SA cấp cho ServiceAccount của DAK đúng hai quyền `create`, `delete` trên `secrets` | Cho biết tên ServiceAccount (mục 4) |
| ④ Hai file thiếu | Mọi thứ cần để implement đã nằm ở hợp đồng và mục 5 dưới đây | Không cần gì thêm |
| ⑤ Luồng cũ | **Giữ cả hai**, hai luồng độc lập | Không đụng `by-column`/YC |
| ⑥ Lọc ngày/giờ | Job **đã hỗ trợ nguồn là bảng**. SA báo từng môi trường; DAK bật ba env theo thông báo | Giữ ba env tắt tới khi có thông báo (mục 6) |

## 2. ① Vault — DAK đọc kho của DAK

SA chọn hướng 1. Đã sửa hợp đồng mục 11: vị trí key bảng **do DAK quyết định**, không còn là `spark-application/{database}.{table}`.
Job và DAG **không đọc Vault** (key đến job qua Secret của phiếu), nên SA không phụ thuộc vào path này.

Hai tài liệu của các bạn hiện nói hai dạng khác nhau, xin xác nhận path thật:

- bản đang ghi: `hla-datalake/datalake/dak/...`
- `DAK_API_SPEC.md`: `kv/data/dak/keys/{tenant}/{workspace}/{database}/{table}`

Yêu cầu SA cần ở key bảng:

- Field `keyPrefix`, là **base64 chuẩn của đúng 32 byte**. Sai thì phiếu vào `SUBMIT_FAILED` với `table key has unsupported format` (hợp đồng mục 6).
- Key một bảng **phải ổn định**: đã khử định danh xong thì không đổi giá trị. Băm không đảo ngược, đổi key là hai lần chạy cho kết quả không nối được với nhau.
- Đọc đúng version đã ghi vào phiếu (`keyVersion`), như `DAK_API_SPEC.md` đã mô tả.

Bản mock của SA đang đọc `spark-application/...` chỉ vì tiện chạy thử; đó là cấu hình của mock, **không phải đặc tả** cho DAK.

## 3. ② Airflow

- **Địa chỉ** (staging và production dùng cùng giá trị):

  | Địa chỉ Airflow (`/api/v1`) | `dagId` |
  | --- | --- |
  | `http://airflow-webserver.vlp-tenantw1xjixm-wsw7vtwvi-data.svc.cluster.local:8080` | `deid_pseudonymize_table` |

- **User:** SA tạo một user Airflow riêng cho DAK, tên `dak-trigger`, quyền tối thiểu (đọc DAG, tạo dag run). Auth là **HTTP Basic**
  (Airflow 2.8 chỉ bật `basic_auth` và `session`, không nhận Bearer Keycloak).
- **Mật khẩu:** SA tạo một Secret Kubernetes tĩnh chứa mật khẩu; **DAK nạp vào pod qua biến môi trường `AIRFLOW_PASSWORD`**
  (`secretKeyRef`). Mật khẩu không nằm trong cấu hình hay repo. Nếu DAK muốn Secret đó do Vault + External Secrets sinh ra thì vẫn được, miễn là pod
  nhận đúng biến `AIRFLOW_PASSWORD`. Tên user (`dak-trigger`) là cấu hình thường.
- Cách gọi, chuỗi `submitError` cố định và cách đọc kết quả dag run: hợp đồng mục 7.

## 4. ③ Kubernetes

- **Namespace:** namespace tạo Secret phải trùng namespace nơi DAG tạo SparkApplication (Pod chỉ mount được Secret cùng namespace).

  | Namespace (staging và production dùng cùng giá trị) |
  | --- |
  | `vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy` |

- **Quyền:** SA tạo `Role` + `RoleBinding` cho ServiceAccount của DAK, **chỉ** hai verb `create` và `delete` trên `secrets`, trong namespace trên.
  Không có `get`/`list`/`update`/`watch`. Mẫu SA đang dùng:

  ```yaml
  rules:
    - apiGroups: [""]
      resources: ["secrets"]
      verbs: ["create", "delete"]
  ```

  Role không giới hạn được theo tên Secret (tên chưa biết lúc tạo), nên DAK chỉ xoá Secret có tên khớp `deid-key-\d{4}-\d{6}` do chính mình tạo.
- Tên ServiceAccount của DAK: xin các bạn cho biết để SA gắn `RoleBinding`.

## 5. ④ Thay cho hai file không gửi kèm

Các điều `JOB_ENV_PLAN.md` và DAG mẫu quy định đã được đưa hết vào hợp đồng; phần cần để allowlist của DAK **khớp DAG** là bảng này.
DAG kiểm đúng bảng này (tên, quy tắc khớp toàn bộ giá trị) và từ chối env ngoài bảng:

| Tên | Quy tắc giá trị | Mặc định |
| --- | --- | --- |
| `OUTPUT_DB` | `[a-z0-9_]{1,128}` | không có; DAK điền lúc duyệt |
| `OUTPUT_TABLE` | `[a-z0-9_]{1,128}` | không có; DAK điền lúc duyệt |
| `OUTPUT_MODE` | `overwrite\|append\|overwrite-partitions` | `overwrite-partitions` nếu có `INPUT_FROM`/`INPUT_TO`/`INPUT_HOURS`, ngược lại `overwrite` |
| `OUTPUT_FORMAT` | `delta\|hive\|iceberg` | `delta` |
| `ENABLE_HIVE_SUPPORT` | `true\|false` | `true` |
| `INPUT_FROM` | `[0-9]{8}([0-9]{2})?` | không có |
| `INPUT_TO` | `[0-9]{8}([0-9]{2})?` | không có |
| `INPUT_HOURS` | `([01]?[0-9]\|2[0-3])(-([01]?[0-9]\|2[0-3]))?` | không có |

(Dấu `\|` trong bảng chỉ để Markdown không tách cột; quy tắc thật dùng dấu `|`.)

Điểm DAG cần DAK gửi đúng:

- `conf` gồm `requestCode`, `database`, `table`, `pseudoColumns`, `keySecretName` và `env` (env **cuối cùng**, đã điền mặc định). Không có `outputDb`/`outputTable` riêng, DAG đọc `env.OUTPUT_DB` và `env.OUTPUT_TABLE`.
- `dag_run_id` dạng `deid-<năm>-<6 số>`; run do DAK tạo **bắt buộc** có đủ `conf`, kể cả `OUTPUT_DB` và `OUTPUT_TABLE` trong `env`.
- DAG kiểm lại mọi giá trị ở task đầu `validate_conf` (cùng luật khoảng ngày/giờ ở hợp đồng 2.2). Sai thì dag run **failed ngay**, chưa tạo job.
- Mọi giá trị trong `env` phải là **chuỗi** (số bị Kubernetes từ chối).

Bản mock và DAG mẫu là mã nguồn; nếu DAK muốn chạy để đối chiếu, báo SA để gửi thư mục `dak-mock/`. Không có chúng vẫn implement được.

## 6. ⑥ Ba env ngày/giờ

- Job `pseudonymize-table-app` **đã hỗ trợ lọc nguồn là bảng** theo cột `date` (`yyyyMMdd`) và `hour` (`HH`), kể cả khoảng qua nhiều ngày, `INPUT_HOURS`
  áp cho mọi ngày, và `overwrite-partitions` (chạy lại cùng khoảng không nhân đôi dữ liệu, không xoá ngày khác). Hợp đồng mục 2.2 đã sửa theo job mới.
- **Mốc thời gian:** SA báo bằng văn bản khi từng môi trường (staging, production) đã có job mới và DAG mới. SA nâng **cả hai cùng lúc** ở mỗi môi trường,
  nên "DAG đã nâng" đồng nghĩa "job đã nâng".
- **Cách DAK biết:** không dò tự động. Danh sách env được phép là cấu hình theo môi trường của DAK (`dak.deid.job-env`); DAK **chỉ thêm ba env vào danh sách của môi trường
  đã được SA báo**. Làm theo đề xuất của các bạn: viết sẵn phần validate, để ba env tắt ở giao diện cho đến khi có thông báo.
- Nếu lỡ bật sớm: với DAG cũ, task `validate_conf` từ chối env lạ nên dag run failed ngay, không tạo job.

## 7. ⑤ Luồng cũ (`by-column`, YC)

**Giữ cả hai.** Luồng YC tiếp tục như cũ; phiếu khử định danh là luồng thứ hai, độc lập. Hợp đồng này không thay đổi gì ở `GET /api/v1/keys/by-column`.

## 8. Việc tiếp theo

| Ai | Việc |
| --- | --- |
| DAK | Xác nhận path Vault thật (mục 2) và tên ServiceAccount (mục 4) |
| DAK | Nạp `AIRFLOW_PASSWORD` từ Secret (mục 3) |
| SA | Tạo user `dak-trigger`, Secret mật khẩu và `Role`/`RoleBinding` (mục 3, 4) |
| SA | Báo từng môi trường khi job và DAG mới sẵn sàng (mục 6) |
| DAK | Làm trước phần không bị chặn như danh sách đã nêu; bật ba env ngày/giờ theo thông báo |
