# Phản hồi của đội dak — về `API_CONTRACT.md` (luồng khử định danh)

Ngày: 2026-10-27. Gửi: đội SA.

Đã đọc hết hợp đồng. Hành vi mô tả rõ, đủ để hiện thực — chúng tôi không có ý kiến khác về
luồng, trạng thái hay cách báo lỗi.

Tài liệu này nêu **bốn điểm chặn** phải chốt trước khi chúng tôi viết phần dak gọi ra Vault,
Kubernetes và Airflow, cộng **hai câu hỏi**: một về luồng cũ, một về mốc thời gian job hỗ trợ lọc
ngày/giờ. Phần phiếu, kiểm file, giao diện và giới hạn thì không chặn gì — chúng tôi làm trước.

Bản hợp đồng mới nhất (thêm `INPUT_FROM`/`INPUT_TO`/`INPUT_HOURS` và `overwrite-partitions`)
chúng tôi đã đọc và đồng ý với cách mô tả. Câu ⑥ ở dưới là về nó.

---

## 1. Tóm tắt một trang

| | |
|---|---|
| **① Vault path** | 🔴 **Chặn nặng nhất.** `API_CONTRACT.md` §11 và `DAK_API_SPEC.md` của chính các anh/chị nói **ngược nhau** về chỗ key bảng nằm. Đọc nhầm kho = băm dữ liệu bằng key sai |
| **② Airflow** | Cần địa chỉ từng môi trường, ai cấp user cho dak, và cách đưa mật khẩu vào dak |
| **③ Kubernetes** | Cần tên namespace chạy job từng môi trường, và ai cấp quyền `create`/`delete` secret ở đó cho ServiceAccount của dak |
| **④ Hai file thiếu** | `JOB_ENV_PLAN.md` và `dak-mock/README.md` được hợp đồng trỏ tới nhưng không có trong bộ chúng tôi nhận |
| **⑤ Luồng cũ** | Hợp đồng không nhắc `by-column`/YC. Giữ nguyên hay thay thế? |
| **⑥ Lọc ngày/giờ** | Ba env mới chưa dùng được: job chỉ lọc với nguồn **thư mục**, luồng này luôn là **bảng**. Xin mốc thời gian + cách dak biết job đã sẵn |
| **Chúng tôi làm trước** | Phiếu + 9 trạng thái, nhận file 64 KB + SHA-256, gom lỗi `problems[]`, allowlist env/DAG, giới hạn 20 phiếu/team + chống trùng, màn gửi phiếu và màn admin |

---

## 2. ① Vault path — hai tài liệu của SA nói ngược nhau

`API_CONTRACT.md` §11 (bảng cấu hình, dòng cuối):

> Vị trí key bảng ở Vault — `kv/hla-datalake/datalake/spark-application/{database}.{table}`, field `keyPrefix`

`DAK_API_SPEC.md` (bản các anh/chị gửi trước) nói về chính path đó rằng đây là path **cũ**, và
**dak không đọc không ghi** ở đó; key của dak nằm dưới kho riêng của dak.

Hiện dak đang ghi key ở `hla-datalake/datalake/dak/...` — kho riêng, đúng theo bản cũ.

**Vì sao đây là chặn chứ không phải chi tiết:** job băm dữ liệu bằng key đọc được. Nếu dak đọc
nhầm kho, job vẫn chạy, vẫn ghi ra bảng kết quả, và **không ai phát hiện ra** cho tới lúc cần
nối dữ liệu giữa hai hệ thống — khi đó dữ liệu đã băm bằng key sai và không sửa được.

**Xin chốt một trong hai:**

1. dak đọc key ở kho của dak (`.../dak/...`) — khi đó §11 của hợp đồng cần sửa; hoặc
2. dak đọc ở `.../spark-application/{database}.{table}` — khi đó xin xác nhận bản `DAK_API_SPEC`
   cũ đã hết hiệu lực ở điểm này, và cho chúng tôi biết **ai ghi key vào đó** (dak hay SA), vì
   dak sẽ phải có quyền đọc một kho không phải của mình.

Chúng tôi không đoán. Phần đọc key để nguyên cho tới khi có trả lời.

---

## 3. ② Airflow — cần ba thứ

Hợp đồng §7.3 nói dùng **một user Airflow riêng cho dak**, quyền tối thiểu (đọc DAG, tạo dag
run), gọi bằng HTTP Basic. Chúng tôi đồng ý cách này.

Cần:

1. **Địa chỉ Airflow** của từng môi trường (staging, production).
2. **Ai tạo user** cho dak, và quy trình xin.
3. **Cách đưa mật khẩu vào dak.** dak không giữ secret trong cấu hình hay trong repo
   (`docs/17-AN-TOAN-THONG-TIN.md` §3) — mọi bí mật đi qua Vault rồi External Secrets. Nếu phía
   các anh/chị có cách khác đang dùng, cho chúng tôi biết để thống nhất.

Ghi chú: hợp đồng nói Airflow 2.8 chỉ bật `basic_auth` và `session` nên không nhận Bearer token
Keycloak. Chúng tôi ghi nhận và không đề nghị đổi.

---

## 4. ③ Kubernetes — namespace và quyền

Hợp đồng §11 nói namespace tạo Secret **phải trùng** namespace nơi DAG tạo SparkApplication, vì
Pod chỉ mount được Secret cùng namespace.

Cần:

1. **Tên namespace** chạy job, từng môi trường.
2. **Ai cấp quyền** cho ServiceAccount của dak: `create` và `delete` trên `secrets` ở namespace
   đó. Hiện dak không có quyền nào ngoài namespace của chính nó.

Chúng tôi sẽ xin đúng hai quyền đó, không xin `get`/`list` — dak không cần đọc lại Secret đã
tạo.

---

## 5. ④ Hai file hợp đồng trỏ tới nhưng chúng tôi không có

| File | Hợp đồng nhắc ở | Chúng tôi cần vì |
|---|---|---|
| `JOB_ENV_PLAN.md` | §12, dòng cuối | Thiết kế chi tiết mục `env` — chúng tôi cần để allowlist khớp với DAG |
| `dak-mock/README.md` §11 | §12, dòng cuối | Cách chạy bản mock. Đây là thứ **duy nhất** đối chiếu được hành vi trước khi có Airflow thật |

Bản mock quan trọng với chúng tôi hơn bình thường: hợp đồng nói dak phải khớp hành vi đến mức
mã lỗi và chuỗi `submitError` cố định. Không có mock thì chúng tôi chỉ đối chiếu được bằng cách
đọc, và sai sót chỉ lộ ra khi chạy thật.

Nếu DAG mẫu `sample_dag_deid_pseudonymize_table.py` (§7.4) gửi kèm được thì càng tốt — chúng
tôi cần nó để chắc `JOB_ENV` của DAG và allowlist env của dak khớp nhau (§7.4 gạch cuối).

---

## 6. ⑤ Luồng cũ còn sống không?

Hợp đồng này không nhắc `GET /api/v1/keys/by-column`, không nhắc YC, cũng không trả lời năm câu
chúng tôi gửi trước đó về mô hình "dak gọi sang SA tạo job".

Xin xác nhận một trong hai:

- **Giữ cả hai**: luồng YC (đã code, đã chạy, hợp đồng `YEU-CAU-TU-SA-YC.md` sáu vòng) tiếp tục
  như cũ, và phiếu khử định danh là luồng thứ hai độc lập; hoặc
- **Thay thế**: hợp đồng mới thay luồng YC, và chúng tôi dừng phần đó lại.

Chúng tôi hỏi vì hai luồng cùng chạm vào key của bảng, và chúng tôi không muốn đoán sai rồi để
lại một đường gọi không ai dùng nhưng vẫn mở.

---

## 6b. ⑥ Ba env lọc ngày/giờ — bao giờ job dùng được với nguồn là bảng?

Bản hợp đồng mới thêm `INPUT_FROM`, `INPUT_TO`, `INPUT_HOURS` và `OUTPUT_MODE=overwrite-partitions`.
Chúng tôi không có ý kiến khác về cách mô tả: dạng chuỗi, quy tắc validate, mặc định đổi theo
việc có khoảng hay không, và ngoại lệ ba env này trong nhóm `INPUT_*` — đều rõ, làm được.

Vấn đề là chính §2.2 nói job `pseudonymize-table-app` hiện **chỉ** lọc được `date`/`hour` khi
nguồn là **thư mục**; nguồn là **bảng** thì job báo lỗi, phần hỗ trợ "đang được bổ sung". Luồng
phiếu khử định danh **luôn** lấy nguồn là bảng.

Nên nếu chúng tôi mở ba env đó ngay, kết quả là: phiếu hợp lệ, admin duyệt, dak tạo Secret, dak
trigger DAG, rồi job chết vì nguồn là bảng. Mất một vòng đầy đủ mà không phải lỗi của dak, và
người gửi không hiểu vì sao.

**Xin hai thứ:**

1. **Mốc thời gian** job hỗ trợ lọc với nguồn là bảng.
2. **Cách dak biết** job ở một môi trường đã hỗ trợ hay chưa — phiên bản image, một cờ trong DAG,
   hay chỉ là các anh/chị thông báo? Chúng tôi cần phân biệt được vì staging và production có thể
   lệch nhau một thời gian.

Trong lúc chờ: chúng tôi viết sẵn phần validate (nó thuần kiểm chuỗi, không phụ thuộc job) nhưng
**để ba env tắt ở giao diện gửi phiếu**. Có câu trả lời là bật được ngay.

---

## 7. Chúng tôi làm gì trong lúc chờ

Không chặn, làm trước:

| Việc | Ghi chú |
|---|---|
| Bảng + entity phiếu, 9 trạng thái, mã `PYC-<năm>-<6 số>` | Đúng sơ đồ §1.2 |
| Nhận file phiếu | 64 KB, SHA-256, **không ghi xuống đĩa**, gom mọi lỗi một lượt vào `problems[]` |
| Allowlist env + nhóm env cấm | Kể cả **từ chối khởi động** nếu cấu hình chứa env thuộc nhóm cấm (§2.2); `INPUT_*` cấm **trừ** ba env khoảng |
| Validate khoảng ngày/giờ | Ngày/giờ có thật, `FROM ≤ TO`, `INPUT_HOURS` số đầu ≤ số sau, `overwrite-partitions` đòi có khoảng — viết sẵn, giao diện tắt (⑥) |
| Allowlist DAG | Cùng một thông điệp `dagId is not allowed` dù DAG có tồn tại hay không |
| Giới hạn và chống trùng | 20 phiếu/team chưa kết thúc; trùng theo (`database`, `table`, tập cột), không tính `env` |
| Màn gửi phiếu + màn admin | Phân trang cursor, lọc trạng thái, phiếu team khác trả `404` như phiếu không tồn tại |

Chặn, để nguyên: đọc key ở Vault (①), tạo/xoá K8s Secret (③), trigger và theo dõi Airflow (②).

Một điểm chúng tôi đọc kỹ và làm đúng: **key không bao giờ vào `conf` của Airflow** — chỉ tên
Secret. Lý do các anh/chị nêu (Airflow lưu `conf` trong DB và hiện trên giao diện) trùng với
nguyên tắc của chúng tôi về chỗ bí mật được phép nằm.

---

Tóm lại: hợp đồng rõ, chúng tôi không đề nghị sửa hành vi nào. Bốn điểm chặn là thông tin môi
trường và một mâu thuẫn giữa hai tài liệu; hai câu còn lại là phạm vi (⑤) và mốc thời gian (⑥) —
có trả lời là chúng tôi chạy tiếp được ngay.
