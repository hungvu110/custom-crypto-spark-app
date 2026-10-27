# Phản hồi của đội dak — về `TRA-LOI-DAK.md`

Ngày: 2026-10-27 (bản 2). Gửi: đội SA.

Nhận được `TRA-LOI-DAK.md`. **Đủ cả sáu câu, không còn gì chặn chúng tôi.** Cảm ơn phần mục 5 —
bảng tám env thay được `JOB_ENV_PLAN.md`, chúng tôi không cần file đó nữa.

> Bản 1 của tài liệu này nêu bốn điểm chặn + hai câu hỏi. Tất cả đã được trả lời; nội dung cũ
> không còn cần thiết nên thay bằng bản này. Kế hoạch của chúng tôi:
> `docs/33-PHIEU-KHU-DINH-DANH.md` §7 ghi lại đúng các giá trị các anh/chị gửi.

---

## 1. Hai thứ các anh/chị hỏi lại

### Path Vault thật

```
hla-datalake/datalake/dak/keys/<...>
```

Đây là giá trị **đang chạy**, lấy từ cấu hình (`dak.vault.key-base-path`), không phải từ tài liệu.

`DAK_API_SPEC.md` ghi `kv/data/dak/keys/{tenant}/{workspace}/{database}/{table}` — **tài liệu đó
sai**, chúng tôi sẽ sửa. Hai điểm lệch: tiền tố `kv/data/` là đường API của KV v2 chứ không phải
path logic, và phần `{tenant}/{workspace}` không có trong path thật.

Ghi nhận cả ba yêu cầu của các anh/chị về key bảng: `keyPrefix` là base64 của đúng 32 byte, đọc
đúng `keyVersion` đã ghi vào phiếu, và **key một bảng phải ổn định**.

Điểm thứ ba chúng tôi coi là ràng buộc nghiệp vụ mới, không chỉ là ghi chú: dak hiện cho phép
xoay key, nên chúng tôi phải thêm rào chắn **"bảng này đã từng khử định danh chưa"** trước khi cho
xoay hay xoá key của một bảng. Lý do các anh/chị nêu (băm không đảo ngược, hai lần chạy không nối
được) là lý do đúng, và nó không sửa được sau khi đã xảy ra.

### Tên ServiceAccount

```
dak
```

Dùng cho pod `dak-api` (`k8s/chart`, giá trị `vault.role`). Xin gắn `RoleBinding` vào
ServiceAccount này trong namespace `vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy`.

---

## 2. Chúng tôi xác nhận lại các giá trị đã nhận

| Mục | Giá trị chúng tôi ghi vào cấu hình |
|---|---|
| Airflow | `http://airflow-webserver.vlp-tenantw1xjixm-wsw7vtwvi-data.svc.cluster.local:8080` |
| `dagId` được phép | `deid_pseudonymize_table` (danh sách một phần tử) |
| User Airflow | `dak-trigger`, HTTP Basic |
| Mật khẩu Airflow | biến môi trường `AIRFLOW_PASSWORD` |
| Namespace tạo Secret | `vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy` |
| Quyền K8s | `create` + `delete` trên `secrets`, không `get`/`list` |

Sai chỗ nào xin nói lại sớm — chúng tôi đưa đúng những giá trị này vào cấu hình.

**Mật khẩu Airflow:** chúng tôi làm theo hướng Vault + External Secrets sinh ra Secret đó, như
các anh/chị cho phép ở mục 3. Pod vẫn nhận đúng biến `AIRFLOW_PASSWORD` qua `secretKeyRef`; phía
các anh/chị không thấy khác gì. Làm vậy để không có bí mật nào nằm ngoài Vault
(`docs/17-AN-TOAN-THONG-TIN.md` §3). Nếu cách này làm chậm phía các anh/chị thì chúng tôi dùng
Secret tĩnh các anh/chị tạo trước, rồi chuyển sau.

---

## 3. Một điểm chúng tôi muốn nói rõ về quyền `delete`

`Role` chỉ có `create` và `delete` trên `secrets`, đúng như chúng tôi xin. Nhưng Kubernetes không
giới hạn `delete` theo tên khi tên chưa biết lúc tạo Role — nghĩa là **quyền này phủ mọi Secret
trong namespace, kể cả Secret của các anh/chị**, trong đó có Secret mật khẩu Airflow.

Chúng tôi chặn phía mình: dak **chỉ xoá Secret có tên khớp `deid-key-\d{4}-\d{6}`**, kiểm trước
khi gọi API Kubernetes, và có test cho cả ca từ chối. Nói ra đây để các anh/chị biết rào chắn nằm
ở phía dak chứ không ở `Role`, và để nếu sau này cụm hỗ trợ giới hạn theo tiền tố tên thì ta siết
thêm được.

Không đề nghị đổi gì ngay — hai verb đó là tập nhỏ nhất làm được việc.

---

## 4. Ba env ngày/giờ: làm theo đúng mục 6

Validate viết đủ ngay (thuần kiểm chuỗi): ngày/giờ có thật, `INPUT_FROM ≤ INPUT_TO`,
`INPUT_HOURS` số đầu ≤ số sau, `overwrite-partitions` đòi có khoảng, và ngoại lệ ba env này trong
nhóm `INPUT_*`.

Nhưng **không mở ở giao diện**, và không đưa vào `dak.deid.job-env` của một môi trường cho tới
khi nhận được thông báo của các anh/chị cho **chính môi trường đó**. Chúng tôi hiểu "DAG đã nâng"
= "job đã nâng", và hiểu rằng bật sớm thì `validate_conf` của DAG cũ từ chối, dag run failed ngay,
không tạo job.

---

## 5. Việc của chúng tôi

| Thứ tự | Việc | Cần gì từ SA |
|---|---|---|
| 1–5 | Phiếu + 9 trạng thái, nhận file, allowlist env/DAG, giới hạn, hai màn hình | — |
| 6 | Đọc key bảng ở Vault | — |
| 7 | Tạo/xoá K8s Secret | `RoleBinding` cho ServiceAccount `dak` |
| 8 | Trigger DAG + theo dõi dag run | user `dak-trigger` + Secret mật khẩu |
| 9 | Dọn Secret, tiến trình nền | 7, 8 |
| sau | Bật ba env ngày/giờ | Thông báo từng môi trường |

Bước 1–6 chúng tôi bắt đầu được ngay, không chờ gì. Bước 7–8 code và test được bằng mock/
Testcontainers trước khi có user và `RoleBinding` thật.
