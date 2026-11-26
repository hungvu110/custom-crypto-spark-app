# Kết nối Airflow cho DAK (tài khoản admin)

Tài liệu này dành cho DAK: địa chỉ Airflow, cách xác thực và các lệnh mẫu để trigger và theo dõi dag run.

## 1. Thông tin kết nối

| Mục | Giá trị |
| --- | --- |
| Địa chỉ gốc (Airflow) | `<http://airflow-host:8080>` (không kèm `/api/v1`) |
| REST API | `<địa chỉ gốc>/api/v1` |
| Giao diện web | `<địa chỉ gốc>` (đăng nhập bằng cùng user/mật khẩu) |
| Xác thực | HTTP Basic (`username:password`) |
| Username | `dak-admin` |
| Mật khẩu | Gửi riêng qua kênh bảo mật. Không ghi vào cấu hình hay repo |
| Quyền | Role `Admin` (trigger, theo dõi, bật/tắt DAG, quản lý Variables) |

Đặt biến để dùng cho các lệnh bên dưới:

```bash
AF=<địa chỉ gốc>
read -rs -p "Airflow password: " AP; echo
```

## 2. Kiểm tra kết nối

```bash
curl -s $AF/health                                   # JSON trạng thái scheduler, metadatabase
curl -s -o /dev/null -w "%{http_code}\n" -u "dak-admin:$AP" $AF/api/v1/dags/deid_pseudonymize_table   # 200
```

| Mã | Ý nghĩa |
| --- | --- |
| `200` | Kết nối và quyền đều đúng |
| `401` | Sai user/mật khẩu |
| `403` | Thiếu quyền (không xảy ra với role Admin) |
| `404` | Sai địa chỉ, hoặc DAG chưa có trên server này |

## 3. Trigger dag run

```bash
curl -s -u "dak-admin:$AP" -H 'Content-Type: application/json' -X POST \
  $AF/api/v1/dags/<dag_id>/dagRuns \
  -d '{"dag_run_id": "<id duy nhất>", "conf": {}}'
```

- `dag_run_id` phải duy nhất trong từng DAG. Trùng sẽ nhận `409`.
- `conf` là tham số truyền cho DAG. Nội dung theo hợp đồng của từng DAG.
- DAG đang tạm dừng (`is_paused: true`) thì dag run được tạo nhưng không chạy. Bật DAG:

```bash
curl -s -u "dak-admin:$AP" -H 'Content-Type: application/json' -X PATCH \
  $AF/api/v1/dags/<dag_id> -d '{"is_paused": false}'
```

## 4. Theo dõi dag run

```bash
# Một dag run (state: queued | running | success | failed)
curl -s -u "dak-admin:$AP" $AF/api/v1/dags/<dag_id>/dagRuns/<dag_run_id>

# Lần chạy gần nhất
curl -s -u "dak-admin:$AP" "$AF/api/v1/dags/<dag_id>/dagRuns?order_by=-start_date&limit=5"

# Trạng thái từng task
curl -s -u "dak-admin:$AP" $AF/api/v1/dags/<dag_id>/dagRuns/<dag_run_id>/taskInstances

# Log của một task (try_number bắt đầu từ 1)
curl -s -u "dak-admin:$AP" $AF/api/v1/dags/<dag_id>/dagRuns/<dag_run_id>/taskInstances/<task_id>/logs/1
```

Cũng có thể xem trên giao diện web: **DAGs → chọn DAG → Grid**.

## 5. Lưu ý

- Role `Admin` đọc được toàn bộ Connections và Variables của Airflow. Giữ mật khẩu kín, đừng đưa vào log hay mã nguồn.
- Chỉ dùng HTTPS nếu Airflow được đặt sau ingress có TLS. Nếu địa chỉ là HTTP thuần, chỉ gọi từ mạng nội bộ.
- Cần đổi mật khẩu hoặc thu hồi tài khoản: liên hệ SA.
