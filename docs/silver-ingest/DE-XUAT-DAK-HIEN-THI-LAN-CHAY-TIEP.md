# Đề xuất cho DAK: hiển thị "lần chạy tiếp theo" của phiếu chuẩn hóa silver

## Vấn đề

`GET /api/silver-requests/{code}` của phiếu `SCHEDULED` đang trả `nextRun: "2026-11-04T20:00:00+00:00"`. Giờ Việt Nam (UTC+7) là **05/11 03:00**. Người dùng đọc là "job chạy lúc 03:00 ngày 05/11", trong khi lúc xem (17:49 ngày 05/11) giờ đó đã qua.

**Nguyên nhân:** `nextRun` là ngày logic (`next_dagrun`) của Airflow, tức **đầu chu kỳ dữ liệu**, không phải lúc run bắt đầu. Run chỉ bắt đầu khi chu kỳ kết thúc:

| Cron của phiếu | `0 3 * * *` (giờ Việt Nam) |
|---|---|
| Chu kỳ dữ liệu của run kế tiếp | 05/11 03:00 → 06/11 03:00 |
| `next_dagrun` (đang hiển thị) | 05/11 03:00 |
| **Run thực sự bắt đầu** | **06/11 03:00** |
| Dữ liệu run đó nạp (`dt`) | `20261105` |

## Đề xuất

Lấy từ `GET /api/v1/dags/{dag_id}` của Airflow (tên trường theo Airflow 2.x, vui lòng đối chiếu OpenAPI của phiên bản đang chạy):

| Trường Airflow | Dùng cho |
|---|---|
| `next_dagrun_create_after` | Lúc run bắt đầu |
| `next_dagrun_data_interval_start` | Ngày (và giờ) dữ liệu sẽ được nạp |

Trả thêm trong phản hồi phiếu `SCHEDULED` (giữ các trường cũ để không phá client):

```json
{
  "nextRunAt": "2026-11-06T03:00:00+07:00",
  "nextRunDataDate": "20261105",
  "nextRunDataHour": null,
  "lastRun": "SUCCEEDED",
  "lastRunDataDate": "20261104"
}
```

- `nextRunAt`: `next_dagrun_create_after` đổi sang `Asia/Ho_Chi_Minh`, ISO-8601 có `+07:00`.
- `nextRunDataDate`: ngày `yyyyMMdd` (múi giờ Việt Nam) của `next_dagrun_data_interval_start`. Cron hằng ngày: luôn là ngày trước ngày của `nextRunAt`.
- `nextRunDataHour`: chỉ cron hằng giờ (`<phút> * * * *`), là giờ `HH` của đầu chu kỳ, là giờ liền trước giờ của `nextRunAt`. Cron hằng ngày thì `null`.
- `nextRun` cũ: không dùng để hiển thị; nếu giữ thì ghi chú là "ngày logic".
- Múi giờ cố định `Asia/Ho_Chi_Minh`, không dùng múi giờ trình duyệt hay máy chủ.

Câu chữ gợi ý trên giao diện:

- Hằng ngày: "Lần chạy tiếp theo: 03:00 06/11/2026, nạp dữ liệu ngày 05/11/2026"
- Hằng giờ: "Lần chạy tiếp theo: 11:05 05/11/2026, nạp dữ liệu giờ 10 ngày 05/11/2026"

## Trường hợp biên

| Tình huống | Hiển thị |
|---|---|
| `is_paused = true` | "DAG đang tạm dừng", không hiện giờ chạy |
| `next_dagrun_create_after` là `null` | "Chưa có lịch chạy" |
| `has_import_errors = true` | Báo lỗi import DAG, không hiện giờ |
| Giờ chạy đã qua mà chưa có run mới | "Đang chờ scheduler", không coi là lỗi |

## Test nên có

- Cron hằng ngày: `nextRunDataDate` là ngày trước ngày của `nextRunAt`.
- Cron hằng giờ: có `nextRunDataHour`, bằng giờ trước giờ của `nextRunAt`.
- Mốc `20:00Z` (= `03:00+07`): ngày không lệch một ngày do đổi múi giờ.
- DAG paused, `next_dagrun_create_after = null`, `has_import_errors = true`.
- Phiếu không ở `SCHEDULED`: không trả các trường `nextRun*`.
