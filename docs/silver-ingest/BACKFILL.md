# Chạy bù theo khoảng ngày / khoảng giờ (cấu hình ở phía DAG, không qua DAK)

DAG của một dataset (`silver_ingest_<nguồn>_<dataset>`) chạy hằng ngày theo lịch của phiếu và nạp MỘT ngày (ngày hôm trước). Muốn nạp lại,
hoặc nạp dữ liệu cũ, dùng hai cách dưới đây trên Airflow. Phiếu và lịch không đổi.

| Muốn | Dùng | Conf |
|---|---|---|
| Một ngày | DAG của dataset | `{"date": "20261013"}` |
| Một khoảng giờ của một ngày | DAG của dataset | `{"date": "20261013", "fromHour": "10", "toHour": "12"}` |
| Một khoảng ngày | DAG `silver_ingest_backfill` | `{"dag_id": "...", "fromDate": "20261010", "toDate": "20261012"}` |
| Cùng một khoảng giờ trên mỗi ngày của một khoảng ngày | DAG `silver_ingest_backfill` | thêm `"fromHour"`, `"toHour"` |

## Cách trigger trên giao diện Airflow

1. Mở danh sách DAG, tìm DAG cần chạy (`silver_ingest_vtnet_ebs_3g_4g`, `silver_ingest_vtnet_nims`, `silver_ingest_vtt_f001_mpre_call`, hoặc `silver_ingest_backfill`).
2. Bấm nút ▶ ở cột Actions, chọn **Trigger DAG w/ config**.
3. Dán conf JSON vào ô **Configuration JSON** rồi bấm **Trigger**.
4. Theo dõi ở tab **Grid**: task `validate_conf` kiểm conf (sai thì dừng, nêu lý do), task `ingest` tạo SparkApplication.

Hoặc dùng CLI trong pod webserver:

```bash
airflow dags trigger silver_ingest_vtnet_ebs_3g_4g --conf '{"date": "20261013", "fromHour": "10", "toHour": "12"}'
```

```bash
airflow dags trigger silver_ingest_backfill --conf '{"dag_id": "silver_ingest_vtnet_ebs_3g_4g", "fromDate": "20261010", "toDate": "20261012"}'
```

## Quy tắc của conf

- `date`: ngày thật, `yyyyMMdd`. Bắt buộc khi có giờ.
- `fromHour`, `toHour`: giờ nguyên 0–23 (số hoặc chữ, `"5"` và `"05"` đều được); cả hai đều gồm trong khoảng; `toHour` bỏ trống thì bằng `fromHour`; không có `fromHour` thì không được có `toHour`; `fromHour` không được sau `toHour`.
- Backfill: `dag_id` phải là DAG `silver_ingest_<vtt|vtnet>_<dataset>`; `toDate` bỏ trống thì bằng `fromDate`; tối đa 31 ngày một lần; DAG của dataset phải tồn tại và không bị pause.
- Khoá lạ trong conf bị từ chối. Bảng đích, cột mã hoá, chế độ ghi luôn lấy từ phiếu đã duyệt, không đổi được qua conf.

## Backfill chạy thế nào

`silver_ingest_backfill` chỉ tạo các dag run của DAG dataset: mỗi ngày một dag run, conf đúng như một lần chạy tay ở bảng trên. DAG dataset
chạy từng lần một (`max_active_runs=1`), nên các run xếp hàng và chạy lần lượt. Nếu một lần tạo run bị lỗi giữa chừng, task báo đã tạo được
bao nhiêu run; các run đã tạo vẫn chạy. Mỗi lần backfill có mã run riêng nên cùng một khoảng có thể chạy lại.

## Cách mỗi dataset lưu giờ (khai ở `DATASETS` trong file DAG, khoá `hour_glob`)

| Dataset | Cách lưu | `hour_glob` | Giờ 10–12 thành |
|---|---|---|---|
| `f001-mpre-call` (VTT) | `date=YYYYMMDD/hour=HH/` | `hour=%H%` | `date=20261013/hour={10,11,12}` |
| `ebs_3g_4g` | `dt=YYYYMMDD/batch=HHmm/` (nhiều batch mỗi giờ) | `batch=%H%*` | `dt=20261013/batch={10,11,12}*` |
| `nims` | `dt=YYYYMMDD/batch=N/infra_cell_4g-HHmmss.csv` (giờ nằm trong TÊN FILE) | `batch=*/infra_cell_4g-%H%*` | `dt=20261013/batch=*/infra_cell_4g-{10,11,12}*` |

`%H%` được thay bằng giờ (một giờ: `10`; nhiều giờ: `{10,11,12}`). Dataset mới có cách lưu giờ khác thì chỉ thêm `hour_glob` của nó; dataset
không có `hour_glob` chỉ nạp được cả ngày. **Các `hour_glob` trên là giả định theo cấu trúc mẫu, cần đối chiếu với dữ liệu thật** (đặc biệt
giờ trong tên file của nims có đúng là giờ của dữ liệu không).

## Cột `dt`, `hour` và cách thay dữ liệu

Mọi bảng silver có hai cột, dù chạy kiểu nào (lịch hằng ngày, một ngày, khoảng giờ, backfill):

| Cột | Giá trị | Nguồn |
|---|---|---|
| `dt` | `yyyyMMdd` (vd `20261013`) | ngày của run (cột lô) |
| `hour` | `HH` (vd `10`), `NA` nếu file không khớp `hour_glob` | suy ra từ ĐƯỜNG DẪN của từng file nguồn |

Một run thay đúng các dòng nó nạp, theo `(dt, hour)`:

| Chạy | Điều kiện thay (Delta `replaceWhere`) |
|---|---|
| Cả ngày (lịch, `{"date"}`, backfill ngày) | `dt = '20261013'` |
| Khoảng giờ | `dt = '20261013' AND hour IN ('10','11','12')` |

Nhờ vậy chạy cả ngày rồi chạy lại giờ 10, hoặc chạy chồng hai khoảng giờ, đều **thay** chứ không ghi trùng; các giờ không nằm trong run giữ nguyên.
Chạy cả ngày thay MỌI giờ của ngày đó (kể cả giờ không còn file). Downstream chỉ cần lọc `dt` và `hour`.

Partition: VTT và `ebs_3g_4g` partition theo `(dt, hour)` (`partition_by_hour: True` trong `DATASETS`). `nims` ít dữ liệu nên chỉ partition theo
`dt`, `hour` là cột thường (không chia 24 partition nhỏ mỗi ngày); `replaceWhere` vẫn thay đúng giờ nhưng phải quét các file của ngày đó.

Giờ được suy ra từ đường dẫn bằng regex sinh từ `hour_glob` (env `HOUR_PATH_REGEX` của job). Nếu một file trong khoảng giờ đã chọn có giờ khác
khoảng đó (hoặc `NA`), Delta từ chối ghi và job báo lỗi, bảng không đổi.

**Bảng cũ** (partition theo `batch_id`, trước khi đổi sang `dt`/`hour`) không dùng được nữa: app từ chối ghi vào bảng đã partition khác. Xoá bảng cũ rồi chạy lại.

## Lưu ý

- Mỗi dag run là một Spark job: backfill 31 ngày VTT (khoảng 165 GB mỗi ngày) chạy rất lâu và lần lượt.
- Spark app nhận `INPUT_PATH` là mẫu Hadoop (`SourceFiles.list` dùng `globStatus`); cần build lại image với bản này trước khi dùng giờ.
- Phải deploy cả `silver_ingest_common.py`, `silver_ingest_factory.py`, hai file DAG nguồn và `silver_ingest_backfill.py` lên thư mục dags của Airflow.
- Task backfill gọi `airflow.api.common.trigger_dag` và đọc `DagModel` (có sẵn trong Airflow 2.x, chạy dưới tài khoản của chính Airflow).
