# Kịch bản test key global: hai dataset join được trên cột đã mã hoá

Mục tiêu: nạp hai dataset VTNET (`sub_profile`, `sub_usage`) từ file CSV **plain** vào silver, mã hoá các cột nhạy cảm bằng **cùng một key global** của một vùng, rồi kiểm tra hai bảng silver **join được với nhau trên cột đã mã hoá**.

## 0. Cần biết trước

- **File plain, DAK không có key by-path** cho `sub_profile`/`sub_usage`: DAK trả `204`, job **không giải mã cả file** mà đọc nguyên trạng, rồi mã hoá cột bằng key global (đã có test tự động trong `SilverAppSpec`).
- **Điều kiện join:** ciphertext của một cột = mã hoá(giá trị, `key global` + **giá trị cột `keyField` của dòng đó**). Hai bảng chỉ join được trên cột đã mã hoá khi **cùng key global** *và* cùng giá trị `keyField` cho cùng một thuê bao. Dữ liệu mẫu dùng `keyField = region_code` có ở cả hai bảng; thuê bao `84961000005` cố ý lệch `region_code` (`CT` ở profile, `HCM` ở usage) để thấy trường hợp không join.
- **Cần có sẵn:** bản DAG và image mới đã lên Airflow; admin DAK đã tạo key global của vùng (dưới đây dùng `key_hla`); có ít nhất một phiếu silver đang hiệu lực chọn vùng đó. Client nội bộ của job lấy được key khi có một phiếu như vậy (không cần mỗi dataset một phiếu).
- Dữ liệu mẫu: [sample-data/global-key/](sample-data/global-key/) (`sub_profile` 7 dòng, `sub_usage` 8 dòng, ngày `20261020`).

## 1. Đưa CSV lên landing

```bash
R=/prod8/landing/vtnet
hdfs dfs -mkdir -p $R/sub_profile/dt=20261020 $R/sub_usage/dt=20261020
hdfs dfs -put -f docs/silver-ingest/sample-data/global-key/sub_profile/dt=20261020/*.csv $R/sub_profile/dt=20261020/
hdfs dfs -put -f docs/silver-ingest/sample-data/global-key/sub_usage/dt=20261020/*.csv   $R/sub_usage/dt=20261020/
```

## 2. Tạo hai Airflow Variable (Admin → Variables)

`table` phải bắt đầu bằng `vtnet_`; `keyField` **không** nằm trong `encryptColumns`.

| Key | Val |
|---|---|
| `silver_ingest_vtnet_sub_profile` | `{"ticket":"SLV-2026-000101","table":"vtnet_sub_profile","cron":"0 2 * * *","encryptColumns":["msisdn","imsi","full_name","id_number"],"keyField":"region_code","globalKey":true,"globalZone":"key_hla"}` |
| `silver_ingest_vtnet_sub_usage` | `{"ticket":"SLV-2026-000102","table":"vtnet_sub_usage","cron":"0 2 * * *","encryptColumns":["msisdn","imsi"],"keyField":"region_code","globalKey":true,"globalZone":"key_hla"}` |

## 3. Chạy hai DAG

Trigger với conf (nạp đúng ngày của dữ liệu mẫu), từng DAG một:

```json
{"date": "20261020"}
```

DAG `silver_ingest_vtnet_sub_profile`, rồi `silver_ingest_vtnet_sub_usage`. Hai task `validate_conf` và `ingest` phải **success**.

## 4. Xem log driver của job (pod `silver-vtnet-sub-*-driver`)

Cần thấy đủ ba dấu hiệu:

1. `3. PATH KEY (DAK by-path)` kèm `DAK has no key for this path: files are not encrypted, reading them as they are`
2. `4. COLUMN KEYS (DAK global key, zone=key_hla, one key for every column, cdr-crypto)` và `global key received for msisdn, imsi, ...`
3. `JOB SUCCEEDED — 1 file(s) written to demo_db.vtnet_sub_profile` (không có chữ "decrypted").

Không được thấy `by-column`. Lỗi thường gặp: `403 access_denied` (chưa có phiếu hiệu lực của vùng, hoặc chưa tạo key vùng), `403 key_disabled` (key vùng bị khoá).

## 5. Kiểm tra bảng silver

```sql
SELECT count(*) FROM demo_db.vtnet_sub_profile;   -- 7
SELECT count(*) FROM demo_db.vtnet_sub_usage;     -- 8
SELECT msisdn, imsi, full_name, region_code, plan_code, dt FROM demo_db.vtnet_sub_profile LIMIT 5;
-- msisdn, imsi, full_name là chuỗi base64 (đã mã hoá); region_code, plan_code, dt còn plaintext
SELECT count(*) FROM demo_db.vtnet_sub_profile WHERE msisdn LIKE '8496%';   -- 0: không còn số điện thoại gốc
```

## 6. Kiểm tra join trên cột đã mã hoá

```sql
-- Join theo msisdn đã mã hoá
SELECT count(*) FROM demo_db.vtnet_sub_profile p JOIN demo_db.vtnet_sub_usage u ON p.msisdn = u.msisdn AND p.dt = u.dt;   -- 5
-- Join theo imsi đã mã hoá
SELECT count(*) FROM demo_db.vtnet_sub_profile p JOIN demo_db.vtnet_sub_usage u ON p.imsi = u.imsi AND p.dt = u.dt;       -- 5
-- Xem chi tiết các dòng join được
SELECT p.region_code AS region_profile, u.region_code AS region_usage, p.plan_code, u.data_mb
FROM demo_db.vtnet_sub_profile p JOIN demo_db.vtnet_sub_usage u ON p.msisdn = u.msisdn AND p.dt = u.dt
ORDER BY u.data_mb;
-- Thuê bao profile không có dòng usage khớp
SELECT p.region_code, p.plan_code FROM demo_db.vtnet_sub_profile p
LEFT ANTI JOIN demo_db.vtnet_sub_usage u ON p.msisdn = u.msisdn AND p.dt = u.dt;                                          -- 3 dòng
```

Kết quả mong đợi từ dữ liệu mẫu:

| Thuê bao (số gốc) | profile | usage | Join trên cột mã hoá |
|---|---|---|---|
| `…001` | HN | HN, 2 dòng | **2 dòng** |
| `…002`, `…003`, `…004` | HCM/DN/HP | khớp | **1 dòng mỗi số** |
| `…005` | `region_code=CT` | `HCM` | **không khớp** (keyField khác nên ciphertext khác) |
| `…006`, `…007` | chỉ có ở profile | không có | không khớp |
| `…008`, `…009` | không có | chỉ có ở usage | không khớp |

Tổng join = **5** dòng (so với 6 dòng nếu join bằng số điện thoại gốc, khác ở `…005`), anti join = **3** (`…005`, `…006`, `…007`).

Đối chứng (tuỳ chọn): đổi `globalZone` của `sub_usage` sang vùng khác (hoặc dùng key khác), nạp lại sang bảng mới; join phải trả **0** dòng, chứng minh kết quả ở bước 6 là do hai bảng dùng chung key.

Lưu ý: theo code hiện tại `cdr_decrypt` của SQL engine lấy key theo cột (`by-column`), chưa lấy key global, nên chưa giải mã ngược các cột này bằng SQL được; kiểm tra bằng join như trên.

## 7. Dọn dẹp

Xoá hai Variable (để DAG không chạy theo `cron 0 2 * * *` vào ngày không có dữ liệu) và bảng thử:

```sql
DROP TABLE demo_db.vtnet_sub_profile;
DROP TABLE demo_db.vtnet_sub_usage;
```
