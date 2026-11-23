# Luồng khử định danh dữ liệu VTT theo phiếu khai thác của team DS

Nguồn yêu cầu: phiếu yêu cầu tạo mới, thay đổi truy cập dữ liệu VTT (mẫu BM01, ngày 19/11/2026; sheet `Dulieusudung`, `Khudinhdanh`),
đối chiếu schema với danh mục `Catalog_Truong_du_lieu_Viettel_Data_Catalog.xlsx` (sheet `VTT`). Hai file Excel nằm ở `docs/schema/` trên máy làm việc, **không commit** (có thông tin cá nhân).

## 1. Luồng

```
landing VTT (parquet, một thư mục mỗi ngày: .../vtt-core/<bảng>/date=YYYYMMDD/result_<uuid>.parquet)
   │  Bước 1: silver-ingest, mode decrypt-encrypt (DAK trigger DAG của bảng)
   │          giải mã các cột VTT đã mã hóa (cdr-crypto, prefix lấy qua by-path, keyField do VTT cung cấp) → gom nhóm tuổi, giờ
   │          → mã hóa các cột nhạy cảm (cdr-crypto, MỘT key global) → ghi silver
   ▼
silver (18 bảng VTT, mọi bảng cùng một key global)
   │  Bước 2: pseudonymize-table-app: HASH các cột có tag "Mã hóa" của DS; 9 bảng danh mục chạy với PSEUDO_COLUMNS=none
   ▼
bảng khử định danh của team DS (18 bảng, cùng một phiếu)
```

| Bước | Công cụ | Làm gì | Không làm gì |
| --- | --- | --- | --- |
| 1 | `silver-ingest-app`, mode `decrypt-encrypt` | Giải mã **từng cột** VTT đã mã hóa (prefix lấy qua `by-path`), gom nhóm tuổi và giờ, mã hóa các cột nhạy cảm bằng một key global, ghi silver (mục 3) | Không đổi mã tỉnh, huyện, xã. Không đổi tên cột |
| 2 | `pseudonymize-table-app` | Băm (`HASH`) các cột trong `PSEUDO_COLUMNS`; các cột còn lại giữ nguyên | Không gom nhóm, không giải mã |

Vì mọi bảng cùng mã hóa bằng một key global nên cùng một giá trị gốc ra cùng một bản mã, và băm bản mã đó ở bước 2 cũng ra cùng một giá trị. **Điều kiện để điều này đúng: mục 3.3.**

## 2. Phạm vi và giả định

- **Bảng trong phạm vi: 18 bảng.** Bỏ EBS-3G, EBS-4G, NIMS-3G, NIMS-4G, view_imsi_isdn_mapping và **Datamon** theo yêu cầu.
- **Ô để trống trong `Khudinhdanh` nghĩa là lấy nguyên giá trị.** Phiếu có 790 cột, chỉ có quy tắc cho một số ít cột; các cột còn lại không đổi.
- **Hai nhóm cột nhạy cảm, xử lý ở hai bước:**
  - **Bước 2 (băm): cột có tag "Mã hóa" của DS:** `isdn`, `customer_id`, `cust_id`, `calling_number`.
  - **Bước 1 (mã hóa global): hợp của cột trên và các cột VTT đã mã hóa sẵn** trong danh mục dữ liệu (STT 135, 226, 420, 421, 424, 599, 777, 1163, 1610).
    Khác biệt duy nhất: `f002_mpre_sms.called_number` và `called_number_cl` được mã hóa ở bước 1 nhưng **không băm ở bước 2** (đã chốt).
  - **Bước 1 giải mã trước khi mã hóa lại chỉ với cột VTT đã mã hóa sẵn.** Cột DS muốn băm mà VTT **không** mã hóa (đã chốt: `f001_mpre_call.calling_number`; suy ra từ danh mục:
    `customer_id`, `cust_id`) thì bước 1 **chỉ mã hóa**, không giải mã, để cùng số thuê bao ra cùng bản mã ở mọi bảng.
- **Cột gom nhóm ở bước 1:** `customer_age`, `hour_id`. `bts_name` đi theo khung giờ của `hour_id` (mục 3.4), không có transform riêng.
- **Mã tỉnh, huyện, xã cũ giữ nguyên y hệt nguồn**, kể cả ngưỡng 20 thuê bao của `precinct` và `village_code` (coi là một phần của quy tắc địa giới đã bỏ):
  `F_CUSTOMER.province/district/precinct`, `F_HOME_BTS_DAILY_AREA.province_code/province_name/district_code/district_name/village_code`, `F022_REGISTER.province_name/district_name`.
- **`nationality` không cần làm nữa:** quy tắc ngưỡng k = 20 trong phiếu bị bỏ, cột ghi nguyên giá trị.
- **Một phiếu khai thác cả 18 bảng** (mục 4.3).
- **Giá trị gốc của các cột bị gom nhóm hoặc mã hóa chỉ còn ở landing.**

## 3. Bước 1: landing → silver

### 3.1 Job làm gì, theo thứ tự

Dùng entry point `DecryptEncryptSilverApp` (mode `decrypt-encrypt`, đã có, hiện dùng cho VTNET `nims`). DAK trigger DAG của bảng; DAG đưa cấu hình của phiếu (cột mã hóa, vùng key global) vào env của job.

1. **Lấy prefix của VTT:** `GET /api/v1/keys/by-path?path=<KEY_PATH>&tag=internal` bằng token Keycloak của client DAK. `by-path` chỉ **trả về một chuỗi key** (`keyPrefix`) lưu ở đường dẫn đó,
   không quan tâm key được dùng cho việc gì, nên dùng được làm prefix giải mã theo cột (mục 3.2).
   - `200` kèm `keyPrefix`, `keyVersion`: có prefix, dùng ở bước 3.
   - `204`: chưa có key cho đường dẫn này. Với bảng có cột VTT mã hóa thì **phải coi là lỗi cấu hình** (nếu cứ chạy tiếp, cột còn là bản mã VTT sẽ bị mã hóa chồng lên); với bảng không có cột VTT mã hóa thì bỏ qua bước này.
   - `202` (cách cũ đã bỏ) hoặc `403 key_disabled` (key bị khóa): job dừng, exit 1.
2. **Lấy key mã hóa:** `GET /api/v1/keys/global?zone=<GLOBAL_KEY_ZONE>`, **một lần ở driver**, một key cho mọi cột và mọi bảng của vùng. Không gọi `by-column`, không có `PENDING` để chờ.
   `403 access_denied` (vùng chưa có phiếu hiệu lực) hoặc `key_disabled`: job dừng, exit 1, chưa đụng dữ liệu.
3. **Đọc file parquet** của thư mục ngày (đọc trực tiếp, **không giải mã cả file**, không cần thư mục tạm).
4. **Gắn cột lô `dt` và cột giờ `hour`** (giờ lấy từ cột `hour_id` của dữ liệu, `hour_column` của DAG).
5. **Giải mã các cột VTT đã mã hóa** bằng `CdrCrypto.decryptColumns`: với mỗi dòng, `keyInput = prefix ‖ giá trị cột keyField của VTT` (ghép thẳng, không dấu phân cách), rồi giải mã bằng thuật toán của VTT (`DataLakeSecurity_jv8.jar`). *(App chưa có bước này, cần thêm.)*
6. **Gom nhóm tuổi và giờ** theo mục 3.4. *(Chưa có trong app, cần thêm.)*
7. **Mã hóa các cột** trong `CDR_ENCRYPT_COLUMNS` bằng `CdrCrypto.encryptColumns` với key global và `keyField` hằng số (mục 3.3).
8. **Ghi bảng silver**, partition `dt`, `hour`.

Thứ tự bắt buộc: giải mã **trước** gom nhóm, vì cột `keyField` của VTT phải còn nguyên giá trị gốc lúc giải mã (nếu keyField là `hour_id` hoặc `customer_age` thì gom nhóm trước sẽ làm giải mã sai).
Job cần `DataLakeSecurity_jv8.jar` trên driver **và executor** (`spark.jars`); jar không commit vào repo (xem [BUILD_AND_TEST.md](../project/BUILD_AND_TEST.md)).

### 3.2 Lưu key trên DAK (ví dụ bảng `f014`)

Dữ liệu hiện tại của `f014` (ảnh HDFS): `/landing/vtt/lakehouse/vtt-core/f014-mpre-tot-charge-d/date=20261104/`, 215 file `result_<uuid>.parquet` (27 đến 150 MB mỗi file).

`by-path` tra key theo **một chuỗi đường dẫn tương đối, khớp nguyên chuỗi** (không khớp tiền tố, giữ hoa thường; theo [API-LAY-KEY-DUONG-DAN-COT.md](../key-access/API-LAY-KEY-DUONG-DAN-COT.md) mục 2 và 6).
Admin DAK **nhập đúng chuỗi đó** ở màn quản trị khi tạo key. Quy tắc chuỗi: không `/` đầu hoặc cuối, không `//`, không `..`, không `hdfs://`.

| Phương án | Chuỗi `path` trên DAK cho `f014` | Số key cho 18 bảng | Nhận xét |
| --- | --- | --- | --- |
| **A. Một key mỗi bảng (khuyến nghị)** | `f014-mpre-tot-charge-d` | 18 | Trùng quy ước hiện có: DAG đặt `KEY_PATH` mặc định bằng tên dataset (như `nims`), không cần cấu hình thêm. Một key dùng cho mọi ngày của bảng |
| B. Một key cho cả nguồn VTT | `vtt-core` | 1 | Chỉ đúng nếu VTT dùng **một** key để mã hóa mọi bảng. Cần đặt `key_path: "vtt-core"` trong `DATASETS` của từng bảng |
| C. Một key mỗi ngày mỗi bảng | `vtt-core/f014-mpre-tot-charge-d/date=20261104` | 18 mỗi ngày | **Không dùng.** Admin phải tạo key mỗi ngày, không hợp DAG chạy định kỳ |

- Tên bảng dùng **dấu gạch ngang như tên thư mục landing** (`f014-mpre-tot-charge-d`), không dùng gạch dưới (`f014_mpre_tot_charge_d`). Sai một ký tự là DAK trả `204`: app hiện coi đó là "không mã hóa" và đọc tiếp, nên với bảng có cột VTT mã hóa phải đổi thành lỗi (mục 3.1, bước 1), nếu không cột còn là bản mã VTT sẽ bị mã hóa chồng lên.
- **Giá trị lưu trên DAK là prefix mà VTT cấp cho loại CDR đó**, đúng bằng chuỗi VTT dùng để mã hóa (xem [PARTNER_CDR_CRYPTO_UDF_PLAN.md](../cdr-crypto/PARTNER_CDR_CRYPTO_UDF_PLAN.md) mục 0:
  `keyInput = prefix ‖ giá trị cột keyField`, prefix cố định theo loại CDR, "gửi riêng cho từng đơn vị"). DAK không tự sinh được prefix cho dữ liệu VTT đã mã hóa, nên VTT giao prefix để admin DAK nhập vào chuỗi `path` ở trên.
  **Không ghi prefix vào tài liệu hay log.**
- **Một `path` chứa đúng một prefix.** Phương án A đúng nếu mỗi bảng (loại CDR) có một prefix chung cho mọi cột VTT mã hóa của bảng đó. Nếu VTT dùng **prefix riêng cho từng cột** của cùng bảng thì `by-path` không đủ
  (một path một key): dùng `by-column` (đã chạy, `onMissing=none` khi giải mã) hoặc tạo path riêng cho từng cột (ví dụ `f014-mpre-tot-charge-d/isdn`).
- **Key global (mã hóa)** nằm ở chỗ khác: theo **vùng** (`zone`, `[a-z0-9_]{1,64}`), không theo đường dẫn. Dùng một vùng chung cho cả 18 bảng, ví dụ `vtt`.
  Vùng cần có phiếu hiệu lực ở DAK ([YEU-CAU-TU-DAK-KEY-GLOBAL.md](../key-access/YEU-CAU-TU-DAK-KEY-GLOBAL.md)).

### 3.3 Hai `keyField` khác nhau, và điều kiện để mọi bảng ra cùng bản mã

Thuật toán của VTT (`TransformDL`) là AES chế độ mặc định của JDK (ECB), **không có IV ngẫu nhiên**, khóa suy ra từ `keyInput = prefix ‖ giá trị keyField`. Vì vậy **cùng giá trị gốc và cùng `keyInput` thì luôn ra cùng một bản mã** (tất định).
Bước 1 dùng thuật toán này ở **hai nơi với hai `keyField` khác nhau**:

| | Giải mã cột VTT (bước 5) | Mã hóa global (bước 7) |
| --- | --- | --- |
| `prefix` | Prefix của VTT, lấy qua `by-path` | Key global của vùng, lấy qua `keys/global` |
| `keyField` | Cột **VTT quy định cho từng bảng** (`sta_date`, `sta_datetime`, `customer_id`, bảng bên dưới) | **Cột hằng số** do job thêm (đề xuất, bên dưới) |
| Thay đổi theo dòng? | Có (đúng như VTT mã hóa) | **Không** |

**`keyField` của VTT theo bảng** (cột `Partner_Column` trong danh mục VTT, đều kiểu `string`):

| Bảng | Cột VTT mã hóa | `keyField` |
| --- | --- | --- |
| `F022_REGISTER` | `isdn` | `sta_date` |
| `f014_mpre_tot_charge_d` | `isdn` | `sta_date` |
| `f015_mpre_tot_charge_d_acc` | `isdn` | `customer_id` |
| `f016_mpre_tot_charge_m` | `isdn` | `customer_id` |
| `f002_mpre_sms` | `calling_number`, `called_number`, `called_number_cl` | `sta_datetime` |
| `f003_mpre_data` | `calling_number` | `sta_datetime` |
| `F_HOME_BTS_DAILY_AREA` | `isdn` | `station_code` |

(`f001_mpre_call.cdrname` có `Partner_Column = sta_datetime` nhưng là "Dữ liệu nội bộ", không mã hóa nên không giải mã.)

- **Không dùng lại `keyField` của VTT để mã hóa global** (giải thích và ví dụ ở mục 3.3.1).
- `CDR_KEY_FIELD` của app hiện **bắt buộc** khi có cột mã hóa, phải là cột có sẵn trong dữ liệu, giữ nguyên giá trị (không mã hóa), **không được là cột lô `dt`**. Các bảng VTT không có cột nào hằng số.
- **Đề xuất:** thêm cho app khả năng dùng một **cột hằng số** do job tự thêm (ví dụ `key_scope` luôn bằng `vtt`) làm `keyField` của bước mã hóa global. Cần sửa `IngestConfig` và `DecryptEncryptSilverApp`.
- Điều kiện để hai bảng ra cùng bản mã cho cùng một số thuê bao: **cùng key global, cùng `keyField` hằng số, và cùng chuỗi giá trị gốc đưa vào mã hóa** (xem `f001` bên dưới).

#### 3.3.1 Vì sao không dùng lại `keyField` của VTT để mã hóa global

`keyField` không phải tham số phụ: nó là **một phần của khóa**. Bản mã = AES(giá trị, khóa suy ra từ `prefix ‖ keyField`), và thuật toán tất định.
Mục tiêu sau bước 1 là **cùng một số thuê bao ra cùng một bản mã ở mọi dòng và mọi bảng**, để bước 2 băm ra cùng một định danh giả và DS đếm, nhóm, nối được. `keyField` của VTT làm điều ngược lại:

- **Trong một bảng nó đổi theo dòng:** `sta_datetime` là thời điểm của từng sự kiện (f002, f003), `customer_id` đổi theo từng khách hàng (f015, f016).
- **Giữa các bảng nó là cột khác nhau:** `sta_date` (F022, f014), `sta_datetime` (f002, f003), `customer_id` (f015, f016).

Ví dụ chạy bằng chính thuật toán VTT (`DataLakeSecurity_jv8.jar`), prefix tự đặt (không phải prefix thật), cùng một số thuê bao `978356099`:

| Nếu mã hóa global bằng… | `keyField` của dòng | Bản mã |
| --- | --- | --- |
| `sta_datetime` của f002, dòng 1 | `20261104103000` | `o4srZ+1j1jL296vVqs3nxA==` |
| `sta_datetime` của f002, dòng 2 | `20261104110000` | `BFVBwzN/zVZcFd5NFE/2Vg==` |
| `sta_date` của f014 | `20261104` | `tlg0qavyuW/F52MorRkOOA==` |
| `customer_id` của f016 | `1234567` | `k1y/p0qPAZLbGP05x+JcdA==` |
| **Cột hằng số `vtt`, ở bất kỳ bảng nào** | `vtt` | `+zLmgZ1RXt1mxnQT9rfKsg==` |

Dùng lại `keyField` của VTT thì một số thuê bao thành **bốn bản mã khác nhau**, tức bốn định danh giả khác nhau sau khi băm: DS thấy bốn "thuê bao", không đếm được số cuộc gọi của một người và không nối được `f002` với `f014`.
Với cột hằng số thì mọi nơi ra **đúng một bản mã**.

Hai bước dùng hai `keyField` khác nhau là có chủ ý: **giải mã bắt buộc dùng đúng `keyField` của VTT** (VTT mã hóa bằng nó, dùng khác thì ra rác), còn **mã hóa lại** là quyền của ta nên chọn `keyField` giúp nối được.
Bản mã tất định theo số thuê bao không làm yếu thêm so với bước 2, vì bước 2 băm đúng cột đó và cũng cần chính tính chất tất định ấy.

**Cột giải mã và mã hóa theo bảng:**

| Bảng | Giải mã (cột VTT đã mã hóa) | Mã hóa global (`CDR_ENCRYPT_COLUMNS`) | Băm ở bước 2 |
| --- | --- | --- | --- |
| `f016_mpre_tot_charge_m` | `isdn` | `isdn`, `customer_id` | `isdn`, `customer_id` |
| `f014_mpre_tot_charge_d` | `isdn` | `isdn`, `customer_id` | `isdn`, `customer_id` |
| `f015_mpre_tot_charge_d_acc` | `isdn` | `isdn`, `customer_id` | `isdn`, `customer_id` |
| `F_HOME_BTS_DAILY_AREA` | `isdn` | `isdn` | `isdn` |
| `F022_REGISTER` | `isdn` | `isdn` | `isdn` |
| `F_CUSTOMER` | không | `cust_id` | `cust_id` |
| `f003_mpre_data` | `calling_number` | `calling_number` | `calling_number` |
| `f001_mpre_call` | **không** (VTT không mã hóa cột này) | `calling_number` | `calling_number` |
| `f002_mpre_sms` | `calling_number`, `called_number`, `called_number_cl` | `calling_number`, `called_number`, `called_number_cl` | `calling_number` |
| 9 bảng `d_*` | không | không | không |

- Cột giải mã lấy từ danh mục dữ liệu của VTT (cột đã phân loại dữ liệu cá nhân và mã hóa sẵn). `customer_id`, `cust_id` **đã xác nhận là số rõ ở nguồn**, chỉ mã hóa.
- `customer_id` vừa là cột cần mã hóa (f014, f016) vừa là `keyField` giải mã `isdn` (f015, f016): **giải mã `isdn` phải chạy trước khi mã hóa `customer_id`** (đúng thứ tự bước 5 rồi bước 7).
- **`f015.customer_id` được thêm vào cột mã hóa và băm** dù DS không khai cột này ở f015 (đã đồng ý). Cột này phải còn ở silver để làm `keyField` giải mã `isdn`; nếu không thêm thì DS thấy mã khách hàng rõ ở f015 trong khi f014, f016 thì băm.
- **Quy tắc chung: cột nào VTT mã hóa mà bước 1 đã giải mã thì phải nằm trong `CDR_ENCRYPT_COLUMNS`.** Nếu giải mã mà không mã hóa lại thì silver và DS nhận **số điện thoại rõ**, tệ hơn lúc VTT còn mã hóa (áp dụng cho `f002_mpre_sms.called_number`, `called_number_cl`).

**`f001_mpre_call.calling_number` (chỉ mã hóa, không giải mã):** muốn băm ra cùng giá trị với `calling_number` của `f002`, `f003` thì sau mã hóa global hai bên phải cùng bản mã, tức **chuỗi số thuê bao rõ phải giống hệt nhau**.
Danh mục ghi `f002.calling_number` là "đã chuẩn hóa"; test vector của VTT dùng số dạng 9 chữ số không có `0` hay `84` đầu (`978356099`). **Đã xác nhận `f001.calling_number` cùng dạng với `f002`, `f003`**, nên không cần chuẩn hóa thêm.
Vẫn nên kiểm tra mẫu sau lần chạy đầu: lấy vài số thuê bao có ở cả `f001` và `f002`, so bản mã sau mã hóa global (phải bằng nhau).

### 3.4 Gom nhóm tuổi, giờ và `bts_name`

Áp dụng sau khi giải mã, trước khi mã hóa. Mọi giá trị pipeline điền vào bảng là **tiếng Anh** (`Under 18`, `65+`, `Unknown`), khác nhãn tiếng Việt trong phiếu (`Dưới 18`, `Từ 65`, `Không xác định`); cần báo DS để họ dùng đúng nhãn khi lọc.

| Bảng | Cột | Quy tắc |
| --- | --- | --- |
| `f014`, `f015`, `f016`, `F_CUSTOMER` | `customer_age` | Gom nhóm tuổi |
| `F022_REGISTER`, `f003`, `f001`, `f002` | `hour_id` | Gom nhóm giờ; `bts_name` (F022, f003) đi theo khung giờ này, giữ từng dòng, không gộp |

**Gom nhóm tuổi (`customer_age`):**

| Giá trị nguồn | Giá trị ghi ra |
| --- | --- |
| 0 đến 17 | `Under 18` |
| 18 đến 24 | `18–24` |
| 25 đến 34 | `25–34` |
| 35 đến 44 | `35–44` |
| 45 đến 54 | `45–54` |
| 55 đến 64 | `55–64` |
| từ 65 trở lên | `65+` |
| null, không phải số, âm, hoặc lớn hơn 120 | `Unknown` |

Dấu gạch giữa là en-dash (`–`), giữ như phiếu. Ngưỡng 120 là quy ước của tôi để loại tuổi vô lý, phiếu không nêu. Kiểu nguồn là `int` ở F_CUSTOMER;
ở f014, f015, f016 chưa xác nhận, nên bước biến đổi phải ép về số trước khi so khung (giá trị không ép được thì `Unknown`).

**Gom nhóm giờ (`hour_id`):**

| Giờ | Giá trị ghi ra |
| --- | --- |
| 0 đến 5 | `0–5h` |
| 6 đến 8 | `6–8h` |
| 9 đến 11 | `9–11h` |
| 12 đến 13 | `12–13h` |
| 14 đến 16 | `14–16h` |
| 17 đến 19 | `17–19h` |
| 20 đến 23 | `20–23h` |
| không phải số 0 đến 23 | `Unknown` |

- `hour_id` có giá trị `0`..`23`, một hoặc hai chữ số, không mã hóa. Có thể dùng lại biểu thức kiểm tra giờ của `HourGroups.SourceHour`.
- **Cột partition `hour` của silver nằm trong bảng mở cho DS (đã chốt).** Cột này lấy từ chính `hour_id` gốc nên chứa giờ chính xác (`00`..`23`): DS vẫn thấy giờ chính xác qua cột `hour`, việc gom nhóm `hour_id` không che được giờ.
- **`bts_name`:** giữ nguyên giá trị, không transform riêng. Mỗi dòng cho biết thuê bao ở trạm nào trong khung giờ nào. Mốc 9h thuộc `9–11h`, mốc 21h thuộc `20–23h`.

### 3.5 Pipeline hiện có đáp ứng được đến đâu

> **Cập nhật:** các mục "Thiếu" dưới đây đã được cài đặt trong `silver-ingest-app` và DAG VTT, xem mục 9.

| Cần | Hiện có | Thiếu |
| --- | --- | --- |
| Lấy key `by-path` (`PathKeySource`) | Có | Dùng được cho prefix của VTT: chỉ cần dùng kết quả làm prefix theo cột thay vì khóa giải mã cả file |
| Giải mã **từng cột** bằng cdr-crypto (`CdrCrypto.decryptColumns`) | Thư viện có sẵn (`cdr-crypto-udf`, bọc `TransformDL`), app chưa gọi | Bước mới trong `IngestJobs`: cấu hình cột giải mã và `keyField` VTT; **không dùng** `FILE_CIPHER` giải mã cả file (`aes-256-gcm` chỉ là giả định, không đúng với VTT) |
| Lấy key global, mã hóa mọi cột bằng một key | Có (`GLOBAL_KEY_ZONE`, `CdrCrypto.encryptColumns`) | `keyField` hằng số (mục 3.3) |
| Gom nhóm tuổi, giờ | Không | Bước biến đổi cột, đặt giữa giải mã và mã hóa |
| DAG cho 18 bảng VTT | Chỉ có `f001-mpre-call`, đang là `passthrough` | Đổi sang `decrypt-encrypt` và thêm 17 bảng vào `DATASETS`, kèm cột giải mã và `keyField` VTT của từng bảng |
| Giải mã `parquet` | Không cần thư mục tạm (giải mã theo cột trên executor) | Hiệu năng cần đo: UDF giải mã chạy trên từng dòng của các cột VTT mã hóa |

### 3.6 Hệ quả cần biết

- **Silver không còn giá trị gốc** của cột bị gom nhóm hay bị mã hóa. Chỉ landing còn giữ. Muốn đổi quy tắc phải nạp lại từ landing.
- **Silver giữ bản mã global**: giải mã lại được bằng key global của vùng, nên key và quyền vùng phải được bảo vệ như dữ liệu gốc.
- Tuổi chính xác vẫn suy ra được từ cột ngày sinh vì cột đó giữ nguyên có chủ ý (mục 5).

## 4. Bước 2: silver → khử định danh

### 4.1 Cột cần băm theo bảng

Một bảng một lần chạy. Chế độ **`HASH`** vì các cột này chỉ dùng để nối và đếm, không cần khôi phục. Đầu vào của băm là **bản mã global của bước 1**.

| Bảng silver | `PSEUDO_COLUMNS` |
| --- | --- |
| `f016_mpre_tot_charge_m` | `isdn:HASH,customer_id:HASH` |
| `f014_mpre_tot_charge_d` | `isdn:HASH,customer_id:HASH` |
| `f015_mpre_tot_charge_d_acc` | `isdn:HASH,customer_id:HASH` (`customer_id` thêm ngoài phiếu DS, mục 3.3) |
| `F_HOME_BTS_DAILY_AREA` | `isdn:HASH` |
| `F022_REGISTER` | `isdn:HASH` |
| `F_CUSTOMER` | `cust_id:HASH` |
| `f003_mpre_data` | `calling_number:HASH` |
| `f001_mpre_call` | `calling_number:HASH` |
| `f002_mpre_sms` | `calling_number:HASH` (`called_number`, `called_number_cl` **không băm**) |
| 9 bảng `d_*` | **`none`** (mục 4.4) |

**9 bảng `d_*` vẫn chạy qua bước 2 nhưng không khử cột nào** (đã chốt), để DS thấy đủ 18 bảng trong cùng một database của phiếu.
`pseudonymize-table-app` hiện **chưa hỗ trợ** danh sách cột rỗng; kế hoạch ở mục 4.4.

Đầu ra: cùng cột, cùng thứ tự với bảng silver. Cột trong `PSEUDO_COLUMNS` thành chuỗi `k1_` + 32 ký tự hex. **Cột kiểu số thành `STRING`.** `NULL` vẫn là `NULL`. Cùng giá trị gốc luôn ra cùng định danh giả.
`HASH` là HMAC-SHA256(key, giá trị), **không phụ thuộc tên cột hay tên bảng**: `isdn` của f014 nối được với `calling_number` của f003 miễn bản mã bước 1 giống nhau.

### 4.2 Cấu hình đọc bảng silver

- Nguồn là bảng: `DB_NAME` + `TABLE_NAME`, đọc theo khoảng ngày bằng `INPUT_FROM`, `INPUT_TO`, `INPUT_HOURS`.
- Silver dùng cột lô **`dt`** và cột giờ **`hour`**, nên đặt `INPUT_DATE_KEY=dt`, `INPUT_HOUR_KEY=hour`, `OUTPUT_PARTITION_BY=dt,hour` (mặc định job dùng `date`, `hour`).
- Một phiếu nhiều bảng chạy **một DAG run mỗi bảng, song song**, `OUTPUT_DB` chung, `OUTPUT_TABLE` mỗi bảng một tên (xem [TRA-LOI-DAK-DEID-NHIEU-BANG.md](../key-access/TRA-LOI-DAK-DEID-NHIEU-BANG.md)).

### 4.3 Điều kiện để các bảng vẫn join được với nhau

1. **Cùng một khóa băm cho mọi bảng: đã thỏa.** Mọi bảng của **cùng một phiếu** dùng chung **một** key (đã kiểm chứng bằng test phiếu nhiều bảng trên dak;
   xem [TRA-LOI-DAK-DEID-NHIEU-BANG.md](../key-access/TRA-LOI-DAK-DEID-NHIEU-BANG.md) mục 5). Vì vậy **cả 18 bảng phải nằm trong cùng một phiếu**.
2. **Cùng giá trị đầu vào trước khi băm.** Sau bước 1 đầu vào là bản mã global, nên điều kiện này **chuyển thành điều kiện ở mục 3.3** (cùng key global, cùng `keyField` hằng số, cùng chuỗi giá trị gốc đưa vào mã hóa).
   Định dạng `isdn`, `calling_number` đã thống nhất giữa các bảng, **nhưng riêng `f001.calling_number` cần kiểm tra lại** vì cột này VTT không mã hóa (mục 3.3).

### 4.4 Kế hoạch: cho phép khử định danh với danh sách cột rỗng

**Mục tiêu:** `pseudonymize-table-app` chạy được với bảng không có cột nào cần khử, ghi bảng đích giống hệt bảng nguồn, không đụng tới khóa.

**Hiện trạng đã kiểm tra trong code:**

| Chỗ | Hiện trạng | Có chặn không |
| --- | --- | --- |
| `JobConfig.scala:394-396` | `PSEUDO_COLUMNS` rỗng là lỗi cấu hình, job thoát exit 2 | **Chặn** |
| `PseudonymizeTableJob.scala:224, 230` | `cfg.pseudoColumns.head.name` làm cột neo cho `FileTracker` (chỉ khi `INPUT_BAD_FILES=skip` với nguồn file) | Chặn nếu dùng `skip`; không chạm ở đường DAK (nguồn bảng) |
| `Pseudonymizer.pseudonymize` | Duyệt `policy.columns` bằng `foldLeft`, rỗng thì trả nguyên DataFrame | Không |
| Khóa DAK | `DakDeidKeyProvider` lấy khóa theo kiểu lazy, nên không gọi `GET /api/v1/keys/deid` nếu không có cột | Không (cần test xác nhận) |
| `verify` | Lặp trên `cfg.columns`, rỗng thì chỉ so số dòng | Không |
| DAG `sample_dag_deid_pseudonymize_table.py:137` | Regex bắt buộc ít nhất một `tên:HASH`; `pseudoColumns` là trường bắt buộc của `conf` | **Chặn** |
| dak-mock `ManifestRenderer.pseudoColumns` | Dựng chuỗi từ danh sách cột của phiếu | Cần xem khi phiếu có bảng không cột |

**Thay đổi đề xuất:**

1. **Cấu hình (đã chốt):** chấp nhận `PSEUDO_COLUMNS=none` (từ khóa tường minh) thành danh sách rỗng. **Không** chấp nhận để trống hoặc không đặt: quên đặt cột cho bảng nhạy cảm thì job chép bảng gốc ra mà không ai biết.
2. **Cột neo của `FileTracker`:** từ chối `PSEUDO_COLUMNS=none` + `INPUT_BAD_FILES=skip` ngay ở bước kiểm tra cấu hình.
3. **Log:** dòng `pseudonymized columns` in `(none)`, thêm WARN rõ ràng khi chạy không có cột.
4. **Khóa:** với `none` không lấy khóa. Có test khẳng định `CRYPTO_PREFIX_SOURCE=dak` mà không có cột thì không có lời gọi HTTP nào tới DAK.
5. **DAG và DAK:** DAG nhận `pseudoColumns=none`; bảng `JOB_ENV` phải khớp `dak.deid.job-env` của dak-mock (có test chống lệch).
   Đề nghị gửi dak: [YEU-CAU-GUI-DAK-PSEUDOCOLUMNS-NONE.md](../key-access/YEU-CAU-GUI-DAK-PSEUDOCOLUMNS-NONE.md).

**Test (viết trước khi sửa, theo TDD):**

- `JobConfigSpec`: `none` hợp lệ; thiếu hoặc rỗng vẫn lỗi; `none` kèm `INPUT_BAD_FILES=skip` bị từ chối; `none` lẫn cột khác (`isdn,none`) bị từ chối.
- `PseudonymizeTableAppSpec`: chạy `none` không cấu hình khóa nào. Bảng đích có cùng schema, kiểu, số dòng, giá trị; có partition `dt,hour` thì ghi theo partition như thường.
- `test_deid_dag.py`: `pseudoColumns=none` qua `validate_conf`; chuỗi sai vẫn bị từ chối.

**Tài liệu cần cập nhật:** `pseudonymize-table-app/README.md` (bảng biến mục 3.1, mục 3.6).
**Rủi ro:** `none` đặt nhầm cho bảng có cột nhạy cảm thì bảng gốc được chép nguyên; giảm bằng từ khóa tường minh, WARN, và việc dak quyết định danh sách cột từ phiếu.

## 5. Cột lấy nguyên giá trị: đã xác nhận có chủ ý

Phiếu để trống các cột này, bước 1 không đổi và bước 2 chỉ xử lý cột "Mã hóa", nên DS nhận **giá trị gốc**. Đã xác nhận đây là chủ ý:

| Bảng | Cột |
| --- | --- |
| `F_CUSTOMER` | `name`, `birth_date`, `id_no`, `email`, `address`, `street`, `street_name`, `street_block`, `street_block_name`, `home`, `nationality` |
| `f014`, `f015`, `f016` | `sub_id`, `user_name` (không có ở f015), `birth_day`, `imei_use`, `home_bts` |
| `F022_REGISTER` | `imsi`, `imei` |
| `f003_mpre_data` | `called_number`, `callednumber`, `calledimei`, `callingimei` |
| `f001_mpre_call` | `called_number` |

`f002_mpre_sms.called_number` và `called_number_cl` **không** nằm trong bảng này: chúng mã hóa ở bước 1 nên DS nhận bản mã global (không băm).
Hai hệ quả cần ghi nhớ:

1. `birth_date` / `birth_day` để nguyên nên tuổi chính xác suy ra được, việc gom nhóm `customer_age` không có tác dụng che tuổi.
2. `called_number` của `f001`, `f003` để nguyên (số rõ) trong khi của `f002` là bản mã: ba bảng không nhất quán với nhau về số bị gọi.

Dữ liệu giả danh vẫn là dữ liệu cá nhân: các cột định danh gián tiếp (ngày sinh, địa giới, giới tính, quốc tịch) vẫn còn.

## 6. Quyết định đã chốt

| Vấn đề | Quyết định |
| --- | --- |
| Định dạng `isdn`, `calling_number` giữa các bảng | Đã thống nhất, không cần bước chuẩn hóa |
| `bts_name` theo khung giờ | Giữ từng dòng, không gộp |
| Datamon | Bỏ khỏi danh mục khai thác VTT |
| Cột partition giờ của silver | Nằm trong bảng mở cho DS |
| 9 bảng `d_*` | Vẫn chạy `pseudonymize` với `PSEUDO_COLUMNS=none` |
| Ký hiệu danh sách cột rỗng | Từ khóa tường minh `none`; để trống hoặc thiếu vẫn là lỗi |
| Tên cột partition của silver | `dt` và `hour` |
| Cột nguyên giá trị ở mục 5 | Có chủ ý |
| Hai phiếu có chung khóa không | Không cần quan tâm; cả 18 bảng ở một phiếu |
| `nationality` | Bỏ quy tắc ngưỡng, ghi nguyên giá trị |
| Ngôn ngữ giá trị điền vào | Tiếng Anh: `Under 18`, `18–24`, …, `65+`, `Unknown` |
| `f002.called_number`, `called_number_cl` | **Không băm** ở bước 2; vẫn mã hóa global ở bước 1 |
| Bước 1 của VTT | Không còn `passthrough`: giải mã từng cột VTT, mã hóa lại bằng một key global |
| Thuật toán giải mã VTT | `cdr-crypto` (`DataLakeSecurity_jv8.jar`), **theo từng cột**, `keyInput = prefix ‖ keyField` |
| Nơi lấy prefix của VTT | `by-path` của DAK (một `path` một prefix) |
| Prefix VTT theo bảng hay theo cột | Theo bảng; admin DAK tự nhập key vào path của bảng |
| `keyField` để giải mã | Theo bảng, lấy từ danh mục VTT (`Partner_Column`): `sta_date`, `sta_datetime`, `customer_id` (mục 3.3) |
| `keyField` để mã hóa global | Cột hằng số do job thêm (đã đồng ý); không dùng lại `keyField` của VTT (mục 3.3.1) |
| Dạng số thuê bao của `f001.calling_number` | Giống `f002`, `f003` |
| `customer_id`, `cust_id` ở nguồn | Số rõ |
| `F_HOME_BTS_DAILY_AREA.isdn` | VTT có mã hóa, `keyField` là `station_code`: bước 1 giải mã rồi mã hóa lại |
| `f015.customer_id` | Thêm vào cột mã hóa và băm (ngoài phiếu DS) để không lộ mã khách hàng rõ |
| `f001.calling_number` | VTT không mã hóa: bước 1 **chỉ mã hóa** (không giải mã), để băm ra cùng giá trị với `f002`, `f003` |

## 7. Câu hỏi còn mở

1. **Giá trị thật của `keyField` trong parquet** (`sta_date`, `sta_datetime`, `station_code`, `customer_id`). Đã biết từ `f001`: mọi cột đều là `string` (mục 8), nên không lo ép `date` hoặc `timestamp`. **Còn phải xem giá trị**: `sta_date` có đúng 8 chữ số, `sta_datetime` có đúng 14 chữ số như test vector của VTT (`20260928123456`) không. `keyInput` ghép **chuỗi thô**, lệch một ký tự là giải mã ra rác. Cũng cần quy ước khi `keyField` là `null`. (mục 3.1)
2. **Kiểu của `customer_age` ở f014, f015, f016:** rất có thể là `string` (ở `f001` mọi cột là `string`, kể cả số tiền và thời lượng). Bước biến đổi đã ép về số nên không chặn; kiểm tra lại trên f014 bằng mục 8. (mục 3.4)
3. **Đội dak:** phiếu có cho khai bảng không có cột khử định danh, và gửi `pseudoColumns=none` được không? Chờ dak trả lời. (mục 4.4)

## 8. Phụ lục: SQL kiểm tra file parquet landing trên sql-engine

sql-engine là Spark Thrift Server, đọc thẳng file theo đường dẫn bằng cú pháp ``parquet.`<đường dẫn>` ``. Đường dẫn dùng URI HDFS đang dùng trong DAG (`silver_ingest_common.HDFS`):
`hdfs://vailakehouse.datalakehla.viettel.com.vn:8020`.

- **Thư mục ngày của `f014`:** `hdfs://vailakehouse.datalakehla.viettel.com.vn:8020/landing/vtt/lakehouse/vtt-core/f014-mpre-tot-charge-d/date=20261104/` (215 file).
- **Một file mẫu** (đọc nhẹ hơn nhiều, nên dùng cho lần kiểm tra đầu; chép tên file từ `hdfs dfs -ls`, ví dụ trong ảnh HDFS):
  `hdfs://vailakehouse.datalakehla.viettel.com.vn:8020/landing/vtt/lakehouse/vtt-core/f014-mpre-tot-charge-d/date=20261104/result_00e21ccc-3ae1-460f-b953-d1caf3601b16.parquet`
- **Bảng khác:** thư mục theo mẫu `.../vtt-core/<tên-bảng-gạch-ngang>/date=YYYYMMDD/`. Chỉ `f014-mpre-tot-charge-d` đã xác nhận qua ảnh; tên thư mục của bảng còn lại xem bằng `hdfs dfs -ls /landing/vtt/lakehouse/vtt-core/`.

**1. Tên cột và kiểu của toàn bộ file** (xem `sta_date`, `isdn`, `customer_id`, `customer_age` là `string` hay kiểu khác):

```sql
DESCRIBE QUERY
SELECT * FROM parquet.`hdfs://vailakehouse.datalakehla.viettel.com.vn:8020/landing/vtt/lakehouse/vtt-core/f014-mpre-tot-charge-d/date=20261104/result_00e21ccc-3ae1-460f-b953-d1caf3601b16.parquet`;
```

**2. Kiểu thực và giá trị mẫu của cột mã hóa và `keyField`** (đổi tên cột theo bảng ở mục 3.3):

```sql
SELECT typeof(isdn) AS isdn_type, isdn,
       typeof(sta_date) AS sta_date_type, sta_date,
       typeof(customer_id) AS customer_id_type, customer_id,
       typeof(customer_age) AS customer_age_type, customer_age
FROM parquet.`hdfs://vailakehouse.datalakehla.viettel.com.vn:8020/landing/vtt/lakehouse/vtt-core/f014-mpre-tot-charge-d/date=20261104/result_00e21ccc-3ae1-460f-b953-d1caf3601b16.parquet`
LIMIT 10;
```

**3. Định dạng và `null` của `keyField`** (phải toàn chuỗi đúng dạng VTT dùng; `sta_date` là 8 chữ số, `sta_datetime` là 14 chữ số):

```sql
SELECT length(sta_date) AS len,
       count(*) AS cnt,
       count(sta_date) AS non_null,
       sum(CASE WHEN sta_date RLIKE '^[0-9]{8}$' THEN 1 ELSE 0 END) AS match_yyyymmdd
FROM parquet.`hdfs://vailakehouse.datalakehla.viettel.com.vn:8020/landing/vtt/lakehouse/vtt-core/f014-mpre-tot-charge-d/date=20261104/result_00e21ccc-3ae1-460f-b953-d1caf3601b16.parquet`
GROUP BY length(sta_date);
```

Với bảng dùng `sta_datetime`, đổi cột và đổi mẫu thành `'^[0-9]{14}$'`. Với `station_code` và `customer_id` chỉ xem `typeof`, `length`, `non_null` (không có mẫu cố định).

### 8.1 Kết quả đã chạy trên `f001-mpre-call` (`date=20261105`)

- **File parquet đọc thẳng được trên sql-engine** (`DESCRIBE QUERY` trả về tên cột và kiểu). Vậy file **không bị mã hóa cả file**, đúng như giả thiết mã hóa theo từng cột: không cần giải mã file, không cần `DECRYPT_STAGING_PATH`.
- **Mọi cột đều là `string`**, kể cả những cột trông là số (`tot_cost`, `voice_consume`, `remain_voice`, `sta_datetime`, `hour_id`...). Hệ quả: gom nhóm giờ và tuổi phải ép chuỗi sang số; `keyField` không có rủi ro `date`/`timestamp`.
- **`f001` có `calling_number`, `called_number`, `hour_id`, `sta_datetime`, `cdrname`** (khớp doc) và **không có** `isdn`, `sta_date`, `customer_id`, `customer_age`. Ba câu SQL ở trên viết cho `f014`; chạy trên `f001` báo `UNRESOLVED_COLUMN` là đúng. Mỗi bảng chỉ có một số cột:

| Bảng | Cột dùng trong câu kiểm tra |
| --- | --- |
| `f001_mpre_call` | `calling_number`, `hour_id`, `sta_datetime` |
| `f002_mpre_sms`, `f003_mpre_data` | `calling_number` (bản mã VTT), `sta_datetime`, `hour_id` |
| `f014_mpre_tot_charge_d` | `isdn`, `sta_date`, `customer_id`, `customer_age` |
| `f015_mpre_tot_charge_d_acc`, `f016_mpre_tot_charge_m` | `isdn`, `customer_id`, `customer_age` |
| `F022_REGISTER` | `isdn`, `sta_date`, `hour_id` |
| `F_HOME_BTS_DAILY_AREA` | `isdn`, `station_code` |

**Câu kiểm tra dành cho `f001`:**

```sql
-- Dạng số thuê bao (phải là số rõ, cùng dạng f002/f003 sau giải mã): độ dài và 2 ký tự đầu
SELECT length(calling_number) AS len, substr(calling_number, 1, 2) AS head2, count(*) AS cnt
FROM parquet.`/landing/vtt/lakehouse/vtt-core/f001-mpre-call/date=20261105/result_0baf9f8f-7e63-45fb-a051-aeeefc2d04c0.parquet`
GROUP BY length(calling_number), substr(calling_number, 1, 2) ORDER BY cnt DESC LIMIT 20;

-- Giá trị giờ thật (để viết đúng quy tắc gom nhóm giờ)
SELECT hour_id, count(*) AS cnt
FROM parquet.`/landing/vtt/lakehouse/vtt-core/f001-mpre-call/date=20261105/result_0baf9f8f-7e63-45fb-a051-aeeefc2d04c0.parquet`
GROUP BY hour_id ORDER BY hour_id;

-- Định dạng sta_datetime (kỳ vọng 14 chữ số; cột này là keyField của f002, f003)
SELECT length(sta_datetime) AS len, count(*) AS cnt, count(sta_datetime) AS non_null,
       sum(CASE WHEN sta_datetime RLIKE '^[0-9]{14}$' THEN 1 ELSE 0 END) AS match_14
FROM parquet.`/landing/vtt/lakehouse/vtt-core/f001-mpre-call/date=20261105/result_0baf9f8f-7e63-45fb-a051-aeeefc2d04c0.parquet`
GROUP BY length(sta_datetime);
```

**Nếu sql-engine báo lỗi quyền hoặc "Table or view not found":** user chạy truy vấn cần quyền đọc đường dẫn HDFS (Ranger hoặc quyền HDFS), hoặc tạo bảng tạm trỏ vào thư mục:
`CREATE TABLE <db>.tmp_f014_check USING parquet LOCATION '<thư mục ngày>'`, rồi dùng `<db>.tmp_f014_check` thay cho ``parquet.`...` ``.

## 9. Trạng thái triển khai (bước 1: silver-ingest)

**Đã cài đặt và có test** (`silver-ingest-app`: 116 test qua; `airflow/tests`: 167 test qua):

| Phần | Nơi | Ghi chú |
| --- | --- | --- |
| `DECRYPT_MODE` = `file` (mặc định, như VTNET), `column`, `none` | `IngestConfig.scala`, `IngestJobs.scala` | `column`: key `by-path` là prefix, giải mã bằng `CdrCrypto.decryptColumns`; chưa có key là **lỗi**. `none`: không gọi `by-path` |
| `PROVIDER_DECRYPT_COLUMNS`, `PROVIDER_KEY_FIELD` | như trên | `keyField` null ở dòng có cột cần giải mã thì job **dừng** (cdr-crypto sẽ âm thầm trả NULL) |
| `CDR_KEY_FIELD_VALUE` | như trên | `CDR_KEY_FIELD` là cột hằng số do job thêm; cột trùng tên là lỗi |
| `COLUMN_TRANSFORMS` = `AGE_BAND`, `HOUR_BAND` | `ColumnTransforms.scala` | Nhãn tiếng Anh như mục 3.4; chạy sau giải mã, trước mã hoá; cột giờ `hour` vẫn lấy từ `hour_id` gốc |
| 18 DAG VTT (9 `decrypt-encrypt`, 9 `passthrough`) | `airflow/dags/silver_ingest_vtt.py` | Cột giải mã, cột mã hoá, `keyField` và transform của từng bảng đúng mục 3.3 và 3.4 |
| Phần chung kết nối DAK và audit | `silver_ingest_common.py` (`dak_connection_env`, `audit_conf`) | VTNET dùng lại, kết quả không đổi |
| Landing là parquet | `BASE_ENV` của DAG VTT | Đọc nguyên trạng, không `INPUT_SCHEMA`, không header |

Chuỗi `path` trên DAK của mỗi bảng chính là tên dataset (`f014-mpre-tot-charge-d`), cố định cho mọi ngày. `f002` và `f003` dùng `key_field` `sta_datetime`; mọi bảng mã hoá dùng cùng cột hằng số `key_scope = vtt`.

**Nạp lần đầu (từ 04/11) và chạy định kỳ:** nạp lần đầu là một **bulk load** được yêu cầu rõ, không tự động: trigger DAG của bảng với conf `{"fromDate": "20261104", "toDate": "20261122"}`
(một dag run, một Spark app, lần lượt từng ngày, ngày đã chạy thử chỉ bị thay đúng partition của ngày đó). Sau đó DAG chạy định kỳ hằng ngày như thường; bật DAG hay duyệt phiếu **không** tự nạp ngày cũ.
`f016` (bảng tháng) có `periodic: false`: không có lịch, chỉ chạy tay hoặc bulk. Chi tiết: [BACKFILL.md](BACKFILL.md). Bước 2 (khử định danh) nhận khoảng ngày sẵn qua `INPUT_FROM` và `INPUT_TO` của phiếu.

**Còn lại, ngoài code của job:**

1. **Đội dak (hợp đồng phiếu VTT):** với bảng VTT mã hoá, `columns` do dataset cố định nên phiếu **chỉ ghi `globalKey: true` và `globalZone`**, không ghi `encryptColumns` và `keyField`
   (DAG từ chối hai khoá này). `dak-mock` hiện chưa theo hợp đồng này: `SilverRequestParser` đòi `encryptColumns` không rỗng khi có `globalZone`, và phiếu VTT có cột bị từ chối (`invalid-vtt-with-columns.json`).
2. **Admin DAK nhập prefix của VTT** vào đúng chuỗi `path` của 7 bảng `column` (tên dataset, phân biệt hoa thường).
3. **Tên thư mục landing** của các bảng ngoài `f001-mpre-call`, `f014-mpre-tot-charge-d` là **giả định** (`f022-register`, `f-home-bts-daily-area`, `f-customer`, `d-*`, `f015…`, `f016…`): kiểm tra bằng `hdfs dfs -ls`.
   Bảng `f016` là bảng tháng, có thể không có thư mục `date=` theo ngày.
4. **Kiểm thử với dữ liệu thật** trước khi bật phiếu: chạy `f002` và `f001` cùng một ngày, rồi kiểm tra cùng một số thuê bao ra **cùng bản mã** ở `f001.calling_number` và `f002.calling_number`.
5. **Hiệu năng chưa đo:** `CdrCipherCore.encrypt` tạo `TransformDL` và dùng reflection cho **từng giá trị**; một ngày VTT khoảng 165 GB. Đặt `resources` của từng bảng sau khi đo.
6. **Bước 2 đã cài đặt** (mục 4.4): `pseudonymize-table-app` nhận `PSEUDO_COLUMNS=none` (96 test qua), DAG `deid_pseudonymize_table` nhận `pseudoColumns=none` (`airflow/tests`: 169 test qua).
   Cần đội dak xác nhận và gửi `none` (xem [YEU-CAU-GUI-DAK-PSEUDOCOLUMNS-NONE.md](../key-access/YEU-CAU-GUI-DAK-PSEUDOCOLUMNS-NONE.md)); `dak-mock` (`ManifestRenderer.pseudoColumns`) chưa theo hợp đồng này. DAG khử định danh đã đặt sẵn `INPUT_DATE_KEY=dt`, `INPUT_HOUR_KEY=hour` cho bảng silver (`SOURCE_PARTITION_ENV`).
