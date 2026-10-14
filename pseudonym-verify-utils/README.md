# pseudonym-verify-utils

Module **độc lập** để kiểm tra khử định danh, hai cách dùng:

- **`validate`** — so bảng nguồn với bảng đã khử định danh theo các cột đã giả danh (dùng sau
  [`pseudonymize-table-app`](../pseudonymize-table-app/README.md)); không cần khoá.
- **`selftest`** — tự sinh dữ liệu có đáp án, giả danh bằng `pseudonym-udf` với khoá tạm trong bộ nhớ rồi kiểm; nghiệm thu lib
  mà không cần bảng thật.

Không phải Spark application: không Docker, không manifest K8s.

> Thay cho `pseudonym-verify-app` cũ (bộ check U/P đầy đủ, mutation, guard, K8s). Bản cũ còn trong lịch sử git.

## 1. Kiểm những gì

Với mỗi cột trong `COLUMNS`:

| Phép kiểm             | Đạt khi                                                                                            |
| --------------------- | -------------------------------------------------------------------------------------------------- |
| null giữ nguyên       | số `NULL` nguồn == đích                                                                            |
| ánh xạ 1-1            | số giá trị khác nhau nguồn == đích (cùng người → cùng định danh giả, khác người → khác)            |
| đúng định dạng chế độ | mọi giá trị đích khớp `k<v>_<32 hex>` (HASH), `e<v>_<base64url>` (ENCRYPT), `t_<32 hex>` (MAPPING) |
| không sót giá trị gốc | không giá trị nào của đích còn nằm trong tập giá trị của nguồn                                     |

Toàn bảng: cùng tên cột, cùng số dòng, và các cột **không** giả danh không đổi (so đa tập `exceptAll` hai chiều).

Giả định bảng đích là kết quả của đúng bảng nguồn (`OUTPUT_MODE=overwrite`); không dùng cho bảng đích đã `append`.

`validate` không kiểm: độ mạnh của khoá, tấn công từ điển, tính 1-1 theo từng dòng (bảng thật không có khoá dòng để
ghép) — các điều đó do `selftest` (mục 1.1) và test của lib `pseudonym-udf` bảo đảm.

### 1.1. `selftest`

Dữ liệu: `row_id, customer_id, isdn, imei, plan, data_mb`, mỗi chủ thể nhiều dòng, ~5% NULL ở cột định danh. Ngoài các phép
kiểm ở mục 1 (nguồn ↔ đích), vì có khoá dòng `row_id` nên kiểm thêm, cho từng cột định danh:

| Phép kiểm                                | Đạt khi                                                   |
| ---------------------------------------- | --------------------------------------------------------- |
| mỗi giá trị gốc ↔ đúng 1 định danh giả   | cùng người → cùng định danh giả ở mọi dòng                |
| không 2 giá trị gốc cùng 1 định danh giả | khác người → khác định danh giả                           |
| tấn công từ điển SHA-256 không khoá      | băm không khoá mọi giá trị gốc rồi tìm trong đích: khớp 0 |
| chạy lại cùng khoá                       | kết quả y hệt (tất định)                                  |
| khoá khác                                | không dòng nào ra cùng định danh giả                      |

Hỗ trợ `HASH` và `ENCRYPT`. `MAPPING` cần bảng ánh xạ Delta nên không có trong selftest.

## 2. Build

```bash
mvn -B -pl pseudonym-verify-utils -am clean package    # 11 test; -> pseudonym-verify-utils/target/pseudonym-verify-utils.jar
```

Jar mỏng. `pseudonym-udf`, `key-prefix-lib`, Tink, Spark, Scala đều `provided` (chạy với `--jars` như mục 3).

## 3. Chạy

CLI:

```
PseudonymValidateApp <source_db.table> <target_db.table> <col1,col2,...> [HASH|ENCRYPT|MAPPING]
PseudonymValidateApp selftest [HASH|ENCRYPT] [rows]
```

```bash
spark-submit \
  --class vai.lakehouse.pseudonymverify.app.PseudonymValidateApp \
  --jars pseudonym-udf-1.0-SNAPSHOT.jar,key-prefix-lib-1.0-SNAPSHOT.jar \
  pseudonym-verify-utils.jar \
  crm.subscriber crm.subscriber_pseudonymized isdn,imei HASH

# nghiệm thu lib bằng dữ liệu tự sinh (không cần bảng, không cần khoá)
spark-submit --class vai.lakehouse.pseudonymverify.app.PseudonymValidateApp \
  --jars pseudonym-udf-1.0-SNAPSHOT.jar,key-prefix-lib-1.0-SNAPSHOT.jar \
  pseudonym-verify-utils.jar selftest ENCRYPT 5000
```

Chạy trên cụm có Hive metastore/Kerberos thì dùng cùng `--conf` như `pseudonymize-table-app` (HDFS, `spark.sql.warehouse.dir`,
Delta nếu bảng là Delta). Exit code (cả hai lệnh): `0` PASS, `1` FAIL, `2` sai tham số hoặc lỗi khi chạy.

Ví dụ output:

```
[PASS] cột giả danh tồn tại ở cả hai bảng: isdn, imei
[PASS] tên cột nguồn == đích: nguồn=id,isdn,imei,plan đích=id,isdn,imei,plan
[PASS] số dòng nguồn == đích: 10 / 10
[PASS] cột không giả danh giữ nguyên: 2 cột, số dòng khác biệt=0
[PASS] isdn: null giữ nguyên: 0 -> 0
[PASS] isdn: ánh xạ 1-1 (số giá trị khác nhau giữ nguyên): 9 -> 9
[PASS] isdn: đúng định dạng HASH: giá trị sai định dạng=0
[PASS] isdn: không sót giá trị gốc: giá trị gốc còn trong đích=0
...
KẾT LUẬN: PASS
```

## 4. Dùng như thư viện

```scala
import vai.lakehouse.pseudonym.PseudonymMode
import vai.lakehouse.pseudonymverify.validate.PseudonymValidator

val report = PseudonymValidator.validate(spark.table("crm.subscriber"),
  spark.table("crm.subscriber_pseudonymized"), Seq("isdn", "imei"), PseudonymMode.Hash)
println(report.render)
require(report.passed)
```

`report.results` là danh sách `CheckResult(name, passed, detail)`; thiếu cột/lệch schema thì chỉ trả các phép kiểm cấu trúc
(không ném exception).
