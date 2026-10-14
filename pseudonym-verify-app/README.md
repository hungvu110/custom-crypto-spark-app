# pseudonym-verify-app

Spark job sinh dữ liệu viễn thông **tổng hợp có đáp án** (biết chính xác dòng nào thuộc chủ thể nào), giả danh hoá bằng
lib [`pseudonym-udf`](../pseudonym-udf), rồi chạy bộ check đo được để trả lời **có/không**: _đã khử định danh thành
công chưa_. Kết quả là verdict `PASS` / `FAIL` / `ERROR`, file `report.json` và exit code.

- Plan lib + quyết định thiết kế: [docs/PSEUDONYMIZATION_UDF_PLAN.md](../docs/PSEUDONYMIZATION_UDF_PLAN.md)
- Plan app + định nghĩa "thành công" + danh sách check: [docs/pseudonym-verify-app/PLAN.md](../docs/pseudonym-verify-app/PLAN.md)

## 1. Cấu trúc thư mục

```
pseudonym-verify-app/
├── README.md
├── Dockerfile                        # build context = GỐC REPO (cần jar của pseudonym-udf + key-prefix-lib)
├── pom.xml                           # lib = provided (nạp qua spark.jars); shade Delta + patch block token
├── scripts/
│   └── run-local.sh                  # chạy main ở máy local (build + classpath + add-opens + tóm tắt kết quả)
├── k8s/
│   ├── pseudonym-verify-full.yaml    # RUN_MODE=full: nghiệm thu lib
│   ├── pseudonym-verify-selftest.yaml# RUN_MODE=selftest: kiểm chứng chính bộ check
│   └── pseudonym-keys-secret.yaml    # mẫu Secret khoá (KEY_SOURCE=file) — chỉ có chỗ trống
└── src/
    ├── main/resources/{demo-policy.yaml, log4j2.properties}
    ├── main/scala/vai/lakehouse/pseudonymverify/
    │   ├── app/        PseudoVerifyApp (main mỏng: execute -> exit code), SparkSessions
    │   ├── config/     VerifyConfig (đọc + validate env, gom mọi lỗi 1 lượt), RunMode
    │   ├── data/       SyntheticData (4 bảng + đáp án), IdentityFormats (hoán vị song ánh)
    │   ├── check/      Check, CheckSupport, LinkabilityChecks (U1–U12), PrivacyChecks (P1–P7),
    │   │               UseCaseMetrics (MyViettel/BTS/Viettel++), AttackDictionary (P4)
    │   ├── job/        VerifyJob (trait) + FullJob / SelftestJob / GuardJob, VerifySession, Pipeline, Mutations (M1–M10)
    │   ├── report/     Verdict, ReportWriter, Preview (in dữ liệu gốc + sau giả danh)
    │   ├── key/        KeyMaterial (khoá ephemeral hoặc qua key-prefix-lib)
    │   └── logging/    Log (logger "VLP", giữ dòng log cho check P7)
    └── test/scala/...  22 test: config, dữ liệu, verdict, main (execute), END-TO-END cả 3 RUN_MODE
```

## 2. Build

Yêu cầu: JDK 11+ (đã kiểm chứng trên JDK 21), Maven 3.8+. Chạy ở **gốc repo**; lần build đầu cần mạng (hoặc Nexus nội bộ, xem
[`internal-repo/README.md`](../internal-repo/README.md)) để tải dependency.

```bash
# Build + test cả app và các module nó phụ thuộc (key-prefix-lib, pseudonym-udf, spark-app -> jar block-token-patch)
mvn -B -pl pseudonym-verify-app -am clean package

# Chỉ cần jar, bỏ test (nhanh hơn; vẫn compile cả test code)
mvn -B -pl pseudonym-verify-app -am clean package -DskipTests
```

| Artifact                                                | Nội dung                                                              | Kích thước |
| ------------------------------------------------------- | --------------------------------------------------------------------- | ---------- |
| `pseudonym-verify-app/target/pseudonym-verify-app.jar`  | Fat jar: app + Delta + patch block token. **Không** chứa lib giả danh | ~6.5 MB    |
| `pseudonym-udf/target/pseudonym-udf-1.0-SNAPSHOT.jar`   | Lib giả danh (jar thuần)                                              | ~100 KB    |
| `key-prefix-lib/target/key-prefix-lib-1.0-SNAPSHOT.jar` | Hạ tầng lấy khoá                                                      | nhỏ        |

Lib là jar **rời** có chủ đích: trên cluster nạp qua `spark.jars` (UDF thật nên executor cũng cần), đúng cách một job
production dùng lib. Kiểm tra fat jar:

```bash
J=pseudonym-verify-app/target/pseudonym-verify-app.jar
unzip -p $J META-INF/MANIFEST.MF | grep Main-Class                  # -> vai.lakehouse.pseudonymverify.app.PseudoVerifyApp
unzip -p $J META-INF/services/org.apache.hadoop.security.token.TokenIdentifier   # -> vai.lakehouse.hdfs.LenientBlockTokenIdentifier
unzip -p $J META-INF/services/org.apache.spark.sql.sources.DataSourceRegister | grep delta
unzip -l $J | grep -c 'vai/lakehouse/pseudonym/'                    # -> 0 (lib KHÔNG nằm trong fat jar)
```

Build image (chỉ khi chạy trên K8s; build context là **gốc repo**):

```bash
IMAGE=hub.vtcc.vn:8989/hungvt0110/pseudonym-verify-app:v0.1
docker build -f pseudonym-verify-app/Dockerfile -t "$IMAGE" .
docker push "$IMAGE"
```

## 3. Chạy

### 3.1. Test (không cần cluster, không cần Vault/HDFS)

```bash
mvn -B -pl pseudonym-verify-app -am test                                                  # toàn bộ test (kèm các module phụ thuộc)
mvn -B -pl pseudonym-verify-app -am test -DwildcardSuites=vai.lakehouse.pseudonymverify   # chỉ test của app
mvn -B -pl pseudonym-verify-app -am test -DwildcardSuites=vai.lakehouse.pseudonymverify.job   # chỉ EndToEndSpec
```

| Spec                  | Kiểm gì                                                                                                       |
| --------------------- | ------------------------------------------------------------------------------------------------------------- |
| `VerifyConfigSpec`    | Mặc định (MAPPING tắt, khoá ephemeral), gom mọi lỗi cấu hình 1 lượt, bảng ánh xạ phải nằm ngoài `output/`     |
| `SyntheticDataSpec`   | Hoán vị song ánh, tất định theo `SEED`, `NULL_RATE`, tách đáp án khỏi dữ liệu thô                             |
| `VerdictSpec`         | Quy tắc `PASS` / `FAIL` / `ERROR` và exit code 0/1/2                                                          |
| `PseudoVerifyAppSpec` | `main` (`execute`): cấu hình sai → exit 2; chạy `full` nhỏ → exit 0                                           |
| `EndToEndSpec`        | `full` cả 3 chế độ → `PASS`; `selftest` → 10/10 mutation bị bắt; `guard` trên parquet + khoá từ file → `PASS` |

Test chạy ~3 phút, chủ yếu là `EndToEndSpec`. Log có dòng `FAILED_EXECUTE_UDF` / `Tag mismatch` là **bình thường** (test cố ý
dùng sai khoá để kiểm lỗi được báo đúng).

### 3.2. Chạy `main` ở máy local (không cần spark-submit)

Cách nhanh nhất — script lo build, classpath, `--add-opens` (JDK 17+) và tóm tắt kết quả:

```bash
pseudonym-verify-app/scripts/run-local.sh                 # full, HASH+ENCRYPT, 2000 chủ thể -> exit code 0/1/2
```

Cấu hình bằng chính các biến môi trường của app (mục 4); script chỉ đặt mặc định cho `RUN_ID` (theo giờ),
`OUTPUT_PATH=file:///tmp/pseudonym-verify`, `N_SUBJECTS=2000`, `PARTITIONS=4`, `PSEUDO_MODES=HASH,ENCRYPT`:

```bash
PSEUDO_MODES=HASH,ENCRYPT,MAPPING pseudonym-verify-app/scripts/run-local.sh    # bật cả bảng ánh xạ
PREVIEW_ROWS=5 N_SUBJECTS=500 pseudonym-verify-app/scripts/run-local.sh        # in 5 dòng gốc/giả danh
RUN_MODE=selftest pseudonym-verify-app/scripts/run-local.sh                    # kiểm chứng chính bộ check
pseudonym-verify-app/scripts/run-local.sh --build                              # ép build lại jar + classpath
```

<details>
<summary>Chạy tay không dùng script</summary>

```bash
mvn -B -q -pl pseudonym-verify-app -am package -DskipTests dependency:build-classpath \
  -Dmdep.outputFile=/tmp/pv-cp.txt -Dmdep.includeScope=provided      # lib là "provided" nên main cần classpath này

RUN_ID=local-01 OUTPUT_PATH=file:///tmp/pv N_SUBJECTS=2000 PARTITIONS=4 PSEUDO_MODES=HASH,ENCRYPT \
java $(for p in java.lang java.lang.invoke java.lang.reflect java.io java.net java.nio java.util \
         java.util.concurrent java.util.concurrent.atomic sun.nio.ch sun.nio.cs sun.security.action sun.util.calendar; \
       do printf -- '--add-opens=java.base/%s=ALL-UNNAMED ' $p; done) \
     -Dspark.master='local[2]' -Dspark.ui.enabled=false \
     -Dlog4j.configurationFile=file:pseudonym-verify-app/src/main/resources/log4j2.properties \
     -cp "pseudonym-verify-app/target/pseudonym-verify-app.jar:$(cat /tmp/pv-cp.txt)" \
     vai.lakehouse.pseudonymverify.app.PseudoVerifyApp
echo "exit=$?"
```

App **không** hardcode `master` (do `spark-submit`/Operator cấp): chạy tay phải truyền `-Dspark.master=...`.
`--add-opens` chỉ cần trên JDK 17+ (JDK 11 bỏ được).

</details>

### 3.2.1. Xem kết quả

Thứ tự trong log (và trong `preview.txt`): **dữ liệu gốc → dữ liệu sau giả danh → kết quả check → verdict**.

```bash
RUN=/tmp/pseudonym-verify/<RUN_ID>
less $RUN/preview.txt                                      # so từng dòng gốc với dòng giả danh (cùng khoá dòng)
python3 -c "import json;r=json.load(open('$RUN/report.json'));print(r['verdict']);[print(m,v['verdict'],[c['id'] for c in v['checks'] if c['status'] in ('FAIL','ERROR')]) for m,v in r['modes'].items()]"
ls $RUN/output/HASH/subscriber                             # parquet đã giả danh (đọc được bằng spark.read.parquet / pandas)
```

Điều cần thấy ở `preview.txt`: `customer_id` đổi tên thành `pseudonym_id`; `isdn`, `imei` thành `k1_<32 hex>` (HASH) hoặc
`e1_...` (ENCRYPT) hoặc `t_<32 hex>` (MAPPING); `NULL` vẫn là `NULL`; các cột không định danh giữ nguyên.

### 3.3. Trên K8s

Chuẩn bị giống `hdfs-load-stress-test`:

- ConfigMap `teamk6gy0b5-hadoop-conf` có sẵn.
- ServiceAccount `spark-application-sa` có sẵn.
- Keytab nằm trên pod Spark Operator.
- Xác nhận quyền ghi `OUTPUT_PATH` trước khi chạy.

Thứ tự đề xuất (tiêu chí nghiệm thu, PLAN.md mục 13):

```bash
NS=vlp-tenantw1xjixm-wsbmkimit-teamk6gy0b5
# 1. Bộ check có đáng tin không? -> phải 8/8 (10/10 nếu bật MAPPING)
kubectl apply -f pseudonym-verify-app/k8s/pseudonym-verify-selftest.yaml
kubectl logs -f pseudonym-verify-selftest-driver -n $NS | grep -E 'MUTATION|CAUGHT|MISSED|VERDICT'

# 2. Nghiệm thu lib trên 1 triệu chủ thể -> mọi MUST = PASS, P6 = 100%
kubectl apply -f pseudonym-verify-app/k8s/pseudonym-verify-full.yaml
kubectl logs -f pseudonym-verify-full-driver -n $NS

# 3. Lặp lại (2) với khoá production: KEY_SOURCE=file + Secret pseudonym-keys (hoặc vault/dak), ĐỔI RUN_ID
```

Muốn chạy lại cùng tên SparkApplication thì phải `kubectl delete sparkapplication <tên> -n $NS` trước.

**Guard trên dữ liệu thật:** dùng manifest `full`, đổi các biến sau:

- `RUN_MODE=guard`
- `INPUT_TABLE=<parquet gốc>`
- `GUARD_TABLE=<tên bảng trong policy>`
- `POLICY_PATH=<file policy mount từ ConfigMap>`
- `KEY_SOURCE=file|vault|dak` (guard bắt buộc dùng khoá thật)

App chỉ ghi `${OUTPUT_PATH}/${RUN_ID}/output/<GUARD_TABLE>` khi verdict là `PASS`.

### 3.4. Log và exit code

Log chỉ còn logger `VLP`, theo thứ tự các mục `1. CONFIG` → `2. SPARK SESSION` → `3. RUN_MODE=...`. Ở mỗi chế độ hoặc mutation, mỗi check được in 1 dòng:

```
HASH           U4   PASS    subjects_with_multiple_pseudonyms      observed=0          threshold=== 0
HASH           P4   PASS    unkeyed_dictionary_matches             observed=0          threshold=== 0
HASH           P6   PASS    keyed_reverse_rate                     observed=1          threshold=== 1.0
verdict HASH                       : PASS
...
[ OK ]   M2: verdict=ERROR fail=[U9,U12,P4,P6] expected=[P4] -> CAUGHT
```

| Exit code | Nghĩa                                                                                   |
| --------- | --------------------------------------------------------------------------------------- |
| 0         | `PASS`                                                                                  |
| 1         | `FAIL`: có check MUST không đạt, hoặc selftest có mutation lọt                          |
| 2         | `ERROR`: không kết luận được (check lỗi, đối chứng P6 không đạt, sai cấu hình, job lỗi) |

## 4. Biến môi trường (driver)

| Biến                                                                          | Mặc định                                             | Ý nghĩa                                                                                                                                                                                 |
| ----------------------------------------------------------------------------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RUN_MODE`                                                                    | `full`                                               | `full` / `selftest` / `guard`                                                                                                                                                           |
| `PSEUDO_MODES`                                                                | `HASH,ENCRYPT`                                       | Các chế độ cần kiểm; thêm `MAPPING` để bật bảng ánh xạ                                                                                                                                  |
| `RUN_ID`                                                                      | (bắt buộc)                                           | `[A-Za-z0-9_-]`, 1–64 ký tự                                                                                                                                                             |
| `OUTPUT_PATH`                                                                 | (bắt buộc)                                           | URI tuyệt đối, vd `hdfs://...:8020/prod6/pseudonym_verify` hoặc `file:///tmp/pv`                                                                                                        |
| `SEED`                                                                        | `s1`                                                 | Dữ liệu tất định theo seed                                                                                                                                                              |
| `N_SUBJECTS`                                                                  | `1000000`                                            | Số chủ thể (≤ 10⁸)                                                                                                                                                                      |
| `APP_EVENTS_PER_SUBJECT` / `BTS_CONN_PER_SUBJECT` / `LOYALTY_TXN_PER_SUBJECT` | `20` / `10` / `3`                                    |                                                                                                                                                                                         |
| `PARTITIONS`                                                                  | `200`                                                |                                                                                                                                                                                         |
| `NULL_RATE`                                                                   | `0.01`                                               | Tỉ lệ định danh bị thay bằng null                                                                                                                                                       |
| `OFFER_SAMPLE`                                                                | `10000`                                              | Số `pseudonym_id` dùng cho U12 (luồng ưu đãi Viettel++)                                                                                                                                 |
| `KEY_SOURCE`                                                                  | `ephemeral`                                          | `ephemeral` / `file` / `vault` / `dak`. Các nguồn sau đọc cấu hình `spark.pseudonym.*` rồi tới biến môi trường `CRYPTO_*`/`VAULT_*`/`DAK_*`                                             |
| `MAPPING_TABLE_PATH`                                                          | `${OUTPUT_PATH}/${RUN_ID}/_restricted/pseudonym_map` | Chỉ dùng khi bật MAPPING; phải nằm ngoài `output/`                                                                                                                                      |
| `POLICY_PATH`                                                                 | `demo-policy.yaml` (trong jar)                       | Đường dẫn file policy (đĩa, rồi classpath)                                                                                                                                              |
| `PREVIEW_ROWS`                                                                | `10`                                                 | `RUN_MODE=full`: in N dòng đầu của dữ liệu gốc và dữ liệu sau giả danh (ra log **trước** các check) và ghi `${RUN_ID}/preview.txt`; `0` = tắt. Không áp dụng cho `guard` (dữ liệu thật) |
| `WRITE_OUTPUT`                                                                | `true`                                               | `false`: không ghi parquet, check chạy trên kết quả trong bộ nhớ                                                                                                                        |
| `INPUT_TABLE` / `GUARD_TABLE` / `IDENTIFIER_SAMPLE_FRACTION`                  | — / — / `0.01`                                       | Chỉ dùng cho `guard`                                                                                                                                                                    |
| `APP_NAME`                                                                    | `PseudonymVerify`                                    | Tên trên Spark UI                                                                                                                                                                       |

## 5. Kết quả

```
<OUTPUT_PATH>/<RUN_ID>/
├── preview.txt                                # dữ liệu gốc + sau giả danh (PREVIEW_ROWS dòng đầu), chỉ full
├── report.json | selftest-report.json | guard-report.json
├── output/<MODE>/<bảng>/part-*.parquet       # dữ liệu đã giả danh (full)
├── output/<GUARD_TABLE>/                      # guard, chỉ khi PASS
├── selftest/<Mx>/output/...                   # output của từng mutation
└── _restricted/                               # bảng ánh xạ + dữ liệu tạm của check — NHẠY CẢM như dữ liệu gốc
    ├── pseudonym_map/                         # Delta (chỉ khi bật MAPPING)
    └── scratch/ , selftest/
```

Report **không** chứa giá trị định danh gốc hay khoá. `samples` chỉ chứa `subject_id` hoặc tên `bảng.cột`.

## 6. Đã kiểm chứng / chưa kiểm chứng

- **Đã chạy** (máy dev, JDK 21, Spark local):
  - Toàn bộ test từ gốc repo (`mvn clean package`): 56 test của `pseudonym-udf`, 22 test của app, các module cũ không đổi.
  - `scripts/run-local.sh` với cả 3 chế độ: mọi check `PASS`, exit 0; `selftest` bắt 10/10 mutation.
- **Chưa chạy:**
  - Trên cluster: HDFS Isilon + Kerberos, image Docker, `KEY_SOURCE=vault/dak`.
  - Quy mô 1 triệu chủ thể. Cấu hình tài nguyên trong manifest là giả định, cần chỉnh theo ResourceQuota thật.
