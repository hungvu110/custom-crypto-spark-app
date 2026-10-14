# pseudonym-verify-app

Spark job sinh dữ liệu viễn thông **tổng hợp có đáp án** (biết chính xác dòng nào thuộc chủ thể nào), giả danh hoá bằng
lib [`pseudonym-udf`](../pseudonym-udf), rồi chạy bộ check đo được để trả lời **có/không**: *đã khử định danh thành
công chưa*. Kết quả là verdict `PASS` / `FAIL` / `ERROR`, file `report.json` và exit code.

- Plan lib + quyết định thiết kế: [docs/PSEUDONYMIZATION_UDF_PLAN.md](../docs/PSEUDONYMIZATION_UDF_PLAN.md)
- Plan app + định nghĩa "thành công" + danh sách check: [docs/pseudonym-verify-app/PLAN.md](../docs/pseudonym-verify-app/PLAN.md)

## 1. Cấu trúc thư mục

```
pseudonym-verify-app/
├── README.md
├── Dockerfile                        # build context = GỐC REPO (cần jar của pseudonym-udf + key-prefix-lib)
├── pom.xml                           # lib = provided (nạp qua spark.jars); shade Delta + patch block token
├── k8s/
│   ├── pseudonym-verify-full.yaml    # RUN_MODE=full: nghiệm thu lib
│   ├── pseudonym-verify-selftest.yaml# RUN_MODE=selftest: kiểm chứng chính bộ check
│   └── pseudonym-keys-secret.yaml    # mẫu Secret khoá (KEY_SOURCE=file) — chỉ có chỗ trống
└── src/
    ├── main/resources/{demo-policy.yaml, log4j2.properties}
    ├── main/scala/vai/lakehouse/pseudonym/verify/
    │   ├── PseudoVerifyApp.scala     # entry point, chọn RUN_MODE, verdict -> exit code
    │   ├── VerifyConfig.scala        # đọc + validate env (gom mọi lỗi 1 lượt)
    │   ├── SyntheticData.scala       # 4 bảng (subscriber, app_event, bts_connection, loyalty_txn) + đáp án
    │   ├── IdentityFormats.scala     # isdn / imei / customer_id = hoán vị song ánh của chủ thể
    │   ├── KeyMaterial.scala         # khoá ephemeral hoặc qua key-prefix-lib; các dạng chuỗi của khoá cho P7
    │   ├── Pipeline.scala            # giả danh -> ghi -> đọc lại; chạy lại (U8); đường SQL (U9)
    │   ├── Runs.scala                # FullRun, SelftestRun
    │   ├── GuardRun.scala            # RUN_MODE=guard trên dữ liệu thật
    │   ├── Mutations.scala           # M1–M10: bản giả danh cố ý sai
    │   ├── Verdict.scala, ReportWriter.scala, Log.scala
    │   └── checks/                   # Check, LinkabilityChecks (U1–U12), PrivacyChecks (P1–P7),
    │                                 # UseCaseMetrics (chỉ số MyViettel/BTS/Viettel++), AttackDictionary (P4)
    └── test/scala/...                # 20 test: config, dữ liệu, verdict, END-TO-END cả 3 RUN_MODE
```

## 2. Build

Chạy ở **gốc repo** (JDK 11+, Maven 3.8+):

```bash
# Fat jar app + 2 jar lib rời. -am build luôn key-prefix-lib, pseudonym-udf và spark-app (jar block-token-patch).
mvn -B -pl pseudonym-verify-app -am clean package
# -> pseudonym-verify-app/target/pseudonym-verify-app.jar        (~6.5 MB: app + Delta + patch, KHÔNG có lib)
# -> pseudonym-udf/target/pseudonym-udf-1.0-SNAPSHOT.jar         (~100 KB)
# -> key-prefix-lib/target/key-prefix-lib-1.0-SNAPSHOT.jar

IMAGE=hub.vtcc.vn:8989/hungvt0110/pseudonym-verify-app:v0.1
docker build -f pseudonym-verify-app/Dockerfile -t "$IMAGE" .
docker push "$IMAGE"
```

Kiểm tra fat jar:

```bash
J=pseudonym-verify-app/target/pseudonym-verify-app.jar
unzip -p $J META-INF/services/org.apache.hadoop.security.token.TokenIdentifier   # -> vai.lakehouse.hdfs.LenientBlockTokenIdentifier
unzip -p $J META-INF/services/org.apache.spark.sql.sources.DataSourceRegister | grep delta
unzip -l $J | grep -c 'vai/lakehouse/pseudonym/[A-Z]'                            # -> 0 (lib KHÔNG nằm trong fat jar)
```

## 3. Chạy

### 3.1. Test (không cần cluster)

```bash
mvn -B -pl pseudonym-verify-app -am test -DwildcardSuites=vai.lakehouse.pseudonym.verify
```

`EndToEndSpec` chạy app thật trên Spark local với dữ liệu nhỏ:

- `full`: cả HASH, ENCRYPT, MAPPING đều phải `PASS`.
- `selftest`: 10/10 mutation phải bị bắt.
- `guard`: parquet + khoá từ file, kết quả `PASS` và output được ghi.

### 3.2. Chạy `main` ở máy local (không có spark-submit)

```bash
mvn -B -q -pl pseudonym-verify-app -am package -DskipTests dependency:build-classpath \
  -Dmdep.outputFile=/tmp/pv-cp.txt -Dmdep.includeScope=provided
RUN_ID=local-01 OUTPUT_PATH=file:///tmp/pv N_SUBJECTS=2000 PARTITIONS=4 PSEUDO_MODES=HASH,ENCRYPT,MAPPING \
java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
     --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
     -Dspark.master='local[2]' -Dspark.ui.enabled=false \
     -Dlog4j.configurationFile=file:pseudonym-verify-app/src/main/resources/log4j2.properties \
     -cp "pseudonym-verify-app/target/pseudonym-verify-app.jar:$(cat /tmp/pv-cp.txt)" \
     vai.lakehouse.pseudonym.verify.PseudoVerifyApp
echo "exit=$?"; cat /tmp/pv/local-01/report.json
```

(JDK 17+ cần các `--add-opens` như trong `pom.xml` gốc; JDK 11 thì bỏ được.)

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

| Exit code | Nghĩa |
|---|---|
| 0 | `PASS` |
| 1 | `FAIL`: có check MUST không đạt, hoặc selftest có mutation lọt |
| 2 | `ERROR`: không kết luận được (check lỗi, đối chứng P6 không đạt, sai cấu hình, job lỗi) |

## 4. Biến môi trường (driver)

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `RUN_MODE` | `full` | `full` / `selftest` / `guard` |
| `PSEUDO_MODES` | `HASH,ENCRYPT` | Các chế độ cần kiểm; thêm `MAPPING` để bật bảng ánh xạ |
| `RUN_ID` | (bắt buộc) | `[A-Za-z0-9_-]`, 1–64 ký tự |
| `OUTPUT_PATH` | (bắt buộc) | URI tuyệt đối, vd `hdfs://...:8020/prod6/pseudonym_verify` hoặc `file:///tmp/pv` |
| `SEED` | `s1` | Dữ liệu tất định theo seed |
| `N_SUBJECTS` | `1000000` | Số chủ thể (≤ 10⁸) |
| `APP_EVENTS_PER_SUBJECT` / `BTS_CONN_PER_SUBJECT` / `LOYALTY_TXN_PER_SUBJECT` | `20` / `10` / `3` | |
| `PARTITIONS` | `200` | |
| `NULL_RATE` | `0.01` | Tỉ lệ định danh bị thay bằng null |
| `OFFER_SAMPLE` | `10000` | Số `pseudonym_id` dùng cho U12 (luồng ưu đãi Viettel++) |
| `KEY_SOURCE` | `ephemeral` | `ephemeral` / `file` / `vault` / `dak`. Các nguồn sau đọc cấu hình `spark.pseudonym.*` rồi tới biến môi trường `CRYPTO_*`/`VAULT_*`/`DAK_*` |
| `MAPPING_TABLE_PATH` | `${OUTPUT_PATH}/${RUN_ID}/_restricted/pseudonym_map` | Chỉ dùng khi bật MAPPING; phải nằm ngoài `output/` |
| `POLICY_PATH` | `demo-policy.yaml` (trong jar) | Đường dẫn file policy (đĩa, rồi classpath) |
| `WRITE_OUTPUT` | `true` | `false`: không ghi parquet, check chạy trên kết quả trong bộ nhớ |
| `INPUT_TABLE` / `GUARD_TABLE` / `IDENTIFIER_SAMPLE_FRACTION` | — / — / `0.01` | Chỉ dùng cho `guard` |
| `APP_NAME` | `PseudonymVerify` | Tên trên Spark UI |

## 5. Kết quả

```
<OUTPUT_PATH>/<RUN_ID>/
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
  - Toàn bộ test: 56 test của `pseudonym-udf`, 20 test của app.
  - `main` thật với `N_SUBJECTS=2000`, cả 3 chế độ: mọi check `PASS`, exit 0.
- **Chưa chạy:**
  - Trên cluster: HDFS Isilon + Kerberos, image Docker, `KEY_SOURCE=vault/dak`.
  - Quy mô 1 triệu chủ thể. Cấu hình tài nguyên trong manifest là giả định, cần chỉnh theo ResourceQuota thật.
