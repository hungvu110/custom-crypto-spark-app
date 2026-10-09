# hdfs-load-stress-test

Spark job sinh dữ liệu tổng hợp rồi ghi Parquet vào HDFS Isilon HKH theo tải cấu hình được, để stress test luồng nạp
**100 GiB/ngày**. Job chạy với Kerberos, wire encryption và patch lenient block token; không có lib crypto.

Plan, kịch bản và tiêu chí đạt: [docs/hdfs-load-stress-test/PLAN.md](../docs/hdfs-load-stress-test/PLAN.md).

## 1. Cấu trúc thư mục

```
hdfs-load-stress-test/               # module Maven của pom gốc (cùng cấp spark-app, key-prefix-lib...)
├── README.md
├── Dockerfile                       # build context = thư mục này
├── pom.xml                          # parent = pom gốc; phụ thuộc jar block-token-patch của spark-app
├── k8s/
│   ├── hdfs-stress-sustained.yaml   # S2: 100 GiB/ngày, batch 5 phút, 24h
│   └── hdfs-stress-burst.yaml       # S0 smoke / S1 burst: ghi nhanh nhất có thể
└── src/
    ├── main/scala/vai/lakehouse/stress/
    │   ├── HdfsLoadStressApp.scala  # entry point, kiểm tra patch block token
    │   ├── StressConfig.scala       # đọc + validate env
    │   ├── StressRunner.scala       # vòng lặp batch, nhịp sustained, resume, retention
    │   ├── BatchPlanner.scala       # tính số dòng/file/lịch
    │   ├── DataGenerator.scala      # sinh dữ liệu (payload SHA-256 không nén được)
    │   ├── RunStore.scala           # thư mục batch + file metric trên HDFS
    │   ├── Metrics.scala            # STRESS_BATCH / STRESS_SUMMARY (dòng log + JSON)
    │   ├── Log.scala                # format log giống spark-app (banner/section/kv, logger "VLP")
    │   └── Units.scala
    ├── main/resources/log4j2.properties
    └── test/scala/...               # 18 test: config, tính toán, chạy thật với Spark local
```

Patch block token **không** chép lại code mà dùng jar classifier `block-token-patch` của module `spark-app`
(`vai.lakehouse.hdfs.*` + `META-INF/services`), shade vào fat jar.

## 2. Build

Cần JDK 11+, Maven 3.8+, Docker. Chạy các lệnh từ **gốc repo**.

```bash
# 1. Build + test -> hdfs-load-stress-test/target/hdfs-load-stress-test.jar (~90 KB)
#    -am build luôn các module phụ thuộc (spark-app -> jar block-token-patch). Thêm -DskipTests để bỏ test của
#    các module đó nếu chỉ cần jar.
mvn -B -pl hdfs-load-stress-test -am clean package

# 2. Build + push image (đổi registry/tag cho đúng, rồi sửa "image:" trong cả 2 manifest)
IMAGE=hub.vtcc.vn:8989/hungvt0110/hdfs-load-stress-test:v0.1
docker build -t "$IMAGE" hdfs-load-stress-test
docker push "$IMAGE"
```

`mvn clean package` ở gốc repo cũng build module này (cùng các module khác).

Kiểm tra jar đã có patch:

```bash
unzip -p hdfs-load-stress-test/target/hdfs-load-stress-test.jar \
  META-INF/services/org.apache.hadoop.security.token.TokenIdentifier
# -> vai.lakehouse.hdfs.LenientBlockTokenIdentifier
```

## 3. Chuẩn bị trên K8s

```bash
NS=vlp-tenantw1xjixm-wsbmkimit-teamk6gy0b5
kubectl get configmap teamk6gy0b5-hadoop-conf -n $NS      # core-site/hdfs-site của cụm HKH
kubectl get serviceaccount spark-application-sa -n $NS
```

- Keytab `/etc/security/keytabs/k8s.keytab` và `/etc/krb5.conf` nằm trên **pod Spark Operator** (giống `crawler-streaming-app`),
  không mount vào app.
- Xác nhận với team storage: quyền ghi và quota của `OUTPUT_PATH`, khung giờ được phép chạy (docs/hdfs-load-stress-test/PLAN.md mục 11).

## 4. Chạy

Thứ tự đề xuất: **S0 smoke → S1 burst → S2 sustained** (docs/hdfs-load-stress-test/PLAN.md mục 4).

### S0 – Smoke (1 GiB)

Trong `k8s/hdfs-stress-burst.yaml`, đặt `RUN_ID=smoke-01`, `TOTAL_GIB=1`, `BATCH_GIB=0.25`, `VERIFY_READ=true`, rồi:

```bash
kubectl apply -f hdfs-load-stress-test/k8s/hdfs-stress-burst.yaml
kubectl logs -f hdfs-stress-burst-driver -n $NS          # toàn bộ log đã được lọc, chỉ còn phần của app (mục 4.1)
```

Kết quả mong đợi (rút gọn):

```
INFO VLP: ====================================================================================
INFO VLP:   HDFS LOAD STRESS TEST — GHI DỮ LIỆU TỔNG HỢP VÀO HDFS (Kerberos + wire encryption + block token)
INFO VLP: ====================================================================================
INFO VLP: ------------------------------------------------------------------------------------
INFO VLP:   1. CONFIG
INFO VLP: ------------------------------------------------------------------------------------
INFO VLP:     MODE                               : burst
INFO VLP:     RUN_ID                             : smoke-01
...
INFO VLP:   3. BLOCK TOKEN PATCH
INFO VLP:     class xử lý HDFS_BLOCK_TOKEN       : vai.lakehouse.hdfs.LenientBlockTokenIdentifier
INFO VLP:     [ OK ]   BLOCK_TOKEN_PATCH driver: active
INFO VLP:     [ OK ]   BLOCK_TOKEN_PATCH executor <pod>: active
...
INFO VLP:   4. LOAD
INFO VLP:     [ OK ]   STRESS_BATCH #000000      250,374 rows    256.0 MiB    2 files  write=9.8 s (26.1 MiB/s)  read=2.1 s  lag=0.0 s
...
INFO VLP:   5. SUMMARY
INFO VLP:     tổng đã ghi / mục tiêu             : 1.00 GiB / 1.00 GiB
INFO VLP:     [ OK ]   STRESS_SUMMARY status=PASS (completed=true, keptUp=true)
INFO VLP:     STRESS_SUMMARY {"runId":"smoke-01","mode":"burst","status":"PASS",...}
```

(Số liệu ở trên là minh hoạ định dạng, không phải kết quả đo.)

### S1 – Burst (100 GiB)

Đặt lại `RUN_ID` (vd `burst-20261010-01`), `TOTAL_GIB=100`, `BATCH_GIB=5`, `VERIFY_READ=false`. SparkApplication cũ phải xoá trước,
vì Operator không chạy lại một app đã kết thúc:

```bash
kubectl delete sparkapplication hdfs-stress-burst -n $NS
kubectl apply  -f hdfs-load-stress-test/k8s/hdfs-stress-burst.yaml
```

### S2 – Sustained 24h (100 GiB/ngày)

```bash
kubectl apply -f hdfs-load-stress-test/k8s/hdfs-stress-sustained.yaml
kubectl logs -f hdfs-stress-sustained-driver -n $NS | grep -E 'STRESS_|BLOCK_TOKEN|FAIL|WARN|Token'
```

- Mỗi 5 phút có một dòng `STRESS_BATCH`. Dòng `STRESS_BEHIND` nghĩa là batch bắt đầu trễ hơn một chu kỳ: hệ thống không theo kịp.
- Nếu pod chết, Operator restart (`OnFailure`, tối đa 3 lần). App đọc `_metrics` rồi tiếp tục từ batch dở, không ghi trùng.
- Dừng sớm: `kubectl delete sparkapplication hdfs-stress-sustained -n $NS`. Apply lại với **cùng** `RUN_ID` thì chạy tiếp; muốn
  một lần chạy mới thì **đổi** `RUN_ID`.

### 4.1. Log

Log được làm gọn giống `spark-app`:

- **`src/main/resources/log4j2.properties`:**
  - chỉ logger `VLP` của app được log ở mức INFO;
  - Spark, Hadoop, Jetty, Netty, Parquet và K8s client bị hạ xuống WARN hoặc ERROR;
  - riêng `org.apache.spark.deploy.security` giữ INFO, để thấy Spark gia hạn delegation token khi chạy 24h.
- **`Log.scala`:** cùng format với `org.example.Log` của `spark-app`:
  - khung banner/section, cặp key-value canh cột;
  - nhãn `[ OK ]` / `[ WARN ]` / `[ FAIL ]`;
  - in chuỗi nguyên nhân (`Caused by`) khi job lỗi.
- **Các section** theo thứ tự `1. CONFIG` → `2. SPARK SESSION` → `3. BLOCK TOKEN PATCH` → `4. LOAD` → `5. SUMMARY` → `KẾT THÚC`.
- **Mỗi batch:** in một dòng `STRESS_BATCH` tóm tắt (số dòng, MiB, số file, thời gian/tốc độ ghi, đọc lại, độ trễ). Số liệu
  đầy đủ dạng JSON nằm ở `_metrics/batch=N.json` (mục 6).
- **Cuối lần chạy:** `STRESS_SUMMARY` vừa in dạng bảng, vừa in một dòng JSON để chép vào báo cáo.

### Exit code của driver

| Code | Nghĩa |
|---|---|
| 0 | Chạy xong; xem `STRESS_SUMMARY.status` (`PASS`/`WARN`) |
| 1 | Lỗi khi chạy (`STRESS_FAILED` kèm stack trace) |
| 2 | Sai cấu hình: mọi lỗi in ra cùng lúc với tiền tố `CONFIG` |
| 3 | Patch block token không active trên driver hoặc executor |

## 5. Biến môi trường

Chỉ driver đọc các biến này. Dung lượng tính theo GiB (1024³ byte).

| Biến | Mặc định | Giá trị hợp lệ | Ý nghĩa |
|---|---|---|---|
| `MODE` | `sustained` | `sustained` / `burst` | `sustained`: chia đều tải theo chu kỳ. `burst`: ghi nhanh nhất có thể |
| `RUN_ID` | (bắt buộc) | `[A-Za-z0-9_-]`, 1–64 ký tự | Tên lần chạy = thư mục con. Cùng `RUN_ID` = resume |
| `OUTPUT_PATH` | (bắt buộc) | URI tuyệt đối có scheme | Thư mục gốc, vd `hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020/prod6/stress_test/hdfs_load` |
| `STRESS_GIB_PER_DAY` | `100` | số > 0 | Chỉ `sustained`: tải mỗi ngày |
| `BATCH_INTERVAL_SECONDS` | `300` | 10–86400 | Chỉ `sustained`: chu kỳ batch |
| `DURATION_HOURS` | `24` | số ≥ 0 | Chỉ `sustained`: thời gian chạy; `0` = chạy tới khi đủ `TOTAL_GIB` |
| `TOTAL_GIB` | sustained: `STRESS_GIB_PER_DAY × DURATION_HOURS / 24`; burst: `100` | số > 0 | Tổng dung lượng cần ghi (tính cả các lần chạy trước của cùng `RUN_ID`) |
| `BATCH_GIB` | `5` | số > 0 | Chỉ `burst`: dung lượng mỗi batch |
| `PAYLOAD_BYTES` | `1024` | 64–1048576 (làm tròn xuống bội 64) | Kích thước cột payload không nén được của mỗi dòng |
| `TARGET_FILE_MIB` | `128` | 1–2048 | Kích thước file mục tiêu, quyết định số file mỗi batch |
| `COMPRESSION` | `snappy` | `snappy` / `zstd` / `gzip` / `lz4` / `none` | Codec Parquet |
| `VERIFY_READ` | `false` | `true` / `false` | Đọc lại mỗi batch và so số dòng (kiểm cả chiều đọc) |
| `RETAIN_BATCHES` | `0` | số nguyên ≥ 0 | Chỉ giữ N batch mới nhất; `0` = giữ hết. Metric vẫn giữ đủ |
| `REQUIRE_BLOCK_TOKEN_PATCH` | `true` | `true` / `false` | `true`: dừng (exit 3) nếu patch không active |
| `APP_NAME` | `HdfsLoadStressTest` | chuỗi | Tên trên Spark UI |

## 6. Kết quả trên HDFS

```
<OUTPUT_PATH>/<RUN_ID>/
├── batch=000000/part-*.snappy.parquet     # dữ liệu (đọc được bằng spark.read.parquet, cột partition "batch")
├── batch=000001/...
└── _metrics/
    ├── batch=000000.json                  # 1 file/batch: số liệu đầy đủ của dòng STRESS_BATCH (JSON)
    └── summary-<epoch ms>.json            # 1 file mỗi lần chạy (resume tạo thêm file mới)
```

Trường chính của `_metrics/batch=N.json`:

| Trường | Ý nghĩa |
|---|---|
| `bytes`, `files` | Dung lượng và số file đo thực tế trên HDFS |
| `writeSeconds`, `writeMiBps` | Thời gian và tốc độ ghi của batch |
| `readSeconds` | Thời gian đọc lại (`null` nếu `VERIFY_READ=false`) |
| `lagSeconds` | Sustained: batch bắt đầu trễ bao nhiêu so với lịch |

`STRESS_SUMMARY` có thêm:

| Trường | Ý nghĩa |
|---|---|
| `status` | `PASS` khi `completed` (đạt ≥ 98% `TOTAL_GIB`) và `keptUp` (sustained: không batch nào trễ ≥ 1 chu kỳ); ngược lại `WARN` |
| `writeMiBps`, `effectiveMiBps` | Tốc độ ghi thuần, và dung lượng chia cho tổng thời gian (gồm cả thời gian chờ) |
| `writeSecondsP50`, `writeSecondsP95`, `writeSecondsMax` | Phân bố thời gian ghi mỗi batch |
| `maxLagSeconds` | Độ trễ lớn nhất so với lịch |

## 7. Dọn dẹp

```bash
kubectl delete sparkapplication hdfs-stress-burst hdfs-stress-sustained -n $NS --ignore-not-found
```

Dữ liệu test **không** tự xoá (trừ khi đặt `RETAIN_BATCHES`). Xoá từ một máy có HDFS client, cấu hình HKH và đã `kinit`:

```bash
hdfs dfs -du -s -h hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020/prod6/stress_test/hdfs_load/<RUN_ID>
hdfs dfs -rm -r -skipTrash hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020/prod6/stress_test/hdfs_load/<RUN_ID>
```

Kiểm tra kỹ đường dẫn trước khi chạy `-rm -r -skipTrash`: lệnh này không thể hoàn tác.

## 8. Xử lý sự cố

| Triệu chứng | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| Exit 2, log `CONFIG ...` | Thiếu hoặc sai env | Sửa đúng các biến được liệt kê; mọi lỗi in ra cùng lúc |
| Exit 3, `BLOCK_TOKEN_PATCH ... active=false` | Image cũ, hoặc jar build thiếu `META-INF/services` | Build lại theo mục 2 và kiểm tra bằng `unzip -p`; giữ `userClassPathFirst=false` |
| `NegativeArraySizeException` khi ghi | Patch không được dùng ở executor | Như trên; xem log executor |
| Lỗi Kerberos hoặc `GSSException` ngay lúc submit | Keytab/krb5 trên pod Operator, hoặc principal sai | Đối chiếu với manifest của `crawler-streaming-app` |
| `AccessControlException` | Principal `k8s` không có quyền ghi `OUTPUT_PATH` | Đổi `OUTPUT_PATH` hoặc xin cấp quyền |
| `DSQuotaExceededException` | Hết quota thư mục | Dọn dữ liệu test; đặt `RETAIN_BATCHES` |
| Nhiều `STRESS_BEHIND`, `status=WARN` | Ghi chậm hơn chu kỳ | Xem `kubectl top`: nếu CPU executor bão hoà thì tăng core; nếu không, nghẽn nằm ở HDFS/mạng, cần báo team storage |
| Pod pending, lỗi quota khi tạo pod | ResourceQuota của namespace | Giảm `instances`/`cores`/`memory` của executor |
