# Build, test và build image — chi tiết

> Tách ra từ README gốc. Lệnh nhanh ở README gốc mục "Build & chạy".

Yêu cầu: JDK 11+, Maven 3.8+ (lần build đầu cần mạng, hoặc Nexus nội bộ — xem [`internal-repo/README.md`](../../internal-repo/README.md)).
Scala/Spark/Hadoop khai `provided`; riêng `delta-spark` đóng gói vào fat jar. Chạy lệnh ở thư mục gốc.

## Các module và phụ thuộc

| Module | Loại | Phụ thuộc module khác (`-am` build luôn) | Sản phẩm (`target/`) | Profile |
| --- | --- | --- | --- | --- |
| `key-prefix-lib` | lib | — | `key-prefix-lib-1.0-SNAPSHOT.jar` | mặc định |
| `column-crypto-lib` | lib | `key-prefix-lib` | `column-crypto-lib-1.0-SNAPSHOT.jar` | mặc định |
| `pseudonym-udf` | lib | `key-prefix-lib` | `pseudonym-udf-1.0-SNAPSHOT.jar` | mặc định |
| `cdr-crypto-udf` | lib | `key-prefix-lib` + jar đối tác `DataLakeSecurity_jv8.jar` | `cdr-crypto-udf-1.0-SNAPSHOT.jar` | **`cdr-crypto`** |
| `spark-app` | app | `column-crypto-lib`, `key-prefix-lib` (chỉ cho test) | `sample-spark-application-privacy-1.0-SNAPSHOT.jar` (+ `-block-token-patch.jar`) | mặc định |
| `hdfs-load-stress-test` | app | `spark-app` (jar `block-token-patch`) | `hdfs-load-stress-test.jar` | mặc định |
| `pseudonymize-table-app` | app | `pseudonym-udf`, `key-prefix-lib`, `spark-app` (patch) | `pseudonymize-table-app.jar` | mặc định |
| `pseudonym-verify-utils` | app | `pseudonym-udf`, `key-prefix-lib` | `pseudonym-verify-utils.jar` | mặc định |
| `silver-ingest-app` | app | `cdr-crypto-udf`, `key-prefix-lib`, `spark-app` (patch) | `silver-ingest-app.jar` | **`cdr-crypto`** |
| `dak-mock` | Spring Boot, **dự án Maven riêng** (không nằm trong `pom.xml` gốc) | — | `dak-mock-0.1.0-SNAPSHOT.jar` | — |

Hai module cần jar đối tác nằm trong profile `cdr-crypto` nên **không** bị build mặc định: người chưa cài jar đó vẫn build được phần còn lại.
Cài jar đối tác một lần (xem mục "Build & test `cdr-crypto-udf`"):

```bash
mvn install:install-file -Dfile=DataLakeSecurity_jv8.jar \
  -DgroupId=com.viettel.datalake -DartifactId=datalake-security -Dversion=jv8 -Dpackaging=jar
```

Mọi lệnh dưới đây chạy ở **thư mục gốc repo**. Thêm `-B` cho CI, `-o` khi `~/.m2` đã đủ dependency và không có mạng, `-DskipTests` để chỉ lấy jar.

## Build toàn bộ

```bash
mvn -B clean package                   # mặc định: 7 module, KHÔNG cần jar đối tác
mvn -B -Pcdr-crypto clean package      # TẤT CẢ 9 module: thêm cdr-crypto-udf và silver-ingest-app (cần jar đối tác)
mvn -B -Pcdr-crypto clean install -DskipTests   # build tất cả và cài vào ~/.m2 (để sau đó build từng module không cần -am)
mvn -f dak-mock/pom.xml clean package  # dak-mock build riêng, không thuộc lệnh ở trên
```

| Lệnh | Module được build | Cần `DataLakeSecurity_jv8.jar` | Dùng khi |
| --- | --- | --- | --- |
| `mvn -B clean package` | `key-prefix-lib`, `column-crypto-lib`, `spark-app`, `hdfs-load-stress-test`, `pseudonym-udf`, `pseudonym-verify-utils`, `pseudonymize-table-app` | Không | Image `no-crypto` / `column-crypto`, phát triển hằng ngày |
| `mvn -B -Pcdr-crypto clean package` | 7 module trên + `cdr-crypto-udf` + `silver-ingest-app` | Có | Trước khi `docker build --target cdr-crypto` hoặc build image `silver-ingest-app` |

Maven tự xếp thứ tự build theo phụ thuộc. `docker build --target cdr-crypto` cần thêm `cdr-crypto-udf/target/*.jar` và `DataLakeSecurity_jv8.jar` ở thư mục gốc, nên phải build bằng lệnh có `-Pcdr-crypto`.

## Build và test từng module

Khuôn chung: `-pl <module>` chọn module, **`-am`** (also-make) build luôn các module nó phụ thuộc. Thiếu `-am` thì Maven tìm phụ thuộc trong `~/.m2` và fail nếu chưa `install`.

| Module | Test | Build jar (bỏ test thêm `-DskipTests`) |
| --- | --- | --- |
| `key-prefix-lib` | `mvn -B -pl key-prefix-lib test` | `mvn -B -pl key-prefix-lib clean package` |
| `column-crypto-lib` | `mvn -B -pl column-crypto-lib -am test` | `mvn -B -pl column-crypto-lib -am clean package` |
| `pseudonym-udf` | `mvn -B -pl pseudonym-udf -am test` | `mvn -B -pl pseudonym-udf -am clean package` |
| `cdr-crypto-udf` | `mvn -B -Pcdr-crypto -pl cdr-crypto-udf -am test` | `mvn -B -Pcdr-crypto -pl cdr-crypto-udf -am clean package` |
| `spark-app` | `mvn -B -pl spark-app -am test` | `mvn -B -pl spark-app -am clean package` |
| `hdfs-load-stress-test` | `mvn -B -pl hdfs-load-stress-test -am test` | `mvn -B -pl hdfs-load-stress-test -am clean package` |
| `pseudonymize-table-app` | `mvn -B -pl pseudonymize-table-app -am test` | `mvn -B -pl pseudonymize-table-app -am clean package` |
| `pseudonym-verify-utils` | `mvn -B -pl pseudonym-verify-utils -am test` | `mvn -B -pl pseudonym-verify-utils -am clean package` |
| `silver-ingest-app` | `mvn -B -Pcdr-crypto -pl silver-ingest-app -am test` | `mvn -B -Pcdr-crypto -pl silver-ingest-app -am clean package` |
| `dak-mock` | `mvn -B -f dak-mock/pom.xml test` | `mvn -B -f dak-mock/pom.xml clean package` |

Test DAG Airflow (Python, không cần cài Airflow, cần `python3` và `jinja2`):

```bash
python3 -B -m unittest discover -s airflow/tests        # DAG khử định danh + hai DAG chuẩn hoá silver
```

**Chạy một suite/package** (Scalatest): đừng kèm `-am` với `-Dsuites`, vì tuỳ chọn áp cho cả module phụ thuộc và Scalatest báo "Missing class"
ở module không có suite đó. Cài phụ thuộc vào `~/.m2` trước, rồi chạy riêng module:

```bash
mvn -B -Pcdr-crypto -pl key-prefix-lib,cdr-crypto-udf -am install -DskipTests      # 1 lần: cài phụ thuộc
mvn -B -Pcdr-crypto -pl silver-ingest-app test \
    -Dsuites=vai.lakehouse.silveringest.config.IngestConfigSpec                     # 1 suite (tên đầy đủ, nhiều suite cách nhau bằng dấu phẩy)
mvn -B -pl pseudonymize-table-app test \
    -DwildcardSuites=vai.lakehouse.pseudonymizetable                                # cả một package
mvn -B -f dak-mock/pom.xml test -Dtest=SilverSampleFilesTest                        # dak-mock dùng JUnit: -Dtest=<Lớp>
```

Khi sửa một lib rồi muốn app dùng bản mới mà không build lại cả chuỗi: `mvn -B -pl <lib> install -DskipTests`, sau đó build app (không cần `-am`).

## Build riêng lib mã hoá (`column-crypto-lib`)

Dùng khi chỉ cần jar để nạp vào sql-engine (xem `docs/column-crypto/COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md`):

```bash
mvn -B clean package -pl column-crypto-lib             # build + test lib
mvn -B clean package -pl column-crypto-lib -DskipTests # bỏ qua test
# -> column-crypto-lib/target/column-crypto-lib-1.0-SNAPSHOT.jar
```

Jar này là jar **thuần** (không shade): chỉ chứa `vai.lakehouse.columncrypto.*`, không kèm
Spark/Jackson/snakeyaml nên không gây xung đột classpath khi nạp vào engine. Nó cần `key-prefix-lib.jar`
(`-pl column-crypto-lib -am` tự build luôn) cùng nằm trên classpath.

## Build riêng app chính (`spark-app`)

```bash
mvn -B clean package -pl spark-app -am                 # -am: build luôn lib (chỉ để chạy test scope test của app)
mvn -B clean package -pl spark-app -am -DskipTests
# -> spark-app/target/sample-spark-application-privacy-1.0-SNAPSHOT.jar               (fat jar: app + Delta + patch, KHÔNG có lib crypto)
# -> spark-app/target/sample-spark-application-privacy-1.0-SNAPSHOT-block-token-patch.jar (thin, chỉ patch block token)
```

Phải có `-am`: `spark-app` dùng `column-crypto-lib` + `key-prefix-lib` ở scope **test** (cho `CryptoStepSpec`), nên
nếu chỉ `-pl spark-app` Maven đi tìm chúng trong `~/.m2` và fail khi chưa `mvn install`. Fat jar của app
**không** đóng gói lib crypto nào: lúc deploy phải nạp jar lib qua Dockerfile + `spark.jars` (mục "Build & push image" bên dưới).
Kiểm tra: `unzip -l <fat jar> | grep -E 'columncrypto|keyprefix'` phải KHÔNG ra dòng nào.

## Chạy test (chi tiết)

Lệnh cho từng module ở bảng "Build và test từng module" phía trên; mục này liệt kê thêm tuỳ chọn và nội dung các suite.

```bash
mvn -B test                                                   # test mọi module mặc định
mvn -B test -pl column-crypto-lib                             # chỉ test lib
mvn -B test -pl spark-app -am                                 # test app (kèm test các lib)
mvn -B test -pl column-crypto-lib \
    -DwildcardSuites=vai.lakehouse.columncrypto.sql           # chỉ 1 suite/package (tên đầy đủ)
mvn -B -pl key-prefix-lib \
    org.scoverage:scoverage-maven-plugin:2.0.5:report \
    -Dscoverage.scalacPluginVersion=2.1.1                     # đo coverage key-prefix-lib (yêu cầu ≥ 80%)
```

| Module              | Test                                                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `key-prefix-lib`    | `PrefixFilesSpec` (đọc file prefix, path traversal), `VaultPrefixSourceSpec` (Vault giả bằng HTTP server cục bộ), `KeycloakTokenProviderSpec` (xin/cache token, đồng thời, lỗi không lộ secret), `DakTableRefSpec` (tách `database.table`), `DakPrefixSourceSpec` (Keycloak + DAK giả: retry 401, mã lỗi, `requestId`), `PrefixSourceSpec` (chain/cache), `PrefixSourceFactorySpec` (cấu hình nguồn, cấm ghép `dak`), `EnvConfigSourceSpec` (ánh xạ env → cấu hình), `ColumnBatchesSpec` (tên cột, chia lô 100 cột / 4096 ký tự), `DakColumnKeySourceSpec` (`by-column` với body giống dak-mock: trạng thái từng cột, "tất cả hoặc không", response sai hợp đồng, mã lỗi), `CachedColumnKeySourceSpec` (cache theo cột), `ColumnKeySourceFactorySpec` (chỉ `source=dak`, `dak.tag`/`dak.caller`) |
| `column-crypto-lib` | `ColumnCryptoSpec` (DataFrame API), `ColumnCryptoConfigSpec` (settings YAML), `ColumnCryptoExtensionSpec` (SQL function qua SparkSession local), `ColumnCryptoExtensionDakSpec` (SQL function với `source=dak`, Keycloak + DAK giả) |
| `spark-app`         | `BusinessLogicSpec` (schema/DDL/sample-data thuần), `CryptoStepSpec` (parse cấu hình, roundtrip qua hàm SQL `column_encrypt` với extension thật ở scope test, lỗi khi hàm chưa nạp) |

Test chạy trên `local[*]`, không cần cluster, Vault hay HDFS thật. Log có các dòng
`AES_CRYPTO_ERROR ... Tag mismatch` là **bình thường**: đó là test cố ý giải mã sai key để kiểm tra
lỗi được báo đúng. Phần I/O thật với Isilon (wire encryption, block token, CREATE DATABASE/TABLE
thật) và việc nạp jar vào sql-engine **không thể** unit test cục bộ — phải verify trên cluster
(checklist trong `SPARK_APP_REFERENCE.md` và `COLUMN_CRYPTO_SQL_ENGINE_GUIDE.md`). Trên JDK 17+, `pom.xml` gốc đã cấu hình sẵn
`--add-opens` cho test JVM.

## Build & test `cdr-crypto-udf` (bài toán 3 — cần jar đối tác)

Module này phụ thuộc `DataLakeSecurity_jv8.jar` (KHÔNG commit vào repo — xem `.gitignore`), nên
KHÔNG build mặc định (`spark-app` không phụ thuộc nó). Phải cài jar đối tác vào local Maven repo trước, rồi build bằng
profile `cdr-crypto`:

```bash
# 1. Cài jar đối tác (chỉ cần làm 1 lần, hoặc khi đối tác đổi version)
mvn install:install-file -Dfile=DataLakeSecurity_jv8.jar \
  -DgroupId=com.viettel.datalake -DartifactId=datalake-security -Dversion=jv8 -Dpackaging=jar

# 2. Build + test (-am: tự build key-prefix-lib trước, module này phụ thuộc nó)
mvn -Pcdr-crypto -pl cdr-crypto-udf -am clean test
mvn -Pcdr-crypto -pl cdr-crypto-udf -am clean package -DskipTests
# -> cdr-crypto-udf/target/cdr-crypto-udf-1.0-SNAPSHOT.jar (~15 KB, chỉ vai.lakehouse.columncrypto.cdr.*)
```

| Test                     | Nội dung                                                                                                                                                                              |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CdrCipherCoreSpec`      | `decrypt()` đọc đúng ciphertext mẫu đối tác, `encrypt()` khớp byte-for-byte (AES/ECB tất định), roundtrip, field rỗng, `verifyCompatibility()`                                        |
| `CdrCryptoExtensionSpec` | `cdr_encrypt`/`cdr_decrypt` qua SQL: roundtrip, khớp byte-for-byte với `CdrCipherCore` gọi trực tiếp, INSERT/SELECT bảng parquet thật, view, namespace conf riêng `spark.cdrcrypto.*` |
| `CdrCryptoSpec` | DataFrame API `CdrCrypto`: roundtrip + NULL, khớp byte-for-byte với `CdrCipherCore` và với SQL `cdr_encrypt`, giải mã ciphertext mẫu thật của đối tác, nhiều cột, lỗi schema, `toString` che prefix |

`mvn clean package`/`mvn test` **không có `-Pcdr-crypto`** (các lệnh ở trên) hoàn toàn không đụng module này — build chính không bao giờ fail vì thiếu jar đối tác. Chi tiết đầy đủ:
[`docs/cdr-crypto/PARTNER_CDR_CRYPTO_UDF_PLAN.md`](../cdr-crypto/PARTNER_CDR_CRYPTO_UDF_PLAN.md).

## Build & test `pseudonym-udf` + `pseudonymize-table-app` + `pseudonym-verify-utils` (bài toán 4)

Không cần jar đối tác. Mọi dependency đều có trên Maven Central/Nexus: Tink 1.9.0 và snakeyaml lấy đúng bản Spark
3.5.1 ship sẵn, Delta 3.2.0 trùng version của `spark-app`.

```bash
# --- Lib giả danh (pseudonym-udf) ---
mvn -B -pl pseudonym-udf -am clean test                     # 59 test: RFC 4231, golden vector (openssl), AES-SIV, Delta, SQL, tên khoá
mvn -B -pl pseudonym-udf -am clean package -DskipTests      # chỉ cần jar
# -> pseudonym-udf/target/pseudonym-udf-1.0-SNAPSHOT.jar   (+ key-prefix-lib/target/key-prefix-lib-1.0-SNAPSHOT.jar)

# --- App dùng lib: -am tự build trước key-prefix-lib, pseudonym-udf, spark-app (jar block-token-patch) ---
mvn -B -pl pseudonymize-table-app -am clean package         # khử định danh bảng/file; + 22 test chạy app thật
mvn -B -pl pseudonym-verify-utils -am clean package         # utils kiểm tra nguồn ↔ đích + selftest tự sinh dữ liệu; + 11 test
# -> pseudonymize-table-app/target/pseudonymize-table-app.jar
# -> pseudonym-verify-utils/target/pseudonym-verify-utils.jar

# Kiểm tra bảng đích sau khi chạy pseudonymize-table-app (exit 0 = PASS, 1 = FAIL, 2 = sai tham số/lỗi)
spark-submit --class vai.lakehouse.pseudonymverify.app.PseudonymValidateApp \
  --jars pseudonym-udf-1.0-SNAPSHOT.jar,key-prefix-lib-1.0-SNAPSHOT.jar pseudonym-verify-utils.jar \
  crm.subscriber crm.subscriber_pseudonymized isdn,imei HASH
# hoặc nghiệm thu lib bằng dữ liệu tự sinh: ... pseudonym-verify-utils.jar selftest HASH 5000
```

Lib `pseudonym-udf` và `key-prefix-lib` là jar **rời**, KHÔNG nằm trong fat jar của app: app khai chúng ở scope
`provided` trong `pom.xml`; image copy 2 jar này vào `/opt/app` và manifest nạp qua `spark.jars` (UDF thật nên executor
cũng cần). Chi tiết app khử định danh bảng, kể cả **bảng env của lib** (`CRYPTO_*`/`VAULT_*`/`DAK_*`/`spark.pseudonym.*`):
[`pseudonymize-table-app/README.md`](../../pseudonymize-table-app/README.md).

Chi tiết các phép kiểm và cách dùng API: [`pseudonym-verify-utils/README.md`](../../pseudonym-verify-utils/README.md).

| Spec | Nội dung |
| --- | --- |
| `HmacPseudonymizerSpec` | HMAC đúng chuẩn (RFC 4231), golden vector tính độc lập bằng `openssl`, đa luồng, serialize sang executor |
| `AesSivPseudonymizerSpec` | Giải mã lại đúng, tất định, khớp primitive Tink, sai khoá hoặc bản mã bị sửa thì báo lỗi xác thực |
| `MappingTableSpec` | Token ổn định qua nhiều lần chạy; 4 job MERGE song song vẫn ra 1 token/giá trị |
| `PseudonymizerSpec` / `PseudonymExtensionSpec` | Đổi tên cột (`pseudonym_id`), null; khoá không có trong `EXPLAIN`; SQL ≡ DataFrame API; `pseudo_reveal` tách extension |
| `ReidentifierSpec` | Luồng ưu đãi Viettel++: danh sách `pseudonym_id` → đúng `customer_id`, cả 3 chế độ |
| `KeyNamesSpec` | Tên khoá `pseudonym.hmac_v1` / `aes_siv_v1` hợp lệ với cả nguồn file, Vault và DAK (DAK chỉ nhận `<db>.<table>`) |
| `PseudonymizeTableAppSpec` (pseudonymize-table-app) | Chạy app thật: nguồn bảng và nguồn file (txt/csv/parquet/json), HASH/ENCRYPT/MAPPING, mỗi cột một chế độ, `append`, DB đích khác, không sửa nguồn, exit 1/2 |
| `PseudonymValidatorSpec` (utils) | `selftest` HASH/ENCRYPT trên dữ liệu tự sinh → `PASS`; bảng đúng → `PASS`; sót giá trị gốc, 2 giá trị gộp 1 token, null bị thay, sai định dạng, cột khác bị sửa, lệch số dòng, thiếu cột → `FAIL`; CLI exit 0/1/2 |

Image, manifest và cách chạy trên K8s: [`pseudonymize-table-app/README.md`](../../pseudonymize-table-app/README.md) (khử định danh bảng), [`pseudonym-verify-utils/README.md`](../../pseudonym-verify-utils/README.md) (kiểm tra bảng đích).

Kiểm tra jar trước khi dùng — thiếu bước này patch có thể **âm thầm** không hoạt động:

```bash
JAR=spark-app/target/sample-spark-application-privacy-1.0-SNAPSHOT.jar

unzip -p $JAR META-INF/services/org.apache.hadoop.security.token.TokenIdentifier
# Phải in ra: vai.lakehouse.hdfs.LenientBlockTokenIdentifier

unzip -l $JAR | grep LenientBlockToken
# Phải thấy: vai/lakehouse/hdfs/LenientBlockTokenIdentifier.class
```

Lưu ý build: `maven-shade-plugin` **bắt buộc** có `ServicesResourceTransformer` (giữ file đăng
ký ServiceLoader) và **không được relocate** `org.apache.hadoop.*` (class con sẽ kế thừa sai
class thật của runtime).

## Build & push image

`Dockerfile` (root project) là **multi-stage**, build trên `apache/spark:3.5.1-scala2.12-java11-ubuntu`
(tag **phải** có hậu tố `-ubuntu`). Stage `base` chứa app + log4j2; mỗi target cuối thêm (hoặc không) các jar lib crypto
rời vào `/opt/app/`. Chọn biến thể bằng `--target`:

| Target                     | Thêm vào image                                                              | Manifest đi kèm                     |
| -------------------------- | --------------------------------------------------------------------------- | ----------------------------------- |
| `no-crypto` (mặc định)     | không                                                                       | (bỏ các biến `CRYPTO_*_FUNCTION`)   |
| `column-crypto`            | `key-prefix-lib` + `column-crypto-lib`                                      | `k8s/spark-application-column.yaml` |
| `cdr-crypto`               | `key-prefix-lib` + `cdr-crypto-udf` + `DataLakeSecurity_jv8.jar` (jar đối tác) | `k8s/spark-application-cdr.yaml`    |

```bash
mvn -B clean package                                   # spark-app + key-prefix-lib + column-crypto-lib

# target column-crypto
docker build --target column-crypto -t hub.vtcc.vn:8989/sample-spark-application-privacy:v1-column-crypto .
docker push hub.vtcc.vn:8989/sample-spark-application-privacy:v1-column-crypto

# target cdr-crypto — cần thêm cdr-crypto-udf (mục "Build & test `cdr-crypto-udf`" bên dưới) và DataLakeSecurity_jv8.jar ở thư mục gốc
mvn -Pcdr-crypto -pl cdr-crypto-udf -am clean package -DskipTests
docker build --target cdr-crypto -t hub.vtcc.vn:8989/sample-spark-application-privacy:v1-cdr-crypto .
docker push hub.vtcc.vn:8989/sample-spark-application-privacy:v1-cdr-crypto
```

### Image của từng app riêng

Mỗi app dưới đây có `Dockerfile` riêng, **build context = gốc repo** (trừ `dak-mock`), build jar trước rồi `docker build`:

| App | Build jar | Build image | Chi tiết |
| --- | --- | --- | --- |
| `pseudonymize-table-app` | `mvn -B -pl pseudonymize-table-app -am clean package -DskipTests` | `docker build -f pseudonymize-table-app/Dockerfile -t <registry>/pseudonymize-table-app:<tag> .` | [README](../../pseudonymize-table-app/README.md) |
| `silver-ingest-app` | `mvn -B -Pcdr-crypto -pl silver-ingest-app -am clean package -DskipTests` | `docker build -f silver-ingest-app/Dockerfile -t <registry>/silver-ingest-app:<tag> .` | [README](../../silver-ingest-app/README.md); cần `DataLakeSecurity_jv8.jar` ở thư mục gốc; đổi tag thì sửa `IMAGE` trong `airflow/dags/silver_ingest_common.py` |
| `hdfs-load-stress-test` | `mvn -B -pl hdfs-load-stress-test -am clean package -DskipTests` | `docker build -t <registry>/hdfs-load-stress-test:<tag> hdfs-load-stress-test` | [README](../../hdfs-load-stress-test/README.md) |
| `dak-mock` | `mvn -B -f dak-mock/pom.xml clean package` (cách B) | `docker build -f dak-mock/Dockerfile.jar -t <registry>/dak-mock:<tag> dak-mock` (hoặc `docker build -t … dak-mock` để build trọn trong Docker) | [README mục 6.1](../../dak-mock/README.md) |

Fat jar của hai app dùng lib (`pseudonymize-table-app`, `silver-ingest-app`) **không** chứa lib crypto: image copy các jar lib rời vào `/opt/app` và DAG/manifest
nạp qua `spark.jars` (xem Dockerfile của từng app). DAG Airflow nằm ở [`airflow/dags/`](../../airflow/dags/), không đóng vào image.

Tên/tag trên là giả định theo convention `hub.vtcc.vn:8989/<tên>:<tag>` — đổi cho khớp
registry thực tế và sửa `image:` trong manifest tương ứng. Image nào chứa jar nào thì manifest phải khớp
(`spark.jars` chỉ tới `local:///opt/app/<jar>` có thật trong image); jar đối tác chỉ COPY nguyên trạng, không repackage.

- **Jar path phải khớp CRD**: `mainApplicationFile` dùng scheme `local://` nên Spark Operator
  không upload jar — jar phải nằm đúng `/opt/app/sample-spark-application-privacy-1.0-SNAPSHOT.jar`
  trong image.
- **Không bake bí mật vào image**: `krb5.conf` (`/etc/krb5.conf`) và keytab
  (`/etc/security/keytabs/k8s.keytab`) phải được mount vào pod lúc chạy (Secret/ConfigMap qua
  `volumes`/`volumeMounts` trong CRD). Thiếu mount thì job fail ngay ở bước Kerberos login,
  trước cả khi chạm tới HDFS.
- **Permission**: mỗi target cuối `chmod -R 777` lên `/opt/app`, `/opt/spark`, `/tmp` nhưng giữ
  `USER 185` (không set root) — để dù `securityContext` của cluster ép UID/GID khác thì
  Spark vẫn đọc/ghi được, mà không vi phạm `runAsNonRoot` của OpenShift / Pod Security
  Admission "restricted".
