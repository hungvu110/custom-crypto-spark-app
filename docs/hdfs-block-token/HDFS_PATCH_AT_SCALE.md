# Dùng patch `vai.lakehouse.hdfs` cho nhiều SparkApplication (quy mô ~100 app)

> Đọc [`SPARK_HDFS_WIRE_ENCRYPTION_TASK.md`](./SPARK_HDFS_WIRE_ENCRYPTION_TASK.md)
> trước nếu chưa hiểu patch này giải quyết vấn đề gì. File này chỉ bàn về
> **cách phân phối** patch đó tới nhiều app, không nhắc lại root cause.
>
> Mục 1–7 dưới đây bàn tổng quát (áp dụng cho cả polyrepo lẫn monorepo).
> Nếu quyết định gom 100 app vào **1 monorepo** (mỗi app 1 `mainClass` riêng)
> — bối cảnh cụ thể của dự án này — nhảy thẳng tới
> **[mục 8](#8-kiến-trúc-cụ-thể-khi-100-app-nằm-chung-1-monorepo)**, đó là kế
> hoạch triển khai cụ thể, không phải phần lý thuyết chung.

## 1. Vấn đề

Hiện tại patch (`LenientBlockTokenIdentifier`, `BlockTokenDiagnostics`,
`BlockTokenFixPlugin` — package `vai.lakehouse.hdfs`) nằm **trong cùng jar**
với business logic của `sample-spark-application-privacy` (đóng gói chung qua
maven-shade-plugin). Cách này ổn cho 1 app, nhưng **không scale** cho ~100
SparkApplication khác nhau (nhiều team, nhiều repo, nhiều pipeline CI riêng):

- Copy 3 file `.scala` + `META-INF/services` vào 100 repo → khi Dell fix
  OneFS (mục 17 của plan gốc) phải sửa 100 chỗ để gỡ patch.
- Mỗi team tự cấu hình `maven-shade-plugin` với `ServicesResourceTransformer`
  — dễ quên, và khi quên thì **lỗi âm thầm** (job vẫn build được, chỉ crash
  lúc chạy thật trên cluster).
- Không có 1 nguồn chân lý (source of truth) để biết bản patch nào đang chạy
  ở app nào.

## 2. Hai chiến lược

|                         | Chiến lược A — Shared Maven artifact (ServiceLoader)                                                                                         | Chiến lược B — Shared jar + SparkPlugin (khuyến nghị)                                                                                        |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Cơ chế                  | Mỗi app `mvn dependency` vào 1 artifact chung, tự đóng vào fat jar riêng                                                                     | 1 jar patch dùng chung, bake sẵn vào base Docker image, bật qua `spark.plugins`                                                              |
| Đổi gì ở mỗi app        | `pom.xml` (thêm dependency + đảm bảo shade config đúng)                                                                                      | **Không cần đổi gì** trong code/pom của app                                                                                                  |
| Rủi ro thứ tự classpath | Vẫn phải đảm bảo jar app được Spark APPEND sau Hadoop (`spark.jars`/`mainApplicationFile`, không dùng `extraClassPath`) — mục 4 của plan gốc | Không phụ thuộc thứ tự classpath — Plugin ghi đè `tokenKindMap` bằng reflection lúc Spark khởi động, không qua ServiceLoader last-write-wins |
| Rollout cho 100 app     | Phải sửa 100 `pom.xml`, rebuild 100 jar                                                                                                      | Sửa 1 base image + 1 dòng `spark.plugins` (có thể set default, xem mục 4)                                                                    |
| Khi Dell fix OneFS      | Gỡ dependency ở 100 `pom.xml`, rebuild 100 jar                                                                                               | Đổi 1 base image / bỏ 1 dòng default                                                                                                         |
| Yêu cầu hạ tầng         | Cần Nexus/Artifactory nội bộ để publish artifact                                                                                             | Chỉ cần registry Docker đã có sẵn                                                                                                            |

**Khuyến nghị: Chiến lược B.** Lý do quan trọng nhất không phải là "ít việc
hơn" mà là **loại bỏ hẳn rủi ro thứ tự classpath** ở quy mô 100 app — với
ServiceLoader (Chiến lược A), CHỈ CẦN 1 trong 100 app lỡ set
`userClassPathFirst: true` hoặc dùng `extraClassPath` là patch mất tác dụng
mà không có gì báo lỗi lúc build. Với SparkPlugin, việc đó không xảy ra vì
Spark nạp plugin bằng `Class.forName` theo tên cấu hình, không phụ thuộc vị
trí trên classpath.

Chiến lược A vẫn có chỗ dùng: khi **không kiểm soát được base image dùng
chung** (mỗi team tự build image riêng, không qua 1 pipeline hạ tầng chung).
Xem mục 5.

---

## 3. Triển khai Chiến lược B (khuyến nghị)

### Bước 1 — Tách patch thành 1 jar độc lập, không kèm business logic

Patch chỉ cần 3 file, tách khỏi `sample-spark-application-privacy`:

```
vai/lakehouse/hdfs/LenientBlockTokenIdentifier.scala
vai/lakehouse/hdfs/BlockTokenDiagnostics.scala
vai/lakehouse/hdfs/BlockTokenFixPlugin.scala
META-INF/services/org.apache.hadoop.security.token.TokenIdentifier
```

**Cách nhanh (không cần dựng repo/Nexus mới ngay)** — lấy trực tiếp từ
`target/classes` đã build sẵn của project này:

```bash
cd /home/hungvt/project/custom-kms-spark-app
mvn -B clean package   # đảm bảo target/classes mới nhất

PATCH_DIR=/tmp/vai-lakehouse-hdfs-compat
rm -rf "$PATCH_DIR" && mkdir -p "$PATCH_DIR/vai/lakehouse/hdfs" "$PATCH_DIR/META-INF/services"

cp target/classes/vai/lakehouse/hdfs/*.class "$PATCH_DIR/vai/lakehouse/hdfs/"
cp target/classes/META-INF/services/org.apache.hadoop.security.token.TokenIdentifier \
   "$PATCH_DIR/META-INF/services/"

jar cf vai-lakehouse-hdfs-compat-1.0.0.jar -C "$PATCH_DIR" .

# Verify — phải in đúng danh sách 5 class file + 1 service file:
unzip -l vai-lakehouse-hdfs-compat-1.0.0.jar
```

**Cách đúng lâu dài** — tách thành 1 Maven module riêng
(`vai-lakehouse-hdfs-compat`, groupId `vai.lakehouse`), publish lên
Nexus/Artifactory nội bộ qua CI, versioning bình thường (semver). Từ đó CI
build của 100 app KHÔNG cần biết tới module này — chỉ base image cần.

### Bước 2 — Bake jar patch vào 1 base Docker image dùng chung

Tất cả 100 `Dockerfile` của các SparkApplication nên `FROM` **1 base image
chung** (không phải `apache/spark:...` thẳng từng app một) — nếu hiện tại
chưa có, đây là lúc tạo:

```dockerfile
# Dockerfile.base — build 1 lần, dùng chung cho tất cả app trong hệ thống.
FROM apache/spark:3.5.1-scala2.12-java11-ubuntu

USER root

# Bake patch vào $SPARK_HOME/jars/ -> tự động nằm trên classpath của
# MỌI SparkApplication dùng base image này, không cần khai spark.jars riêng.
COPY vai-lakehouse-hdfs-compat-1.0.0.jar $SPARK_HOME/jars/

# Bật patch làm MẶC ĐỊNH cho mọi app dùng base image này — xem mục 4 về
# giới hạn của cách này (app tự set lại spark.plugins sẽ ghi đè, không cộng dồn).
RUN echo "spark.plugins vai.lakehouse.hdfs.BlockTokenFixPlugin" \
      >> $SPARK_HOME/conf/spark-defaults.conf

RUN chmod -R 777 $SPARK_HOME/jars $SPARK_HOME/conf /tmp

USER 185
```

```bash
docker build -f Dockerfile.base -t hub.vtcc.vn:8989/spark-base-hdfs-compat:1.0.0 .
docker push hub.vtcc.vn:8989/spark-base-hdfs-compat:1.0.0
```

Mỗi app (100 app) đổi dòng `FROM` trong `Dockerfile` riêng của họ thành:

```dockerfile
FROM hub.vtcc.vn:8989/spark-base-hdfs-compat:1.0.0
# ... phần còn lại giữ nguyên (chỉ COPY jar business logic của app đó)
```

Đây là **thay đổi duy nhất** mỗi app cần làm — 1 dòng `FROM`, không đụng tới
`pom.xml`, không đụng tới code Scala/Java của app.

### Bước 3 — Với app KHÔNG dùng `spark-defaults.conf` mặc định

Nếu 1 app tự khai `spark.plugins` cho mục đích khác (ví dụ dùng thêm plugin
observability), giá trị trong `sparkConf` của CRD sẽ **ghi đè hoàn toàn**
default trong `spark-defaults.conf` (không cộng dồn). App đó phải tự thêm
FQN patch vào danh sách của mình, phân tách bằng dấu phẩy:

```yaml
sparkConf:
  spark.plugins: vai.lakehouse.hdfs.BlockTokenFixPlugin,com.example.other.SomePlugin
```

Nếu không dùng `spark-defaults.conf` (ví dụ base image không set default, chỉ
bake jar), MỌI app đều phải tự khai dòng này trong CRD của mình:

```yaml
sparkConf:
  spark.plugins: vai.lakehouse.hdfs.BlockTokenFixPlugin
```

### Bước 4 — Verify (áp dụng cho từng app, hoặc lấy mẫu vài app đại diện)

Giống hệt mục 14 của `SPARK_HDFS_WIRE_ENCRYPTION_TASK.md`, chỉ khác câu lệnh
grep log một chút vì giờ chạy qua Plugin thay vì ServiceLoader:

```bash
kubectl logs <driver-pod> -n <namespace> | grep -E "BlockTokenFixPlugin|tokenKindMap"
# Phải thấy: [BlockTokenFixPlugin] Đã inject LenientBlockTokenIdentifier vào tokenKindMap
```

Section `HDFS SECURITY DIAGNOSTICS` do `BlockTokenDiagnostics` log ra (nếu
app có gọi, như `sample-spark-application-privacy`) vẫn hoạt động y hệt vì
class không đổi, chỉ đổi cách nó được nạp vào JVM.

---

## 4. Giới hạn cần biết của `spark-defaults.conf` làm default toàn cụm

- `spark.plugins` trong CRD (`sparkConf`) **ghi đè**, không **merge** với giá
  trị trong `spark-defaults.conf`. Nếu 1 app tự set `spark.plugins` mà quên
  thêm `vai.lakehouse.hdfs.BlockTokenFixPlugin`, app đó **âm thầm mất patch**
  dù dùng đúng base image. Đây là lý do nên có 1 checklist/lint kiểm tra CRD
  trước khi apply (xem mục 6).
- Nếu hạ tầng dùng Spark Operator với khả năng set default `sparkConf` cho
  toàn namespace/cluster (một số bản Spark Operator hỗ trợ
  `sparkApplicationDefaults` hoặc tương đương qua Helm values của operator) —
  ưu tiên set patch ở tầng đó thay vì `spark-defaults.conf` trong image, vì
  nó merge theo namespace thay vì bị ghi đè toàn bộ bởi `sparkConf` của từng
  app. Kiểm tra version Spark Operator đang dùng trước khi áp dụng.

---

## 5. Chiến lược A — Shared Maven artifact (khi không kiểm soát được base image chung)

Dùng khi mỗi team tự build image riêng, không đi qua 1 base image chung.

1. Publish module `vai-lakehouse-hdfs-compat` (xem Bước 1 ở trên) lên
   Nexus/Artifactory nội bộ, ví dụ:

   ```xml
   <dependency>
     <groupId>vai.lakehouse</groupId>
     <artifactId>hdfs-compat</artifactId>
     <version>1.0.0</version>
     <!-- provided: patch chỉ cần lúc runtime cùng Spark/Hadoop của app,
          không đóng gói version riêng của Spark/Hadoop vào lại -->
     <scope>compile</scope>
   </dependency>
   ```

2. Mỗi app **bắt buộc** phải có `maven-shade-plugin` với
   `ServicesResourceTransformer` trong `pom.xml` (xem `pom.xml` của
   `sample-spark-application-privacy` làm mẫu) — thiếu transformer này thì
   file `META-INF/services` của dependency bị merge sai/mất, patch vô hiệu
   mà build vẫn xanh.

3. Mỗi app **bắt buộc** submit qua `spark.jars`/`mainApplicationFile`
   (append sau Hadoop), **không** dùng `spark.*.extraClassPath` (prepend —
   patch thua last-write-wins) và **không** bật `userClassPathFirst=true`.
   Đây chính xác là các ràng buộc ở mục 4 và mục 16 của
   `SPARK_HDFS_WIRE_ENCRYPTION_TASK.md`, giờ áp dụng cho từng app trong 100
   app thay vì chỉ 1 app.

Rủi ro của cách này ở quy mô 100 app: 3 điều kiện trên (shade transformer,
append classpath, không bật userClassPathFirst) phải đúng **ở cả 100 app**,
và sai ở bất kỳ app nào cũng không có gì báo lỗi lúc build — chỉ lộ ra khi
job chạy thật và crash giống hệt lỗi gốc trong `SPARK_HDFS_WIRE_ENCRYPTION_TASK.md`
mục 2.

---

## 6. Checklist khi thêm 1 app mới vào hệ thống (100+1)

- [ ] `Dockerfile` của app `FROM` đúng base image dùng chung có patch
      (Chiến lược B), hoặc có dependency + shade config đúng (Chiến lược A).
- [ ] CRD của app dùng `mainApplicationFile`/`spark.jars` để nạp jar app,
      **không** dùng `spark.driver.extraClassPath` /
      `spark.executor.extraClassPath` cho bất kỳ jar liên quan tới patch.
- [ ] `spark.driver.userClassPathFirst` và `spark.executor.userClassPathFirst`
      là `"false"` (mặc định) hoặc không được set.
- [ ] Nếu app tự khai `spark.plugins` cho mục đích khác — đã thêm
      `vai.lakehouse.hdfs.BlockTokenFixPlugin` vào danh sách, phân tách bằng
      dấu phẩy (không ghi đè mất).
- [ ] Verify 1 lần theo mục 3 Bước 4 trước khi coi app đó "đã có patch".

---

## 7. Rollback ở quy mô 100 app (khi Dell fix OneFS)

Xem mục 17 của `SPARK_HDFS_WIRE_ENCRYPTION_TASK.md` cho bối cảnh. Ở quy mô
100 app, lợi thế của Chiến lược B thể hiện rõ nhất ở bước này:

- **Chiến lược B**: build lại `Dockerfile.base` KHÔNG bake jar patch nữa
  (hoặc bỏ dòng `spark.plugins` default), push đè lên cùng tag/tag mới. 100
  app không cần đổi gì nếu dùng tag cũ đã gỡ patch, hoặc chỉ cần bump 1 số
  tag base image nếu dùng semver riêng cho base.
- **Chiến lược A**: phải gỡ dependency khỏi 100 `pom.xml`, rebuild và
  re-deploy 100 jar riêng biệt.

---

## 8. Kiến trúc cụ thể khi 100 app nằm chung 1 monorepo

Bối cảnh: tất cả ~100 Spark app nằm trong **1 repo Git duy nhất**, mỗi app có
`mainClass` riêng. Đây vẫn là **Chiến lược B** (mục 2–4), nhưng monorepo giải
quyết luôn nhược điểm lớn nhất của Chiến lược A (version skew giữa các app) —
vì tất cả build từ cùng 1 commit, cùng 1 reactor. Kế hoạch dưới đây tận dụng
điều đó.

### 8.1. Cấu trúc thư mục

```
spark-apps/                                        <- root monorepo
├── pom.xml                                        # parent, packaging=pom
├── modules/
│   ├── hdfs-compat/                                # patch dùng chung
│   │   ├── pom.xml
│   │   └── src/main/
│   │       ├── scala/vai/lakehouse/hdfs/
│   │       │   ├── LenientBlockTokenIdentifier.scala
│   │       │   ├── BlockTokenDiagnostics.scala
│   │       │   └── BlockTokenFixPlugin.scala
│   │       └── resources/META-INF/services/
│   │           └── org.apache.hadoop.security.token.TokenIdentifier
│   └── apps/
│       ├── sample-spark-application-privacy/       # app hiện tại -> app #1
│       │   ├── pom.xml                             # mainClass riêng
│       │   └── src/main/scala/org/example/...
│       ├── app-002-<ten>/
│       ├── app-003-<ten>/
│       └── ...                                     # tới app-100
├── docker/
│   ├── base/Dockerfile                             # build 1 lần, bake hdfs-compat
│   └── app/Dockerfile                              # 1 file DÙNG CHUNG cho mọi app
├── k8s/
│   ├── base/                                        # Kustomize base / Helm common
│   └── apps/
│       ├── sample-spark-application-privacy.yaml
│       ├── app-002-<ten>.yaml
│       └── ...
├── scripts/
│   ├── new-app.sh                                  # scaffold 1 app module + CRD
│   ├── changed-modules.sh                          # git diff -> module nào đổi
│   └── build-and-push.sh                           # build + docker build/push 1 app
├── HDFS_PATCH_AT_SCALE.md
└── SPARK_HDFS_WIRE_ENCRYPTION_TASK.md
```

Nguyên tắc quan trọng nhất: **`hdfs-compat` không nằm trong fat jar của 100
app**. Nó chỉ được build 1 lần rồi bake vào 1 base Docker image dùng chung
(giống mục 3, nhưng giờ cả patch lẫn 100 app đều ở chung 1 repo/1 lần
`mvn install` của reactor — không cần Nexus).

### 8.2. `pom.xml` gốc (parent) — mẫu

```xml
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>vai.lakehouse</groupId>
  <artifactId>spark-apps-parent</artifactId>
  <version>1.0.0</version>
  <packaging>pom</packaging>

  <modules>
    <module>modules/hdfs-compat</module>
    <module>modules/apps/sample-spark-application-privacy</module>
    <module>modules/apps/app-002-ten</module>
    <!-- ... 98 dòng <module> còn lại -->
  </modules>

  <properties>
    <scala.version>2.12.18</scala.version>
    <scala.binary.version>2.12</scala.binary.version>
    <spark.version>3.5.1</spark.version>
    <hadoop.version>3.3.4</hadoop.version>
  </properties>

  <dependencyManagement>
    <dependencies>
      <!-- QUAN TRỌNG: scope provided ở đây -> app nào dependency vào
           hdfs-compat để gọi BlockTokenDiagnostics cho mục đích log KHÔNG
           bị shade nhầm vào fat jar của app (xem mục 8.4). -->
      <dependency>
        <groupId>vai.lakehouse</groupId>
        <artifactId>hdfs-compat</artifactId>
        <version>${project.version}</version>
        <scope>provided</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <build>
    <pluginManagement>
      <plugins>
        <!-- scala-maven-plugin, maven-shade-plugin cấu hình chung như
             pom.xml hiện tại của sample-spark-application-privacy — mỗi app
             module chỉ cần khai <mainClass> riêng trong <executions>. -->
      </plugins>
    </pluginManagement>
  </build>
</project>
```

### 8.3. `modules/hdfs-compat/pom.xml` — module patch

Giống hệt nội dung `vai.lakehouse.hdfs.*` hiện có, tách thành module riêng,
**không** cần `maven-shade-plugin` (không đóng fat jar, chỉ build jar thuần —
Spark/Hadoop vẫn `provided` vì patch chỉ cần API compile-time của chúng).

### 8.4. Mỗi app module — điểm khác biệt duy nhất so với hiện tại

`sample-spark-application-privacy/pom.xml` (và 99 app khác) **không còn**
đóng gói `vai.lakehouse.hdfs.*` vào fat jar của mình. Nếu app muốn tự log
trạng thái patch (như `SparkApp.reportHdfsSecurity` đang làm qua
`BlockTokenDiagnostics`), thêm dependency **scope `provided`** (đã set default
`provided` ở `dependencyManagement` của parent, mục 8.2):

```xml
<dependency>
  <groupId>vai.lakehouse</groupId>
  <artifactId>hdfs-compat</artifactId>
</dependency>
```

`provided` nghĩa là: compile được lời gọi `BlockTokenDiagnostics.isPatchActive()`,
nhưng **maven-shade-plugin không đóng class này vào fat jar** — lúc chạy
thật, class đã có sẵn trong `$SPARK_HOME/jars/` của base image (mục 8.5), nên
không cần đóng gói lại. App không cần dependency này nếu không cần tự log —
patch vẫn hoạt động (được bật qua `spark.plugins`, độc lập với việc app có
biết tới nó hay không).

**Đảo ngược checklist verify so với 1 app đơn lẻ**: trước đây (mục 12 của
`SPARK_HDFS_WIRE_ENCRYPTION_TASK.md`) checklist là "jar app PHẢI CHỨA
`vai/lakehouse/hdfs/LenientBlockTokenIdentifier.class`". Trong kiến trúc
monorepo này, checklist **đảo ngược**: jar của 100 app **KHÔNG ĐƯỢC CHỨA**
`vai/lakehouse/hdfs/*.class` (trừ khi cố tình để `provided` bị cấu hình sai
thành `compile`). Thêm bước lint vào CI:

```bash
# Phải rỗng — nếu có output nghĩa là 1 app nào đó lỡ để scope compile.
unzip -l target/<app>-*.jar | grep "vai/lakehouse/hdfs/.*\.class"
```

### 8.5. `docker/base/Dockerfile` — build 1 lần cho cả 100 app

```dockerfile
FROM apache/spark:3.5.1-scala2.12-java11-ubuntu
USER root

COPY modules/hdfs-compat/target/hdfs-compat-1.0.0.jar $SPARK_HOME/jars/

RUN echo "spark.plugins vai.lakehouse.hdfs.BlockTokenFixPlugin" \
      >> $SPARK_HOME/conf/spark-defaults.conf

RUN chmod -R 777 $SPARK_HOME/jars $SPARK_HOME/conf /tmp
USER 185
```

```bash
mvn -pl modules/hdfs-compat -am package
docker build -f docker/base/Dockerfile -t hub.vtcc.vn:8989/spark-base-hdfs-compat:1.0.0 .
docker push hub.vtcc.vn:8989/spark-base-hdfs-compat:1.0.0
```

Chỉ build lại khi `modules/hdfs-compat/` đổi — KHÔNG phụ thuộc 100 app có
đổi hay không.

### 8.6. `docker/app/Dockerfile` — 1 file dùng chung cho mọi app

```dockerfile
ARG BASE_IMAGE=hub.vtcc.vn:8989/spark-base-hdfs-compat:1.0.0
FROM ${BASE_IMAGE}

ARG APP_JAR
COPY ${APP_JAR} /opt/app/app.jar
RUN chmod 644 /opt/app/app.jar
```

Build cho 1 app cụ thể:

```bash
mvn -pl modules/apps/sample-spark-application-privacy -am package

docker build \
  --build-arg APP_JAR=modules/apps/sample-spark-application-privacy/target/sample-spark-application-privacy-1.0-SNAPSHOT.jar \
  -t hub.vtcc.vn:8989/sample-spark-application-privacy:v1 \
  -f docker/app/Dockerfile .

docker push hub.vtcc.vn:8989/sample-spark-application-privacy:v1
```

Vì `Dockerfile` luôn rename jar về `/opt/app/app.jar`, field
`mainApplicationFile` trong **mọi** CRD của 100 app đều **giống hệt nhau**:

```yaml
mainApplicationFile: local:///opt/app/app.jar
```

Chỉ `image`, `mainClass`, `metadata.name`, `arguments` khác nhau giữa các
app — đúng phần "mỗi app có mainClass riêng" mà bạn mô tả.

### 8.7. K8s manifest cho 100 app — tránh lặp lại 90% nội dung

`k8s/spark-application.yaml` hiện tại (~90 dòng) có tới ~80 dòng giống hệt
nhau nếu nhân ra 100 app (toàn bộ `sparkConf` Kerberos/HDFS/security,
`driver`/`executor` sizing mặc định, `restartPolicy`, `hadoopConfigMap`...).
Chỉ thực sự khác nhau: `metadata.name`, `spec.image`, `spec.mainClass`,
`spec.arguments`.

Khuyến nghị dùng **Kustomize** (không cần cài thêm gì ngoài `kubectl`, có
sẵn từ `kubectl kustomize`):

```
k8s/
├── base/
│   ├── kustomization.yaml
│   └── spark-application.yaml     # bản hiện tại, dùng placeholder qua patch
└── apps/
    ├── sample-spark-application-privacy/
    │   └── kustomization.yaml     # patch 4 field khác nhau
    └── app-002-ten/
        └── kustomization.yaml
```

`k8s/apps/sample-spark-application-privacy/kustomization.yaml`:

```yaml
resources:
  - ../../base
patches:
  - target:
      kind: SparkApplication
      name: PLACEHOLDER
    patch: |-
      - op: replace
        path: /metadata/name
        value: sample-spark-application-privacy
      - op: replace
        path: /spec/image
        value: hub.vtcc.vn:8989/sample-spark-application-privacy:v1
      - op: replace
        path: /spec/mainClass
        value: org.example.SparkApp
      - op: replace
        path: /spec/arguments
        value: ["2026", "09", "09", "all"]
```

Deploy 1 app: `kubectl apply -k k8s/apps/sample-spark-application-privacy/`.
Sửa `sparkConf` chung (ví dụ đổi Kerberos principal) → sửa 1 chỗ ở
`k8s/base/spark-application.yaml`, tự áp dụng cho cả 100 app ở lần apply kế
tiếp.

Nếu không muốn học Kustomize ngay, phương án tạm: 1 script
`scripts/render-crd.sh <app-name> <mainClass> <image-tag>` dùng `envsubst`
render từ 1 file template — kém "đúng chuẩn" hơn Kustomize nhưng triển khai
được trong 1 buổi.

### 8.8. CI/CD — chỉ build lại app nào thực sự đổi

Monorepo 100 module mà build lại toàn bộ mỗi lần push là lãng phí. Cơ chế
tối thiểu (không phụ thuộc CI cụ thể — GitHub Actions/GitLab CI/Jenkins đều
áp dụng được cùng logic):

1. **Xác định module đổi**: `git diff --name-only <base-sha> <head-sha>`,
   map path `modules/apps/<name>/...` → tên app đổi; path
   `modules/hdfs-compat/...` đổi → đánh dấu "cần rebuild base image".
2. **`hdfs-compat` đổi** → chạy lại mục 8.5 (build + push base image mới,
   ví dụ bump tag `1.0.0` → `1.1.0`). Sau đó **không bắt buộc** rebuild 100
   app image ngay — app nào build lại lần sau sẽ tự lấy base image mới nếu
   `ARG BASE_IMAGE` trỏ tag mới nhất; nếu cần "vá nóng" cả 100 app cùng lúc,
   chạy job riêng lặp `docker/app/Dockerfile` cho cả 100 app với
   `--build-arg BASE_IMAGE=...:1.1.0`.
3. **App module đổi** → chỉ với app đó: `mvn -pl modules/apps/<name> package`
   → `docker build/push` (mục 8.6) → `kubectl apply -k k8s/apps/<name>/`.
4. Reactor Maven hỗ trợ sẵn build có chọn lọc bằng `-pl`/`-amd`/`-am`, không
   cần công cụ ngoài (Bazel, Nx, Turborepo...) trừ khi sau này thấy CI quá
   chậm vì `mvn` resolve dependency chậm ở quy mô lớn hơn.

### 8.9. Kế hoạch di chuyển `sample-spark-application-privacy` thành app #1

Các bước cụ thể để biến project hiện tại thành module đầu tiên của monorepo:

1. Tạo repo/khung monorepo theo cây thư mục ở mục 8.1 (parent `pom.xml` mục
   8.2, `modules/hdfs-compat` mục 8.3).
2. `git mv src/main/scala/vai/lakehouse/hdfs` →
   `modules/hdfs-compat/src/main/scala/vai/lakehouse/hdfs`; tương tự
   `META-INF/services/...`.
3. `git mv` phần còn lại (`src/main/scala/org/example/*`,
   `src/main/resources/conf`, `src/main/resources/log4j2.properties`,
   `src/test/...`) → `modules/apps/sample-spark-application-privacy/`.
4. Sửa `pom.xml` của module app: xoá phần cấu hình Spark/Hadoop/scala-maven
   version (kế thừa từ parent), giữ lại `<mainClass>org.example.SparkApp</mainClass>`
   trong shade config, thêm dependency `vai.lakehouse:hdfs-compat` (scope kế
   thừa `provided` từ parent) để giữ được `BlockTokenDiagnostics` cho log.
5. `mvn -pl modules/hdfs-compat,modules/apps/sample-spark-application-privacy -am clean package`
   — verify app jar **không** còn chứa `vai/lakehouse/hdfs/*.class` (mục 8.4),
   `hdfs-compat` build ra jar riêng.
6. Build `docker/base/Dockerfile` (mục 8.5), build `docker/app/Dockerfile`
   cho riêng app này (mục 8.6).
7. Chuyển `k8s/spark-application.yaml` hiện tại thành `k8s/base/` +
   `k8s/apps/sample-spark-application-privacy/kustomization.yaml` (mục 8.7).
8. Verify lại đúng mục 3 Bước 4 (grep log `BlockTokenFixPlugin` thay vì
   ServiceLoader) trước khi coi migration hoàn tất.
9. Từ app #2 trở đi, dùng `scripts/new-app.sh` để scaffold — không cần lặp
   lại thủ công bước 2–7 mỗi lần.
