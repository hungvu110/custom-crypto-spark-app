# Build và mở project bằng Nexus nội bộ (máy công ty)

Thư mục này chứa các file cần để build project (Maven) và chạy Metals trên VS Code qua Nexus nội bộ
`http://10.30.154.118:8888/repository/`. Maven và VS Code **không** tự nạp thư mục này, nên nó không ảnh
hưởng tới build hiện tại cho tới khi các file được copy ra đúng vị trí.

Bản tương đương cho sbt nằm ở `crawler-streaming-app/internal-repo`.

## Mỗi file làm gì

| File trong thư mục này | Copy tới | Vai trò | Tương đương bên crawler (sbt) |
| --- | --- | --- | --- |
| `settings.xml` | `~/.m2/settings.xml` | Mirror `*` sang Nexus: dependency, plugin và `bloop-maven-plugin` của Metals đều tải qua Nexus | `project/repositories` + `.sbtopts` |
| `mvnw` + `.mvn/wrapper/maven-wrapper.properties` | `./mvnw`, `./.mvn/wrapper/` | Tải bản Maven 3.9.9 từ Nexus. Dùng khi máy chưa cài Maven, hoặc Maven quá cũ | `sbtw` |
| `.vscode/settings.json` | `./.vscode/settings.json` | Metals tải server/Bloop qua Nexus và import build bằng `./mvnw` | `.vscode/settings.json` |
| (trong `settings.xml`, phần `<servers>`) | `~/.m2/settings.xml` | Credential khi Nexus bắt đăng nhập, hoặc khi `mvn deploy` | `credentials.example` |
| `dak-mock/pom.xml` | `./dak-mock/pom.xml` | Pom của mock DAK, thêm `<repositories>`, `<pluginRepositories>`, `<distributionManagement>` theo tài liệu công ty | `build.sbt` (`publishTo`) |
| `dak-mock/Dockerfile` | `./dak-mock/Dockerfile` | Maven trong container dùng `settings.xml` có mirror; base image đổi được qua `--build-arg` | (crawler: `SPARK_BASE_IMAGE`) |

Đối chiếu với tài liệu công ty "Cách sử dụng Local Repository":

- **Mirror** (`internal-mirror`, `mirrorOf *`, `maven-public`): giống hệt.
- **`<repositories>`/`<pluginRepositories>` trong `pom.xml`: không thêm.** Mirror `*` đã chuyển mọi
  repo (kể cả plugin repo) sang Nexus, nên hai khối này thừa. Còn nếu không có mirror thì Maven ≥ 3.8.1 lại
  chặn chính các repo `http://` đó (lỗi "Blocked mirror"). Thêm vào cũng không hại, nhưng mirror mới là
  thứ quyết định.
- **Maven wrapper:** tài liệu dùng `mvn -N io.takari:maven:wrapper` rồi sửa `distributionUrl`. Cách đó
  cần máy đã có `mvn`. `mvnw` ở đây làm cùng việc (tải Maven từ Nexus) nhưng chạy được khi máy chưa cài
  Maven, và không còn `wrapperUrl` trỏ ra `repo.maven.apache.org`. Muốn theo đúng tài liệu thì cứ tạo bằng
  takari: Metals chỉ cần `metals.mavenScript` trỏ tới file `mvnw` đó.
- **`<distributionManagement>`:** chỉ dùng khi deploy, xem phần Publish.

## Vì sao Metals import fail nếu chỉ có `customRepositories`

Có hai đường tải tách biệt nhau:

1. **Metals server, Bloop, mtags** do Coursier tải. Cấu hình bằng `metals.coursierMirror` và
   `metals.customRepositories`.
2. **Import build** do Maven làm: Metals chạy `mvn ch.epfl.scala:bloop-maven-plugin:<ver>:bloopInstall`.
   Đường này **không** đọc các setting Coursier ở trên. Nó đọc `~/.m2/settings.xml`.
   Khi `metals.mavenScript` trống, Metals dùng Maven wrapper nhúng trong extension. Wrapper này tải
   `apache-maven-3.9.9` từ `repo.maven.apache.org`, nên trong mạng nội bộ bước import fail.
   Khi đó `.bloop/` không được tạo, và log chỉ toàn dòng
   `no build target found ... Using presentation compiler with project's scala-library version: 3.3.8`.

## Áp dụng: từng bước trên máy công ty

Chạy từ thư mục gốc project:

### Bước 0: Kiểm tra JDK

```bash
ls /usr/lib/jvm/                 # cần có JDK 17 cho Metals (vd java-17-openjdk-amd64)
java -version                    # Java mặc định 11 hoặc 17 đều được để chạy Maven
```

Nếu chưa có JDK 17: `sudo apt install openjdk-17-jdk-headless` (qua apt mirror nội bộ).

### Bước 1: Maven settings (bắt buộc)

```bash
mkdir -p ~/.m2
ls ~/.m2/settings.xml 2>/dev/null && echo "ĐÃ CÓ: gộp khối <mirrors> thay vì copy đè"
cp internal-repo/settings.xml ~/.m2/settings.xml     # chỉ khi chưa có file
```

Nexus bắt đăng nhập mới cho đọc? Thì bỏ comment `<servers>`, điền user/password vào server
`id=internal-mirror`, rồi chạy `chmod 600 ~/.m2/settings.xml`.

### Bước 2: Maven wrapper (khuyến nghị)

```bash
mkdir -p .mvn/wrapper
cp internal-repo/.mvn/wrapper/maven-wrapper.properties .mvn/wrapper/
cp internal-repo/mvnw mvnw && chmod +x mvnw
./mvnw -v                        # lần đầu tải Maven 3.9.9 từ Nexus về ~/.m2/wrapper/nexus/
```

Nếu `./mvnw -v` báo 404 (Nexus không có `apache-maven` bản `.tar.gz`), có hai cách:

- Dùng `mvn` đã cài trên máy: `sudo apt install maven`, rồi ở Bước 4 đặt `metals.mavenScript` thành
  `/usr/bin/mvn`.
- Hoặc mang file `apache-maven-3.9.9-bin.tar.gz` vào, giải nén thành
  `~/.m2/wrapper/nexus/apache-maven-3.9.9/`. `mvnw` thấy bản có sẵn sẽ không tải nữa.

### Bước 3: Build thử ngoài terminal (bắt buộc pass trước khi mở VS Code)

```bash
./mvnw -B -DskipTests clean package
```

Bước này xác nhận Nexus có đủ Spark, Delta, Scala, `scala-maven-plugin`... Lần đầu sẽ tải vài trăm MB.
Muốn kiểm tra luôn bước Metals sẽ chạy:

```bash
./mvnw -B ch.epfl.scala:bloop-maven-plugin:2.0.1:bloopInstall -DdownloadSources=true
ls .bloop/                       # phải có key-prefix-lib.json, column-crypto-lib.json, spark-app.json, ...
```

Nếu Nexus không có version `2.0.1` của plugin, xem các version đang có tại
`http://10.30.154.118:8888/repository/maven-public/ch/epfl/scala/bloop-maven-plugin/` và dùng version đó.
Metals tự chọn version của nó khi import; lệnh ở đây chỉ để kiểm tra đường tải.

### Bước 4: VS Code / Metals

```bash
mkdir -p .vscode
cp internal-repo/.vscode/settings.json .vscode/settings.json
```

Mở `.vscode/settings.json` và sửa 2 chỗ:

- `metals.mavenScript`: **path tuyệt đối** tới `mvnw` của project (vd
  `/home/<user>/projects/custom-crypto-spark-app/mvnw`), hoặc `/usr/bin/mvn` nếu không dùng wrapper.
- `metals.javaHome`: đúng thư mục JDK 17 thấy ở Bước 0.

Sau đó trong VS Code:

1. **Developer: Reload Window**.
2. **Metals: Import build**. Mở Output → Metals sẽ thấy dòng chạy `bloopInstall`.
3. Import xong thì xuất hiện `.bloop/`. Mở một file `.scala`, thử Ctrl+click vào một class Spark: phải
   nhảy được tới định nghĩa.
4. Còn lỗi thì chạy **Metals: Run Doctor**, hoặc xem log:
   ```bash
   grep -n -i "maven\|mvn\|bloop\|ERROR" .metals/metals.log | grep -v "no build target\|empty definition" | tail -60
   ```

## Mock DAK (`dak-mock`): build riêng

`dak-mock` là project Maven độc lập (parent `spring-boot-starter-parent`, Java 17). Nó **không** nằm trong
danh sách module của `pom.xml` gốc, nên `./mvnw clean package` ở gốc không build nó.

```bash
cp internal-repo/dak-mock/pom.xml    dak-mock/pom.xml
cp internal-repo/dak-mock/Dockerfile dak-mock/Dockerfile
cp internal-repo/settings.xml        dak-mock/settings.xml   # chỉ cần cho docker build; KHÔNG điền mật khẩu

# Build jar (cần JDK 17; ~/.m2/settings.xml ở Bước 1 lo mirror)
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 ./mvnw -B -f dak-mock/pom.xml clean package

# Build image (Maven chạy trong container nên cần dak-mock/settings.xml)
docker build -t hub.vtcc.vn:8989/dak-mock:0.1.0 dak-mock
# máy build không pull được Docker Hub:
#   --build-arg MAVEN_IMAGE=<registry>/maven:3.9-eclipse-temurin-17 \
#   --build-arg RUNTIME_IMAGE=<registry>/eclipse-temurin:17-jre-jammy
```

Vì sao vẫn cần mirror dù pom đã có `<repositories>`: Nexus chạy HTTP, và Maven ≥ 3.8.1 chặn repo `http://`
khai báo trong pom (lỗi "Blocked mirror for repositories"). Chỉ mirror `mirrorOf *` trong settings mới
mở được đường này. Ngoài ra Maven Central (khai báo sẵn trong Maven) vẫn còn, và Maven sẽ thử nó khi
Nexus không có artifact. Image `maven:3.9` không có `~/.m2/settings.xml`, nên Dockerfile copy
`settings.xml` vào và chạy `mvn -s settings.xml`.

Muốn deploy jar `dak-mock` lên Nexus: bỏ comment server `maven-snapshots`/`maven-releases` trong
`~/.m2/settings.xml`, rồi chạy `./mvnw -B -f dak-mock/pom.xml -DskipTests deploy`. Pom đã có
`<distributionManagement>` nên không cần `-DaltDeploymentRepository`.

## Publish lên Nexus (tuỳ chọn)

Cần bỏ comment server `maven-releases`/`maven-snapshots` trong `~/.m2/settings.xml`. Version hiện tại
là `1.0-SNAPSHOT`, nên publish vào `maven-snapshots`:

```bash
./mvnw -B -DskipTests deploy \
  -DaltDeploymentRepository=maven-snapshots::default::http://10.30.154.118:8888/repository/maven-snapshots/
```

Version không có đuôi `-SNAPSHOT` thì dùng `maven-releases::default::.../maven-releases/`.
Tài liệu công ty đặt hai repo này trong `<distributionManagement>` của `pom.xml`, với cùng id. Nếu thêm
khối đó vào pom thì chỉ cần chạy `./mvnw deploy`, không cần `-DaltDeploymentRepository`. Nexus
`maven-releases` mặc định không cho deploy đè cùng một version. Cú pháp `id::default::url` chạy được
với cả `maven-deploy-plugin` 2.x (Maven 3.8) lẫn 3.x (Maven 3.9).

## Lưu ý

- **Module `cdr-crypto-udf`** nằm trong profile `cdr-crypto`, không bật mặc định. Import và build ở trên
  không cần `DataLakeSecurity_jv8.jar`. Muốn Metals thấy cả module này thì phải `install:install-file` jar
  đối tác trước (xem `pom.xml`). Sau đó chạy `bloopInstall` tay kèm `-Pcdr-crypto`.
- **Cache đi theo URL repo.** Lần build đầu trên máy công ty tải lại toàn bộ dependency từ Nexus.
- **`maven-public` phải proxy Maven Central.** Mọi dependency và plugin của project (kể cả
  `bloop-maven-plugin`) đều có trên Central.
- **Docker:** các `Dockerfile` không chạy Maven (jar được build sẵn ở ngoài), nên không bị ảnh hưởng.
  Riêng base image `apache/spark:3.5.1-scala2.12-java11-ubuntu` lấy từ Docker Hub. Nếu máy build không
  pull được thì phải đổi sang registry nội bộ: đây là việc riêng, nằm ngoài thư mục này.

## Đã kiểm tra / chưa kiểm tra

- Đã kiểm tra (ngoài mạng công ty): `mvnw` đúng cú pháp bash. Logic tải, giải nén, dùng lại bản Maven
  đã chạy thử với URL Maven Central. `settings.xml` là XML hợp lệ, và Maven nạp được mirror `*` từ file này.
- **Chưa** kiểm tra (cần mạng công ty): tải thực tế từ Nexus, việc Nexus có `apache-maven-3.9.9-bin.tar.gz`
  và `bloop-maven-plugin` hay không, Nexus có bắt đăng nhập không, quyền deploy của tài khoản.
