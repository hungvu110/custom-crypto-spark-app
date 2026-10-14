# Task: Spark 3.5.1 ghi HDFS (Dell Isilon) với wire encryption bật

> Tài liệu này dành cho coding agent. Đọc hết phần 1–4 trước khi viết dòng code nào.
> Toàn bộ implementation bằng **Scala**. Không dùng Java, không dùng Python.

---

## 1. Bối cảnh hệ thống

| Thành phần         | Giá trị                                                                         |
| ------------------ | ------------------------------------------------------------------------------- |
| Spark              | 3.5.1 (Scala 2.12)                                                              |
| Hadoop client      | 3.3.4 (đi kèm Spark, dạng shaded `hadoop-client-api` / `hadoop-client-runtime`) |
| HDFS backend       | Dell PowerScale / Isilon OneFS **9.5.0.6** (không phải Apache HDFS)             |
| Namenode URI       | `hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020`                           |
| Auth               | Kerberos, realm `VAILAKEHOUSE.VIETTEL.COM`, principal `k8s`                     |
| Chạy trên          | Kubernetes, qua Spark Operator (`SparkApplication` CRD)                         |
| Namespace app      | `vlp-tenantw1xjixm-wsytjjtr0-ingestion`                                         |
| Namespace operator | `vlp-tenantw1xjixm-wsytjjtr0-data` (deployment `s-ops`)                         |
| Image              | `hub.vtcc.vn:8989/test_spark_kms:v1`                                            |
| Main class         | `org.example.spark_write`                                                       |

**Ràng buộc cứng:** Isilon **ép** mã hoá đường truyền (`dfs.data.transfer.protection = privacy`,
cipher `AES/CTR/NoPadding` 256-bit). Đây là yêu cầu bắt buộc từ phía tổ chức.
**KHÔNG được hạ xuống `authentication` hoặc `integrity` để né lỗi.**

---

## 2. Vấn đề

Job Spark submit thành công, driver chạy, executor bắt đầu ghi dữ liệu, rồi **fail khi mở block
output stream tới DataNode của Isilon**.

### Stack trace (rút gọn, giữ nguyên thứ tự nhân quả)

```
org.apache.spark.SparkException: [TASK_WRITE_FAILED] Task failed while writing rows to
    hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020/test_kms/test_path/20260909
    at org.apache.spark.sql.errors.QueryExecutionErrors$.taskFailedWhileWritingRowsError(QueryExecutionErrors.scala:775)
    at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:420)
    at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
    ...

Caused by: java.lang.IllegalStateException: Error closing the output.
    at com.univocity.parsers.common.AbstractWriter.close(AbstractWriter.java:1000)
    at org.apache.spark.sql.catalyst.csv.UnivocityGenerator.close(UnivocityGenerator.scala:124)
    at org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter.close(CsvOutputWriter.scala:48)
    ...

Caused by: java.io.IOException: java.lang.NegativeArraySizeException: -1
    at org.apache.hadoop.hdfs.ExceptionLastSeen.set(ExceptionLastSeen.java:45)
    at org.apache.hadoop.hdfs.DataStreamer.run(DataStreamer.java:823)

Caused by: java.lang.NegativeArraySizeException: -1
    at org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.readFieldsLegacy(BlockTokenIdentifier.java:253)
    at org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.readFields(BlockTokenIdentifier.java:232)
    at org.apache.hadoop.security.token.Token.decodeIdentifier(Token.java:172)
    at org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient.doSaslHandshake(SaslDataTransferClient.java:521)
    at org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient.getEncryptedStreams(SaslDataTransferClient.java:333)
    at org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient.send(SaslDataTransferClient.java:267)
    at org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient.checkTrustAndSend(SaslDataTransferClient.java:236)
    at org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient.socketSend(SaslDataTransferClient.java:203)
    at org.apache.hadoop.hdfs.DataStreamer.createBlockOutputStream(DataStreamer.java:1780)
    at org.apache.hadoop.hdfs.DataStreamer.nextBlockOutputStream(DataStreamer.java:1728)
    at org.apache.hadoop.hdfs.DataStreamer.run(DataStreamer.java:713)
```

### Đọc stack trace này như thế nào

Đọc **từ dưới lên**. Chuỗi nhân quả là:

1. `DataStreamer` mở block output stream tới DataNode.
2. Vì wire encryption bật, nó đi vào `SaslDataTransferClient.getEncryptedStreams()`.
3. Trong `doSaslHandshake()`, client gọi `accessToken.decodeIdentifier()` để **parse block token**.
4. `BlockTokenIdentifier.readFields()` peek byte đầu tiên, chọn nhánh `readFieldsLegacy()`.
5. `readFieldsLegacy` gọi `WritableUtils.readString()`, đọc ra length = `-1`,
   thực thi `new byte[-1]` → `NegativeArraySizeException: -1`.

---

## 3. Root cause (đã điều tra xong — KHÔNG cần điều tra lại)

Đây là **known incompatibility** giữa Hadoop client ≥ 3.2.1 và các HDFS implementation không
phải Apache (Isilon OneFS).

### Hai thay đổi upstream chồng lên nhau

| JIRA                                                                                                                            | Version      | Nội dung                                                                                                                       |
| ------------------------------------------------------------------------------------------------------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| [HDFS-11026](https://issues.apache.org/jira/browse/HDFS-11026)                                                                  | 3.0.0-alpha4 | Đổi `BlockTokenIdentifier` sang serialize bằng protobuf; giữ logic decode được cả 2 định dạng bằng cách **peek byte đầu tiên** |
| [HDFS-13617](https://issues.apache.org/jira/browse/HDFS-13617) / [HDFS-13699](https://issues.apache.org/jira/browse/HDFS-13699) | **3.2.1**    | Selective wire encryption: client bắt đầu **tự parse block token** để lấy `handshakeSecret`                                    |
| [HDFS-15191](https://issues.apache.org/jira/browse/HDFS-15191)                                                                  | —            | Case gần giống hệt: client nâng 3.2.0 → 3.2.1 thì vỡ khi nói chuyện với NameNode cũ                                            |

**Trước 3.2.1**, block token là một khối byte **đục (opaque)** với client: chỉ chuyển tiếp nguyên
xi xuống DataNode, không hề đọc nội dung. Isilon tự sinh và tự verify block token theo định dạng
nội bộ của riêng nó — chừng nào không ai parse thì không ai phát hiện khác biệt.

**Từ 3.2.1**, client Apache parse token đó → gặp byte không đúng cấu trúc Writable → crash.

### Bằng chứng thực nghiệm đã có

- Pod hdfs client dùng **Hadoop 2.7.0** (`flokkr/hadoop:2.7.0`) → **ghi được**.
- Pod hdfs client dùng **Hadoop 3.x** → **fail** đúng lỗi trên.

### Tại sao KHÔNG thể fix bằng config

Đã kiểm tra source Hadoop 3.3.4:

- `DFSClient.shouldEncryptData()` trả về `getServerDefaults().getEncryptDataTransfer()` —
  **do server quyết định hoàn toàn**. Isilon bật cipher → client bắt buộc đi nhánh encrypted.
  Không config client nào can thiệp được.
- `DataTransferSaslUtil.createSaslPropertiesForEncryption()` **hardcode**
  `Sasl.QOP = QualityOfProtection.PRIVACY` — nên `dfs.data.transfer.protection` phía client
  **bị bỏ qua** trong nhánh này.
- Lời gọi `accessToken.decodeIdentifier()` trong `doSaslHandshake` là **vô điều kiện**,
  không có config nào bọc quanh, không có try/catch.

→ Không có config Spark hay Hadoop nào giải quyết được. Phải can thiệp ở tầng class.

---

## 4. Giải pháp: ghi đè registry `tokenKindMap`

### Cơ chế

`Token.decodeIdentifier()` **không hardcode** `BlockTokenIdentifier`. Nó tra bảng.
Trong `org.apache.hadoop.security.token.Token.getClassForIdentifier()`:

```java
if (tokenKindMap == null) {
  tokenKindMap = Maps.newHashMap();
  final Iterator<TokenIdentifier> tokenIdentifiers =
      ServiceLoader.load(TokenIdentifier.class).iterator();
  while (tokenIdentifiers.hasNext()) {
    TokenIdentifier id = tokenIdentifiers.next();
    LOG.debug("Added {}:{} into tokenKindMap", id.getKind(), id.getClass());
    tokenKindMap.put(id.getKind(), id.getClass());   // <-- HashMap: LAST WRITE WINS
  }
}
cls = tokenKindMap.get(kind);
```

Đây là `HashMap.put`. **Class nào được ServiceLoader nạp sau sẽ ghi đè class nạp trước cho cùng
một `kind`.**

### Cách khai thác

Đăng ký một class con của `BlockTokenIdentifier` với:

- `readFields()` nuốt lỗi parse
- `getHandshakeMsg()` luôn trả `null`

Khi đó `doSaslHandshake` rơi vào nhánh đã có sẵn trong code Hadoop:

```java
if (handshakeSecret == null || handshakeSecret.length == 0) {
  LOG.debug("Handshake secret is null, sending without handshake secret.");
  sendSaslMessage(out, first);      // <-- nhánh này
}
```

rồi SASL handshake **tiếp tục bình thường với QOP `auth-conf`**.

### Mã hoá KHÔNG bị ảnh hưởng — điểm này phải hiểu đúng

| Thành phần                                   | Trạng thái sau khi áp dụng                     |
| -------------------------------------------- | ---------------------------------------------- |
| Mã hoá đường truyền (auth-conf, AES/CTR 256) | **Giữ nguyên**                                 |
| Kerberos authentication                      | **Giữ nguyên**                                 |
| Block token được Isilon verify               | **Giữ nguyên**                                 |
| RPC encryption tới NameNode                  | **Giữ nguyên**                                 |
| Selective-QOP override (`handshakeSecret`)   | Bỏ qua — Isilon vốn không phát hành trường này |

`decodeIdentifier()` chỉ tạo ra **bản parse để client tự đọc**. Byte token thật vẫn được gửi
nguyên vẹn xuống DataNode qua `accessToken.getIdentifier()`. Parse hỏng **không** làm sai lệch
dữ liệu trên đường truyền.

Về bản chất: khôi phục đúng hành vi của client Hadoop ≤ 3.2.0.

### Yêu cầu về thứ tự classpath — QUAN TRỌNG

Jar chứa class custom phải nằm **SAU** `hadoop-client-api` trên classpath để thắng last-write-wins.

- ✅ `spark.jars` / `mainApplicationFile` → jar người dùng được **append** sau jar hệ thống. **ĐÚNG.**
- ❌ `spark.driver.extraClassPath` / `spark.executor.extraClassPath` → **prepend**, nạp TRƯỚC Hadoop → **THUA**. Không dùng.
- ❌ `spark.driver.userClassPathFirst = true` → đảo thứ tự → **THUA**. Phải giữ `false` (mặc định).

---

## 5. Plan implement

Thực hiện theo đúng thứ tự này.

### Bước 1 — Tạo class `LenientBlockTokenIdentifier`

File: `src/main/scala/vn/viettel/vlp/hdfs/LenientBlockTokenIdentifier.scala`
Chỉ override 2 method. **Không** copy source của `BlockTokenIdentifier`.

### Bước 2 — Đăng ký qua ServiceLoader

File: `src/main/resources/META-INF/services/org.apache.hadoop.security.token.TokenIdentifier`
Nội dung: đúng một dòng, FQN của class ở bước 1.

### Bước 3 — Tạo diagnostics helper

File: `src/main/scala/vn/viettel/vlp/hdfs/BlockTokenDiagnostics.scala`
Đọc (chỉ đọc, không ghi) `tokenKindMap` bằng reflection để xác nhận class nào đang thắng.

### Bước 4 — Tạo logging helper

File: `src/main/scala/org/example/Log.scala`
Banner / section / key-value, dùng slf4j. Mục tiêu: log dễ trace bằng mắt.

### Bước 5 — Viết main app

File: `src/main/scala/org/example/spark_write.scala`
Giữ nguyên logic nghiệp vụ của bản gốc, thêm logging và HDFS diagnostics.

### Bước 6 — Cấu hình build fat jar

File: `pom.xml`
Spark + Hadoop để scope `provided`. Shade plugin **phải** có `ServicesResourceTransformer`.

### Bước 7 — Cập nhật SparkApplication CRD

Bật DEBUG cho 2 logger để verify.

### Bước 8 — Verify

Xem mục 12. **Bắt buộc làm, không được bỏ qua.**

---

## 6. Cấu trúc project

```
atspcall-bigdata-v2/
├── pom.xml
└── src/main/
    ├── resources/
    │   ├── META-INF/services/
    │   │   └── org.apache.hadoop.security.token.TokenIdentifier
    │   └── conf/config.properties
    └── scala/
        ├── org/example/
        │   ├── Log.scala
        │   └── spark_write.scala
        └── vn/viettel/vlp/hdfs/
            ├── LenientBlockTokenIdentifier.scala
            └── BlockTokenDiagnostics.scala
```

---

## 7. `LenientBlockTokenIdentifier.scala`

```scala
package vn.viettel.vlp.hdfs

import java.io.DataInput

import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier

/**
 * Dell Isilon OneFS phát hành HDFS block token theo định dạng nội bộ của nó,
 * không tương thích với parser Writable/protobuf của Apache Hadoop.
 *
 * Từ Hadoop 3.2.1 (HDFS-13617 / HDFS-13699), client tự parse block token trong
 * SASL handshake để lấy `handshakeSecret` phục vụ selective-QOP. Với token của
 * Isilon, việc parse này ném NegativeArraySizeException và làm hỏng mọi thao tác
 * ghi/đọc block khi wire encryption đang bật.
 *
 * Class này khôi phục hành vi của client Hadoop <= 3.2.0: coi block token là
 * opaque, gửi SASL message không kèm handshake secret.
 *
 * QUAN TRỌNG: mã hoá đường truyền KHÔNG bị ảnh hưởng. SASL vẫn thương lượng
 * QOP auth-conf với cipher AES/CTR 256-bit. Byte token thật vẫn được gửi nguyên
 * vẹn xuống DataNode và vẫn được Isilon verify đầy đủ.
 *
 * Class được nạp qua ServiceLoader và ghi đè entry HDFS_BLOCK_TOKEN trong
 * `Token.tokenKindMap` theo cơ chế last-write-wins.
 */
class LenientBlockTokenIdentifier extends BlockTokenIdentifier {

  override def readFields(in: DataInput): Unit = {
    try {
      super.readFields(in)
    } catch {
      case _: Throwable =>
      // Có chủ đích: định dạng token không tương thích Apache.
      // Không log ở đây — method này nằm trên hot path của mọi block I/O,
      // log sẽ làm ngập stderr. Việc xác nhận class nào đang được dùng
      // đã có BlockTokenDiagnostics lo lúc khởi động.
    }
  }

  /**
   * Luôn trả null để `SaslDataTransferClient.doSaslHandshake` đi nhánh
   * "Handshake secret is null, sending without handshake secret".
   *
   * Không phụ thuộc vào việc `super.readFields` đã set field tới đâu trước
   * khi ném lỗi.
   */
  override def getHandshakeMsg(): Array[Byte] = null
}
```

---

## 8. `META-INF/services/org.apache.hadoop.security.token.TokenIdentifier`

```
vn.viettel.vlp.hdfs.LenientBlockTokenIdentifier
```

Đúng một dòng. Không comment, không dòng trống thừa.

---

## 9. `BlockTokenDiagnostics.scala`

```scala
package vn.viettel.vlp.hdfs

import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

/**
 * Chỉ phục vụ chẩn đoán. Đọc (KHÔNG ghi) `Token.tokenKindMap` để biết class nào
 * đang thực sự được dùng khi decode HDFS_BLOCK_TOKEN.
 */
object BlockTokenDiagnostics {

  /** FQN của class đang thắng cho kind HDFS_BLOCK_TOKEN, hoặc None nếu không đọc được. */
  def registeredBlockTokenClass(): Option[String] = {
    try {
      // Ép Hadoop build tokenKindMap nếu chưa được build.
      // Token rỗng -> kind rỗng -> decodeIdentifier trả null, nhưng map đã được populate.
      try {
        new Token[TokenIdentifier]().decodeIdentifier()
      } catch {
        case _: Throwable => // bỏ qua
      }

      val field = classOf[Token[_]].getDeclaredField("tokenKindMap")
      field.setAccessible(true)

      Option(field.get(null))
        .map(_.asInstanceOf[java.util.Map[Text, Class[_]]])
        .flatMap(m => Option(m.get(BlockTokenIdentifier.KIND_NAME)))
        .map(_.getName)
    } catch {
      case _: Throwable => None
    }
  }

  /** true nếu patch đã thắng cuộc đua last-write-wins. */
  def isPatchActive(): Boolean =
    registeredBlockTokenClass().contains(classOf[LenientBlockTokenIdentifier].getName)
}
```

---

## 10. `Log.scala`

```scala
package org.example

import org.slf4j.{Logger, LoggerFactory}

/**
 * Logging helper cho output dễ đọc, dễ trace bằng mắt trong `kubectl logs`.
 */
object Log {

  private val logger: Logger = LoggerFactory.getLogger("VLP")
  private val WIDTH = 84

  private def pad(s: String, w: Int): String =
    if (s.length >= w) s else s + " " * (w - s.length)

  /** Khung đôi — dùng cho tiêu đề toàn job. */
  def banner(title: String): Unit = {
    val line = "=" * WIDTH
    logger.info("")
    logger.info(line)
    logger.info(pad(s"  $title", WIDTH))
    logger.info(line)
  }

  /** Khung đơn — dùng cho từng giai đoạn. */
  def section(title: String): Unit = {
    logger.info("")
    logger.info("-" * WIDTH)
    logger.info(s"  $title")
    logger.info("-" * WIDTH)
  }

  /** Cặp key-value canh cột. */
  def kv(key: String, value: Any): Unit =
    logger.info(f"    ${pad(key, 34)} : $value")

  def info(msg: String): Unit  = logger.info(s"    $msg")
  def ok(msg: String): Unit    = logger.info(s"    [ OK ]   $msg")
  def warn(msg: String): Unit  = logger.warn(s"    [ WARN ] $msg")
  def fail(msg: String): Unit  = logger.error(s"    [ FAIL ] $msg")

  def fail(msg: String, t: Throwable): Unit = logger.error(s"    [ FAIL ] $msg", t)

  /** In cây nguyên nhân của exception — rất hữu ích cho lỗi HDFS nhiều tầng. */
  def causeChain(t: Throwable): Unit = {
    var cur: Throwable = t
    var depth = 0
    while (cur != null && depth < 12) {
      val prefix = if (depth == 0) "    Exception   : " else "    " + ("  " * depth) + "Caused by : "
      logger.error(s"$prefix${cur.getClass.getName}: ${cur.getMessage}")
      cur = cur.getCause
      depth += 1
    }
  }

  def elapsed(label: String, startNanos: Long): Unit = {
    val ms = (System.nanoTime() - startNanos) / 1000000L
    logger.info(f"    ${pad(label, 34)} : $ms%,d ms")
  }
}
```

---

## 11. `spark_write.scala`

Giữ nguyên logic nghiệp vụ bản gốc: nhận `year month day` từ args, tạo DataFrame mẫu,
lọc theo `thresh_age`, ghi CSV tab-delimited vào `/test_kms/test_path/yyyyMMdd`.

```scala
package org.example

import java.io.{File, FileInputStream, InputStream}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties

import scala.util.Random

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import vn.viettel.vlp.hdfs.BlockTokenDiagnostics

object spark_write {

  private val NAMENODE   = "hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020"
  private val OUTPUT_DIR = "/test_kms/test_path"
  private val CONFIG_PATH = "conf/config.properties"

  private val firstNames = List(
    "Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Hannah", "Ivy", "Jack")
  private val lastNames = List(
    "Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor")

  def randomName(): String =
    s"${firstNames(Random.nextInt(firstNames.length))} ${lastNames(Random.nextInt(lastNames.length))}"

  // ------------------------------------------------------------------------
  // Config
  // ------------------------------------------------------------------------

  /**
   * Nạp properties: ưu tiên file trên đĩa, fallback sang classpath resource.
   * Thiếu file không phải lỗi chí mạng — dùng giá trị mặc định.
   */
  private def loadProperties(path: String): Properties = {
    val props = new Properties()
    val file = new File(path)

    val stream: Option[InputStream] =
      if (file.exists()) Some(new FileInputStream(file))
      else Option(getClass.getClassLoader.getResourceAsStream(path))

    stream match {
      case Some(in) =>
        try {
          props.load(in)
          Log.ok(s"Đã nạp config từ: $path (${props.size()} key)")
        } finally in.close()
      case None =>
        Log.warn(s"Không tìm thấy $path — dùng giá trị mặc định")
    }
    props
  }

  // ------------------------------------------------------------------------
  // Diagnostics
  // ------------------------------------------------------------------------

  /** Bật DEBUG cho 2 logger cần thiết khi verify. Không làm gì nếu backend không phải log4j2. */
  private def enableHdfsSaslDebug(): Unit = {
    try {
      val configurator = Class.forName("org.apache.logging.log4j.core.config.Configurator")
      val levelClass   = Class.forName("org.apache.logging.log4j.Level")
      val debug        = levelClass.getMethod("valueOf", classOf[String]).invoke(null, "DEBUG")
      val setLevel     = configurator.getMethod("setLevel", classOf[String], levelClass)

      Seq(
        "org.apache.hadoop.hdfs.protocol.datatransfer.sasl",
        "org.apache.hadoop.security.token.Token"
      ).foreach { name =>
        setLevel.invoke(null, name, debug)
        Log.kv("DEBUG enabled", name)
      }
    } catch {
      case t: Throwable => Log.warn(s"Không bật được DEBUG log4j2: ${t.getClass.getSimpleName}")
    }
  }

  /**
   * Kiểm tra trạng thái wire encryption và trạng thái patch block token.
   * Đây là phần quan trọng nhất để trace khi job fail.
   */
  private def reportHdfsSecurity(spark: SparkSession): Unit = {
    Log.section("HDFS SECURITY DIAGNOSTICS")

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    Log.kv("hadoop.security.authentication", hadoopConf.get("hadoop.security.authentication", "<unset>"))
    Log.kv("hadoop.rpc.protection",          hadoopConf.get("hadoop.rpc.protection", "<unset>"))
    Log.kv("dfs.data.transfer.protection",   hadoopConf.get("dfs.data.transfer.protection", "<unset>"))
    Log.kv("dfs.encrypt.data.transfer.cipher.suites",
      hadoopConf.get("dfs.encrypt.data.transfer.cipher.suites", "<unset>"))
    Log.kv("fs.defaultFS", hadoopConf.get("fs.defaultFS", "<unset>"))

    // Server defaults: đây mới là thứ QUYẾT ĐỊNH client có mã hoá hay không.
    try {
      val fs = FileSystem.get(new java.net.URI(NAMENODE), hadoopConf)
      val defaults = fs.getServerDefaults(new Path("/"))
      Log.kv("server encryptDataTransfer", defaults.getEncryptDataTransfer)
      if (defaults.getEncryptDataTransfer) {
        Log.info("=> Isilon ĐANG ép mã hoá. Client bắt buộc đi nhánh getEncryptedStreams().")
      } else {
        Log.info("=> Isilon KHÔNG ép mã hoá. Client sẽ không đi nhánh encrypted.")
      }
    } catch {
      case t: Throwable => Log.warn(s"Không đọc được server defaults: ${t.getMessage}")
    }

    // Trạng thái patch — nếu dòng này báo FAIL thì job sẽ crash khi ghi.
    BlockTokenDiagnostics.registeredBlockTokenClass() match {
      case Some(cls) =>
        Log.kv("HDFS_BLOCK_TOKEN handler", cls)
        if (BlockTokenDiagnostics.isPatchActive()) {
          Log.ok("Patch ĐANG hoạt động — LenientBlockTokenIdentifier đã thắng tokenKindMap")
        } else {
          Log.fail("Patch KHÔNG hoạt động — vẫn đang dùng class gốc của Hadoop.")
          Log.fail("Kiểm tra: jar đã nằm trong spark.jars chưa? userClassPathFirst có bị bật không?")
        }
      case None =>
        Log.warn("Không đọc được tokenKindMap (có thể do JDK module restriction) — bỏ qua kiểm tra")
    }
  }

  // ------------------------------------------------------------------------
  // Main
  // ------------------------------------------------------------------------

  def main(args: Array[String]): Unit = {
    val jobStart = System.nanoTime()

    Log.banner("VLP SPARK WRITE — HDFS (Isilon) with wire encryption")

    // -- 1. Tham số ------------------------------------------------------
    Log.section("1. ARGUMENTS")
    if (args.length < 3) {
      Log.fail(s"Cần ít nhất 3 tham số: <year> <month> <day>. Nhận được ${args.length}.")
      Log.info("Ví dụ: spark-submit ... app.jar 2026 09 09")
      sys.exit(2)
    }

    val year  = args(0).toInt
    val month = args(1).toInt
    val day   = args(2).toInt

    Log.kv("year",  year)
    Log.kv("month", month)
    Log.kv("day",   day)
    if (args.length > 3) Log.kv("extra args (bỏ qua)", args.drop(3).mkString(", "))

    val partitionDate = LocalDate.of(year, month, day)
    val partition     = partitionDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    val output        = s"$OUTPUT_DIR/$partition"

    Log.kv("partition", partition)
    Log.kv("output path", output)

    // -- 2. Config -------------------------------------------------------
    Log.section("2. CONFIG")
    val config = loadProperties(CONFIG_PATH)
    val thresholdAge = config.getProperty("thresh_age", "26").toInt
    Log.kv("thresh_age", thresholdAge)

    // -- 3. SparkSession -------------------------------------------------
    Log.section("3. SPARK SESSION")
    val spark = SparkSession
      .builder()
      .appName("write data to hdfs")
      .config("spark.hadoop.fs.defaultFS", NAMENODE)
      .getOrCreate()

    Log.kv("spark version",   spark.version)
    Log.kv("application id",  spark.sparkContext.applicationId)
    Log.kv("master",          spark.sparkContext.master)
    Log.kv("default parallelism", spark.sparkContext.defaultParallelism)

    enableHdfsSaslDebug()

    var exitCode = 0
    try {
      // -- 4. Diagnostics ------------------------------------------------
      reportHdfsSecurity(spark)

      // -- 5. Build DataFrame -------------------------------------------
      Log.section("4. BUILD DATAFRAME")
      val buildStart = System.nanoTime()

      val schema = StructType(List(
        StructField("id",   IntegerType, nullable = false),
        StructField("name", StringType,  nullable = false),
        StructField("age",  IntegerType, nullable = false)
      ))
      Log.kv("schema", schema.fields.map(f => s"${f.name}:${f.dataType.simpleString}").mkString(", "))

      val rows = Seq(
        Row(1, "Alice",   30),
        Row(2, "Bob",     25),
        Row(3, "Charlie", 35)
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      val totalCount = df.count()
      Log.kv("rows trước khi lọc", totalCount)
      Log.elapsed("thời gian build", buildStart)

      Log.info("Nội dung DataFrame:")
      df.show(truncate = false)

      // -- 6. Filter -----------------------------------------------------
      Log.section("5. FILTER")
      import spark.implicits._
      val filtered = df.filter($"age" >= thresholdAge)
      val keptCount = filtered.count()

      Log.kv("điều kiện",          s"age >= $thresholdAge")
      Log.kv("rows sau khi lọc",   keptCount)
      Log.kv("rows bị loại",       totalCount - keptCount)

      if (keptCount == 0) Log.warn("Không còn dòng nào sau khi lọc — sẽ ghi ra file rỗng")

      // -- 7. Write ------------------------------------------------------
      Log.section("6. WRITE TO HDFS")
      Log.kv("target",     s"$NAMENODE$output")
      Log.kv("format",     "csv (delimiter=TAB, header=false)")
      Log.kv("mode",       SaveMode.Overwrite.name())
      Log.kv("partitions", 1)
      Log.info("Bắt đầu ghi — nếu wire encryption có vấn đề, lỗi sẽ xuất hiện ở bước này")

      val writeStart = System.nanoTime()
      filtered
        .coalesce(1)
        .write
        .option("delimiter", "\t")
        .option("header", false)
        .mode(SaveMode.Overwrite)
        .csv(output)
      Log.elapsed("thời gian ghi", writeStart)
      Log.ok("Ghi thành công")

      // -- 8. Verify -----------------------------------------------------
      Log.section("7. VERIFY OUTPUT")
      try {
        val fs = FileSystem.get(new java.net.URI(NAMENODE), spark.sparkContext.hadoopConfiguration)
        val files = fs.listStatus(new Path(output))
        Log.kv("số file", files.length)
        files.foreach { st =>
          Log.info(f"${st.getPath.getName}%-40s ${st.getLen}%,10d bytes")
        }
        val totalBytes = files.map(_.getLen).sum
        Log.kv("tổng dung lượng", f"$totalBytes%,d bytes")
      } catch {
        case t: Throwable => Log.warn(s"Không liệt kê được output: ${t.getMessage}")
      }

      Log.banner("JOB THÀNH CÔNG")
      Log.elapsed("tổng thời gian", jobStart)

    } catch {
      case t: Throwable =>
        exitCode = 1
        Log.banner("JOB THẤT BẠI")
        Log.causeChain(t)

        // Nhận diện đúng lỗi block token để người trace không phải đoán.
        val isBlockTokenIssue = Iterator
          .iterate[Throwable](t)(_.getCause)
          .takeWhile(_ != null)
          .exists { c =>
            c.isInstanceOf[NegativeArraySizeException] ||
            Option(c.getStackTrace).exists(_.exists(_.getClassName.contains("BlockTokenIdentifier")))
          }

        if (isBlockTokenIssue) {
          Log.fail("Lỗi khớp với vấn đề parse block token của Isilon.")
          Log.fail("Patch LenientBlockTokenIdentifier có thể chưa được nạp.")
          Log.fail("Xem lại mục 'HDFS SECURITY DIAGNOSTICS' ở đầu log.")
        }
        Log.elapsed("thời gian đến khi fail", jobStart)

    } finally {
      spark.stop()
      Log.info("SparkSession đã đóng")
    }

    sys.exit(exitCode)
  }
}
```

---

## 12. `pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>vn.viettel.vlp</groupId>
  <artifactId>atspcall-bigdata-v2</artifactId>
  <version>1.0-SNAPSHOT</version>

  <properties>
    <scala.version>2.12.18</scala.version>
    <scala.binary.version>2.12</scala.binary.version>
    <spark.version>3.5.1</spark.version>
    <hadoop.version>3.3.4</hadoop.version>
    <maven.compiler.source>11</maven.compiler.source>
    <maven.compiler.target>11</maven.compiler.target>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.scala-lang</groupId>
      <artifactId>scala-library</artifactId>
      <version>${scala.version}</version>
      <scope>provided</scope>
    </dependency>

    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql_${scala.binary.version}</artifactId>
      <version>${spark.version}</version>
      <scope>provided</scope>
    </dependency>

    <!--
      Cần để compile LenientBlockTokenIdentifier.
      Class con CHỈ tham chiếu DataInput + API public của BlockTokenIdentifier,
      KHÔNG chạm protobuf -> compile với bản thường là đủ, không cần
      hadoop-client-api shaded.
    -->
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-hdfs-client</artifactId>
      <version>${hadoop.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-common</artifactId>
      <version>${hadoop.version}</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>

  <build>
    <sourceDirectory>src/main/scala</sourceDirectory>
    <plugins>

      <plugin>
        <groupId>net.alchim31.maven</groupId>
        <artifactId>scala-maven-plugin</artifactId>
        <version>4.8.1</version>
        <executions>
          <execution>
            <goals>
              <goal>compile</goal>
              <goal>testCompile</goal>
            </goals>
          </execution>
        </executions>
        <configuration>
          <scalaVersion>${scala.version}</scalaVersion>
          <args>
            <arg>-deprecation</arg>
            <arg>-feature</arg>
          </args>
        </configuration>
      </plugin>

      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.1</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
              <transformers>
                <!--
                  BẮT BUỘC: giữ và merge META-INF/services.
                  Thiếu transformer này thì file đăng ký ServiceLoader sẽ bị mất
                  và toàn bộ giải pháp không hoạt động.
                -->
                <transformer
                  implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                <transformer
                  implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>org.example.spark_write</mainClass>
                </transformer>
              </transformers>
              <filters>
                <filter>
                  <artifact>*:*</artifact>
                  <excludes>
                    <exclude>META-INF/*.SF</exclude>
                    <exclude>META-INF/*.DSA</exclude>
                    <exclude>META-INF/*.RSA</exclude>
                  </excludes>
                </filter>
              </filters>
              <!--
                TUYỆT ĐỐI KHÔNG relocate org.apache.hadoop.*
                LenientBlockTokenIdentifier phải kế thừa đúng class thật
                mà Hadoop runtime đang dùng.
              -->
            </configuration>
          </execution>
        </executions>
      </plugin>

    </plugins>
  </build>
</project>
```

Build:

```bash
mvn -B clean package
# -> target/atspcall-bigdata-v2-1.0-SNAPSHOT.jar
```

Kiểm tra nhanh jar đã đúng chưa:

```bash
unzip -p target/atspcall-bigdata-v2-1.0-SNAPSHOT.jar \
  META-INF/services/org.apache.hadoop.security.token.TokenIdentifier
# Phải in ra: vn.viettel.vlp.hdfs.LenientBlockTokenIdentifier

unzip -l target/atspcall-bigdata-v2-1.0-SNAPSHOT.jar | grep LenientBlockToken
# Phải thấy: vn/viettel/vlp/hdfs/LenientBlockTokenIdentifier.class
```

---

## 13. SparkApplication CRD

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: vlp-test-job-hungvt
  namespace: vlp-tenantw1xjixm-wsytjjtr0-ingestion
spec:
  type: Scala
  mode: cluster
  sparkVersion: 3.5.1
  image: hub.vtcc.vn:8989/test_spark_kms:v1
  imagePullPolicy: Always

  # Fat jar chứa CẢ app code LẪN LenientBlockTokenIdentifier + META-INF/services.
  # Spark tự thêm mainApplicationFile vào spark.jars -> jar được append SAU
  # jar hệ thống trên classpath của cả driver lẫn executor. Đúng thứ tự cần.
  mainApplicationFile: local:///opt/app/atspcall-bigdata-v2-1.0-SNAPSHOT.jar
  mainClass: org.example.spark_write

  arguments:
    - "2026"
    - "09"
    - "09"

  hadoopConfigMap: hdfs-hadoop-hkh
  restartPolicy:
    type: Never
  timeToLiveSeconds: 1800

  driver:
    cores: 1
    coreLimit: 1000m
    memory: 1g
    serviceAccount: spark-application-sa
    labels:
      version: 3.5.1

  executor:
    cores: 1
    coreLimit: 1000m
    instances: 1
    memory: 1g
    serviceAccount: spark-application-sa
    labels:
      version: 3.5.1

  sparkConf:
    # --- Kerberos ---
    spark.kerberos.principal: k8s@VAILAKEHOUSE.VIETTEL.COM
    spark.kerberos.keytab: /etc/security/keytabs/k8s.keytab
    spark.kubernetes.kerberos.krb5.path: /etc/krb5.conf
    spark.kerberos.access.hadoopFileSystems: hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020
    spark.security.credentials.hadoopfs.enabled: "true"

    # --- HDFS security: GIỮ NGUYÊN privacy, KHÔNG hạ xuống authentication ---
    spark.hadoop.hadoop.security.authentication: kerberos
    spark.hadoop.hadoop.security.authorization: "true"
    spark.hadoop.hadoop.security.token.service.use_ip: "false"

    # --- BẮT BUỘC giữ false, nếu true thì thứ tự classpath đảo và patch mất tác dụng ---
    spark.driver.userClassPathFirst: "false"
    spark.executor.userClassPathFirst: "false"

    # --- Spark ---
    spark.serializer: org.apache.spark.serializer.KryoSerializer
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.sources.ignoreDataLocality.enabled: "true"
    spark.hadoop.fs.permissions.umask-mode: "002"
    spark.jars.ivy: /tmp
    spark.eventLog.enabled: "false"
    spark.scheduler.mode: FAIR

    spark.driver.extraJavaOptions: "-Dsun.security.krb5.debug=true"
    spark.executor.extraJavaOptions: "-Dsun.security.krb5.debug=true"
```

**KHÔNG thêm** `spark.driver.extraClassPath` / `spark.executor.extraClassPath` trỏ tới jar này —
sẽ prepend và làm patch thua cuộc đua.

---

## 14. Verify — bắt buộc

### 14.1 Ngay trong log của job

Ở section `HDFS SECURITY DIAGNOSTICS` phải thấy:

```
    server encryptDataTransfer         : true
    HDFS_BLOCK_TOKEN handler           : vn.viettel.vlp.hdfs.LenientBlockTokenIdentifier
    [ OK ]   Patch ĐANG hoạt động — LenientBlockTokenIdentifier đã thắng tokenKindMap
```

Nếu thấy `[ FAIL ] Patch KHÔNG hoạt động` → sang mục 15.

### 14.2 Trong log DEBUG của executor

```bash
kubectl logs <executor-pod> -n vlp-tenantw1xjixm-wsytjjtr0-ingestion | \
  grep -E "tokenKindMap|encrypted handshake|Handshake secret"
```

Ba dòng cần thấy, theo thứ tự:

| Dòng                                                                                             | Ý nghĩa                                                      |
| ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------ |
| `Added HDFS_BLOCK_TOKEN:class vn.viettel.vlp.hdfs.LenientBlockTokenIdentifier into tokenKindMap` | Patch đã đăng ký (phải xuất hiện **sau** dòng của class gốc) |
| `SASL client doing encrypted handshake for addr = ...`                                           | Vẫn đang mã hoá — đúng yêu cầu                               |
| `Handshake secret is null, sending without handshake secret.`                                    | Đã né được crash                                             |

Nếu thấy `SASL client skipping handshake` → mã hoá **đã bị tắt**, sai yêu cầu, phải điều tra lại.

### 14.3 Kiểm tra dữ liệu

```bash
hdfs dfs -ls  /test_kms/test_path/20260909
hdfs dfs -cat /test_kms/test_path/20260909/part-*
```

---

## 15. Fallback nếu patch thua cuộc đua classpath

Rủi ro đã biết: `tokenKindMap` là static, **build một lần rồi cache**. Nếu có thứ gì trong Spark
gọi `Token.decodeIdentifier()` trước khi classloader người dùng sẵn sàng, map bị chốt mà không có
class custom.

Khi đó dùng **Spark Plugin** — chạy rất sớm trên cả driver lẫn executor, không phụ thuộc thứ tự
classpath:

```scala
package vn.viettel.vlp.hdfs

import java.util.{Map => JMap}

import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin._

class BlockTokenFixPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin     = new Injector
  override def executorPlugin(): ExecutorPlugin = new Injector
}

private class Injector extends DriverPlugin with ExecutorPlugin {

  private def inject(): Unit = {
    try {
      // Ép populate tokenKindMap trước khi ghi đè.
      try new Token[TokenIdentifier]().decodeIdentifier() catch { case _: Throwable => }

      val field = classOf[Token[_]].getDeclaredField("tokenKindMap")
      field.setAccessible(true)
      val map = field.get(null).asInstanceOf[JMap[Text, Class[_]]]

      if (map != null) {
        map.put(BlockTokenIdentifier.KIND_NAME, classOf[LenientBlockTokenIdentifier])
        println("[BlockTokenFixPlugin] Đã inject LenientBlockTokenIdentifier vào tokenKindMap")
      }
    } catch {
      case t: Throwable =>
        println(s"[BlockTokenFixPlugin] Inject thất bại: ${t.getClass.getName}: ${t.getMessage}")
    }
  }

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    inject()
    java.util.Collections.emptyMap()
  }

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = inject()
}
```

Bật bằng:

```yaml
sparkConf:
  spark.plugins: vn.viettel.vlp.hdfs.BlockTokenFixPlugin
```

Đánh đổi: dùng reflection vào private static field. Chỉ bật khi cách ServiceLoader thất bại.

---

## 16. Những điều TUYỆT ĐỐI KHÔNG làm

| Không làm                                                            | Lý do                                                                                                                           |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Hạ `dfs.data.transfer.protection` xuống `authentication`/`integrity` | Vi phạm yêu cầu bắt buộc; và trong nhánh encrypted thì config này bị Hadoop bỏ qua nên **cũng không có tác dụng**               |
| Dùng `spark.driver.extraClassPath` cho jar này                       | Prepend → nạp trước Hadoop → thua last-write-wins                                                                               |
| Bật `spark.*.userClassPathFirst = true`                              | Đảo thứ tự → patch mất tác dụng                                                                                                 |
| Relocate `org.apache.hadoop.*` trong shade plugin                    | Class con sẽ kế thừa sai class, `instanceof` fail                                                                               |
| Đóng gói Spark/Hadoop vào fat jar (scope `compile`)                  | Xung đột với runtime, phình jar, có thể vỡ `META-INF/services`                                                                  |
| Bỏ `ServicesResourceTransformer`                                     | File đăng ký ServiceLoader biến mất → giải pháp vô hiệu                                                                         |
| Copy nguyên source `BlockTokenIdentifier.java` để patch              | Dính protobuf shaded (`org.apache.hadoop.shaded.com.google.protobuf`) → `NoClassDefFoundError`; và biến thành fork phải bảo trì |
| Sửa trực tiếp `hadoop-client-api-3.3.4.jar` trong image              | Jar mang version 3.3.4 nhưng checksum khác upstream → cờ đỏ supply-chain khi audit                                              |
| Hạ Hadoop client xuống 3.2.0 cho Spark 3.5.1                         | Spark 3.5 compile trên API Hadoop 3.3.x, sẽ vỡ ở chỗ khác                                                                       |

---

## 17. Ghi chú vận hành

Đây là **workaround**, không phải fix gốc. Fix thật là Dell phát hành bản OneFS sinh block token
đúng chuẩn Apache. Cần mở case với Dell song song, kèm thông tin: OneFS 9.5.0.6, wire encryption
bật, Hadoop client 3.3.4 fail / 2.7.0 pass, stack trace ở mục 2, tham chiếu HDFS-11026 và
HDFS-13699.

Khi Dell fix: gỡ `LenientBlockTokenIdentifier.scala` + file `META-INF/services`, build lại, hết.
Phần app code không phải đụng tới.

Cần rà soát an ninh trước khi lên production. Nội dung trình bày: mã hoá đường truyền nguyên vẹn
(auth-conf, AES/CTR 256), Kerberos nguyên vẹn, block token vẫn được Isilon verify đầy đủ; thứ bị
bỏ qua là selective-QOP override — trường mà Isilon vốn không phát hành.
