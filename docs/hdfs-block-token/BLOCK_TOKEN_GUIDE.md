# Hướng dẫn xử lý: ghi file lên HDFS thành công nhưng file rỗng (SeaTunnel Zeta, Hadoop client 3.3.6)

Tài liệu dành cho đội vận hành / phát triển SeaTunnel Zeta ghi dữ liệu xuống
HDFS. Mục tiêu:

1. Xác định sự cố "tạo được file nhưng không có dữ liệu" có phải do lỗi
   `NegativeArraySizeException: -1` khi xử lý block token hay không.
2. Nếu đúng, cài thư viện bổ trợ (jar) kèm theo tài liệu này để khắc phục.
3. Nếu không đúng, có dữ liệu để loại trừ và gửi lại cho chúng tôi phân tích.

## 1. Tóm tắt

| Nội dung | Thông tin |
| --- | --- |
| Triệu chứng | Job ghi HDFS không báo lỗi, file được tạo nhưng `cat` ra rỗng |
| Nghi vấn | Client Hadoop ≥ 3.2.1 tự giải mã block token trong bước SASL handshake. Token do HDFS server (ví dụ Dell Isilon OneFS) phát hành có định dạng nội bộ không theo chuẩn Apache nên bước giải mã ném `NegativeArraySizeException: -1` |
| Điều kiện xảy ra | Hadoop client **≥ 3.2.1** (phiên bản 3.3.6 của quý đối tác nằm trong diện này) **và** HDFS bật mã hoá đường truyền (`dfs.data.transfer.protection = privacy` hoặc `dfs.encrypt.data.transfer = true`) |
| Cách khắc phục | Thêm 1 jar nhỏ (~20 KB) vào classpath của SeaTunnel, khởi động lại. Không sửa mã Hadoop/SeaTunnel, **không giảm mức mã hoá** |

Lưu ý quan trọng: phiên bản 3.3.6 chỉ cho biết lỗi này **có thể** xảy ra.
Cần làm các bước kiểm tra ở mục 3 đến 5 để xác nhận trước khi cài jar.

## 2. Vì sao file rỗng mà không thấy lỗi

- Bước tạo file chỉ là yêu cầu tới NameNode, chưa gửi dữ liệu tới DataNode, nên
  file luôn được tạo ra với kích thước 0.
- Việc ghi block chạy ở luồng nền `DataStreamer` của Hadoop client. Nếu luồng
  này gặp lỗi, lỗi chỉ được báo ra khi ứng dụng gọi `hflush()` hoặc `close()`.
- Nếu tầng bên trên không gọi `close()` đúng cách, hoặc log của Hadoop client bị
  đặt ở mức quá cao (chỉ hiện ERROR), thì kết quả nhìn từ bên ngoài là "không
  có log, file rỗng".

Vì vậy cần xem log ở mức chi tiết hơn bình thường (mục 4).

## 3. Bước 1: xác nhận phiên bản và điều kiện

### 3.1 Phiên bản Hadoop client mà SeaTunnel thực sự nạp

Cần kiểm tra jar Hadoop trong SeaTunnel, không phải phiên bản Hadoop của
cluster HDFS:

```bash
find $SEATUNNEL_HOME -name '*hadoop*.jar' | sort
```

Nếu là uber jar, xác nhận nội dung và package:

```bash
unzip -l <ten-jar-hadoop>.jar | grep -E 'BlockTokenIdentifier|SaslDataTransferClient'
```

Kết quả cần có: đường dẫn dạng
`org/apache/hadoop/hdfs/security/token/block/BlockTokenIdentifier.class`.

- Nếu package **đúng như trên**: có thể dùng jar patch.
- Nếu package bị đổi tên (ví dụ `shaded/org/apache/hadoop/...`): jar patch
  **không dùng được**, vui lòng gửi lại kết quả cho chúng tôi.

### 3.2 Cấu hình mã hoá đường truyền của HDFS

Chạy trên máy có Hadoop CLI và file cấu hình HDFS của quý đối tác:

```bash
hdfs getconf -confKey dfs.data.transfer.protection
hdfs getconf -confKey dfs.encrypt.data.transfer
```

Kết quả `privacy` (hoặc `true`) nghĩa là điều kiện của lỗi đã đủ.

## 4. Bước 2: tìm lỗi trong log

Cần xem log của **các node Zeta thực sự chạy task ghi HDFS** (worker/server),
không chỉ log của lệnh submit job. Với cluster nhiều node, tìm trên từng node.

```bash
PATTERN='NegativeArraySize|DataStreamer|SaslDataTransferClient|BlockTokenIdentifier'
```

Nếu chạy trên Kubernetes:

```bash
kubectl -n <namespace> get pods | grep -i seatunnel
kubectl -n <namespace> logs <pod> --since=6h | grep -n -E "$PATTERN"
kubectl -n <namespace> logs <pod> --previous | grep -n -E "$PATTERN"   # nếu pod từng restart
```

Nếu chạy trên máy/VM:

```bash
ls -lt $SEATUNNEL_HOME/logs | head
grep -rn -E "$PATTERN" $SEATUNNEL_HOME/logs/
```

Tên file log phụ thuộc phiên bản và cấu hình `config/log4j2.properties`, nên
quý đối tác có thể cần điều chỉnh đường dẫn cho phù hợp.

### 4.1 Nếu log không có gì: hạ mức log của Hadoop HDFS client

Thêm vào `config/log4j2.properties` của SeaTunnel (trên các node Zeta):

```properties
logger.hdfsclient.name = org.apache.hadoop.hdfs
logger.hdfsclient.level = DEBUG
```

Khởi động lại Zeta, chạy lại job, tìm lại theo `PATTERN`. Sau khi debug xong,
nên đặt lại mức cũ vì DEBUG tạo rất nhiều log.

### 4.2 Cách đọc kết quả

| Thấy trong log | Ý nghĩa | Việc tiếp theo |
| --- | --- | --- |
| `NegativeArraySizeException: -1` gần `DataStreamer` hoặc `SaslDataTransferClient` | Đúng lỗi block token | Sang mục 6 (kiểm chứng), rồi mục 7 (cài jar) |
| `DataStreamer` báo lỗi khác (`Broken pipe`, `Connection reset`, `Could not obtain block`...) | Lỗi mạng hoặc DataNode | Không cài jar, xử lý nguyên nhân đó |
| Không có exception nào | Chưa kết luận được | Làm mục 5 |

Ví dụ nhận dạng đúng lỗi: stack trace có `NegativeArraySizeException`, đi qua
`WritableUtils.readString`, `BlockTokenIdentifier.readFieldsLegacy`,
`Token.decodeIdentifier`, `SaslDataTransferClient`.

## 5. Bước 3: kiểm tra trạng thái file rỗng

```bash
hdfs dfs -ls <duong-dan-file>
hdfs fsck <duong-dan-file> -files -blocks -openforwrite
```

| Kết quả | Ý nghĩa |
| --- | --- |
| Có `OPENFORWRITE` | File chưa được `close()`. Có thể job chưa hoàn tất, hoặc lỗi bị nuốt trước khi đóng file |
| Kích thước 0, không có block | Dữ liệu chưa hề tới DataNode. Phù hợp với lỗi block token nếu mục 4 có exception |
| File nằm ở thư mục tạm của connector, chưa được đổi tên | Connector ghi vào thư mục tạm và chỉ đổi tên khi commit (checkpoint hoặc kết thúc job). Kiểm tra job đã hoàn tất và bước commit không lỗi |

## 6. Bước 4: kiểm chứng độc lập bằng Hadoop CLI

Bước này tái hiện lỗi **không cần SeaTunnel**, giúp tách biệt lỗi thư viện với
cấu hình job. Chạy trên **bất kỳ máy hoặc pod nào** có Hadoop client cùng dòng
3.3.x (cùng 3.3.6 là tốt nhất), cùng file cấu hình HDFS (`core-site.xml`,
`hdfs-site.xml`) và, nếu cluster dùng Kerberos, `krb5.conf` cùng keytab. Ví dụ
dựng pod trên Kubernetes có ở Phụ lục A.

Chuẩn bị (Kerberos, nếu có):

```bash
export HADOOP_CONF_DIR=/etc/hadoop
kinit -kt <file.keytab> <principal>@<REALM>
klist
hadoop version              # ghi lại phiên bản
```

Chuẩn bị dữ liệu thử và thư mục có quyền ghi:

```bash
echo "hello block token" > /tmp/t.txt
HDFS_DIR=<thu-muc-hdfs-co-quyen-ghi>
hdfs dfs -mkdir -p $HDFS_DIR
```

**A. Khi chưa có jar patch:**

```bash
unset HADOOP_CLASSPATH
HADOOP_ROOT_LOGGER=DEBUG,console hdfs dfs -put -f /tmp/t.txt $HDFS_DIR/a.txt 2>&1 | tee /tmp/put-nopatch.log
grep -n -E 'NegativeArraySize|DataStreamer|SaslDataTransferClient' /tmp/put-nopatch.log
hdfs dfs -ls $HDFS_DIR
hdfs dfs -cat $HDFS_DIR/a.txt
```

- Thấy `NegativeArraySizeException: -1` (và file `a.txt` rỗng hoặc không có): xác nhận lỗi.
- Ghi thành công, cat ra `hello block token`: Hadoop CLI không gặp lỗi. Cần gửi lại
  cho chúng tôi thông tin ở mục 9 để tìm hướng khác.
- Lỗi khác (Kerberos, mạng, quyền): xử lý riêng, chưa liên quan tới jar.

**B. Khi có jar patch** (jar đặt ở `/tmp/patch/block-token-patch.jar`):

```bash
export HADOOP_CLASSPATH=/tmp/patch/block-token-patch.jar
hadoop classpath | tr ':' '\n' | grep -n -E 'hdfs-client|block-token-patch'
hdfs dfs -put -f /tmp/t.txt $HDFS_DIR/b.txt
hdfs dfs -cat $HDFS_DIR/b.txt        # phải in: hello block token
hdfs dfs -ls $HDFS_DIR               # b.txt có kích thước khác 0
```

Ở dòng `hadoop classpath`, jar patch phải hiện **sau** jar `hadoop-hdfs-client`
(lý do ở mục 7). Không đặt `HADOOP_USER_CLASSPATH_FIRST=true`.

A lỗi mà B thành công là bằng chứng jar khắc phục được sự cố trong môi trường
của quý đối tác.

## 7. Cài jar patch vào SeaTunnel Zeta

Chỉ thực hiện khi mục 4 hoặc mục 6 đã xác nhận đúng lỗi.

**Cách hoạt động ngắn gọn:** Hadoop tra cứu lớp giải mã block token qua cơ chế
`ServiceLoader` của Java. Jar patch khai báo một lớp giải mã "khoan dung" thay
cho lớp gốc: nếu giải mã token thất bại thì bỏ qua thay vì ném lỗi, và không gửi
"handshake secret" (đúng hành vi của Hadoop ≤ 3.2.0). Nếu nhiều jar cùng khai
báo, jar **nạp sau** được ưu tiên, do đó jar patch phải nằm **sau** jar Hadoop
trong classpath.

Các bước (thực hiện trên **tất cả** node Zeta, gồm cả master và worker):

1. Sao chép jar vào thư mục `lib/` của SeaTunnel. Nên đặt tên sao cho xếp sau
   jar Hadoop theo thứ tự chữ cái:

   ```bash
   cp block-token-patch.jar $SEATUNNEL_HOME/lib/zz-block-token-patch.jar
   ```

2. Khởi động lại **toàn bộ** cluster Zeta. Cơ chế nạp chỉ chạy một lần khi JVM
   khởi động, không tự cập nhật khi đang chạy.
3. Chạy lại job ghi HDFS nhỏ, sau đó kiểm tra:

   ```bash
   hdfs dfs -ls <duong-dan-file>
   hdfs dfs -cat <duong-dan-file> | head
   ```

4. Nếu file vẫn rỗng và log vẫn có `NegativeArraySizeException`, thứ tự classpath
   có thể chưa đúng. Vui lòng gửi lại kết quả `ls -l $SEATUNNEL_HOME/lib` và
   dòng khởi động JVM của Zeta (có tham số `-cp` hoặc `-classpath`, ví dụ
   `ps -ef | grep -i seatunnel`), chúng tôi sẽ hỗ trợ điều chỉnh.

**Gỡ bỏ:** xoá jar khỏi `lib/` và khởi động lại Zeta.

## 8. Phạm vi ảnh hưởng và giới hạn

- **Mã hoá đường truyền không đổi.** SASL vẫn thương lượng mức bảo vệ
  `auth-conf` (AES 256-bit) như cũ. Token thật vẫn được gửi nguyên vẹn tới
  DataNode và được server kiểm tra như trước.
- Jar chỉ ảnh hưởng loại token `HDFS_BLOCK_TOKEN` trong JVM đã nạp nó. Các loại
  token khác (delegation token...) không bị đụng tới.
- Với HDFS chuẩn Apache (token đúng định dạng), jar không thay đổi hành vi.
- Đây là giải pháp tạm thời ở phía client. Giải pháp gốc là để HDFS server sinh
  token đúng chuẩn Apache.
- Chi phí hiệu năng không đáng kể: bước giải mã token chỉ xảy ra một lần cho mỗi
  block khi mở kết nối, không nằm trong luồng truyền dữ liệu. Không nên bật cờ
  JVM `-XX:-OmitStackTraceInFastThrow` khi chạy khối lượng lớn với jar này.
- Jar được biên dịch với Hadoop 3.3.4 (bytecode Java 8). Với Hadoop 3.3.6 chưa
  được kiểm chứng riêng, nên bước kiểm chứng ở mục 6 rất quan trọng.

## 9. Thông tin cần gửi lại cho chúng tôi

Nếu sự cố chưa được giải quyết, vui lòng gửi:

- [ ] Kết quả `find $SEATUNNEL_HOME -name '*hadoop*.jar'` và kết quả kiểm tra
      package ở mục 3.1.
- [ ] Giá trị `dfs.data.transfer.protection` và `dfs.encrypt.data.transfer`.
- [ ] Đoạn log Zeta (đã hạ mức DEBUG theo mục 4.1) xung quanh thời điểm ghi file,
      hoặc xác nhận đã tìm mà không thấy `NegativeArraySizeException`.
- [ ] Kết quả `hdfs fsck ... -openforwrite` trên file rỗng.
- [ ] Kết quả bước A và bước B của mục 6 (kèm file `put-nopatch.log`).
- [ ] Nếu đã cài jar: kết quả `ls -l $SEATUNNEL_HOME/lib` và dòng lệnh JVM.
- [ ] Cấu hình sink HDFS trong job (đã ẩn thông tin nhạy cảm).

## Phụ lục A. Ví dụ dựng Hadoop client trên Kubernetes

Chỉ là **một ví dụ** môi trường để thực hiện mục 6. Có thể thay bằng VM, container
hoặc máy bất kỳ có Hadoop client tương đương. Thay các giá trị trong `<...>` cho
phù hợp.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: hdfs-client
  namespace: <namespace>
  annotations:
    linkerd.io/inject: disabled          # bỏ dòng này nếu không dùng service mesh Linkerd
spec:
  restartPolicy: Never
  containers:
    - name: hdfs-client
      image: <registry>/flokkr/hadoop:3.3.0   # hoặc image Hadoop 3.3.x khác
      command: ["/bin/bash", "-c", "sleep 3600000"]
      env:
        - name: HADOOP_CONF_DIR
          value: /etc/hadoop
      resources:
        requests: { cpu: "1", memory: 1Gi }
        limits:   { cpu: "2", memory: 4Gi }
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: { drop: ["ALL"] }
        runAsNonRoot: true
        runAsUser: 1000
      volumeMounts:
        - { name: krb5-config,  mountPath: /etc/krb5.conf,            subPath: krb5.conf }
        - { name: hdfs-config,  mountPath: /etc/hadoop/core-site.xml, subPath: core-site.xml }
        - { name: hdfs-config,  mountPath: /etc/hadoop/hdfs-site.xml, subPath: hdfs-site.xml }
        - { name: hdfs-keytab,  mountPath: /etc/security/keytabs,     readOnly: true }
  volumes:
    - name: krb5-config
      configMap:
        name: <configmap-krb5>
        items: [{ key: krb5.conf, path: krb5.conf }]
    - name: hdfs-config
      configMap:
        name: <configmap-hdfs>
        items:
          - { key: core-site.xml, path: core-site.xml }
          - { key: hdfs-site.xml, path: hdfs-site.xml }
    - name: hdfs-keytab
      secret:
        secretName: <secret-keytab>
```

Lưu ý: image trong ví dụ là Hadoop 3.3.0, cùng nhóm ≥ 3.2.1 nên có thể dùng để
tái hiện lỗi. Nếu có image 3.3.6 thì nên ưu tiên để khớp với SeaTunnel.

Đưa jar vào pod (thư mục `/tmp` ghi được với user 1000):

```bash
kubectl -n <namespace> exec hdfs-client -- mkdir -p /tmp/patch
kubectl -n <namespace> cp block-token-patch.jar hdfs-client:/tmp/patch/block-token-patch.jar
kubectl -n <namespace> exec -it hdfs-client -- bash
```

Nếu `kinit` không ghi được cache, đặt `export KRB5CCNAME=/tmp/krb5cc_1000`.
