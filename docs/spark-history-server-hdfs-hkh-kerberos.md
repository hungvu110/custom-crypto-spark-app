# Spark History Server đọc event log từ HDFS HKH (Kerberos) — Tổng hợp cấu hình

> Tài liệu này ghi lại hiện trạng, các thay đổi đã thực hiện, lỗi đã gặp và việc còn tồn đọng.
> Mục tiêu: một coding agent / người khác đọc vào có thể hiểu toàn bộ bối cảnh và tiếp tục công việc.
>
> Ngày cập nhật: 15/09/2026

---

## 0. TL;DR

- **Mục tiêu:** Spark History Server (SHS) đọc event log tại `hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020/spark_history` trên cụm HDFS **HKH** (Dell Isilon OneFS, Kerberos, wire encryption `privacy`). Các SparkApplication (Spark Operator) ghi event log vào cùng thư mục đó.
- **Principal dùng chung:** `k8s@VAILAKEHOUSE.VIETTEL.COM`. Thư mục `/spark_history` do user `k8s` sở hữu.
- **Deployment SHS:** đã sửa env, volumes, volumeMounts và apply thành công.
- **Blocker hiện tại:** SHS crash với lỗi `Keytab file: /etc/security/keytabs/spark.keytab does not exist`. Nguyên nhân là template trong ConfigMap `history-server-properties` hardcode sai đường dẫn keytab. **Cần sửa template** (xem mục 3.3 và 6).
- **SparkApplication:** cần bật `spark.eventLog.enabled` và set `spark.eventLog.dir` (xem mục 4).

---

## 1. Bối cảnh môi trường

### 1.1 Hạ tầng

| Thành phần | Giá trị |
|---|---|
| Nền tảng | VLP / vailakehouse (data lakehouse nội bộ Viettel) |
| Cluster K8s chạy SHS | `k8s-datalake` (quản lý qua Rancher) |
| Namespace SHS | `vlp-tenantw1xjixm-wsytjjtr0-data` |
| Namespace SparkApplication | `vlp-tenantw1xjixm-wsytjjtr0-ingestion` |
| HDFS đích | HKH: `hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020` (Dell Isilon OneFS 9.5.0.6) |
| HDFS khác (không dùng ở đây) | HLA: `hdfs://vailakehouse.datalakehla.viettel.com.vn:8020` |
| Kerberos realm | `VAILAKEHOUSE.VIETTEL.COM` (**HLA và HKH dùng chung tên realm nhưng có KDC riêng**) |
| KDC | MIT Kerberos 1.17 custom trên K8s; Service LB `krb5-kdc-lb-tcp` **chỉ TCP** (88/749/464) |
| Ranger KMS | `kms://http@10.221.149.66:9292/kms` (LB IP; SPN là `HTTP/10.221.149.66@VAILAKEHOUSE.VIETTEL.COM`) |
| Spark | 3.5.1, Hadoop client **3.3.4** (`hadoop-client-runtime-3.3.4.jar`) |
| Spark Operator | Deployment `s-ops`, ns `...-data`, chỉ watch ns `...-ingestion` |

### 1.2 Tài nguyên K8s liên quan (ns `vlp-tenantw1xjixm-wsytjjtr0-data`)

| Loại | Tên | Key | Mục đích |
|---|---|---|---|
| ConfigMap | `hdfs-hadoop-hkh` | `core-site.xml`, `hdfs-site.xml` | Hadoop client config cho HKH |
| ConfigMap | `krb5-config-hkh` | `krb5.conf` | Kerberos client config trỏ về KDC HKH |
| Secret | `k8s-team-keytab` | `k8s.keytab` | Keytab của principal `k8s` |
| ConfigMap | `history-server-properties` | `spark-properties.conf.template` | Template properties của SHS (qua `envsubst`) |
| ServiceAccount | `spark-history-server-sa` | | SA của SHS |
| Secret | `vlp-registry` | | imagePullSecret |

### 1.3 Nội dung ConfigMap `hdfs-hadoop-hkh`

**core-site.xml**
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020</value>
    <description>NameNode URI</description>
  </property>
  <property>
    <name>hadoop.security.authentication</name>
    <value>kerberos</value>
  </property>
  <property>
    <name>hadoop.security.key.provider.path</name>
    <value>kms://http@10.221.149.66:9292/kms</value>
  </property>
  <property>
    <name>hadoop.security.authorization</name>
    <value>true</value>
  </property>
  <property>
    <name>hadoop.rpc.protection</name>
    <value>privacy</value>
  </property>
  <property>
    <name>hadoop.security.token.service.use_ip</name>
    <value>false</value>
  </property>
</configuration>
```

**hdfs-site.xml**
```xml
<configuration>
  <property>
    <name>dfs.namenode.kerberos.principal</name>
    <value>hdfs/vailakehouse.datalakehkh.viettel.com.vn@VAILAKEHOUSE.VIETTEL.COM</value>
  </property>
  <property>
    <name>dfs.data.transfer.protection</name>
    <value>privacy</value>
  </property>
  <property>
    <name>dfs.encryption.key.provider.uri</name>
    <value>kms://http@10.221.149.66:9292/kms</value>
  </property>
  <property>
    <name>dfs.block.access.token.enable</name>
    <value>true</value>
  </property>
  <!-- data transfer config -->
  <property>
    <name>dfs.encrypt.data.transfer.cipher.suites</name>
    <value>AES/CTR/NoPadding</value>
  </property>
  <property>
    <name>dfs.encrypt.data.transfer.cipher.key.bitlength</name>
    <value>256</value>
  </property>
</configuration>
```

> `hadoop.security.token.service.use_ip=false` là **bắt buộc** với Isilon SmartConnect (DNS round-robin), và phải đặt trong `core-site.xml`, không đặt qua sparkConf, vì `SecurityUtil` đọc giá trị này trong static initializer.

### 1.4 Ràng buộc quan trọng

- Isilon **ép** wire encryption (`privacy`). Tổ chức yêu cầu không được hạ xuống `authentication`/`integrity`.
- Client Hadoop 3.x đọc/ghi block trên Isilon có thể lỗi `NegativeArraySizeException: -1` tại `BlockTokenIdentifier.readFieldsLegacy`. Fix nằm phía Isilon: `isi hdfs settings modify --hadoop-version-3-or-later=true` trên đúng access zone. Nếu chưa fix, SHS vẫn list được thư mục (NameNode RPC) nhưng **không đọc được nội dung event log**.
- Ranger KMS ACL hiện giới hạn `GET_KEYS/GET_METADATA/GENERATE_EEK/DECRYPT_EEK` cho `keyadmin,hdfs`. User `k8s` từng bị 403. Chỉ ảnh hưởng nếu `/spark_history` nằm trong encryption zone.

---

## 2. Cơ chế hoạt động

```
SparkApplication (ns ingestion)                    Spark History Server (ns data)
  driver ── delegation token (principal k8s) ──┐     login keytab k8s.keytab
           ghi event log                       │     đọc event log
                                               ▼          │
            hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020/spark_history
                                  (owner: k8s)
```

- Container SHS chạy `bash -c`: `envsubst < template > /opt/spark/conf/spark-properties.conf`, sau đó `start-history-server.sh --properties-file ...`.
  - **Env chỉ có tác dụng khi template tham chiếu `${VAR}`.**
- Spark Command thực tế (từ log): `java -cp /opt/spark/conf/:/opt/spark/jars/* -Djava.security.krb5.conf=/etc/krb5.conf -Xmx1g org.apache.spark.deploy.history.HistoryServer --properties-file /opt/spark/conf/spark-properties.conf`
  - Vì `/opt/spark/conf/` nằm trên classpath, `core-site.xml`/`hdfs-site.xml` mount vào đó sẽ được Hadoop nạp.
- Khi `spark.history.kerberos.enabled=true`, `HistoryServer.initSecurity()` gọi `SparkHadoopUtil.loginUserFromKeytab(principal, keytab)`.
- Ở chế độ Kerberos, `HADOOP_USER_NAME` bị Hadoop bỏ qua.

---

## 3. Spark History Server — Các thay đổi

### 3.1 Tóm tắt diff

| Vị trí | Trước | Sau |
|---|---|---|
| env `HADOOP_USER_NAME` | `hadoop` | **Xoá** (vô tác dụng khi dùng Kerberos) |
| env `SPARK_HISTORY_OPTS` | rỗng | `-Djava.security.krb5.conf=/etc/krb5.conf` |
| env `SPARK_HISTORY_FS_LOG_DIRECTORY` | `hdfs://vailakehouse.datalakehla...:8020/prod4/spark_history` (HLA) | `hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020/spark_history` (HKH) |
| env `SPARK_HISTORY_KERBEROS_ENABLED` | `'false'` | `'true'` |
| env `SPARK_HISTORY_PRINCIPAL` | `hdfs@realm` | `k8s@VAILAKEHOUSE.VIETTEL.COM` |
| env `SPARK_HISTORY_KEYTAB` | — | **Thêm** `/etc/security/keytabs/k8s.keytab` |
| volume `core-site`, `hdfs-site` (ConfigMap `core-site`, `hdfs-site`) | 2 volume riêng | **Thay** bằng 1 volume `hadoop-conf-hkh` → ConfigMap `hdfs-hadoop-hkh` |
| volume `krb5-conf-hkh` | — | **Thêm** → ConfigMap `krb5-config-hkh`, mount `/etc/krb5.conf` |
| volume `k8s-keytab` | — | **Thêm** → Secret `k8s-team-keytab`, mount dir `/etc/security/keytabs` |
| Template `history-server-properties` | (có dòng hardcode `spark.keytab`) | **Cần sửa**, xem 3.3 |

### 3.2 Manifest Deployment sau khi sửa

> Phần labels/probes/resources được dựng lại từ output của `kubectl apply`.
> **S3 access key/secret key đã được che (`<REDACTED>`).** Manifest thật đang hardcode chúng trong `args`, xem TODO ở mục 7.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    app.kubernetes.io/instance: spark-history-server
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: spark-history-server
    app.kubernetes.io/version: 1.0.6
    helm.sh/chart: spark-history-server-1.2.0
  name: spark-history-server
  namespace: vlp-tenantw1xjixm-wsytjjtr0-data
spec:
  progressDeadlineSeconds: 600
  replicas: 1
  revisionHistoryLimit: 10
  selector:
    matchLabels:
      app.kubernetes.io/instance: spark-history-server
      app.kubernetes.io/name: spark-history-server
  strategy:
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 50%
    type: RollingUpdate
  template:
    metadata:
      labels:
        app.kubernetes.io/instance: spark-history-server
        app.kubernetes.io/name: spark-history-server
    spec:
      containers:
        - name: spark-history-server
          image: registry.cyberspace.vn/vlp/spark-history-server:3.5.1-v1.7.0-uid185-3
          imagePullPolicy: Always
          command:
            - /bin/bash
            - '-c'
          args:
            - >-
              export SPARK_HADOOP_FS_S3A_ACCESS_KEY=<REDACTED> &&
              export SPARK_HADOOP_FS_S3A_SECRET_KEY=<REDACTED>
              && mkdir -p /opt/spark/conf/ &&  envsubst <
              /opt/spark/template/spark-properties.conf.template >
              /opt/spark/conf/spark-properties.conf &&
              /opt/spark/sbin/start-history-server.sh --properties-file
              /opt/spark/conf/spark-properties.conf
          env:
            - name: SPARK_NO_DAEMONIZE
              value: 'false'
            - name: SPARK_HISTORY_OPTS
              value: >-
                -Djava.security.krb5.conf=/etc/krb5.conf
            - name: SPARK_USER
              value: spark
            - name: SPARK_EVENTLOG_ENABLED
              value: 'true'
            - name: SPARK_HISTORY_UI_PORT
              value: '18080'
            - name: SPARK_HISTORY_FS_LOG_DIRECTORY
              value: hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020/spark_history
            - name: SPARK_EVENT_LOG_DIR
              value: s3a://thaipd-dev-bucket/spark
            - name: SPARK_HADOOP_FS_S3A_ENDPOINT
              value: http://10.30.132.14:9890
            - name: SPARK_HADOOP_FS_S3A_PATH_STYLE_ACCESS
              value: 'true'
            - name: SPARK_HADOOP_FS_S3A_CONNECTION_SSL_ENABLED
              value: 'false'
            - name: SPARK_HADOOP_FS_S3A_IMPL
              value: org.apache.hadoop.fs.s3a.S3AFileSystem
            - name: TEAM_USER
              value: hadoop
            - name: SPARK_EVENT_LOG_ROLLING_ENABLED
              value: 'true'
            - name: SPARK_UPDATE_INTERVAL
              value: 5s
            - name: SPARK_MAX_APPLICATIONS
              value: '20'
            - name: SPARK_CLEANER_ENABLED
              value: 'true'
            - name: SPARK_CLEANER_INTERVAL
              value: 10m
            - name: SPARK_CLEANER_MAX_AGE
              value: 24h
            - name: SPARK_IO_COM_PRESSION_CODEC
              value: snappy
            - name: SPARK_HISTORY_KERBEROS_ENABLED
              value: 'true'
            - name: SPARK_HISTORY_PRINCIPAL
              value: k8s@VAILAKEHOUSE.VIETTEL.COM
            - name: SPARK_HISTORY_KEYTAB
              value: /etc/security/keytabs/k8s.keytab
          ports:
            - containerPort: 18080
              name: http
              protocol: TCP
          livenessProbe:
            failureThreshold: 3
            httpGet:
              path: /
              port: 18080
              scheme: HTTP
            periodSeconds: 30
            successThreshold: 1
            timeoutSeconds: 5
          readinessProbe:
            failureThreshold: 3
            httpGet:
              path: /
              port: 18080
              scheme: HTTP
            periodSeconds: 30
            successThreshold: 1
            timeoutSeconds: 5
          resources:
            limits:
              cpu: '1'
              memory: 2G
            requests:
              cpu: 100m
              memory: 1G
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop:
                - ALL
            runAsGroup: 185
            runAsNonRoot: true
            runAsUser: 185
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          volumeMounts:
            - mountPath: /opt/spark/template/spark-properties.conf.template
              name: spark-properties
              subPath: spark-properties.conf.template
            # Hadoop conf HKH (nằm trong /opt/spark/conf -> có trên classpath)
            - mountPath: /opt/spark/conf/core-site.xml
              name: hadoop-conf-hkh
              subPath: core-site.xml
            - mountPath: /opt/spark/conf/hdfs-site.xml
              name: hadoop-conf-hkh
              subPath: hdfs-site.xml
            # krb5.conf trỏ KDC HKH
            - mountPath: /etc/krb5.conf
              name: krb5-conf-hkh
              subPath: krb5.conf
              readOnly: true
            # keytab: mount cả thư mục (không subPath) để Secret update được propagate
            - mountPath: /etc/security/keytabs
              name: k8s-keytab
              readOnly: true
      dnsPolicy: ClusterFirst
      imagePullSecrets:
        - name: vlp-registry
      restartPolicy: Always
      schedulerName: default-scheduler
      securityContext:
        fsGroup: 185
      serviceAccount: spark-history-server-sa
      serviceAccountName: spark-history-server-sa
      terminationGracePeriodSeconds: 30
      volumes:
        - name: spark-properties
          configMap:
            name: history-server-properties
            defaultMode: 420
            items:
              - key: spark-properties.conf.template
                path: spark-properties.conf.template
        - name: hadoop-conf-hkh
          configMap:
            name: hdfs-hadoop-hkh
            defaultMode: 420
            items:
              - key: core-site.xml
                path: core-site.xml
              - key: hdfs-site.xml
                path: hdfs-site.xml
        - name: krb5-conf-hkh
          configMap:
            name: krb5-config-hkh
            defaultMode: 420
            items:
              - key: krb5.conf
                path: krb5.conf
        - name: k8s-keytab
          secret:
            secretName: k8s-team-keytab
            # 288 = 0440. Pod có fsGroup 185 nên kubelet set group 185 -> uid 185 đọc được.
            # (Bản đã apply đang để 420 = 0644, vẫn chạy nhưng lỏng hơn.)
            defaultMode: 288
            items:
              - key: k8s.keytab
                path: k8s.keytab
```

### 3.3 Template `spark-properties.conf.template` (ConfigMap `history-server-properties`)

Template **phải** có các dòng sau. Mỗi key chỉ được xuất hiện **một lần**, vì trong file properties dòng sau đè dòng trước:

```properties
spark.history.fs.logDirectory=${SPARK_HISTORY_FS_LOG_DIRECTORY}
spark.history.kerberos.enabled=${SPARK_HISTORY_KERBEROS_ENABLED}
spark.history.kerberos.principal=${SPARK_HISTORY_PRINCIPAL}
spark.history.kerberos.keytab=${SPARK_HISTORY_KEYTAB}
```

Các lưu ý:
- Xoá mọi giá trị hardcode kiểu `/etc/security/keytabs/spark.keytab` hoặc principal `spark@...`.
- Xoá mọi `spark.hadoop.fs.defaultFS` hay `spark.hadoop.*` trỏ về HLA, vì chúng sẽ đè `core-site.xml`.
- Các property khác đang có (ui.port, cleaner, update interval, retainedApplications, ...) giữ nguyên.
- ConfigMap mount bằng `subPath` nên **không tự cập nhật** vào pod. Sau khi sửa phải `kubectl rollout restart`.

---

## 4. SparkApplication — Ghi event log vào Spark History

### 4.1 Thay đổi

| Key sparkConf | Trước | Sau |
|---|---|---|
| `spark.eventLog.enabled` | `'false'` | `'true'` |
| `spark.eventLog.dir` | — | `hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020/spark_history` |
| `spark.eventLog.rolling.enabled` | — | `'true'` (tuỳ chọn, nên dùng cho job dài / streaming) |
| `spark.eventLog.rolling.maxFileSize` | — | `128m` (tuỳ chọn) |

Không cần thêm volume. Driver ghi event log bằng delegation token lấy lúc `spark-submit`, và `spark.eventLog.dir` cùng filesystem với `spark.kerberos.access.hadoopFileSystems`.

**Quy tắc:** `spark.eventLog.dir` phải khớp **chính xác scheme + host + port + path** với `spark.history.fs.logDirectory` của SHS.

### 4.2 Manifest sau khi sửa

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: vlp-test-job-hungvt
  namespace: vlp-tenantw1xjixm-wsytjjtr0-ingestion
spec:
  arguments:
    - '2026'
    - '09'
    - '11'
    - 'all'

  driver:
    coreLimit: 1000m
    cores: 1
    labels:
      version: 3.5.1
    memory: 1g
    serviceAccount: spark-application-sa

  executor:
    coreLimit: 1000m
    cores: 1
    instances: 1
    labels:
      version: 3.5.1
    memory: 1g
    serviceAccount: spark-application-sa

  image: hub.vtcc.vn:8989/hungvt0110/spark-wire-encryption-app:v0.1
  imagePullPolicy: Always
  mainApplicationFile: local:///opt/app/sample-spark-application-privacy-1.0-SNAPSHOT.jar
  mainClass: org.example.SparkApp
  mode: cluster

  restartPolicy:
    type: Never

  hadoopConfigMap: hdfs-hadoop-hkh

  sparkConf:
    # ------ Kerberos -------
    spark.kerberos.principal: k8s@VAILAKEHOUSE.VIETTEL.COM
    spark.kerberos.keytab: /etc/security/keytabs/k8s.keytab
    spark.kubernetes.kerberos.krb5.path: /etc/krb5.conf
    spark.kerberos.access.hadoopFileSystems: hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020
    spark.security.credentials.hadoopfs.enabled: 'true'
    spark.hadoop.security.authentication: kerberos
    spark.hadoop.security.authorization: 'true'
    spark.hadoop.fs.permissions.umask-mode: '002'

    spark.driver.userClassPathFirst: "false"
    spark.executor.userClassPathFirst: "false"

    spark.driver.extraJavaOptions: '-Dlog4j.configuration=file:/opt/app/conf/config.properties -Dsun.security.krb5.debug=true -Dsun.security.spnego.debug=true'
    spark.executor.extraJavaOptions: '-Dlog4j.configuration=file:/opt/app/conf/config.properties -Dsun.security.krb5.debug=true'

    # ----- Event log -> Spark History Server -----
    spark.eventLog.enabled: 'true'
    spark.eventLog.dir: hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020/spark_history
    spark.eventLog.rolling.enabled: 'true'
    spark.eventLog.rolling.maxFileSize: 128m

    # ----- Spark -----
    spark.jars.ivy: /tmp
    spark.scheduler.mode: FAIR
    spark.serializer: org.apache.spark.serializer.KryoSerializer
    spark.sql.adaptive.coalescePartitions.enabled: 'true'
    spark.sql.adaptive.enabled: 'true'
    spark.sql.sources.ignoreDataLocality.enabled: 'true'

  sparkVersion: 3.5.1
  timeToLiveSeconds: 1800
  type: Scala
```

### 4.3 Lưu ý phía SparkApplication

- `spark.kerberos.keytab` được đọc tại nơi chạy `spark-submit`, tức **pod operator `s-ops`**, không phải driver pod. Secret keytab phải được mount vào pod operator đúng path `/etc/security/keytabs/k8s.keytab`.
- App (`org.example.SparkApp`) phải gọi `spark.stop()` khi kết thúc. Nếu không, event log giữ hậu tố `.inprogress` và SHS hiển thị app ở tab *Incomplete* mãi.
- Spark tạo event log với quyền `660` (file) / `770` (thư mục rolling), owner là principal submit. App và SHS cùng dùng `k8s` nên đọc/xoá được. Nếu có app dùng principal khác, cần xử lý quyền (ví dụ chmod `1777` cho `/spark_history` hoặc ACL/Ranger).
- SHS đang có `SPARK_CLEANER_MAX_AGE=24h`. Nếu template map biến này vào `spark.history.fs.cleaner.maxAge`, log cũ hơn 1 ngày sẽ bị xoá khỏi HDFS.

---

## 5. Lịch sử lỗi đã gặp

### 5.1 `kubectl apply` bị reject: strict decoding error

```
strict decoding error: unknown field "spec.template.spec.volumes[1].configMap.default",
                       unknown field "spec.template.spec.volumes[1].items"
```

**Nguyên nhân** (volume `hadoop-conf-hkh`):
1. Viết `default:` thay vì `defaultMode:`.
2. `items:` bị thụt lề ngang cấp `configMap:` (phải nằm trong `configMap`).
3. Typo `path: hdf-site.xml` (phải là `hdfs-site.xml`). Lỗi này không làm apply fail, nhưng subPath không tồn tại sẽ khiến kubelet tạo **thư mục rỗng** tại `/opt/spark/conf/hdfs-site.xml`.

**Trạng thái:** đã sửa, apply thành công.

Warning `missing the kubectl.kubernetes.io/last-applied-configuration annotation` không phải lỗi: resource được tạo bởi Helm, kubectl tự patch annotation.

### 5.2 SHS crash: keytab không tồn tại

```
Exception in thread "main" org.apache.spark.SparkException:
  Keytab file: /etc/security/keytabs/spark.keytab does not exist
    at org.apache.spark.deploy.SparkHadoopUtil.loginUserFromKeytab(SparkHadoopUtil.scala:142)
    at org.apache.spark.deploy.history.HistoryServer$.initSecurity(HistoryServer.scala:352)
```

**Nguyên nhân:** keytab được mount tại `/etc/security/keytabs/k8s.keytab`, nhưng template `history-server-properties` hardcode `spark.history.kerberos.keytab=/etc/security/keytabs/spark.keytab` (hoặc có dòng trùng key đè lên).

**Cách xử lý:** sửa template theo mục 3.3, sau đó `rollout restart`.

**Trạng thái:** ⏳ **CHƯA XỬ LÝ**. Deployment đang `Updating`, Ready 0/1, pod restart liên tục.

Kiểm tra template:
```powershell
kubectl -n vlp-tenantw1xjixm-wsytjjtr0-data get configmap history-server-properties -o yaml | Select-String "kerberos|logDirectory"
```

---

## 6. Checklist kiểm tra / debug

### 6.1 Ngoài manifest

- [ ] `krb5-config-hkh`: `kdc`/`admin_server` trỏ tới KDC của **HKH** (không phải HLA, vì hai cụm trùng realm). Trong `[libdefaults]` có `udp_preference_limit = 1` (LB KDC chỉ TCP) và `dns_lookup_kdc = false`.
- [ ] Isilon access zone HKH đã bật `hadoop-version-3-or-later=true` (client Hadoop 3.3.4).
- [ ] `/spark_history` có nằm trong encryption zone không? Nếu có: cấp `GENERATE_EEK` (app ghi) và `DECRYPT_EEK` (SHS đọc) cho `k8s` trên Ranger KMS.
- [ ] Pod resolve được `vailakehouse.datalakehkh.viettel.com.vn` (nếu không, dùng `hostAliases`).
- [ ] Principal trong keytab khớp chính xác `k8s@VAILAKEHOUSE.VIETTEL.COM`.
- [ ] Pod operator `s-ops` có mount keytab tại `/etc/security/keytabs/k8s.keytab`.

### 6.2 Lệnh kiểm tra SHS

```powershell
$NS = "vlp-tenantw1xjixm-wsytjjtr0-data"

# restart sau khi sửa ConfigMap
kubectl -n $NS rollout restart deployment spark-history-server

# log
kubectl -n $NS logs deploy/spark-history-server | Select-String -Pattern "kerberos|Exception"

# khi pod đã Running:
kubectl -n $NS exec deploy/spark-history-server -- klist -kt /etc/security/keytabs/k8s.keytab
kubectl -n $NS exec deploy/spark-history-server -- grep -E "logDirectory|kerberos" /opt/spark/conf/spark-properties.conf
kubectl -n $NS exec deploy/spark-history-server -- env | Select-String CONF_DIR
```

Log mong đợi khi login thành công:
```
INFO SparkHadoopUtil: Attempting to login to Kerberos using principal: k8s@VAILAKEHOUSE.VIETTEL.COM and keytab: /etc/security/keytabs/k8s.keytab
```

Để debug Kerberos, tạm thêm vào `SPARK_HISTORY_OPTS`:
```
-Dsun.security.krb5.debug=true -Dsun.security.spnego.debug=true
```

### 6.3 Kiểm tra phía SparkApplication

- Log driver có dòng: `Logging events to hdfs://vailakehouse.datalakehkh.viettel.com.vn:8020/spark_history/...`
- Trên HDFS khi app đang chạy:
  - Không rolling: `spark-<appId>.inprogress`
  - Có rolling: `eventlog_v2_spark-<appId>/` gồm `events_1_...` + `appstatus_spark-<appId>.inprogress`
- App kết thúc → mất hậu tố `.inprogress` → sau ~5s (`SPARK_UPDATE_INTERVAL`) app hiện trên UI SHS.

---

## 7. TODO / Việc còn tồn đọng

1. **[Blocker]** Sửa template trong ConfigMap `history-server-properties` (mục 3.3), rồi `rollout restart` SHS.
2. Apply SparkApplication với event log đã bật (mục 4.2) và xác nhận SHS hiển thị app.
3. **[Bảo mật]** S3 access key/secret key đang hardcode plaintext trong `args` của Deployment SHS và đã lộ qua ảnh chụp màn hình. Cần chuyển vào Secret (`env.valueFrom.secretKeyRef`) và **rotate** cặp key.
4. **[GitOps]** Deployment SHS do **Helm** quản lý (`helm.sh/chart: spark-history-server-1.2.0`). Các thay đổi bằng `kubectl apply` sẽ bị ghi đè bởi `helm upgrade` hoặc ArgoCD sync/self-heal. Sau khi chạy ổn, cần đưa env/volumes/volumeMounts/template vào values/template của chart.
5. Đổi `defaultMode` của volume keytab từ `420` → `288` (0440).
6. Cân nhắc dọn các env/cấu hình S3 (`SPARK_EVENT_LOG_DIR=s3a://...`, `SPARK_HADOOP_FS_S3A_*`) nếu SHS không còn đọc S3.
7. Xem lại retention `SPARK_CLEANER_MAX_AGE=24h` có phù hợp không.
