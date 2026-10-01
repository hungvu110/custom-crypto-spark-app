# Dockerfile cho sample-spark-application-privacy — multi-stage, mỗi target là 1 biến thể crypto:
#
#   docker build --target no-crypto     -t <repo>:<tag> .   # (mặc định) app thuần, KHÔNG có lib crypto
#   docker build --target column-crypto -t <repo>:<tag> .   # + key-prefix-lib + column-crypto-lib
#   docker build --target cdr-crypto    -t <repo>:<tag> .   # + key-prefix-lib + cdr-crypto-udf + jar đối tác
#
# Jar của app KHÔNG nhúng lib crypto nào (xem spark-app/pom.xml). Lib crypto chỉ là các jar rời được
# COPY vào /opt/app/ ở từng target; COPY KHÔNG tự đưa jar lên classpath — manifest phải nạp chúng qua
# spark.jars (+ spark.sql.extensions nếu gọi bằng SQL function), xem k8s/spark-application-column.yaml và
# k8s/spark-application-cdr.yaml. Image nào chứa jar nào thì manifest phải khớp.
# App gọi lib bằng hàm SQL theo tên qua CRYPTO_ENCRYPT_FUNCTION/CRYPTO_DECRYPT_FUNCTION (column_* hoặc cdr_*).
# Không có --target thì Docker build stage CUỐI (no-crypto). Không dùng --target base làm image chạy.
#
# Build jar trước khi build image (context Docker build = thư mục gốc project):
#   mvn -B clean package                                         # -> spark-app + key-prefix-lib + column-crypto-lib
#   mvn install:install-file -Dfile=DataLakeSecurity_jv8.jar -DgroupId=com.viettel.datalake \
#     -DartifactId=datalake-security -Dversion=jv8 -Dpackaging=jar   # chỉ cho target cdr-crypto, làm 1 lần
#   mvn -Pcdr-crypto -pl cdr-crypto-udf -am clean package -DskipTests # chỉ cho target cdr-crypto
#
# Build base image chính thức apache/spark:3.5.1-scala2.12-java11-ubuntu
# (Scala 2.12, Java 11 — khớp scala.version/hadoop.version trong pom.xml).
# LƯU Ý: tag KHÔNG có suffix "-ubuntu" (vd 3.5.1-scala2.12-java11) không tồn
# tại trên Docker Hub cho apache/spark — luôn phải kèm hậu tố OS.
#
# krb5.conf (spark.kubernetes.kerberos.krb5.path) và keytab
# (spark.kerberos.keytab) KHÔNG được bake vào image ở đây — đó là bí mật,
# phải mount vào pod lúc chạy qua Secret/ConfigMap (spec.driver/executor.
# volumeMounts trong SparkApplication CRD) hoặc image nền khác đã có sẵn
# (ví dụ hub.vtcc.vn:8989/test_spark_kms:v1). Nếu dùng base image này mà job
# fail ở bước Kerberos login (chưa tới bước ghi/đọc HDFS), nghĩa là krb5.conf/
# keytab chưa được mount đúng — không phải lỗi của Dockerfile này.
FROM apache/spark:3.5.1-scala2.12-java11-ubuntu AS base

USER root

# Nếu cần công cụ debug Kerberos thủ công (kinit/klist) trong container, bỏ
# comment dòng dưới. KHÔNG bắt buộc: Spark tự login bằng keytab qua JVM
# (UserGroupInformation.loginUserFromKeytab), không cần binary krb5-user.
# RUN apt-get update && apt-get install -y --no-install-recommends krb5-user \
#     && rm -rf /var/lib/apt/lists/*

# mainApplicationFile trong SparkApplication CRD dùng scheme "local://" ->
# Spark Operator KHÔNG upload jar lúc submit, nó chỉ chạy đúng path có sẵn
# TRONG IMAGE của driver/executor. Jar phải nằm ĐÚNG path khai trong CRD:
#   local:///opt/app/sample-spark-application-privacy-1.0-SNAPSHOT.jar
RUN mkdir -p /opt/app
COPY spark-app/target/sample-spark-application-privacy-1.0-SNAPSHOT.jar \
     /opt/app/sample-spark-application-privacy-1.0-SNAPSHOT.jar

# --- log4j2: PHẢI đặt ở /opt/app, KHÔNG được chỉ dựa vào /opt/spark/conf ---
#
# Khi chạy trên Kubernetes (Spark Operator / spark-submit master=k8s://), Spark
# tự tạo 1 ConfigMap (spark-conf-volume-driver / spark-conf-volume-exec) chứa
# spark.properties rồi MOUNT ĐÈ lên đúng /opt/spark/conf của pod
# (hằng số SPARK_CONF_DIR_INTERNAL="/opt/spark/conf" trong spark-kubernetes jar).
# Volume mount của K8s thay thế TOÀN BỘ nội dung thư mục -> mọi file bake sẵn
# trong image tại /opt/spark/conf (kể cả log4j2.properties) BỊ CHE, không còn
# tồn tại lúc chạy. Log4j2 không thấy config nào -> rơi về fallback nhúng trong
# spark-core (org/apache/spark/log4j2-defaults.properties, rootLogger.level=info)
# -> log INFO của TaskSetManager/DAGScheduler/MemoryStore... tràn ra.
#
# Cách chuẩn cho K8s: để file ở path KHÔNG bị mount đè (/opt/app) rồi trỏ JVM
# tới nó bằng -Dlog4j.configurationFile (xem sparkConf trong
# k8s/spark-application*.yaml). System property này có độ ưu tiên cao nhất trong
# ConfigurationFactory của Log4j2 và được áp ngay lúc JVM khởi động.
COPY spark-app/src/main/resources/log4j2.properties /opt/app/log4j2.properties

# Vẫn giữ thêm bản ở /opt/spark/conf để các kiểu chạy KHÔNG qua K8s (docker run
# thủ công, spark-submit local trong container) vẫn được lọc log đúng — lúc đó
# không có ConfigMap nào mount đè nên file này có tác dụng bình thường.
COPY spark-app/src/main/resources/log4j2.properties /opt/spark/conf/log4j2.properties


# ======================================================================
# Target: column-crypto — hàm SQL column_encrypt/column_decrypt (AES-256-GCM, built-in expression).
# key-prefix-lib (hạ tầng lấy keyPrefix) + column-crypto-lib đều chỉ là jar mỏng, executor không cần
# chúng (biểu thức chỉ gồm hàm built-in của Spark), nhưng driver cần cả 2 trên classpath.
# ======================================================================
FROM base AS column-crypto
COPY key-prefix-lib/target/key-prefix-lib-1.0-SNAPSHOT.jar /opt/app/key-prefix-lib-1.0-SNAPSHOT.jar
COPY column-crypto-lib/target/column-crypto-lib-1.0-SNAPSHOT.jar /opt/app/column-crypto-lib-1.0-SNAPSHOT.jar
# Nới lỏng quyền hết mức, xem giải thích ở target no-crypto bên dưới.
RUN chmod -R 777 /opt/app /opt/spark /tmp
USER 185


# ======================================================================
# Target: cdr-crypto — hàm SQL cdr_encrypt/cdr_decrypt bọc lib mã hoá CDR của đối tác (UDF thật,
# executor CŨNG cần cả 3 jar dưới đây trên classpath -> spark.jars trong manifest).
# jar đối tác KHÔNG phải tài sản của mình: chỉ COPY nguyên trạng, không repackage/shade.
# ======================================================================
FROM base AS cdr-crypto
COPY key-prefix-lib/target/key-prefix-lib-1.0-SNAPSHOT.jar /opt/app/key-prefix-lib-1.0-SNAPSHOT.jar
COPY cdr-crypto-udf/target/cdr-crypto-udf-1.0-SNAPSHOT.jar /opt/app/cdr-crypto-udf-1.0-SNAPSHOT.jar
COPY DataLakeSecurity_jv8.jar /opt/app/DataLakeSecurity_jv8.jar
RUN chmod -R 777 /opt/app /opt/spark /tmp
USER 185


# ======================================================================
# Target: no-crypto (mặc định vì là stage cuối) — app thuần, không có lib crypto nào.
# ======================================================================
FROM base AS no-crypto

# Nới lỏng quyền hết mức trên MỌI thư mục Spark cần đọc/ghi lúc chạy, để
# tránh lỗi permission khi securityContext của pod trên K8s ép UID/GID khác
# với UID mặc định "spark" (185) của base image — ví dụ runAsUser ngẫu nhiên,
# fsGroup lạ, hoặc cluster tự áp securityContext riêng ngoài tầm kiểm soát
# của SparkApplication CRD. 777 (rwx cho owner/group/other) là mức lỏng nhất
# có thể ở tầng filesystem; không có gì lỏng hơn ngoài việc chạy container
# bằng root (không tự làm ở đây, xem USER cuối file).
RUN chmod -R 777 /opt/app /opt/spark /tmp

# Giữ lại user mặc định "spark" (uid 185) của base image thay vì set USER root
# ở đây: nhiều cluster (OpenShift, Pod Security Admission "restricted", ...)
# CHẶN pod chạy bằng root (runAsNonRoot: true) — set root cứng trong image sẽ
# khiến pod bị từ chối thẳng thay vì chỉ lỗi permission. Vì thư mục đã 777 ở
# trên, dù K8s securityContext ép chạy bằng UID nào (kể cả khác 185) thì vẫn
# đọc/ghi được — không cần image phải chạy đúng UID đó.
USER 185
