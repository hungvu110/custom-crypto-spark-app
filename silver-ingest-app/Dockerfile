# Image cho silver-ingest-app (VTT và VTNET dùng chung image, khác mainClass). Build context = GỐC REPO.
#
# Build jar trước (cần jar đối tác đã cài vào ~/.m2, xem docs/project/BUILD_AND_TEST.md), chạy ở gốc repo:
#   mvn -B -Pcdr-crypto -pl silver-ingest-app -am clean package -DskipTests
#   docker build -f silver-ingest-app/Dockerfile -t <registry>/silver-ingest-app:<tag> .
#
# Fat jar CHỈ gồm app + Delta + spark-avro + patch lenient block token. Lib mã hoá cột (cdr-crypto-udf), hạ tầng lấy key
# (key-prefix-lib) và jar đối tác (DataLakeSecurity_jv8.jar) là jar RỜI, DAG nạp qua spark.jars (cdr_encrypt là UDF thật nên
# executor cũng cần), giống target "cdr-crypto" của Dockerfile gốc.
#
# KHÔNG bake vào image: client secret DAK, keytab, krb5.conf — cấp lúc chạy (xem DAG).
FROM apache/spark:3.5.1-scala2.12-java11-ubuntu

USER root

# mainApplicationFile và spark.jars dùng scheme local:// -> jar phải nằm đúng các path này trong image.
RUN mkdir -p /opt/app
COPY silver-ingest-app/target/silver-ingest-app.jar /opt/app/silver-ingest-app.jar
COPY cdr-crypto-udf/target/cdr-crypto-udf-1.0-SNAPSHOT.jar /opt/app/cdr-crypto-udf-1.0-SNAPSHOT.jar
COPY key-prefix-lib/target/key-prefix-lib-1.0-SNAPSHOT.jar /opt/app/key-prefix-lib-1.0-SNAPSHOT.jar
COPY DataLakeSecurity_jv8.jar /opt/app/DataLakeSecurity_jv8.jar

# /opt/spark/conf bị ConfigMap của Spark mount đè trên K8s -> đặt log4j2 ở /opt/app và trỏ bằng -Dlog4j.configurationFile.
COPY silver-ingest-app/src/main/resources/log4j2.properties /opt/app/log4j2.properties

# Chỉ cần quyền đọc cho mọi UID (pod có thể bị ép runAsUser khác 185); work-dir cần ghi.
RUN chmod -R a+rX /opt/app && chmod 777 /opt/spark/work-dir

USER 185
