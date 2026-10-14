# Image cho pseudonym-verify-app. Build context = GỐC REPO (cần jar của 3 module).
#
# Build jar trước (xem pseudonym-verify-app/README.md mục 2), chạy ở gốc repo:
#   mvn -B -pl pseudonym-verify-app -am clean package     # -am: build key-prefix-lib, pseudonym-udf, spark-app (patch)
#   docker build -f pseudonym-verify-app/Dockerfile -t <registry>/pseudonym-verify-app:<tag> .
#
# Fat jar CHỈ gồm app + Delta + patch lenient block token. Lib giả danh (pseudonym-udf) và hạ tầng lấy khoá
# (key-prefix-lib) là 2 jar RỜI, manifest nạp qua spark.jars — đúng cách một job thật dùng lib (UDF thật nên executor
# cũng cần 2 jar này). AES-SIV dùng Tink 1.9.0 có sẵn trong /opt/spark/jars của image Spark.
# Khoá giả danh và keytab/krb5.conf KHÔNG bake vào image.
FROM apache/spark:3.5.1-scala2.12-java11-ubuntu

USER root

# mainApplicationFile và spark.jars dùng scheme local:// -> jar phải nằm đúng các path này trong image.
RUN mkdir -p /opt/app
COPY pseudonym-verify-app/target/pseudonym-verify-app.jar /opt/app/pseudonym-verify-app.jar
COPY pseudonym-udf/target/pseudonym-udf-1.0-SNAPSHOT.jar /opt/app/pseudonym-udf-1.0-SNAPSHOT.jar
COPY key-prefix-lib/target/key-prefix-lib-1.0-SNAPSHOT.jar /opt/app/key-prefix-lib-1.0-SNAPSHOT.jar

# /opt/spark/conf bị ConfigMap của Spark mount đè trên K8s -> đặt log4j2 ở /opt/app và trỏ bằng
# -Dlog4j.configurationFile trong sparkConf của manifest.
COPY pseudonym-verify-app/src/main/resources/log4j2.properties /opt/app/log4j2.properties

# Chỉ cần quyền đọc cho mọi UID (pod có thể bị ép runAsUser khác 185); work-dir cần ghi.
RUN chmod -R a+rX /opt/app && chmod 777 /opt/spark/work-dir

USER 185
