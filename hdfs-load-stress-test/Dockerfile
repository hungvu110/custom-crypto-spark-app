# Image cho hdfs-load-stress-test. Build context = THƯ MỤC CHỨA FILE NÀY (hdfs-load-stress-test/).
#
# Build jar trước (xem README.md mục 2), chạy ở gốc repo:
#   mvn -B -pl hdfs-load-stress-test -am clean package   # -am tự build spark-app (jar patch block token) trước
#   docker build -t <registry>/hdfs-load-stress-test:<tag> hdfs-load-stress-test
#
# Image KHÔNG chứa lib crypto, Delta hay jar nào khác: fat jar chỉ gồm app + patch lenient block token.
# keytab/krb5.conf KHÔNG bake vào image: spark-submit (trên pod Spark Operator) đọc chúng rồi tự đưa vào driver.
FROM apache/spark:3.5.1-scala2.12-java11-ubuntu

USER root

# mainApplicationFile dùng scheme local:// -> jar phải nằm đúng path này trong image.
RUN mkdir -p /opt/app
COPY target/hdfs-load-stress-test.jar /opt/app/hdfs-load-stress-test.jar

# /opt/spark/conf bị ConfigMap của Spark mount đè trên K8s -> đặt log4j2 ở /opt/app và trỏ bằng
# -Dlog4j.configurationFile trong sparkConf của manifest.
COPY src/main/resources/log4j2.properties /opt/app/log4j2.properties

# Chỉ cần quyền đọc cho mọi UID (pod có thể bị ép runAsUser khác 185); work-dir cần ghi.
RUN chmod -R a+rX /opt/app && chmod 777 /opt/spark/work-dir

USER 185
