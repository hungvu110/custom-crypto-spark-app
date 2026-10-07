# syntax=docker/dockerfile:1
#
# Image cho dak-mock. Build từ thư mục dak-mock (build context = dak-mock/):
#   docker build -t hub.vtcc.vn:8989/dak-mock:0.1.0 dak-mock
#
# Hai bước: build jar bằng Maven + JDK 17, rồi chỉ copy jar sang image JRE 17 (không có Maven, mã nguồn, .env).

# ---------- Bước 1: build ----------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
# Tải dependency trước, tách khỏi mã nguồn: sửa code không làm mất cache layer dependency.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

# ---------- Bước 2: runtime ----------
# jammy (Ubuntu 22.04): image noble (24.04) đã có sẵn user "ubuntu" UID 1000, tạo user 1000 sẽ trùng.
FROM eclipse-temurin:17-jre-jammy

RUN groupadd --system --gid 1000 dak \
 && useradd --system --uid 1000 --gid 1000 --home-dir /app --no-create-home --shell /usr/sbin/nologin dak

WORKDIR /app
# Jar thuộc root, user 1000 chỉ đọc được, không sửa được.
COPY --from=build /src/target/dak-mock-0.1.0-SNAPSHOT.jar /app/app.jar

# Chạy bằng UID/GID số để khớp runAsUser/runAsGroup của Deployment và runAsNonRoot kiểm tra được.
USER 1000:1000

EXPOSE 8085
# JVM tự tính heap theo memory limit của container (75%); OOM thì thoát hẳn để K8s restart pod.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError -Djava.io.tmpdir=/tmp"

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
