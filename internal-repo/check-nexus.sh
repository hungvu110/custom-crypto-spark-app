#!/usr/bin/env bash
# Kiểm tra Nexus nội bộ có đủ artifact để build pseudonym-udf / pseudonymize-table-app / pseudonym-verify-utils KHÔNG,
# TRƯỚC khi chạy mvn (lỗi thiếu artifact của Maven khá khó đọc khi có mirror).
#
#   internal-repo/check-nexus.sh                                   # dùng Nexus mặc định của công ty
#   NEXUS=http://host:8888/repository/maven-public internal-repo/check-nexus.sh
#   NEXUS_USER=u NEXUS_PASSWORD=p internal-repo/check-nexus.sh     # khi Nexus bắt đăng nhập
#
# Danh sách dưới đây là các artifact TRỰC TIẾP của 2 module mới + plugin build. Các module mới không cần artifact bên thứ
# ba nào mà module cũ (spark-app, column-crypto-lib, key-prefix-lib) chưa dùng — nếu máy build được module cũ qua Nexus
# thì phần còn lại (dependency bắc cầu của Spark) đã có sẵn.
#
# Exit code: 0 = đủ, 1 = thiếu ít nhất 1 artifact, 2 = không kết nối được Nexus.
set -uo pipefail

NEXUS="${NEXUS:-http://10.30.154.118:8888/repository/maven-public}"
NEXUS="${NEXUS%/}"
AUTH=()
[[ -n "${NEXUS_USER:-}" ]] && AUTH=(-u "${NEXUS_USER}:${NEXUS_PASSWORD:-}")

# group:artifact:version  (version khớp pom.xml gốc: spark 3.5.1, scala 2.12.18, delta 3.2.0, hadoop 3.3.4)
ARTIFACTS=(
  # --- dependency trực tiếp của pseudonym-udf / pseudonymize-table-app / pseudonym-verify-utils ---
  org.apache.spark:spark-sql_2.12:3.5.1
  org.apache.spark:spark-core_2.12:3.5.1
  org.scala-lang:scala-library:2.12.18
  io.delta:delta-spark_2.12:3.2.0
  io.delta:delta-storage:3.2.0
  com.google.crypto.tink:tink:1.9.0
  org.yaml:snakeyaml:2.2
  org.apache.hadoop:hadoop-common:3.3.4
  org.scalatest:scalatest_2.12:3.2.18
  # --- plugin build ---
  net.alchim31.maven:scala-maven-plugin:4.8.1
  org.scalatest:scalatest-maven-plugin:2.2.0
  org.apache.maven.plugins:maven-shade-plugin:3.5.1
  org.apache.maven.plugins:maven-jar-plugin:3.4.1
)

missing=0
echo ">> Nexus: $NEXUS"
for gav in "${ARTIFACTS[@]}"; do
  IFS=: read -r g a v <<< "$gav"
  url="$NEXUS/${g//.//}/$a/$v/$a-$v.pom"
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 "${AUTH[@]}" -I "$url") || code=000
  case "$code" in
    200) printf '   [ OK ]   %s\n' "$gav" ;;
    000) printf '   [ ERR ]  %s — không kết nối được %s\n' "$gav" "$NEXUS"; exit 2 ;;
    401|403) printf '   [ AUTH ] %s — Nexus yêu cầu đăng nhập (đặt NEXUS_USER/NEXUS_PASSWORD, và <servers> trong ~/.m2/settings.xml)\n' "$gav"; missing=1 ;;
    *) printf '   [ MISS ] %s — HTTP %s (%s)\n' "$gav" "$code" "$url"; missing=1 ;;
  esac
done

if [[ $missing -eq 0 ]]; then
  echo ">> Đủ artifact: có thể build — ./mvnw -B -pl pseudonymize-table-app -am clean package"
else
  echo ">> THIẾU artifact: nhờ admin Nexus đảm bảo maven-public proxy Maven Central (artifact gốc đều có trên Central)."
fi
exit $missing
