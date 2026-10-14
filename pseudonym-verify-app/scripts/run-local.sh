#!/usr/bin/env bash
# Chạy PseudoVerifyApp.main ở máy local (Spark local[2], không cần spark-submit/cluster) rồi tóm tắt kết quả.
#
#   pseudonym-verify-app/scripts/run-local.sh                       # full, HASH+ENCRYPT, 2000 chủ thể
#   PSEUDO_MODES=HASH,ENCRYPT,MAPPING pseudonym-verify-app/scripts/run-local.sh
#   RUN_MODE=selftest pseudonym-verify-app/scripts/run-local.sh
#   pseudonym-verify-app/scripts/run-local.sh --build               # build lại jar + classpath trước khi chạy
#
# Cấu hình = đúng các biến môi trường của app (README mục 4); biến nào không đặt thì dùng mặc định bên dưới.
# Exit code của script = exit code của app (0 PASS / 1 FAIL / 2 ERROR).
set -uo pipefail

cd "$(dirname "$0")/../.."          # gốc repo
APP=pseudonym-verify-app
JAR=$APP/target/pseudonym-verify-app.jar
CP_FILE=$APP/target/local-classpath.txt

if [[ "${1:-}" == "--build" || ! -f "$JAR" || ! -f "$CP_FILE" ]]; then
  echo ">> build jar + classpath (mvn -pl $APP -am package -DskipTests)"
  mvn -B -q -pl "$APP" -am package -DskipTests dependency:build-classpath \
    -Dmdep.outputFile="$PWD/$CP_FILE" -Dmdep.includeScope=provided || exit 2
fi

export RUN_ID="${RUN_ID:-local-$(date +%Y%m%d-%H%M%S)}"
export OUTPUT_PATH="${OUTPUT_PATH:-file:///tmp/pseudonym-verify}"
export N_SUBJECTS="${N_SUBJECTS:-2000}"
export PARTITIONS="${PARTITIONS:-4}"
export PSEUDO_MODES="${PSEUDO_MODES:-HASH,ENCRYPT}"

# Spark 3.5 dùng reflection vào package nội bộ của JDK: JDK 17+ phải mở (giống argLine trong pom.xml gốc).
OPENS=()
for p in java.lang java.lang.invoke java.lang.reflect java.io java.net java.nio java.util java.util.concurrent \
         java.util.concurrent.atomic sun.nio.ch sun.nio.cs sun.security.action sun.util.calendar; do
  OPENS+=("--add-opens=java.base/$p=ALL-UNNAMED")
done

java "${OPENS[@]}" \
  -Dspark.master='local[2]' -Dspark.ui.enabled=false -Dspark.sql.shuffle.partitions=4 \
  -Dlog4j.configurationFile="file:$PWD/$APP/src/main/resources/log4j2.properties" \
  -cp "$JAR:$(cat "$CP_FILE")" \
  vai.lakehouse.pseudonymverify.app.PseudoVerifyApp 2>&1 | grep 'VLP' | sed 's/^.*VLP: //'
code=${PIPESTATUS[0]}

RUN_DIR="${OUTPUT_PATH#file://}/$RUN_ID"
echo
echo "================ KẾT QUẢ ================"
echo "exit code : $code  (0 PASS / 1 FAIL / 2 ERROR)"
echo "thư mục   : $RUN_DIR"
for f in preview.txt report.json selftest-report.json guard-report.json; do
  [[ -f "$RUN_DIR/$f" ]] && echo "file      : $RUN_DIR/$f"
done
if [[ -f "$RUN_DIR/report.json" ]] && command -v python3 >/dev/null; then
  python3 - "$RUN_DIR/report.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
print("verdict   :", r["verdict"], "| rows:", r["rows"])
for mode, v in r["modes"].items():
    failed = [c["id"] for c in v["checks"] if c["status"] in ("FAIL", "ERROR")]
    print(f"  {mode:8} {v['verdict']:6} check không đạt: {failed or 'không'}")
PY
fi
exit "$code"
