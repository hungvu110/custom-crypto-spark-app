#!/usr/bin/env bash
# Fetch / renew / cancel the HDFS delegation token of HKH and put it into the Secret that the silver ingest job (HLA namespace) mounts to
# read the HKH landing. Run it where kubectl works (exec on the pod in POD_NS, get/apply on the Secret in JOB_NS), Linux or Git Bash on
# Windows. docs/silver-ingest/VTNET-EBS-HKH.md.
#
#   tools/hkh-hdfs-token.sh fetch    # NEW token -> read test with the token only -> update the Secret (run it daily, before the DAG)
#   tools/hkh-hdfs-token.sh renew    # extend the current token (Secret unchanged); this client cannot renew the KMS token
#   tools/hkh-hdfs-token.sh check    # show the token, read test with the token only, check the Secret and use_ip of HLA
#   tools/hkh-hdfs-token.sh cancel   # cancel the token now (the job cannot read HKH until the next fetch)
#
# Cron example (every day at 01:00): 0 1 * * * /path/to/tools/hkh-hdfs-token.sh fetch >> /var/log/hkh-hdfs-token.log 2>&1
# The token content is never printed nor written to a local file. Every value below can be overridden by an env variable of the same name.
set -euo pipefail

# Git Bash: do not turn /tmp/... arguments into Windows paths when calling kubectl.exe (they are paths INSIDE the pod).
export MSYS_NO_PATHCONV=1

POD_NS="${POD_NS:-lakehouse}"
POD="${POD:-hdfs-kerberos-client-hkh}"
JOB_NS="${JOB_NS:-vlp-tenantw1xjixm-wsw7vtwvi-teamtscauiy}"
SECRET="${SECRET:-hkh-hdfs-token}"
SECRET_KEY="${SECRET_KEY:-hadoop.token}"
HADOOP_CONFIG_MAP="${HADOOP_CONFIG_MAP:-teamtscauiy-hadoop-conf}"
KEYTAB="${KEYTAB:-/tmp/k8s.keytab}"                       # in the pod
PRINCIPAL="${PRINCIPAL:-k8s@VAILAKEHOUSE.VIETTEL.COM}"
RENEWER="${RENEWER:-k8s}"                                 # short name of PRINCIPAL: only the renewer can renew/cancel
TOKEN="${TOKEN:-/tmp/hkh.token}"                          # in the pod
CHECK_PATH="${CHECK_PATH:-/landing/ebs_3g_4g}"            # HKH folder used for the read test

log() { printf '%s [%s] %s\n' "$(date '+%F %T')" "${1}" "${2}"; }
die() { log FAIL "$1"; exit 1; }

# Run a bash script (read from stdin) in the pod; the values go through env, so their characters do not matter.
in_pod() {
  kubectl -n "$POD_NS" exec -i "$POD" -- env KEYTAB="$KEYTAB" PRINCIPAL="$PRINCIPAL" RENEWER="$RENEWER" TOKEN="$TOKEN" \
    CHECK_PATH="$CHECK_PATH" bash -euo pipefail -s
}

# Read test with the token ONLY (own empty ticket cache, the Kerberos ticket of the pod is not used), like the job reads.
# $1 = suffix after TOKEN ("" or ".new"): the path always comes from TOKEN in the pod.
token_only_read() {
  local suffix="${1:-}"
  in_pod <<EOF
export KRB5CCNAME=FILE:/tmp/no-ticket-\$\$
trap 'rm -f /tmp/no-ticket-\$\$' EXIT
HADOOP_TOKEN_FILE_LOCATION="\$TOKEN$suffix" hdfs dfs -ls "\$CHECK_PATH" > /dev/null
EOF
}

check_use_ip() {
  local value
  value="$(kubectl -n "$JOB_NS" get cm "$HADOOP_CONFIG_MAP" -o jsonpath='{.data.core-site\.xml}' 2>/dev/null \
    | tr -d '\r\n' | grep -o 'token.service.use_ip</name>[[:space:]]*<value>[^<]*' | sed 's/.*<value>//' || true)"
  if [[ "$value" == "false" ]]; then
    log OK "use_ip=false in $HADOOP_CONFIG_MAP: the job finds the token by host:port"
  else
    log WARN "use_ip='${value:-<not set>}' in $HADOOP_CONFIG_MAP: it must be false (core-site.xml), otherwise the job does not find the HKH token"
  fi
}

secret_bytes() {
  kubectl -n "$JOB_NS" get secret "$SECRET" -o jsonpath="{.data.${SECRET_KEY//./\\.}}" 2>/dev/null | tr -d '\r' | base64 -d 2>/dev/null | wc -c | tr -d ' '
}

# The token goes from the pod to the Secret as base64 text through a pipe: no local file (Windows kubectl.exe cannot read MSYS /tmp paths)
# and no binary data through a Windows pipe.
update_secret() {
  local data
  data="$(in_pod <<< 'base64 -w0 "$TOKEN"' | tr -d '\r\n')"
  [[ -n "$data" ]] || die "the token file in the pod is empty: $TOKEN"
  printf 'apiVersion: v1\nkind: Secret\nmetadata:\n  name: %s\n  namespace: %s\ntype: Opaque\ndata:\n  %s: %s\n' \
    "$SECRET" "$JOB_NS" "$SECRET_KEY" "$data" | kubectl apply -f - > /dev/null
  local size
  size="$(secret_bytes)"
  [[ "${size:-0}" -gt 0 ]] || die "Secret $JOB_NS/$SECRET is empty after the update"
  log OK "Secret $JOB_NS/$SECRET updated ($size bytes)"
}

cmd_fetch() {
  log INFO "fetching a new token in pod $POD_NS/$POD"
  in_pod <<'EOF'
kinit -kt "$KEYTAB" "$PRINCIPAL"
hdfs fetchdt --renewer "$RENEWER" "$TOKEN.new"
hdfs fetchdt --print "$TOKEN.new"
hdfs fetchdt --renew "$TOKEN.new" || echo "WARN: renew failed (the token keeps the end it was issued with)"
EOF
  token_only_read ".new" || die "reading $CHECK_PATH with the new token only failed: the Secret is NOT changed"
  log OK "reading $CHECK_PATH with the token only: ok"
  in_pod <<< 'mv -f "$TOKEN.new" "$TOKEN"'
  update_secret
  check_use_ip
}

cmd_renew() {
  log INFO "renewing token $TOKEN in pod $POD_NS/$POD"
  in_pod <<'EOF'
test -s "$TOKEN" || { echo "no token at $TOKEN: run fetch"; exit 1; }
kinit -kt "$KEYTAB" "$PRINCIPAL"
hdfs fetchdt --renew "$TOKEN"
EOF
  token_only_read "" || die "reading $CHECK_PATH with the token only failed: run fetch"
  log OK "renewed, the token can read (Secret unchanged; the KMS token cannot be renewed: use fetch if the landing is in an encryption zone)"
}

cmd_check() {
  in_pod <<< 'test -s "$TOKEN" || { echo "no token at $TOKEN"; exit 1; }; hdfs fetchdt --print "$TOKEN"'
  if token_only_read ""; then
    log OK "reading $CHECK_PATH with the token only: ok"
  else
    log FAIL "reading $CHECK_PATH with the token only: FAILED (run fetch)"
  fi
  log INFO "Secret $JOB_NS/$SECRET: $(secret_bytes) bytes"
  check_use_ip
}

cmd_cancel() {
  log INFO "cancelling token $TOKEN"
  in_pod <<'EOF'
kinit -kt "$KEYTAB" "$PRINCIPAL"
hdfs fetchdt --cancel "$TOKEN"
EOF
  log OK "cancelled: the job cannot read HKH until the next fetch"
}

case "${1:-}" in
  fetch) cmd_fetch ;;
  renew) cmd_renew ;;
  check) cmd_check ;;
  cancel) cmd_cancel ;;
  *) echo "usage: $0 fetch|renew|check|cancel" >&2; exit 2 ;;
esac
