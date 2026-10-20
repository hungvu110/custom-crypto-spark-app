#!/usr/bin/env bash
# Stands in for the SA (Spark Application) system: gets a token from the local Keycloak, then
# asks dak for the key of one table — exactly the way the real caller does, per the agreed
# contract (docs/SA-docs/PHAN-HOI-DAK.md §6, docs/SA-docs/DAK_API_SPEC.md §5):
#
#   GET /api/v1/keys/{database}/{table}   with a client_credentials bearer token
#
#   ./sa-fetch-key.sh --table demo_db.users_cdr
#   ./sa-fetch-key.sh --team billing --table demo_db.invoices
#   ./sa-fetch-key.sh --table demo_db.users_cdr --reveal   # prints the key prefix
#
# `--table` takes `database.table`, the same shape sql-engine passes to its decrypt function
# and SparkApplication passes as ${DB_NAME}.${TABLE_NAME} (spec §6). Like the real caller's
# library, this script splits and validates the name BEFORE calling dak: a malformed name
# never leaves the machine (contract §A — 400 invalid_table is checked before auth).
#
# Nothing is passed on the command line to curl as a plain -d secret: arguments are visible to
# every process on the machine through /proc, same reason dev/run-backend.sh reads dev/.env.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck disable=SC1091
set -a; . ./.env; set +a

TEAM=analytics
TABLE_ARG=""
REVEAL=0

while [ $# -gt 0 ]; do
  case "$1" in
    --team)   TEAM="$2"; shift 2 ;;
    --table)  TABLE_ARG="$2"; shift 2 ;;
    --reveal) REVEAL=1; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

[ -n "$TABLE_ARG" ] || { echo "LOI: thieu --table database.table" >&2; exit 2; }

# Step 1 of the caller's behaviour (spec §6): split, lowercase, validate — exactly one dot,
# each side [a-z0-9_]{1,128}. A missing or second dot is a malformed name; wrong shape exits
# here without sending anything, like key-prefix-lib does for the real caller.
if printf '%s' "$TABLE_ARG" | grep -Eqv '^[A-Za-z0-9_]{1,128}\.[A-Za-z0-9_]{1,128}$'; then
  echo "LOI: --table phai dung dang database.table, moi doan chi gom chu, so, dau gach duoi, toi da 128 ky tu (hop dong §A)" >&2
  exit 2
fi
DATABASE=$(printf '%s' "$TABLE_ARG" | cut -d. -f1 | tr '[:upper:]' '[:lower:]')
TABLE=$(printf '%s' "$TABLE_ARG" | cut -d. -f2 | tr '[:upper:]' '[:lower:]')

case "$TEAM" in
  analytics) CLIENT_ID=sa-vlp-datalake-analytics; CLIENT_SECRET="$SA_ANALYTICS_SECRET" ;;
  billing)   CLIENT_ID=sa-vlp-datalake-billing;   CLIENT_SECRET="$SA_BILLING_SECRET" ;;
  *) echo "LOI: --team chi nhan analytics hoac billing" >&2; exit 2 ;;
esac

KEYCLOAK="http://127.0.0.1:${KEYCLOAK_PORT:-8085}"
DAK="http://127.0.0.1:${DAK_PORT:-8090}"

echo "== Lay token tu Keycloak ($CLIENT_ID)"
# --data-urlencode for the secret: a "+" in a form-urlencoded body arrives as a space, which
# comes back as a bare 401 that looks like a wrong password.
TOKEN_JSON=$(curl -fsS -X POST \
  "$KEYCLOAK/realms/vlp/protocol/openid-connect/token" \
  -d grant_type=client_credentials \
  -d "client_id=$CLIENT_ID" \
  --data-urlencode "client_secret=$CLIENT_SECRET") || {
    echo "LOI: khong lay duoc token. Keycloak chay chua? ./setup.sh" >&2; exit 1; }

TOKEN=$(printf '%s' "$TOKEN_JSON" | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

printf '%s' "$TOKEN" | cut -d. -f2 | python3 -c '
import sys, base64, json, datetime
raw = sys.stdin.read().strip()
raw += "=" * (-len(raw) % 4)
claims = json.loads(base64.urlsafe_b64decode(raw))
exp = datetime.datetime.fromtimestamp(claims["exp"], datetime.timezone.utc).astimezone()
print("   azp=" + str(claims.get("azp")) + "  iss=" + str(claims.get("iss")))
print(f"   het han luc {exp:%H:%M:%S}")'

echo
echo "== Goi dak xin key (GET /api/v1/keys/{database}/{table})"
echo "   database $DATABASE"
echo "   table    $TABLE"

RESP=$(mktemp); HDRS=$(mktemp); trap 'rm -f "$RESP" "$HDRS"' EXIT
CODE=$(curl -s -o "$RESP" -D "$HDRS" -w '%{http_code}' \
  "$DAK/api/v1/keys/$DATABASE/$TABLE" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Accept: application/json')

REQUEST_ID=$(awk 'tolower($1)=="x-request-id:" {sub(/^[^:]*:[[:space:]]*/, ""); gsub(/\r/, ""); print; exit}' "$HDRS")

echo "   HTTP $CODE  requestId=${REQUEST_ID:-<khong co>}"

if [ "$CODE" != 200 ]; then
  echo
  python3 -c 'import sys,json;print(json.dumps(json.load(sys.stdin),ensure_ascii=False,indent=2))' \
    < "$RESP" 2>/dev/null || cat "$RESP"
  echo
  # Contract §C: what the caller should do per error. 401 and 403 are deliberately distinct —
  # 401 means "try again with a fresh token, exactly once"; 403 means "retrying is useless".
  case "$CODE" in
    400) echo "400 invalid_table    = ten bang sai dinh dang (khong nen xay ra: script da kiem truoc)." ;;
    401) echo "401 invalid_token    = van de danh tinh: chu ky, iss, exp, hoac (realm, client_id) khong dang ky / bi khoa. Xin token moi va thu lai dung 1 lan." ;;
    403) echo "403 access_denied    = khong co grant con han cho bang nay (gồm ca quyen da het han, bang khong ton tai, bang khac workspace) — hoac key_disabled = key dang bi khoa. Thu lai vo ich." ;;
    429) echo "429 rate_limited     = vuot gioi han tan suat." ;;
    500) echo "500 internal_error   = loi dak hoac du lieu Vault khong hop le." ;;
    503) echo "503 upstream_unavailable = Keycloak (JWKS chua cache) hoac Vault khong phan hoi." ;;
  esac
  exit 1
fi

# The prefix stays out of stdout unless asked for: terminal history and CI logs are two places
# a key should never end up. This mirrors key-prefix-lib, which caches the prefix in memory
# and never logs it (spec §6).
REVEAL=$REVEAL python3 -c '
import sys, json, os
d = json.load(sys.stdin)
print("   database  " + str(d.get("database")))
print("   table     " + str(d.get("table")))
print("   version   " + str(d.get("keyVersion")) + "  (version KV dak vua doc, khong phai moi nhat)")
if os.environ["REVEAL"] == "1":
    print(f"   keyPrefix {d.get(\"keyPrefix\")}")
else:
    prefix = str(d.get("keyPrefix") or "")
    print(f"   keyPrefix da nhan, dai {len(prefix)} ky tu (dung --reveal de hien)")' < "$RESP"
