#!/usr/bin/env python3
"""Create a sample file of a VTNET dataset and, by default, encrypt the whole file the way the silver ingest job decrypts it.

Encrypted file format (AesGcmFileCipher): IV 12 bytes || ciphertext || tag 16 bytes, AES-256-GCM, 32-byte key. The key printed is the
standard base64 of those 32 bytes, exactly the value to store as the `keyPrefix` of the by-path key of the DAK dataset.

Usage:
  python3 make_encrypted_sample.py <out dir> [--dataset nims|ebs_3g_4g|f001-mpre-call] [--rows 50] [--key <base64 of 32 bytes>] [--plain]

Output in <out dir>:
  <file name>        the file to upload to HDFS (encrypted; with --plain it is the clear csv)
  <file name>.plain  the clear csv, only to compare with the table afterwards (never upload it). Not written with --plain.
"""
import argparse
import base64
import hashlib
import os
import random
import sys

DATASETS = {
    # NIMS cells: nims/dt=YYYYMMDD/batch=N/infra_cell_4g-<time>.csv
    "nims": {
        "file": "infra_cell_4g-130012.csv",
        "header": "tac,eci,lng,lat,city,district,cell_status",
    },
    # EBS 3G/4G: ebs_3g_4g/dt=YYYYMMDD/batch=HHmm/ebs_3g_4g-<time>.csv
    "ebs_3g_4g": {
        "file": "ebs_3g_4g-160000.csv",
        "header": "imsi,lac,sac,eci,event_time,event_ids,event_result,date_hour",
    },
    # VTT call records (already encrypted by column by VTT): f001-mpre-call/date=YYYYMMDD/hour=HH/part-0001.csv
    "f001-mpre-call": {
        "file": "part-0001.csv",
        "header": "msisdn,imei,start_time,duration,cell_id",
    },
}
EVENTS = ["l_service_request", "l_handover", "l_attach", "l_detach", "l_tau"]
RESULTS = ["success", "success", "success", "failure"]
DISTRICTS = ["Phường Phan Đình Phùng", "Phường Gia Sàng", "Phường Quang Trung", "Xã Phúc Hà"]
BASE_TIME_MS = 1793002785000
SEED = 20261012  # fixed so that running again gives the same rows


def nims_rows(count: int) -> list:
    rng = random.Random(SEED)
    rows = []
    for i in range(count):
        eci = 53734000 + i
        lng = f"{105.83 + rng.random() / 100:.10f}"
        lat = f"{21.58 + rng.random() / 100:.10f}"
        rows.append(",".join([str(12000 + i % 50), str(eci), lng, lat, "Tỉnh Thái Nguyên", rng.choice(DISTRICTS), str(rng.randint(1, 3))]))
    return rows


def ebs_rows(count: int) -> list:
    rng = random.Random(SEED)
    rows = []
    for i in range(count):
        imsi = hashlib.md5(f"imsi-{i}".encode()).hexdigest()
        lac = "" if rng.random() < 0.6 else str(rng.randint(1000, 60000))
        sac = "" if rng.random() < 0.6 else str(rng.randint(1000, 60000))
        eci = str(rng.randint(10_000_000, 999_999_999))
        event_time = str(BASE_TIME_MS + i * 137 + rng.randint(0, 99))
        rows.append(",".join([imsi, lac, sac, eci, event_time, rng.choice(EVENTS), rng.choice(RESULTS), "2026-10-26-16"]))
    return rows


def vtt_rows(count: int) -> list:
    rng = random.Random(SEED)
    rows = []
    for i in range(count):
        # The VTT columns arrive encrypted: opaque base64-looking values stand for them.
        msisdn = base64.b64encode(hashlib.sha256(f"msisdn-{i}".encode()).digest()[:12]).decode()
        imei = base64.b64encode(hashlib.sha256(f"imei-{i}".encode()).digest()[:12]).decode()
        rows.append(",".join([msisdn, imei, str(BASE_TIME_MS + i * 1000), str(rng.randint(1, 3600)), str(rng.randint(10_000_000, 99_999_999))]))
    return rows


MAKERS = {"nims": nims_rows, "ebs_3g_4g": ebs_rows, "f001-mpre-call": vtt_rows}


def encrypt(plain: bytes, key: bytes) -> bytes:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # imported here: --plain needs no cryptography

    iv = os.urandom(12)
    return iv + AESGCM(key).encrypt(iv, plain, None)  # AESGCM returns ciphertext || tag


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("out_dir")
    parser.add_argument("--dataset", choices=sorted(DATASETS), default="nims")
    parser.add_argument("--rows", type=int, default=50)
    parser.add_argument("--key", help="standard base64 of exactly 32 bytes; a random key when missing")
    parser.add_argument("--plain", action="store_true", help="write the clear csv only, no encryption (no key is printed)")
    args = parser.parse_args()

    dataset = DATASETS[args.dataset]
    os.makedirs(args.out_dir, exist_ok=True)
    plain = ("\n".join([dataset["header"]] + MAKERS[args.dataset](args.rows)) + "\n").encode("utf-8")
    target = os.path.join(args.out_dir, dataset["file"])

    if args.plain:
        with open(target, "wb") as f:
            f.write(plain)
        print(f"rows: {args.rows} (clear file, not encrypted): {target}")
        return 0

    key = base64.b64decode(args.key, validate=True) if args.key else os.urandom(32)
    if len(key) != 32:
        print("key must be exactly 32 bytes", file=sys.stderr)
        return 2
    with open(target + ".plain", "wb") as f:
        f.write(plain)
    with open(target, "wb") as f:
        f.write(encrypt(plain, key))

    print(f"rows: {args.rows}")
    print(f"file: {target}")
    print(f"key (base64, 32 bytes): {base64.b64encode(key).decode()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
