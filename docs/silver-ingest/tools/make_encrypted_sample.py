#!/usr/bin/env python3
"""Tạo file mẫu ebs_3g_4g (csv) và mã hoá cả file theo đúng định dạng job silver-ingest-app giải mã được.

Định dạng file mã hoá (AesGcmFileCipher): IV 12 byte || ciphertext || tag 16 byte, AES-256-GCM, key 32 byte.
Key in ra là base64 chuẩn của 32 byte, đúng giá trị cần đặt làm `keyPrefix` của key đường dẫn trên DAK.

Dùng:
  python3 make_encrypted_sample.py <thư-mục-ra> [--rows 50] [--key <base64 32 byte>]
Ra: <thư-mục-ra>/ebs_3g_4g-160000.csv.plain (csv gốc, để đối chiếu) và <thư-mục-ra>/ebs_3g_4g-160000.csv (đã mã hoá).
"""
import argparse
import base64
import hashlib
import os
import random
import sys

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

HEADER = "imsi,lac,sac,eci,event_time,event_ids,event_result,date_hour"
EVENTS = ["l_service_request", "l_handover", "l_attach", "l_detach", "l_tau"]
RESULTS = ["success", "success", "success", "failure"]
FILE_NAME = "ebs_3g_4g-160000.csv"
BASE_TIME_MS = 1793002785000


def make_rows(count: int) -> list:
    rng = random.Random(20261012)  # cố định để chạy lại ra cùng dữ liệu
    rows = []
    for i in range(count):
        imsi = hashlib.md5(f"imsi-{i}".encode()).hexdigest()
        lac = "" if rng.random() < 0.6 else str(rng.randint(1000, 60000))
        sac = "" if rng.random() < 0.6 else str(rng.randint(1000, 60000))
        eci = str(rng.randint(10_000_000, 999_999_999))
        event_time = str(BASE_TIME_MS + i * 137 + rng.randint(0, 99))
        rows.append(",".join([imsi, lac, sac, eci, event_time, rng.choice(EVENTS), rng.choice(RESULTS), "2026-10-26-16"]))
    return rows


def encrypt(plain: bytes, key: bytes) -> bytes:
    iv = os.urandom(12)
    return iv + AESGCM(key).encrypt(iv, plain, None)  # AESGCM trả ciphertext || tag


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("out_dir")
    parser.add_argument("--rows", type=int, default=50)
    parser.add_argument("--key", help="base64 chuẩn của đúng 32 byte; không có thì sinh ngẫu nhiên")
    args = parser.parse_args()

    key = base64.b64decode(args.key, validate=True) if args.key else os.urandom(32)
    if len(key) != 32:
        print("key must be exactly 32 bytes", file=sys.stderr)
        return 2

    os.makedirs(args.out_dir, exist_ok=True)
    plain = ("\n".join([HEADER] + make_rows(args.rows)) + "\n").encode("utf-8")
    with open(os.path.join(args.out_dir, FILE_NAME + ".plain"), "wb") as f:
        f.write(plain)
    with open(os.path.join(args.out_dir, FILE_NAME), "wb") as f:
        f.write(encrypt(plain, key))

    print(f"rows: {args.rows}")
    print(f"key (base64, 32 bytes): {base64.b64encode(key).decode()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
