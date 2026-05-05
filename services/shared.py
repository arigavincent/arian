"""
Arian Shared Utilities — HMAC signing, API requests, DB connection.
Used by all services (discovery, enrichment, collections).
"""

import base64
import hashlib
import hmac
import json
import os
import re
import time
import urllib.parse

import psycopg2
import requests

GK = os.getenv("ARIAN_GATEWAY_SECRET_B64", "76iRl07s0xSN9jqmEWAt79EBJZulIQIsV64FZr2O")
TK = os.getenv(
    "ARIAN_AUTHORIZATION_TOKEN",
    "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUyNjUzMzI5ODM2MDY2OTQxNTIsImV4cCI6MTc4MzgwMzE4MiwiaWF0IjoxNzc2MDI2ODgyfQ.r5VfgM_olW1OYsffeNFEaQWpDwz5E2uET1KOwagsJs0",
)
XCI = json.dumps(
    {
        "package_name": "com.community.oneroom",
        "version_name": "3.0.11.1230.03",
        "version_code": 50020080,
        "os": "android",
        "os_version": "5.1",
        "install_ch": "google-play",
        "device_id": "55c20416405ccb8cd07008fb2ddc75ee",
        "install_store": "gp",
        "gaid": "ec6a4dc4-2edd-41fe-a999-177067e603ee",
        "brand": "TECNO",
        "model": "TECNO CM5",
        "system_language": "en",
        "net": "NETWORK_WIFI",
        "region": "US",
        "timezone": "Africa/Nairobi",
        "sp_code": "63910",
        "X-Play-Mode": "1",
        "X-Family-Mode": "0",
        "X-Content-Mode": "0",
    }
)
UA = (
    "com.community.oneroom/50020080 (Linux; U; Android 5.1; en_US; "
    "TECNO CM5; Build/AP3A.240905.015.A2; Cronet/146.0.7680.144)"
)
BD = os.getenv("ARIAN_BASE_DOMAIN", "https://api6.aoneroom.com")
AV = os.getenv("ARIAN_API_VERSION", "663997536af5c372f70b9f394dbefe22")
DB_URL = os.getenv("DATABASE_URL", "postgresql://arian:arian@localhost:5432/arian")

_db_conn = None


def get_db():
    global _db_conn
    if _db_conn is None or _db_conn.closed:
        _db_conn = psycopg2.connect(DB_URL)
        _db_conn.autocommit = True
    return _db_conn


def sig(method, path):
    k = base64.b64decode(GK)
    ts = str(int(time.time() * 1000))
    u = urllib.parse.urlsplit(path)
    sq = ""
    if u.query:
        p = [x.split("=", 1) if "=" in x else (x, "") for x in u.query.split("&")]
        p.sort(key=lambda x: x[0])
        sq = "&".join(f"{a}={b}" for a, b in p)
    np = f"{u.path}?{sq}" if sq else u.path
    sts = f"{method.upper()}\n\n\n\n{ts}\n\n{np}"
    h = hmac.new(k, sts.encode(), hashlib.md5).digest()
    return f"{ts}|2|{base64.b64encode(h).decode()}"


def send(method, endpoint, query):
    path_query = f"{endpoint}?{query}" if query else endpoint
    s2 = sig(method, path_query)
    headers = {
        "x-play-mode": "1",
        "x-family-mode": "0",
        "x-content-mode": "0",
        "x-client-info": XCI,
        "x-client-status": "1",
        "authorization": TK,
        "user-agent": UA,
        "x-tr-signature": s2,
        "accept-encoding": "gzip, deflate",
    }
    sess = requests.Session()
    req = requests.Request(method, BD + endpoint + "?" + query, headers=headers)
    prepped = sess.prepare_request(req)
    if "Accept" in prepped.headers:
        del prepped.headers["Accept"]
    if "Connection" in prepped.headers:
        del prepped.headers["Connection"]
    try:
        return sess.send(prepped, timeout=15)
    except Exception as e:
        print(f"Request failed: {e}")
        mock = requests.Response()
        mock.status_code = 408
        return mock


def slugify(t):
    return re.sub(r"[-\s]+", "-", re.sub(r"[^\w\s-]", "", t.lower().strip()))[:200]


def log_sync(service_name, mode="fast", status="running"):
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO sync_runs(service_name, source_name, mode, status) VALUES(%s, 'arian', %s, %s) RETURNING id",
            (service_name, mode, status),
        )
        return cur.fetchone()[0]


def complete_sync(run_id, discovered=0, updated=0, failed=0, notes=None):
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            """UPDATE sync_runs 
               SET status = 'completed', finished_at = now(),
                   discovered_count = %s, updated_count = %s, 
                   failed_count = %s, notes = %s
               WHERE id = %s""",
            (discovered, updated, failed, notes, run_id),
        )


def notify_channel(channel, payload=""):
    db = get_db()
    with db.cursor() as cur:
        cur.execute(f"NOTIFY {channel}, %s", (payload,))
