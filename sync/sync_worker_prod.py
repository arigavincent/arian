#!/usr/bin/env python3
"""
MovieBox Sync Worker PRODUCTION - Full catalog ingestion with retry logic
"""

import argparse
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
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
GK = os.getenv("ARIAN_GATEWAY_SECRET_B64", "76iRl07s0xSN9jqmEWAt79EBJZulIQIsV64FZr2O")
TK = os.getenv("ARIAN_AUTHORIZATION_TOKEN", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUyNjUzMzI5ODM2MDY2OTQxNTIsImV4cCI6MTc4MzgwMzE4MiwiaWF0IjoxNzc2MDI2ODgyfQ.r5VfgM_olW1OYsffeNFEaQWpDwz5E2uET1KOwagsJs0")
DB = os.getenv("DATABASE_URL")

XCI = json.dumps({
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
})

UA = "com.community.oneroom/50020080 (Linux; U; Android 5.1; en_US; TECNO CM5; Build/AP3A.240905.015.A2; Cronet/146.0.7680.144)"
BD = os.getenv("MOVIEBOX_BASE_DOMAIN", "https://api6.aoneroom.com")
AV = os.getenv("MOVIEBOX_API_VERSION", "663997536af5c372f70b9f394dbefe22")

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

def send(method, endpoint, query, retries=3):
    for attempt in range(retries):
        try:
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
            response = sess.send(prepped, timeout=30)
            if response.status_code == 200:
                return response
            logger.warning(f"Attempt {attempt+1} failed with status {response.status_code}")
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} error: {e}")
            time.sleep(2 ** attempt)
    
    mock = requests.Response()
    mock.status_code = 408
    return mock

def slugify(t):
    return re.sub(r"[-\s]+", "-", re.sub(r"[^\w\s-]", "", t.lower().strip()))[:200]

def sync(mode="fast"):
    logger.info(f"🚀 Starting MovieBox sync in {mode.upper()} mode")
    
    conn = psycopg2.connect(DB)
    conn.autocommit = True
    cur = conn.cursor()
    
    # Log sync run
    cur.execute(
        "INSERT INTO sync_runs(service_name, source_name, mode, status) VALUES(%s, %s, %s, 'running') RETURNING id",
        ("moviebox", "moviebox", mode),
    )
    run_id = cur.fetchone()[0]
    logger.info(f"Sync run ID: {run_id}")
    
    # Load existing IDs
    cur.execute("SELECT source_title_id FROM titles WHERE source_name='moviebox'")
    known = {r[0] for r in cur.fetchall()}
    logger.info(f"Found {len(known)} existing titles in database")
    
    if mode == "full":
        max_pages = 9999
        max_known_streak = 9999
        max_no_progress = 9999
        max_errors = 20
    else:
        max_pages = 80
        max_known_streak = 3
        max_no_progress = 15
        max_errors = 8
    
    # PHASE 1: DISCOVERY
    logger.info("=== PHASE 1: DISCOVERY ===")
    new = 0
    failed = 0
    
    for tab in list(range(1, 100)) + [0]:
        logger.info(f"Processing Tab {tab}")
        no_progress = 0
        known_streak = 0
        err_streak = 0
        seen = set()
        
        for page in range(1, max_pages + 1):
            r = send("GET", "/wefeed-mobile-bff/tab-operating", f"page={page}&tabId={tab}&version={AV}")
            
            if r.status_code != 200:
                err_streak += 1
                failed += 1
                if err_streak >= max_errors:
                    logger.warning(f"Max errors reached for tab {tab}")
                    break
                continue
            
            try:
                p = r.json()
            except Exception:
                err_streak += 1
                failed += 1
                if err_streak >= max_errors:
                    break
                continue
            
            err_streak = 0
            items = p.get("data", {}).get("items", [])
            if not items:
                break
            
            new_page = 0
            known_page = 0
            total_page = 0
            found = False
            
            for item in items:
                subs = item.get("subjects", [])
                if item.get("type") == "BANNER":
                    for b in item.get("banner", {}).get("banners", []):
                        if b.get("subject"):
                            subs.append(b["subject"])
                if subs:
                    found = True
                
                for sub in subs:
                    sid = str(sub.get("subjectId"))
                    if sid == "None":
                        continue
                    total_page += 1
                    if sid in seen:
                        break
                    seen.add(sid)
                    if sid in known:
                        known_page += 1
                        continue
                    
                    known.add(sid)
                    title = sub.get("title", "Untitled")
                    slug = slugify(title) + "-" + sid
                    cover = sub.get("cover", "")
                    if isinstance(cover, dict):
                        cover = cover.get("url", "")
                    
                    try:
                        cur.execute(
                            """INSERT INTO titles(source_name, source_title_id, slug, title, poster_url, raw_payload, discovered_at, updated_at, status)
                               VALUES(%s,%s,%s,%s,%s,%s,now(),now(),'active')
                               ON CONFLICT (source_name, source_title_id) DO UPDATE SET title=EXCLUDED.title, updated_at=now()""",
                            ("moviebox", sid, slug, title, cover, json.dumps(sub)),
                        )
                        new += 1
                        new_page += 1
                        if new % 50 == 0:
                            logger.info(f"  +{new} new titles (latest: {title})")
                    except Exception as e:
                        logger.error(f"Error inserting {title}: {e}")
            
            if not found:
                break
            if total_page > 0 and known_page == total_page and new_page == 0:
                known_streak += 1
            else:
                known_streak = 0
            if new_page == 0:
                no_progress += 1
            else:
                no_progress = 0
            if known_streak >= max_known_streak:
                logger.info(f"Known-only streak {known_streak}, moving on")
                break
            if no_progress >= max_no_progress:
                logger.info(f"No progress {no_progress} pages, moving on")
                break
    
    logger.info(f"Discovery done: {new} new titles, {failed} errors")
    
    # PHASE 2: ENRICHMENT
    logger.info("=== PHASE 2: ENRICHMENT ===")
    cur.execute(
        """SELECT id, source_title_id, title
           FROM titles WHERE source_name='moviebox' 
           AND id NOT IN (SELECT DISTINCT title_id FROM playback_units)"""
    )
    todo = cur.fetchall()
    logger.info(f"Found {len(todo)} titles to enrich")
    
    enriched = 0
    for tid, sid, title in todo:
        try:
            r2 = send("GET", "/wefeed-mobile-bff/subject-api/resource", f"subjectId={sid}&version={AV}")
            if r2.status_code != 200:
                logger.warning(f"Failed to fetch {title}: status {r2.status_code}")
                continue
            
            d = r2.json().get("data", {})
            eps = d.get("list", []) or ([d] if d.get("resourceLink") else [])
            if not eps:
                continue
            
            seid = None
            if len(eps) > 1:
                cur.execute("UPDATE titles SET title_type='series', updated_at=now() WHERE id=%s", (tid,))
                cur.execute(
                    "INSERT INTO seasons(title_id, season_number, title) VALUES(%s,1,'Season 1') ON CONFLICT DO NOTHING RETURNING id",
                    (tid,),
                )
                row = cur.fetchone()
                seid = row[0] if row else None
            
            for i, ep in enumerate(eps):
                en = ep.get("episode") or ep.get("ep", i + 1)
                et = ep.get("title", f"Episode {en}")
                cur.execute(
                    """INSERT INTO episodes(title_id, season_id, source_episode_id,
                       episode_number, absolute_episode_number, title, sort_order, raw_payload)
                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id""",
                    (tid, seid, str(en), int(en) if str(en).isdigit() else i + 1, i + 1, et, i, json.dumps(ep)),
                )
                row2 = cur.fetchone()
                eid = row2[0] if row2 else None
                cur.execute(
                    """INSERT INTO playback_units(title_id, episode_id, unit_type,
                       source_subject_id, source_episode_ref)
                       VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (tid, eid, "episode" if seid else "movie", sid, str(en)),
                )
            
            enriched += 1
            if enriched % 10 == 0:
                logger.info(f"  Enriched {enriched}/{len(todo)}")
            time.sleep(0.3)
        except Exception as e:
            logger.error(f"Failed {title} ({sid}): {e}")
    
    logger.info(f"Enrichment done: {enriched} titles enriched")
    
    # PHASE 3: COLLECTIONS
    logger.info("=== PHASE 3: COLLECTIONS ===")
    for slug, label, order_col in [
        ("trending-now", "Trending Now", "t.updated_at DESC"),
        ("new-releases", "New Releases", "t.discovered_at DESC"),
        ("top-rated", "Top Rated", "t.rating_value DESC NULLS LAST"),
    ]:
        cur.execute(
            """INSERT INTO collections(slug, label, collection_type)
               VALUES(%s,%s,'dynamic') ON CONFLICT(slug) DO UPDATE SET label=EXCLUDED.label
               RETURNING id""",
            (slug, label),
        )
        cid = cur.fetchone()[0]
        cur.execute("DELETE FROM collection_items WHERE collection_id=%s", (cid,))
        cur.execute(
            f"""INSERT INTO collection_items(collection_id, title_id, sort_order)
                SELECT %s, t.id, ROW_NUMBER() OVER (ORDER BY {order_col})
                FROM titles t WHERE t.status='active' LIMIT 100""",
            (cid,),
        )
        logger.info(f"  ✅ {label} rebuilt")
    
    # Mark sync complete
    cur.execute(
        """UPDATE sync_runs SET status='completed', finished_at=now(),
           discovered_count=%s, updated_count=%s, failed_count=%s WHERE id=%s""",
        (new, enriched, failed, run_id),
    )
    
    cur.close()
    conn.close()
    logger.info(f"\n✅ DONE. Discovered: {new}  Enriched: {enriched}  Failed: {failed}")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="MovieBox catalog sync (PostgreSQL)")
    p.add_argument("--mode", choices=["fast", "full"], default="full")
    args = p.parse_args()
    sync(args.mode)
