"""
Arian Discovery Service — Finds new titles and queues them for enrichment.
"""

import json
import os
import time

import requests
from shared import send, slugify, get_db, log_sync, complete_sync, notify_channel

AV = os.getenv("ARIAN_API_VERSION", "663997536af5c372f70b9f394dbefe22")


def discover(mode="fast"):
    db = get_db()
    cur = db.cursor()
    run_id = log_sync("discovery", mode)

    cur.execute("SELECT source_title_id FROM titles WHERE source_name='arian'")
    known = {r[0] for r in cur.fetchall()}
    print(f"Found {len(known)} existing titles in database")

    if mode == "full":
        max_pages, max_known_streak, max_no_progress, max_errors = 9999, 9999, 9999, 20
    else:
        max_pages, max_known_streak, max_no_progress, max_errors = 80, 3, 15, 8

    new, queued, failed = 0, 0, 0
    s = requests.Session()
    s.headers.clear()

    for tab in list(range(1, 100)) + [0]:
        print(f"\nTab {tab}")
        no_progress, known_streak, err_streak = 0, 0, 0
        seen = set()

        for page in range(1, max_pages + 1):
            r = send("GET", "/wefeed-mobile-bff/tab-operating", f"page={page}&tabId={tab}&version={AV}")

            if r.status_code != 200:
                err_streak += 1; failed += 1
                if err_streak >= max_errors: break
                continue

            try:
                p = r.json()
            except Exception:
                err_streak += 1; failed += 1
                if err_streak >= max_errors: break
                continue

            err_streak = 0
            items = p.get("data", {}).get("items", [])
            if not items: break

            new_page, known_page, total_page, found = 0, 0, 0, False

            for item in items:
                subs = item.get("subjects", [])
                if item.get("type") == "BANNER":
                    for b in item.get("banner", {}).get("banners", []):
                        if b.get("subject"): subs.append(b["subject"])
                if subs: found = True

                for sub in subs:
                    sid = str(sub.get("subjectId"))
                    if sid == "None": continue
                    total_page += 1
                    if sid in seen: break
                    seen.add(sid)
                    if sid in known:
                        known_page += 1
                        continue
                    known.add(sid)
                    title = sub.get("title", "Untitled")
                    slug = slugify(title) + "-" + sid[:8]
                    cover = sub.get("cover", "")
                    if isinstance(cover, dict): cover = cover.get("url", "")

                    cur.execute(
                        """INSERT INTO titles(source_name, source_title_id, slug, title, poster_url, raw_payload, discovered_at, updated_at)
                           VALUES ('arian', %s, %s, %s, %s, %s, now(), now())
                           ON CONFLICT (source_name, source_title_id) DO UPDATE SET title = EXCLUDED.title, updated_at = now()
                           RETURNING id""",
                        (sid, slug, title, cover, json.dumps(sub)),
                    )
                    title_id = cur.fetchone()[0]

                    cur.execute(
                        "INSERT INTO enrichment_queue (title_id, source_title_id, priority) VALUES (%s, %s, 0) ON CONFLICT (title_id) DO NOTHING",
                        (title_id, sid),
                    )
                    if cur.rowcount > 0: queued += 1
                    new += 1; new_page += 1
                    if new % 50 == 0: print(f"  +{new} new titles, {queued} queued (latest: {title})")

            if not found: break
            if total_page > 0 and known_page == total_page and new_page == 0: known_streak += 1
            else: known_streak = 0
            if new_page == 0: no_progress += 1
            else: no_progress = 0
            if known_streak >= max_known_streak:
                print(f"  Known-only streak {known_streak}, moving on"); break
            if no_progress >= max_no_progress:
                print(f"  No progress {no_progress} pages, moving on"); break

    print(f"\nDiscovery done: {new} new titles, {queued} queued, {failed} errors")
    if queued > 0: notify_channel("enrichment_channel", f"{queued} new titles queued")
    complete_sync(run_id, discovered=new, failed=failed, notes=f"{queued} queued")
    cur.close()
    return new, queued, failed


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Arian Discovery Service")
    p.add_argument("--mode", choices=["fast", "full"], default="fast")
    args = p.parse_args()
    discover(args.mode)
