"""
Arian Enrichment Service — Continuously processes the enrichment queue.
"""

import json
import os
import select
import time

import psycopg2
from shared import send, get_db, log_sync, complete_sync, notify_channel

AV = os.getenv("ARIAN_API_VERSION", "663997536af5c372f70b9f394dbefe22")
BATCH_SIZE, IDLE_SLEEP = 50, 5


def enrich_single_title(cur, title_id, sid):
    r = send("GET", "/wefeed-mobile-bff/subject-api/resource", f"subjectId={sid}&version={AV}")
    if r.status_code != 200:
        raise Exception(f"Upstream returned {r.status_code}")

    d = r.json().get("data", {})
    eps = d.get("list", []) or ([d] if d.get("resourceLink") else [])
    if not eps:
        raise Exception("No episodes found")

    is_series = len(eps) > 1
    seid = None
    if is_series:
        cur.execute("UPDATE titles SET title_type='series', updated_at=now() WHERE id=%s", (title_id,))
        cur.execute("INSERT INTO seasons(title_id, season_number, title) VALUES (%s, 1, 'Season 1') ON CONFLICT DO NOTHING RETURNING id", (title_id,))
        row = cur.fetchone()
        seid = row[0] if row else None

    for i, ep in enumerate(eps):
        en = ep.get("episode") or ep.get("ep", i + 1)
        et = ep.get("title", f"Episode {en}")
        ep_num = int(en) if str(en).isdigit() else i + 1

        cur.execute(
            """INSERT INTO episodes(title_id, season_id, source_episode_id, episode_number, absolute_episode_number, title, sort_order, raw_payload)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING id""",
            (title_id, seid, str(en), ep_num, i + 1, et, i, json.dumps(ep)),
        )
        row2 = cur.fetchone()
        eid = row2[0] if row2 else None

        cur.execute(
            """INSERT INTO playback_units(title_id, episode_id, unit_type, source_subject_id, source_episode_ref)
               VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
            (title_id, eid, "episode" if seid else "movie", sid, str(en)),
        )
    return len(eps)


def process_batch(cur, batch):
    enriched = 0
    for title_id, sid in batch:
        try:
            enrich_single_title(cur, title_id, sid)
            cur.execute("UPDATE enrichment_queue SET status = 'completed', completed_at = now() WHERE title_id = %s", (title_id,))
            enriched += 1
        except Exception as e:
            cur.execute("UPDATE enrichment_queue SET status = 'failed', error_message = %s, retry_count = retry_count + 1 WHERE title_id = %s", (str(e)[:500], title_id))
            print(f"  Failed {sid}: {e}")
    return enriched


def enrich_worker():
    print("Arian Enrichment Service starting...")
    run_id = log_sync("enrichment", "continuous")
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("LISTEN enrichment_channel")
    print("Listening on enrichment_channel...")
    total_enriched, total_failed = 0, 0

    try:
        while True:
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                print(f"  NOTIFY: {notify.payload}")

            cur.execute("""
                WITH batch AS (
                    SELECT id, title_id, source_title_id FROM enrichment_queue
                    WHERE status = 'pending' OR (status = 'failed' AND retry_count < 3 AND started_at < now() - INTERVAL '5 minutes')
                    ORDER BY priority DESC, created_at ASC LIMIT %s FOR UPDATE SKIP LOCKED
                )
                UPDATE enrichment_queue SET status = 'processing', started_at = now()
                FROM batch WHERE enrichment_queue.id = batch.id
                RETURNING batch.title_id, batch.source_title_id
            """, (BATCH_SIZE,))
            batch = cur.fetchall()

            if batch:
                print(f"Processing batch of {len(batch)} titles...")
                enriched = process_batch(cur, batch)
                total_enriched += enriched
                total_failed += len(batch) - enriched
                print(f"  Done: {enriched} | Total: {total_enriched} enriched, {total_failed} failed")
                if enriched > 0: notify_channel("collections_channel", f"{enriched} titles enriched")

            cur.execute("SELECT COUNT(*) FROM enrichment_queue WHERE status = 'pending'")
            pending = cur.fetchone()[0]
            if pending == 0:
                print("Queue empty. Idle...")
                select.select([conn], [], [], IDLE_SLEEP)
            else:
                print(f"{pending} pending")
                time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        complete_sync(run_id, updated=total_enriched, failed=total_failed, notes="Service stopped")
        cur.close(); conn.close()


if __name__ == "__main__":
    enrich_worker()
