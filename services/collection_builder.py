"""
Arian Collections Service — Rebuilds dynamic collections on trigger.
"""

import os
import select
import time

import psycopg2
from shared import get_db, log_sync, complete_sync

REBUILD_COOLDOWN = 300
COLLECTIONS = [
    ("trending-now", "Trending Now", "t.updated_at DESC"),
    ("new-releases", "New Releases", "t.discovered_at DESC"),
    ("top-rated", "Top Rated", "t.rating_value DESC NULLS LAST"),
]


def rebuild_collections(cur):
    for slug, label, order_col in COLLECTIONS:
        cur.execute(
            "INSERT INTO collections(slug, label, collection_type) VALUES (%s, %s, 'dynamic') ON CONFLICT (slug) DO UPDATE SET label = EXCLUDED.label RETURNING id",
            (slug, label),
        )
        cid = cur.fetchone()[0]
        cur.execute("DELETE FROM collection_items WHERE collection_id = %s", (cid,))
        cur.execute(
            f"INSERT INTO collection_items(collection_id, title_id, sort_order) SELECT %s, t.id, ROW_NUMBER() OVER (ORDER BY {order_col}) FROM titles t WHERE t.status = 'active' AND t.id IN (SELECT DISTINCT title_id FROM playback_units) LIMIT 50",
            (cid,),
        )
    print(f"  Collections rebuilt at {time.strftime('%H:%M:%S')}")


def collections_worker():
    print("Arian Collections Service starting...")
    run_id = log_sync("collections", "continuous")
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("LISTEN collections_channel")
    print("Listening on collections_channel...")
    pending_rebuild, last_rebuild, rebuild_count = False, 0, 0

    try:
        while True:
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                print(f"  NOTIFY: {notify.payload}")
                pending_rebuild = True
            now = time.time()
            if pending_rebuild and (now - last_rebuild) >= REBUILD_COOLDOWN:
                print("Rebuilding collections...")
                rebuild_collections(cur)
                rebuild_count += 1
                pending_rebuild, last_rebuild = False, now
            select.select([conn], [], [], min(REBUILD_COOLDOWN if pending_rebuild else 60, 10))
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        complete_sync(run_id, updated=rebuild_count, notes="Service stopped")
        cur.close(); conn.close()


if __name__ == "__main__":
    collections_worker()
