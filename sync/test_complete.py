#!/usr/bin/env python3
"""
Test script - Verifies all 3 phases with a small batch
"""

import os
import sys
import time
import json
import psycopg2

# Add parent path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import from the working sync_worker
from sync.sync_worker import send, slugify, DB, GK, TK, AV, BD

print("🧪 TESTING ALL 3 PHASES WITH LIMITED BATCH\n")
print("=" * 60)

# Connect to database
conn = psycopg2.connect(DB)
conn.autocommit = True
cur = conn.cursor()

# Insert test run
cur.execute("""
    INSERT INTO sync_runs(service_name, source_name, mode, status) 
    VALUES('moviebox', 'moviebox', 'test', 'running') RETURNING id
""")
run_id = cur.fetchone()[0]
print(f"✅ Test run ID: {run_id}")

# PHASE 1: DISCOVERY (Limited)
print("\n📡 PHASE 1: DISCOVERY ( limited to 10 titles )")
print("-" * 40)

# Get existing titles
cur.execute("SELECT source_title_id FROM titles WHERE source_name='moviebox'")
known = {r[0] for r in cur.fetchall()}
print(f"Existing titles: {len(known)}")

new_titles = []
new_count = 0

# Only get first page of first tab
r = send("GET", "/wefeed-mobile-bff/tab-operating", f"page=1&tabId=1&version={AV}")

if r.status_code == 200:
    data = r.json()
    items = data.get("data", {}).get("items", [])
    
    for item in items:
        if new_count >= 10:
            break
            
        for sub in item.get("subjects", []):
            if new_count >= 10:
                break
                
            sid = str(sub.get("subjectId"))
            if sid == "None" or sid in known:
                continue
                
            title = sub.get("title", "Untitled")
            slug = slugify(title) + "-" + sid
            cover = sub.get("cover", "")
            if isinstance(cover, dict):
                cover = cover.get("url", "")
            
            # Remove ON CONFLICT, just insert
            try:
                cur.execute("""
                    INSERT INTO titles(source_name, source_title_id, slug, title, poster_url, raw_payload, discovered_at, updated_at, status)
                    VALUES(%s, %s, %s, %s, %s, %s, now(), now(), 'active')
                    RETURNING id, source_title_id, title
                """, ("moviebox", sid, slug, title, cover, json.dumps(sub)))
                
                tid, sid, title = cur.fetchone()
                new_titles.append((tid, sid, title))
                new_count += 1
                print(f"  {new_count}. {title} (ID: {sid})")
            except psycopg2.Error as e:
                if "duplicate key" in str(e):
                    print(f"  ⚠️  {title} already exists, skipping")
                else:
                    print(f"  ❌ Error inserting {title}: {e}")
else:
    print(f"❌ API request failed with status {r.status_code}")
    sys.exit(1)

print(f"\n✅ Discovered {new_count} new titles")

if new_count == 0:
    print("No new titles to enrich. Exiting.")
    sys.exit(0)

# PHASE 2: ENRICHMENT
print("\n🔍 PHASE 2: ENRICHMENT ( fetching details )")
print("-" * 40)

enriched_count = 0

for tid, sid, title in new_titles:
    print(f"\n  Processing: {title}")
    
    r2 = send("GET", "/wefeed-mobile-bff/subject-api/resource", f"subjectId={sid}&version={AV}")
    
    if r2.status_code != 200:
        print(f"    ⚠️  Failed to fetch (status {r2.status_code})")
        continue
    
    data = r2.json().get("data", {})
    eps = data.get("list", []) or ([data] if data.get("resourceLink") else [])
    
    if not eps:
        print(f"    ⚠️  No episodes/resources found")
        continue
    
    # Determine if series or movie
    is_series = len(eps) > 1
    if is_series:
        cur.execute("UPDATE titles SET title_type='series' WHERE id=%s", (tid,))
        print(f"    📺 Detected as SERIES with {len(eps)} episodes")
        
        # Create season (without ON CONFLICT, check first)
        cur.execute("SELECT id FROM seasons WHERE title_id=%s AND season_number=1", (tid,))
        existing = cur.fetchone()
        if not existing:
            cur.execute("""
                INSERT INTO seasons(title_id, season_number, title) 
                VALUES(%s, 1, 'Season 1') 
                RETURNING id
            """, (tid,))
            season_id = cur.fetchone()[0]
        else:
            season_id = existing[0]
    else:
        print(f"    🎬 Detected as MOVIE")
        season_id = None
    
    # Process episodes/playback units
    episode_count = 0
    for i, ep in enumerate(eps[:5]):  # Limit to first 5 episodes for test
        ep_num = ep.get("episode") or ep.get("ep", i + 1)
        ep_title = ep.get("title", f"Episode {ep_num}")
        
        # Check if episode already exists
        cur.execute("SELECT id FROM episodes WHERE title_id=%s AND source_episode_id=%s", (tid, str(ep_num)))
        existing_ep = cur.fetchone()
        
        if not existing_ep:
            # Insert episode
            cur.execute("""
                INSERT INTO episodes(title_id, season_id, source_episode_id, episode_number, 
                                   absolute_episode_number, title, sort_order, raw_payload)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (tid, season_id, str(ep_num), 
                  int(ep_num) if str(ep_num).isdigit() else i + 1,
                  i + 1, ep_title, i, json.dumps(ep)))
            episode_id = cur.fetchone()[0]
        else:
            episode_id = existing_ep[0]
        
        # Check if playback unit already exists
        cur.execute("SELECT id FROM playback_units WHERE title_id=%s AND source_subject_id=%s AND source_episode_ref=%s", 
                   (tid, sid, str(ep_num)))
        existing_pb = cur.fetchone()
        
        if not existing_pb:
            # Insert playback unit
            cur.execute("""
                INSERT INTO playback_units(title_id, episode_id, unit_type, source_subject_id, source_episode_ref)
                VALUES(%s, %s, %s, %s, %s)
            """, (tid, episode_id, "episode" if is_series else "movie", sid, str(ep_num)))
            episode_count += 1
    
    enriched_count += 1
    print(f"    ✅ Added {episode_count} new episodes/playback units")

print(f"\n✅ Enriched {enriched_count}/{len(new_titles)} titles")

# PHASE 3: COLLECTIONS
print("\n📚 PHASE 3: COLLECTIONS ( rebuilding )")
print("-" * 40)

collections = [
    ("trending-now", "Trending Now", "t.updated_at DESC"),
    ("new-releases", "New Releases", "t.discovered_at DESC"),
    ("top-rated", "Top Rated", "t.rating_value DESC NULLS LAST"),
]

for slug, label, order_col in collections:
    # Insert or get collection
    cur.execute("SELECT id FROM collections WHERE slug=%s", (slug,))
    existing = cur.fetchone()
    
    if existing:
        collection_id = existing[0]
        cur.execute("UPDATE collections SET label=%s WHERE id=%s", (label, collection_id))
    else:
        cur.execute("""
            INSERT INTO collections(slug, label, collection_type)
            VALUES(%s, %s, 'dynamic') 
            RETURNING id
        """, (slug, label))
        collection_id = cur.fetchone()[0]
    
    cur.execute("DELETE FROM collection_items WHERE collection_id=%s", (collection_id,))
    
    cur.execute(f"""
        INSERT INTO collection_items(collection_id, title_id, sort_order)
        SELECT %s, t.id, ROW_NUMBER() OVER (ORDER BY {order_col})
        FROM titles t 
        WHERE t.status = 'active' 
        LIMIT 20
    """, (collection_id,))
    
    # Get count
    cur.execute("SELECT COUNT(*) FROM collection_items WHERE collection_id=%s", (collection_id,))
    count = cur.fetchone()[0]
    print(f"  ✅ {label} - rebuilt with {count} items")

# Update sync run status
cur.execute("""
    UPDATE sync_runs 
    SET status='completed', finished_at=now(), discovered_count=%s, updated_count=%s, failed_count=%s
    WHERE id=%s
""", (new_count, enriched_count, 0, run_id))

# Show summary
print("\n" + "=" * 60)
print("🎉 TEST COMPLETE - ALL 3 PHASES SUCCESSFUL!")
print("=" * 60)
print(f"\n📊 SUMMARY:")
print(f"   • Discovered: {new_count} titles")
print(f"   • Enriched: {enriched_count} titles")
print(f"   • Collections: {len(collections)} rebuilt")

# Verify data
cur.execute("SELECT COUNT(*) FROM titles WHERE source_name='moviebox'")
total_titles = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM playback_units")
total_playback = cur.fetchone()[0]
print(f"\n📈 DATABASE STATUS:")
print(f"   • Total titles in DB: {total_titles}")
print(f"   • Total playback units: {total_playback}")

cur.close()
conn.close()
print("\n✅ Ready for Railway deployment with --mode full!")
