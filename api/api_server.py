"""
Arian API Server — FastAPI REST API backed by PostgreSQL.
Full catalog, search, detail, collections, and on-demand stream resolution.
"""

import base64
import hashlib
import hmac
import json
import os
import time
import urllib.parse
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Configuration ─────────────────────────────────────────────────
GK = os.getenv(
    "ARIAN_GATEWAY_SECRET_B64", "76iRl07s0xSN9jqmEWAt79EBJZulIQIsV64FZr2O"
)
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
DB_URL = os.getenv(
    "DATABASE_URL", "postgresql://arian:arian@localhost:5432/arian"
)

SUPPORTED_RESOLUTIONS = ["2160", "1080", "720", "480", "360"]
STREAM_CACHE_TTL = 300  # 5 minutes
STREAM_OPTIONS_CACHE_TTL = 120  # 2 minutes

# ── Global state ──────────────────────────────────────────────────
_db_conn = None
_stream_cache: dict[str, tuple[float, dict]] = {}  # key -> (timestamp, data)


def get_db():
    """Get or create a database connection."""
    global _db_conn
    if _db_conn is None or _db_conn.closed:
        _db_conn = psycopg2.connect(DB_URL)
        _db_conn.autocommit = True
    return _db_conn


# ── HMAC signing (preserved from original arian_server.py) ─────

def generate_gateway_signature(method: str, url_path_query: str) -> str:
    """Recreate Transsion API Gateway HMAC-MD5 signature."""
    key = base64.b64decode(GK)
    ts_millis = str(int(time.time() * 1000))
    parsed = urllib.parse.urlsplit(url_path_query)

    sorted_query = ""
    if parsed.query:
        params = [
            x.split("=", 1) if "=" in x else (x, "")
            for x in parsed.query.split("&")
        ]
        params.sort(key=lambda x: x[0])
        sorted_query = "&".join(f"{k}={v}" for k, v in params)

    normalized_path = (
        f"{parsed.path}?{sorted_query}" if sorted_query else parsed.path
    )
    sts = f"{method.upper()}\n\n\n\n{ts_millis}\n\n{normalized_path}"
    sig = hmac.new(key, sts.encode("utf-8"), hashlib.md5).digest()
    return f"{ts_millis}|2|{base64.b64encode(sig).decode('utf-8')}"


def make_signed_request(
    session: requests.Session, method: str, endpoint: str, query: str
) -> requests.Response:
    """Send an HMAC-signed request to the upstream Arian API."""
    path_query = f"{endpoint}?{query}" if query else endpoint
    sig = generate_gateway_signature(method, path_query)

    headers = {
        "x-play-mode": "1",
        "x-family-mode": "0",
        "x-content-mode": "0",
        "x-client-info": XCI,
        "x-client-status": "1",
        "authorization": TK,
        "user-agent": UA,
        "x-tr-signature": sig,
        "accept-encoding": "gzip, deflate",
    }

    req = requests.Request(
        method, f"{BD}{endpoint}?{query}", headers=headers
    )
    prepared = session.prepare_request(req)

    # Must delete — their presence breaks the HMAC match
    if "Accept" in prepared.headers:
        del prepared.headers["Accept"]
    if "Connection" in prepared.headers:
        del prepared.headers["Connection"]

    try:
        return session.send(prepared, timeout=15)
    except requests.RequestException:
        mock = requests.Response()
        mock.status_code = 408
        return mock


def extract_episode_stream(
    data: dict, ep_num: str, requested_resolution: str
) -> dict | None:
    """Extract stream URL from /subject-api/resource response."""
    # Series: look for matching episode in list
    if "list" in data and data["list"]:
        target_ep = None
        for ep in data["list"]:
            if (
                str(ep.get("episode")) == str(ep_num)
                or str(ep.get("ep")) == str(ep_num)
            ):
                target_ep = ep
                break
        if not target_ep:
            target_ep = data["list"][0]

        return {
            "url": target_ep.get("resourceLink"),
            "codec": target_ep.get("codecName"),
            "resolution": requested_resolution,
        }

    # Movie: resourceLink is on the data object
    if "resourceLink" in data:
        return {
            "url": data.get("resourceLink"),
            "codec": data.get("codecName"),
            "resolution": requested_resolution,
        }

    return None


def resolve_streams_from_upstream(
    sub_id: str, ep_num: str
) -> dict[str, dict]:
    """
    Call the upstream API for all supported resolutions.
    Results cached in memory for STREAM_OPTIONS_CACHE_TTL seconds.
    """
    cache_key = f"{sub_id}:{ep_num}"
    now = time.time()
    cached = _stream_cache.get(cache_key)
    if cached and (now - cached[0]) < STREAM_OPTIONS_CACHE_TTL:
        return cached[1]

    streams: dict[str, dict] = {}
    with requests.Session() as session:
        session.headers.clear()
        for resolution in SUPPORTED_RESOLUTIONS:
            try:
                resp = make_signed_request(
                    session,
                    "GET",
                    "/wefeed-mobile-bff/subject-api/resource",
                    f"resolution={resolution}&subjectId={sub_id}&version={AV}",
                )
                if resp.status_code == 200:
                    payload = resp.json()
                    stream_data = extract_episode_stream(
                        payload.get("data", {}), ep_num, resolution
                    )
                    if stream_data and stream_data.get("url"):
                        streams[resolution] = stream_data
            except Exception:
                continue

    _stream_cache[cache_key] = (now, streams)
    return streams


def persist_stream_to_db(
    db,
    playback_unit_id: str,
    requested_resolution: str,
    stream_data: dict,
):
    """Store resolved stream URL in stream_cache table for audit/debugging."""
    with db.cursor() as cur:
        cur.execute(
            """INSERT INTO stream_cache (
                playback_unit_id, requested_resolution, resolved_resolution,
                codec, stream_url, expires_at, raw_payload
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (
                playback_unit_id,
                requested_resolution,
                stream_data.get("resolution"),
                stream_data.get("codec"),
                stream_data.get("url"),
                datetime.now(timezone.utc)
                + timedelta(seconds=STREAM_CACHE_TTL),
                json.dumps(stream_data),
            ),
        )


def get_cached_stream_from_db(
    db, playback_unit_id: str, resolution: str
) -> dict | None:
    """Check if a fresh stream URL exists in the database cache."""
    with db.cursor() as cur:
        cur.execute(
            """SELECT stream_url, codec, resolved_resolution, raw_payload
               FROM stream_cache
               WHERE playback_unit_id = %s
                 AND resolved_resolution = %s
                 AND expires_at > now()
               ORDER BY created_at DESC LIMIT 1""",
            (playback_unit_id, resolution),
        )
        row = cur.fetchone()
        if row:
            return {
                "url": row[0],
                "codec": row[1],
                "resolution": row[2],
            }
    return None


# ── Pydantic models ───────────────────────────────────────────────

class TitleSummary(BaseModel):
    id: str
    title: str
    slug: str
    type: str
    posterUrl: Optional[str] = None
    backdropUrl: Optional[str] = None
    releaseYear: Optional[int] = None
    runtimeMinutes: Optional[int] = None
    maturityRating: Optional[str] = None
    genres: list[str] = []
    tags: list[str] = []


class TitleDetail(BaseModel):
    id: str
    title: str
    slug: str
    type: str
    synopsisShort: Optional[str] = None
    synopsisLong: Optional[str] = None
    tagline: Optional[str] = None
    releaseYear: Optional[int] = None
    runtimeMinutes: Optional[int] = None
    maturityRating: Optional[str] = None
    studio: Optional[str] = None
    genres: list[str] = []
    tags: list[str] = []
    images: dict = {}
    cast: list[dict] = []
    seasons: list[dict] = []
    episodes: list[dict] = []
    playbackUnits: list[dict] = []
    relatedTitles: list[dict] = []


class PaginatedResponse(BaseModel):
    items: list[dict]
    page: int
    limit: int
    total: int


# ── Helper functions ──────────────────────────────────────────────

def _get_genres_for_title(db, title_id: str) -> list[str]:
    with db.cursor() as cur:
        cur.execute(
            """SELECT g.name FROM genres g
               JOIN title_genres tg ON tg.genre_id = g.id
               WHERE tg.title_id = %s ORDER BY g.name""",
            (title_id,),
        )
        return [r[0] for r in cur.fetchall()]


def _get_tags_for_title(db, title_id: str) -> list[str]:
    with db.cursor() as cur:
        cur.execute(
            """SELECT t.name FROM tags t
               JOIN title_tags tt ON tt.tag_id = t.id
               WHERE tt.title_id = %s""",
            (title_id,),
        )
        return [r[0] for r in cur.fetchall()]


def _row_to_summary(row, genres: list[str], tags: list[str]) -> dict:
    return {
        "id": str(row[0]),
        "title": row[1],
        "slug": row[2],
        "type": row[3],
        "posterUrl": row[4],
        "backdropUrl": row[5],
        "releaseYear": row[6],
        "runtimeMinutes": row[7],
        "maturityRating": row[8],
        "genres": genres,
        "tags": tags,
    }


# ── App lifecycle ─────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: warm the DB connection
    get_db()
    yield
    # Shutdown: close DB connection
    global _db_conn
    if _db_conn and not _db_conn.closed:
        _db_conn.close()


app = FastAPI(
    title="Arian API",
    description="Catalog, search, detail, collections & on-demand stream resolution",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)


# ── Routes ────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    """Health check with database verification."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM titles WHERE status = 'active'")
        title_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM playback_units")
        pu_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM episodes")
        ep_count = cur.fetchone()[0]
    return {
        "ok": True,
        "db": "up",
        "titleCount": title_count,
        "playbackUnitCount": pu_count,
        "episodeCount": ep_count,
    }


# ── Titles ───────────────────────────────────────────────────────

@app.get("/api/titles")
def list_titles(
    q: Optional[str] = Query(None, description="Search by title or synopsis"),
    type: Optional[str] = Query(None, description="Filter: movie, series, etc."),
    genre: Optional[str] = Query(None, description="Filter by genre name"),
    year: Optional[int] = Query(None, description="Filter by release year"),
    tag: Optional[str] = Query(None, description="Filter by tag"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    sort: str = Query(
        "discovered_at",
        description="Sort: discovered_at, title, rating, year",
    ),
):
    """
    List, search, and filter titles.

    - `q`: Full-text search across title and synopsis
    - `genre`: Exact match on genre name
    - `tag`: Exact match on tag name
    """
    db = get_db()
    with db.cursor() as cur:
        conditions = ["t.status = 'active'"]
        params: list = []
        extra_joins = []

        # Full-text search
        if q:
            conditions.append(
                """to_tsvector('english', coalesce(t.title,'') || ' ' ||
                   coalesce(t.synopsis_short,'')) @@ plainto_tsquery('english', %s)"""
            )
            params.append(q)

        # Type filter
        if type:
            conditions.append("t.title_type = %s")
            params.append(type)

        # Year filter
        if year:
            conditions.append("t.release_year = %s")
            params.append(year)

        # Genre filter
        if genre:
            extra_joins.append(
                "JOIN title_genres tg2 ON tg2.title_id = t.id "
                "JOIN genres g2 ON g2.id = tg2.genre_id"
            )
            conditions.append("g2.name = %s")
            params.append(genre)

        # Tag filter
        if tag:
            extra_joins.append(
                "JOIN title_tags tt2 ON tt2.title_id = t.id "
                "JOIN tags ta2 ON ta2.id = tt2.tag_id"
            )
            conditions.append("ta2.name = %s")
            params.append(tag)

        where_clause = " AND ".join(conditions)
        join_clause = " ".join(extra_joins)

        # Count total
        cur.execute(
            f"SELECT COUNT(*) FROM titles t {join_clause} WHERE {where_clause}",
            params,
        )
        total = cur.fetchone()[0]

        # Sort
        sort_map = {
            "title": "t.title ASC",
            "rating": "t.rating_value DESC NULLS LAST",
            "year": "t.release_year DESC NULLS LAST",
            "discovered_at": "t.discovered_at DESC",
        }
        order_by = sort_map.get(sort, "t.discovered_at DESC")

        # Fetch page
        offset = (page - 1) * limit
        cur.execute(
            f"""SELECT t.id, t.title, t.slug, t.title_type,
                       t.poster_url,
                       t.backdrop_url,
                       t.release_year, t.runtime_minutes, t.maturity_rating
                FROM titles t {join_clause}
                WHERE {where_clause}
                ORDER BY {order_by}
                LIMIT %s OFFSET %s""",
            params + [limit, offset],
        )
        rows = cur.fetchall()

    items = []
    for row in rows:
        title_id = str(row[0])
        genres = _get_genres_for_title(db, title_id)
        tags = _get_tags_for_title(db, title_id)
        items.append(_row_to_summary(row, genres, tags))

    return {"items": items, "page": page, "limit": limit, "total": total}


@app.get("/api/titles/{title_id}")
def get_title(title_id: str):
    """
    Full title detail: metadata, images, genres, tags, cast,
    seasons, episodes, and playback units.
    """
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            """SELECT id, title, slug, title_type,
                      synopsis_short, synopsis_long, tagline,
                      release_year, runtime_minutes, maturity_rating,
                      studio, poster_url, backdrop_url, trailer_url,
                      trailer_type, popularity_score, rating_value,
                      rating_count, raw_payload
               FROM titles WHERE id = %s""",
            (title_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Title not found")

        title_uuid = str(row[0])

        # Images
        cur.execute(
            """SELECT image_type, url FROM title_images
               WHERE title_id = %s ORDER BY sort_order""",
            (title_uuid,),
        )
        images: dict[str, str] = {}
        for img_type, url in cur.fetchall():
            if img_type not in images:
                images[img_type] = url
        # Fallback: use poster_url from titles table
        if "poster" not in images and row[11]:
            images["poster"] = row[11]
        if "backdrop" not in images and row[12]:
            images["backdrop"] = row[12]

        # Genres
        genres = _get_genres_for_title(db, title_uuid)

        # Tags
        tags = _get_tags_for_title(db, title_uuid)

        # Cast & crew
        cur.execute(
            """SELECT p.full_name, tp.role_type, tp.character_name
               FROM title_people tp
               JOIN people p ON p.id = tp.person_id
               WHERE tp.title_id = %s ORDER BY tp.billing_order""",
            (title_uuid,),
        )
        cast = [
            {"name": r[0], "role": r[1], "characterName": r[2]}
            for r in cur.fetchall()
        ]

        # Seasons & episodes
        cur.execute(
            """SELECT id, season_number, title, synopsis
               FROM seasons WHERE title_id = %s ORDER BY season_number""",
            (title_uuid,),
        )
        seasons = []
        all_episodes = []
        for s in cur.fetchall():
            sid = str(s[0])
            cur.execute(
                """SELECT id, episode_number, title, synopsis, thumbnail_url,
                          runtime_minutes, air_date
                   FROM episodes WHERE season_id = %s ORDER BY episode_number""",
                (sid,),
            )
            eps = [
                {
                    "id": str(e[0]),
                    "episodeNumber": e[1],
                    "title": e[2],
                    "synopsis": e[3],
                    "thumbnailUrl": e[4],
                    "runtimeMinutes": e[5],
                    "airDate": str(e[6]) if e[6] else None,
                }
                for e in cur.fetchall()
            ]
            seasons.append(
                {
                    "id": sid,
                    "seasonNumber": s[1],
                    "title": s[2],
                    "synopsis": s[3],
                    "episodes": eps,
                }
            )
            all_episodes.extend(eps)

        # Playback units
        cur.execute(
            """SELECT pu.id, pu.unit_type, pu.source_subject_id,
                      pu.source_episode_ref, pu.default_runtime_minutes,
                      e.episode_number, e.title AS episode_title
               FROM playback_units pu
               LEFT JOIN episodes e ON e.id = pu.episode_id
               WHERE pu.title_id = %s ORDER BY e.episode_number NULLS FIRST""",
            (title_uuid,),
        )
        playback_units = [
            {
                "id": str(p[0]),
                "unitType": p[1],
                "sourceSubjectId": p[2],
                "sourceEpisodeRef": p[3],
                "defaultRuntimeMinutes": p[4],
                "episodeNumber": p[5],
                "episodeTitle": p[6],
            }
            for p in cur.fetchall()
        ]

        # Related titles (same genre, exclude self)
        cur.execute(
            """SELECT t2.id, t2.title, t2.slug, t2.title_type,
                       t2.poster_url, t2.release_year
                FROM title_genres tg
                JOIN title_genres tg2 ON tg2.genre_id = tg.genre_id
                JOIN titles t2 ON t2.id = tg2.title_id
                WHERE tg.title_id = %s AND t2.id != %s
                  AND t2.status = 'active'
                GROUP BY t2.id, t2.title, t2.slug, t2.title_type, t2.poster_url, t2.release_year
                ORDER BY t2.popularity_score DESC NULLS LAST
                LIMIT 6""",
            (title_uuid, title_uuid),
        )
        related = [
            {
                "id": str(r[0]),
                "title": r[1],
                "slug": r[2],
                "type": r[3],
                "posterUrl": r[4],
                "releaseYear": r[5],
            }
            for r in cur.fetchall()
        ]

    return {
        "id": title_uuid,
        "title": row[1],
        "slug": row[2],
        "type": row[3],
        "synopsisShort": row[4],
        "synopsisLong": row[5],
        "tagline": row[6],
        "releaseYear": row[7],
        "runtimeMinutes": row[8],
        "maturityRating": row[9],
        "studio": row[10],
        "genres": genres,
        "tags": tags,
        "images": images,
        "cast": cast,
        "seasons": seasons,
        "episodes": all_episodes,
        "playbackUnits": playback_units,
        "relatedTitles": related,
    }


# ── Collections ──────────────────────────────────────────────────

@app.get("/api/collections")
def list_collections():
    """Return all available collections (shelves)."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            """SELECT slug, label, description, collection_type
               FROM collections ORDER BY created_at"""
        )
        return {
            "items": [
                {
                    "slug": r[0],
                    "label": r[1],
                    "description": r[2],
                    "type": r[3],
                }
                for r in cur.fetchall()
            ]
        }


@app.get("/api/collections/{slug}")
def get_collection(slug: str):
    """Return titles for a specific collection/shelf."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            "SELECT id, label, description FROM collections WHERE slug = %s",
            (slug,),
        )
        coll = cur.fetchone()
        if not coll:
            raise HTTPException(status_code=404, detail="Collection not found")

        cur.execute(
            """SELECT t.id, t.title, t.slug, t.title_type,
                       t.poster_url, t.backdrop_url,
                       t.release_year, t.runtime_minutes, t.maturity_rating
                FROM collection_items ci
                JOIN titles t ON t.id = ci.title_id
                WHERE ci.collection_id = %s AND t.status = 'active'
                ORDER BY ci.sort_order""",
            (coll[0],),
        )
        items = []
        for row in cur.fetchall():
            title_id = str(row[0])
            genres = _get_genres_for_title(db, title_id)
            tags = _get_tags_for_title(db, title_id)
            items.append(_row_to_summary(row, genres, tags))

    return {
        "slug": slug,
        "label": coll[1],
        "description": coll[2],
        "items": items,
    }


# ── Playback / Stream Resolution ─────────────────────────────────

@app.get("/api/playback/{playback_unit_id}/options")
def playback_options(playback_unit_id: str):
    """
    Return available resolutions for a playback unit.
    Resolves live from upstream if not cached.
    """
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            """SELECT source_subject_id, source_episode_ref
               FROM playback_units WHERE id = %s""",
            (playback_unit_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Playback unit not found")

        sub_id, ep_ref = row
        ep_num = ep_ref or "1"

    streams = resolve_streams_from_upstream(sub_id, ep_num)

    if not streams:
        raise HTTPException(status_code=404, detail="No streams available")

    available = [r for r in SUPPORTED_RESOLUTIONS if r in streams]
    recommended = available[0] if available else None

    return {
        "playbackUnitId": playback_unit_id,
        "subjectId": sub_id,
        "episode": ep_num,
        "availableResolutions": available,
        "recommendedResolution": recommended,
    }


@app.get("/api/playback/{playback_unit_id}/stream")
def playback_stream(
    playback_unit_id: str,
    resolution: str = Query("1080", description="Preferred resolution"),
):
    """
    Resolve and return a playable stream URL.
    Checks DB cache first, then resolves live from upstream.
    Results are cached in stream_cache for 5 minutes.
    """
    if resolution not in SUPPORTED_RESOLUTIONS:
        resolution = "360"

    db = get_db()

    with db.cursor() as cur:
        cur.execute(
            """SELECT source_subject_id, source_episode_ref
               FROM playback_units WHERE id = %s""",
            (playback_unit_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Playback unit not found")

        sub_id, ep_ref = row
        ep_num = ep_ref or "1"

    # ── Check DB cache first ──
    cached = get_cached_stream_from_db(db, playback_unit_id, resolution)
    if cached:
        return {
            "url": cached["url"],
            "codec": cached["codec"],
            "resolution": cached["resolution"],
            "requestedResolution": resolution,
            "source": "db-cache",
        }

    # ── Resolve live from upstream ──
    streams = resolve_streams_from_upstream(sub_id, ep_num)

    if not streams:
        raise HTTPException(status_code=404, detail="No streams available")

    # Pick best match
    if resolution in streams:
        selected = resolution
    else:
        available = [r for r in SUPPORTED_RESOLUTIONS if r in streams]
        if not available:
            raise HTTPException(status_code=404, detail="No streams available")
        selected = available[0]

    stream_data = streams[selected]

    # Persist to database cache
    try:
        persist_stream_to_db(db, playback_unit_id, resolution, stream_data)
    except Exception:
        pass  # Non-critical

    expires_at = (
        datetime.now(timezone.utc) + timedelta(seconds=STREAM_CACHE_TTL)
    ).isoformat()

    return {
        "url": stream_data.get("url"),
        "codec": stream_data.get("codec"),
        "resolution": selected,
        "requestedResolution": resolution,
        "availableResolutions": [r for r in SUPPORTED_RESOLUTIONS if r in streams],
        "expiresAt": expires_at,
        "source": "live",
    }