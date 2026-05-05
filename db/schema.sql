-- Arian Database Schema
-- Core tables
CREATE TABLE IF NOT EXISTS titles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name TEXT NOT NULL DEFAULT 'arian',
    source_title_id TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    title_type TEXT NOT NULL DEFAULT 'movie' CHECK (title_type IN ('movie', 'series')),
    synopsis_short TEXT,
    synopsis_long TEXT,
    tagline TEXT,
    release_year INT,
    runtime_minutes INT,
    maturity_rating TEXT,
    studio TEXT,
    poster_url TEXT,
    backdrop_url TEXT,
    trailer_url TEXT,
    trailer_type TEXT,
    popularity_score NUMERIC(10,4),
    rating_value NUMERIC(4,2),
    rating_count INT DEFAULT 0,
    is_featured BOOLEAN DEFAULT false,
    raw_payload JSONB,
    discovered_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'hidden')),
    UNIQUE (source_name, source_title_id)
);

CREATE TABLE IF NOT EXISTS title_images (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title_id UUID NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
    image_type TEXT NOT NULL CHECK (image_type IN ('poster', 'backdrop', 'thumbnail', 'banner', 'logo')),
    url TEXT NOT NULL,
    width INT, height INT, sort_order INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS seasons (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title_id UUID NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
    season_number INT NOT NULL,
    title TEXT, synopsis TEXT, release_year INT, sort_order INT DEFAULT 0,
    UNIQUE (title_id, season_number)
);

CREATE TABLE IF NOT EXISTS episodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title_id UUID NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
    season_id UUID REFERENCES seasons(id) ON DELETE CASCADE,
    source_episode_id TEXT,
    episode_number INT NOT NULL,
    absolute_episode_number INT,
    title TEXT,
    synopsis TEXT,
    runtime_minutes INT,
    thumbnail_url TEXT,
    air_date DATE,
    sort_order INT DEFAULT 0,
    raw_payload JSONB
);

CREATE TABLE IF NOT EXISTS playback_units (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title_id UUID NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
    episode_id UUID REFERENCES episodes(id) ON DELETE SET NULL,
    unit_type TEXT NOT NULL CHECK (unit_type IN ('movie', 'episode')),
    source_subject_id TEXT NOT NULL,
    source_episode_ref TEXT,
    default_runtime_minutes INT,
    UNIQUE (title_id, source_subject_id, source_episode_ref)
);

CREATE TABLE IF NOT EXISTS genres (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS title_genres (
    title_id UUID NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
    genre_id UUID NOT NULL REFERENCES genres(id) ON DELETE CASCADE,
    PRIMARY KEY (title_id, genre_id)
);

CREATE TABLE IF NOT EXISTS tags (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    tag_type TEXT NOT NULL CHECK (tag_type IN ('mood', 'badge', 'theme', 'keyword'))
);

CREATE TABLE IF NOT EXISTS title_tags (
    title_id UUID NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
    tag_id UUID NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (title_id, tag_id)
);

CREATE TABLE IF NOT EXISTS people (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_person_id TEXT,
    full_name TEXT NOT NULL,
    profile_image_url TEXT,
    raw_payload JSONB
);

CREATE TABLE IF NOT EXISTS title_people (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title_id UUID NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
    person_id UUID NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    role_type TEXT NOT NULL CHECK (role_type IN ('actor', 'director', 'writer', 'producer', 'creator')),
    character_name TEXT,
    billing_order INT
);

CREATE TABLE IF NOT EXISTS collections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    description TEXT,
    collection_type TEXT DEFAULT 'dynamic' CHECK (collection_type IN ('dynamic', 'manual')),
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS collection_items (
    collection_id UUID NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    title_id UUID NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
    sort_order INT DEFAULT 0,
    PRIMARY KEY (collection_id, title_id)
);

CREATE TABLE IF NOT EXISTS stream_cache (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    playback_unit_id UUID REFERENCES playback_units(id) ON DELETE CASCADE,
    requested_resolution TEXT,
    resolved_resolution TEXT,
    codec TEXT,
    stream_url TEXT NOT NULL,
    expires_at TIMESTAMPTZ,
    raw_payload JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS enrichment_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title_id UUID NOT NULL REFERENCES titles(id) ON DELETE CASCADE UNIQUE,
    source_title_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    priority INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    retry_count INT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_enrichment_queue_status ON enrichment_queue(status, priority DESC, created_at ASC);

CREATE TABLE IF NOT EXISTS sync_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name TEXT NOT NULL,
    source_name TEXT NOT NULL DEFAULT 'arian',
    mode TEXT DEFAULT 'fast',
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    started_at TIMESTAMPTZ DEFAULT now(),
    finished_at TIMESTAMPTZ,
    discovered_count INT DEFAULT 0,
    updated_count INT DEFAULT 0,
    failed_count INT DEFAULT 0,
    notes TEXT
);

-- Default collections
INSERT INTO collections(slug, label, description, collection_type) VALUES
    ('trending-now', 'Trending Now', 'Most recently updated titles', 'dynamic'),
    ('new-releases', 'New Releases', 'Recently added to Arian', 'dynamic'),
    ('top-rated', 'Top Rated', 'Highest rated titles', 'dynamic')
ON CONFLICT (slug) DO NOTHING;
