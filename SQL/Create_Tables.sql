-- Creating the Track Table (Fact Table)
CREATE TABLE IF NOT EXISTS spotify_schema.spotify_track(
    unique_identifier TEXT PRIMARY KEY NOT NULL,
    song_id TEXT NOT NULL,
    song_name TEXT,
    duration_ms INTEGER,
    url TEXT,
    popularity SMALLINT,
    date_time_played TIMESTAMP,
    album_id TEXT,
    artist_id TEXT,
    date_time_inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Creating the Album Table (Dimension Table)
CREATE TABLE IF NOT EXISTS spotify_schema.spotify_album(
    album_id TEXT NOT NULL PRIMARY KEY,
    name TEXT,
    release_date TEXT,
    total_tracks SMALLINT,
    url TEXT
    );

-- Creating the Artist Table (Dimension Table)
CREATE TABLE IF NOT EXISTS spotify_schema.spotify_artists(
    artist_id TEXT PRIMARY KEY NOT NULL,
    name TEXT,
    url TEXT);
