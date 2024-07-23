CREATE TABLE IF NOT EXISTS core.spotify_tracks (
	id serial PRIMARY KEY,
	track_name text NULL,
	popularity int4 NULL,
	track_id text NULL,
	duration_ms int4 NULL,
	album_name text NULL,
	release_date text NULL,
	total_tracks int4 NULL,
	artist_names text NULL,
	artist_ids text NULL,
	genre text NULL
);