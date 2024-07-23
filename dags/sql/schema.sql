CREATE TABLE "tracks" (
  "id" integer PRIMARY KEY,
  "track_name" varchar,
  "popularity" varchar,
  "track_id" integer,
  "duration" integer,
  "genre" varchar,
  "album_id" integer
);

CREATE TABLE "album" (
  "album_id" integer PRIMARY KEY,
  "album_name" varchar,
  "total_tracks" integer
);

CREATE TABLE "artists" (
  "artist_id" integer PRIMARY KEY,
  "artist_name" varchar,
  "artist_ids" integer
);

CREATE TABLE "track_artists" (
  "track_id" integer,
  "artist_id" integer,
  PRIMARY KEY ("track_id", "artist_id")
);

ALTER TABLE "track_artists" ADD FOREIGN KEY ("artist_id") REFERENCES "artists" ("artist_id");

ALTER TABLE "track_artists" ADD FOREIGN KEY ("track_id") REFERENCES "tracks" ("id");

ALTER TABLE "tracks" ADD FOREIGN KEY ("album_id") REFERENCES "album" ("album_id");
