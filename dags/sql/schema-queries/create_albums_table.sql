CREATE TABLE IF NOT EXISTS consumption.albums (
                album_id SERIAL PRIMARY KEY,
                album_name TEXT NOT NULL,
                release_date DATE,
                total_tracks INT NOT NULL
            );