CREATE TABLE IF NOT EXISTS consumption.tracks (
                id SERIAL PRIMARY KEY,
                track_name TEXT NOT NULL,
                popularity INT,
                track_id TEXT NOT NULL,
                duration_ms INT,
                genre TEXT,
                album_id INT,
                FOREIGN KEY (album_id) REFERENCES consumption.albums (album_id)
            );