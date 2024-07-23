CREATE TABLE IF NOT EXISTS consumption.track_artists (
                id int,  
                track_id INT,
                artist_id INT,
                FOREIGN KEY (track_id) REFERENCES consumption.tracks (id),
                FOREIGN KEY (artist_id) REFERENCES consumption.artists (artist_id),
                PRIMARY KEY (id, track_id, artist_id)
            );

