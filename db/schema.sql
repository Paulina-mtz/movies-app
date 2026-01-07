DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS movies;

CREATE TABLE movies (
    movie_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    genres TEXT,
    year INTEGER
);

CREATE TABLE ratings (
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating NUMERIC(2,1) NOT NULL,
    ts BIGINT NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
