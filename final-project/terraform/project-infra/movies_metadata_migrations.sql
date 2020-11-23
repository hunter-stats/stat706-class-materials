ALTER TABLE movies_metadata
ADD COLUMN parsed_genres JSONB,
ADD COLUMN cleaned_imdb_id TEXT;

ALTER TABLE movies_metadata
ADD PRIMARY KEY (imdb_id);

ALTER TABLE movies_metadata
ADD PRIMARY KEY cleaned_imdb_id;

ALTER TABLE movies_metadata
ADD COLUMN genres_copy TEXT;

UPDATE movies_metadata
SET genres_copy = genres;

UPDATE movies_metadata SET genres_copy = replace(genres_copy, '''', '"');
UPDATE movies_metadata SET parsed_genres = genres_copy::JSONB;

UPDATE movies_metadata 
SET cleaned_imdb_id = regexp_replace(imdb_id, '^tt[0]?', '');

DROP TABLE movie_genres;
CREATE TABLE movie_genres (
    genre_id INTEGER PRIMARY KEY,
    genre_name TEXT
);

WITH genres_json AS (
    SELECT DISTINCT jsonb_array_elements(parsed_genres) as genre_object
    FROM movies_metadata
) 
INSERT INTO movie_genres (
    genre_id, 
    genre_name
) SELECT 
    (genre_object->>'id')::INTEGER as genre_id,
    genre_object->>'name' as genre_name
FROM 
    genres_json;

DELETE FROM movies_metadata WHERE budget ILIKE '%.jpg';