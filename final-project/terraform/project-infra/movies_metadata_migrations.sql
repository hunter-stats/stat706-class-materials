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

WITH jsonb_genres AS (
    SELECT idmgenres_copy::JSONB as jbg
    FROM movies_metadata
)
UPDATE movies_metadata 
SET parsed_genres = (
    SELECT jbg FROM
);
/*
    TODO : remove tt0 from imdb_id and insert into imdb_id 
    TODO : replace single quotes with double quotes in genres column
*/