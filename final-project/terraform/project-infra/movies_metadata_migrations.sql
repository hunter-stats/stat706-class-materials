ALTER TABLE movies_metadata
ADD COLUMN parsed_genres JSONB;

CREATE TABLE movie_ratings (
    movieId BIGINT,
    average_rating DEC(2,1)
);

INSERT INTO movie_ratings (
    movieId,
    average_rating
) 
SELECT movieId, AVG(rating) 
FROM ratings 
GROUP BY movieId;