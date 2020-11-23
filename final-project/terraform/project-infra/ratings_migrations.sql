DROP TABLE movie_ratings;
CREATE TABLE movie_ratings (
    movieId BIGINT PRIMARY KEY,
    average_rating DEC(2,1)
);

INSERT INTO movie_ratings (
    movieId,
    average_rating
) 
SELECT movieId, AVG(rating)::DEC(2,1) 
FROM ratings 
GROUP BY movieId;
