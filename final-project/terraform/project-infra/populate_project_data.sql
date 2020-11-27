DELETE FROM project_data;
WITH movies AS (
    SELECT
        original_title,
        revenue::DEC(12,1),
        budget::DEC(32,1),
        cleaned_imdb_id,
        release_date
    FROM
        movies_metadata
), movies_and_ratings AS (
    SELECT
        movies.*,
        movie_ratings.movieId,
        movie_ratings.average_rating
    FROM movies
    INNER JOIN links
        ON movies.cleaned_imdb_id = links.imdbId
    INNER JOIN movie_ratings 
        ON links.movieId::INTEGER = movie_ratings.movieId::INTEGER
), assembled_project_data AS (
    SELECT 
        movies_and_ratings.cleaned_imdb_id::INTEGER as imdb_id,
        movies_and_ratings.movieId as movie_id,
        movies_and_ratings.average_rating as average_rating,
        movies_and_ratings.revenue as revenue,
        movies_and_ratings.budget as budget,
        movies_and_ratings.original_title as original_title,
        movies_and_ratings.release_date as release_date
    FROM movies_and_ratings
)
INSERT INTO project_data (
    imdb_id,
    movie_id,
    average_rating,
    revenue,
    budget,
    original_title,
    release_date
) SELECT * FROM assembled_project_data;
