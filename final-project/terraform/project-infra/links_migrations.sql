ALTER TABLE links
ADD COLUMN 
    cleaned_tmdb_id TEXT;

UPDATE links
SET cleaned_tmdb_id = (SELECT tmdbid::DECIMAL(10,2)::INTEGER::TEXT);
