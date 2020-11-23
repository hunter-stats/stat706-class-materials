ALTER TABLE links
ADD COLUMN 
    cleaned_tmdb_id TEXT;

UPDATE links
SET cleaned_tmdb_id = tmdbid::DECIMAL(10,2)::INTEGER::TEXT;
