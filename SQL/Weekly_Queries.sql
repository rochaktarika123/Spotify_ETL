--I created functions to run these queries for my weekly email that shows the statistics, displayed in the email. Creating Functions allow me to have cleaner code
--when writing the email which is a perfect use case for them.
--I have included the code to create the functions as well as the queries as a stand alone.

-- TOP 5 Songs Last 7 Days By Time Listened.
-- QUERY:
SELECT   st.song_name, 
         Round(Sum(Cast(duration_ms AS DECIMAL)/60000),2) AS min_duration 
FROM     spotify_track                                    AS st 
WHERE    date_time_played > CURRENT_DATE - interval '7 days' 
GROUP BY st.song_name 
ORDER BY min_duration DESC limit 5;

-- FUNCTION:
CREATE FUNCTION function_last_7_days_top_5_songs_duration() 
returns TABLE (song_name text, min_duration decimal) language plpgsql AS $$ 
BEGIN 
  RETURN query 
  SELECT   st.song_name, 
           round(sum(cast(duration_ms AS decimal)/60000),2) AS min_duration 
  FROM     spotify_track                                    AS st 
  WHERE    date_time_played > CURRENT_DATE - interval '7 days' 
  GROUP BY st.song_name 
  ORDER BY min_duration DESC limit 5; 

end;$$

-- Total Time Listened in Last 7 Days (Hours):
-- Query:
SELECT ROUND(SUM(CAST (duration_ms AS decimal)/3600000),2) AS total_time_listened_hrs
FROM spotify_track
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days';

-- Function:
CREATE FUNCTION function_last_7_days_hrs_listened() 
RETURNS TABLE (total_time_listened_hrs decimal) LANGUAGE plpgsql AS $$ 
BEGIN 
  RETURN query 
  SELECT   ROUND(SUM(CAST (st.duration_ms AS decimal)/3600000),2) AS total_time_listened_hrs 
  FROM     spotify_schema.spotify_track AS st 
  WHERE    date_time_played > CURRENT_DATE - INTERVAL '7 days';
end;$$ 

-- Most Popular Songs and Arists Names by Number of Plays
-- Query: 
SELECT st.song_name, sa.name AS artist_name,COUNT(st.*) AS times_played
FROM spotify_track AS st
INNER JOIN spotify_artists AS sa 
ON st.artist_id = sa.artist_id
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY st.song_name, sa.name
ORDER BY times_played DESC
LIMIT 5;

-- Function: 
CREATE FUNCTION function_last_7_days_songs_artist_played() 
RETURNS TABLE (song_name TEXT, artist_name TEXT, times_played INT) LANGUAGE plpgsql AS $$ 
BEGIN 
  RETURN query 
  SELECT st.song_name, sa.name AS artist_name,COUNT(st.*)::INT AS times_played
    FROM spotify_track AS st
    INNER JOIN spotify_artists AS sa 
    ON st.artist_id = sa.artist_id
    WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
    GROUP BY st.song_name, sa.name
    ORDER BY times_played DESC
    LIMIT 5;
end;$$ 

-- Top Artists Listened To
-- Query:
 SELECT art.name, COUNT(track.*):: INT AS number_plays
    FROM spotify_schema.spotify_track AS track
    INNER JOIN spotify_schema.spotify_artists AS art ON track.artist_id=art.artist_id
    WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
    GROUP BY art.name
    ORDER BY number_plays DESC
    LIMIT 5;
-- Function:
CREATE FUNCTION function_last_7_days_artist_played() 
RETURNS TABLE (name TEXT, number_plays INT) LANGUAGE plpgsql AS $$ 
BEGIN 
  RETURN query 
 SELECT art.name, COUNT(track.*):: INT AS number_plays
    FROM spotify_schema.spotify_track AS track
    INNER JOIN spotify_schema.spotify_artists AS art ON track.artist_id=art.artist_id
    WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
    GROUP BY art.name
    ORDER BY number_plays DESC
    LIMIT 5;
end;$$ 

-- Top Decades - Most Advanced:

-- Create View:
CREATE OR REPLACE VIEW track_decades AS
SELECT *,
CASE 
WHEN subqry.release_year >= 1950 AND subqry.release_year <= 1959  THEN '1950''s'
WHEN subqry.release_year >= 1960 AND subqry.release_year <= 1969  THEN '1960''s'
WHEN subqry.release_year >= 1970 AND subqry.release_year <= 1979  THEN '1970''s'
WHEN subqry.release_year >= 1980 AND subqry.release_year <= 1989  THEN '1980''s'
WHEN subqry.release_year >= 1990 AND subqry.release_year <= 1999  THEN '1990''s'
WHEN subqry.release_year >= 2000 AND subqry.release_year <= 2009  THEN '2000''s'
WHEN subqry.release_year >= 2010 AND subqry.release_year <= 2019  THEN '2010''s'
WHEN subqry.release_year >= 2020 AND subqry.release_year <= 2029  THEN '2020''s'
WHEN subqry.release_year >= 2030 AND subqry.release_year <= 2039  THEN '2030''s'
WHEN subqry.release_year >= 2040 AND subqry.release_year <= 2049  THEN '2040''s'
ELSE 'Other'
END AS decade
FROM 
(SELECT album.album_id,album.name,album.release_date,track.unique_identifier,track.date_time_played,track.song_name,CAST(SPLIT_PART(release_date,'-',1) AS INT) AS release_year
FROM spotify_album AS album
INNER JOIN spotify_track AS track ON track.album_id = album.album_id) AS subqry;

-- Query:
SELECT decade, COUNT(unique_identifier) AS total_plays
FROM track_decades
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY decade
ORDER BY total_plays DESC;
-- Function:
CREATE FUNCTION function_last_7_days_top_decades() 
RETURNS TABLE (decade TEXT, total_plays INT) LANGUAGE plpgsql AS $$ 
BEGIN 
  RETURN query 
SELECT track_decades.decade, COUNT(unique_identifier)::INT AS total_plays
FROM spotify_schema.track_decades
WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY track_decades.decade
ORDER BY total_plays DESC;
end;$$ 
