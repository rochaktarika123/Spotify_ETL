--I created functions to run these queries for my weekly email that shows the statistics, displayed in the email.
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
