--I created functions to run these queries for my weekly email that shows the statistics, displayed in the email.
--I have included the code to create the functions as well as the queries as a stand alone.

-- TOP 5 Songs Last 7 Days By Time Listened.
-- QUERY:
SELECT 
  st.song_name, 
  ROUND(SUM(CAST(duration_ms AS decimal)/60000),2) AS min_duration
FROM 
  spotify_track AS st
WHERE 
  date_time_played > CURRENT_DATE - INTERVAL '7 days'
GROUP BY 
  st.song_name
ORDER BY 
  min_duration DESC
LIMIT 
  5;
-- FUNCTION:
CREATE FUNCTION function_last_7_days_top_5_songs_duration()
    RETURNS TABLE (song_name TEXT,
                   min_duration DECIMAL) LANGUAGE plpgsql 
AS $$
BEGIN
    RETURN QUERY
    SELECT st.song_name, ROUND(SUM(CAST(duration_ms AS decimal)/60000),2) AS min_duration
    FROM spotify_track AS st
    WHERE date_time_played > CURRENT_DATE - INTERVAL '7 days'
    GROUP BY st.song_name
    ORDER BY min_duration DESC
    LIMIT 5;
END;$$
