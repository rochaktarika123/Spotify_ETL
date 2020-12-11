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
