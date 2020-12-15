#This is the Python file to extract the songs from Spotify, transform the data and then load it into PostgreSQL.
#It is placed into a function for my Airflow DAG to call

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sys

def spotify_etl_func():
    spotify_client_id = ""
    spotify_client_secret = ""
    spotify_redirect_url = "http://localhost"

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id,
                                                   client_secret=spotify_client_secret,
                                                   redirect_uri=spotify_redirect_url,
                                                   scope="user-read-recently-played"))
    recently_played = sp.current_user_recently_played(limit=50)
    #if the length of recently_played is 0 for some reason just exit the program
    if len(recently_played) ==0:
        sys.exit("No results recieved from Spotify")

    #Creating the Album Data Structure:
    album_list = []
    for row in recently_played['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id,'name':album_name,'release_date':album_release_date,
                        'total_tracks':album_total_tracks,'url':album_url}
        album_list.append(album_element)

    #Creating the Artist Data Structure:
    #As we can see here this is another way to store data with using a dictionary of lists. Personally, for this project
    #I think using the strategy with the albums dicts(lists) is better. It allows for more functionality if we have to sort for example.
    # Additionally we do not need to make the temporary lists. There may be a more pythonic method to creating this but it is not my preferred method
    artist_dict = {}
    id_list = []
    name_list = []
    url_list = []
    for item in recently_played['items']:
        for key,value in item.items():
            if key == "track":
                for data_point in value['artists']:
                    id_list.append(data_point['id'])
                    name_list.append(data_point['name'])
                    url_list.append(data_point['external_urls']['spotify'])
    artist_dict = {'artist_id':id_list,'name':name_list,'url':url_list}

    #Creating the Track(Song) Data Structure:
    song_list = []
    for row in recently_played['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_time_played = row['played_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'date_time_played':song_time_played,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_list.append(song_element)

    #Now that we have these two lists and one dictionary ready lets convert them to DataFrames
    #We will need to do some cleaning and add our Unique ID for the Track
    #Then load into PostgresSQL from the dataframe

    #Album = We can also just remove duplicates here. We dont want to load two of the same albums just to have SQL drop it later
    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(subset=['album_id'])

    #Artist = We can also just remove duplicates here. We dont want to load two of the same artists just to have SQL drop it later
    artist_df = pd.DataFrame.from_dict(artist_dict)
    artist_df = artist_df.drop_duplicates(subset=['artist_id'])

    #Song Dataframe
    song_df = pd.DataFrame.from_dict(song_list)
    #date_time_played is an object (data type) changing to a timestamp
    song_df['date_time_played'] = pd.to_datetime(song_df['date_time_played'])
    #converting to my timezone of Central
    song_df['date_time_played'] = song_df['date_time_played'].dt.tz_convert('US/Central')
    #I have to remove the timezone part from the date/time/timezone.
    song_df['date_time_played'] = song_df['date_time_played'].astype(str).str[:-7]
    song_df['date_time_played'] = pd.to_datetime(song_df['date_time_played'])
    #Creating a Unix Timestamp for Time Played. This will be one half of our unique identifier
    song_df['UNIX_Time_Stamp'] = (song_df['date_time_played'] - pd.Timestamp("1970-01-01"))//pd.Timedelta('1s')
    # I need to create a new unique identifier column because we dont want to be insterting the same song played at the same song
    # I can have the same song multiple times in my database but I dont want to have the same song played at the same time
    song_df['unique_identifier'] = song_df['song_id'] + "-" + song_df['UNIX_Time_Stamp'].astype(str)
    song_df = song_df[['unique_identifier','song_id','song_name','duration_ms','url','popularity','date_time_played','album_id','artist_id']]
    song_df.to_csv(r"/Users/GrantCulp/Downloads/test.csv")
    #NOW We are going to Load PostgresSQL

    #Loading the data into the Temporary Table
    conn = psycopg2.connect(host = "localhost",port="5432", dbname = 'spotify_db')
    cur = conn.cursor()
    engine = create_engine('postgresql+psycopg2://@localhost:5432/spotify_db')
    conn_eng = engine.raw_connection()
    cur_eng = conn_eng.cursor()


    #TRACKS: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_track AS SELECT * FROM spotify_schema.spotify_track LIMIT 0
    """)
    song_df.to_sql("tmp_track", con = engine, if_exists='append', index = False)
    #Moving data from temp table to production table
    cur.execute(
    """
    INSERT INTO spotify_schema.spotify_track
    SELECT tmp_track.*
    FROM   tmp_track
    LEFT   JOIN spotify_schema.spotify_track USING (unique_identifier)
    WHERE  spotify_schema.spotify_track.unique_identifier IS NULL;
    
    DROP TABLE tmp_track""")
    conn.commit()

    #ALBUM: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_album AS SELECT * FROM spotify_schema.spotify_album LIMIT 0
    """)
    album_df.to_sql("tmp_album", con = engine, if_exists='append', index = False)
    conn_eng.commit()
    #Moving from Temp Table to Production Table
    cur.execute(
    """
    INSERT INTO spotify_schema.spotify_album
    SELECT tmp_album.*
    FROM   tmp_album
    LEFT   JOIN spotify_schema.spotify_album USING (album_id)
    WHERE  spotify_schema.spotify_album.album_id IS NULL;
    
    DROP TABLE tmp_album""")
    conn.commit()

    #Artist: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_artist AS SELECT * FROM spotify_schema.spotify_artists LIMIT 0
    """)
    artist_df.to_sql("tmp_artist", con = engine, if_exists='append', index = False)
    conn_eng.commit()
    #Moving data from temp table to production table
    cur.execute(
    """
    INSERT INTO spotify_schema.spotify_artists
    SELECT tmp_artist.*
    FROM   tmp_artist
    LEFT   JOIN spotify_schema.spotify_artists USING (artist_id)
    WHERE  spotify_schema.spotify_artists.artist_id IS NULL;
    
    DROP TABLE tmp_artist""")
    conn.commit()
    return "Finished Extract, Transform, Load - Spotify"
