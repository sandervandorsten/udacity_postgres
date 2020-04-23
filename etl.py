import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from sqlalchemy import create_engine


def get_files(filepath):
    """Recursively extracts all json files from a directory. """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files


def process_songs_metadata_files(cur, filepath):
    """Extracts all metadata from the songs in the library and returns it as a pd.DataFrame"""
    song_files = get_files(filepath)
    songs_metadata = pd.concat([pd.read_json(song_file, lines=True) for song_file in song_files])
    return songs_metadata
    
def process_artists(engine, songs_metadata):
    """Extract artist information from songs metadata and write it to PostgreSQL."""
    artist_data = (
        songs_metadata[["artist_id", 'artist_name', 'artist_location', 'artist_longitude', 'artist_latitude']]
        .drop_duplicates('artist_id')
    )
    artist_data.to_sql('artists', engine, if_exists='append', index=False)
    return artist_data
    
    
def process_songs(engine, songs_metadata):
    """Extract song information from songs metadata and write it to PostgreSQL."""
    song_data = (
        songs_metadata[["song_id", 'title', 'artist_id', 'year', 'duration']]
        .drop_duplicates('song_id')
    )
    song_data.to_sql('songs', engine, if_exists='append', index=False)
    return song_data
    
    
def process_log_files(cur, filepath):
    """Extracts all log data from sparkify json dumps and returns it as a pd.DataFrame"""

    log_files = get_files(filepath)
    raw_log_data = pd.concat([pd.read_json(log, lines=True) for log in log_files])

    log_data = (raw_log_data
                .loc[lambda d: d['page'] == 'NextSong']
                .assign(datetime = lambda d: d.ts.apply(lambda x: pd.to_datetime(x, unit='ms').replace(microsecond=0)),
                        start_time = lambda d: d.datetime.dt.time,
                        hour = lambda d: d.datetime.dt.hour, 
                        day = lambda d: d.datetime.dt.day, 
                        week = lambda d: d.datetime.dt.week, 
                        month = lambda d: d.datetime.dt.month, 
                        year = lambda d: d.datetime.dt.year, 
                        weekday = lambda d: d.datetime.dt.weekday, 
                       )
               )
    
    return log_data


def process_time(engine, log_data):
    """Extract time information from logdata and write it to PostgreSQL."""
    time_data = (log_data
                 [['ts', 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]
                 .drop_duplicates()
                )
    
    time_data.to_sql('time', engine, if_exists='append', index=False)
    return time_data


def process_users(engine, log_data):
    """Extract user information from logdata and write it to PostgreSQL."""
    user_data = (log_data
                 [['userId', 'firstName', 'lastName', 'gender', 'level']]
                 .assign(userId = lambda d: d.userId.astype(int))
                 .rename({
                     'userId': 'user_id',
                     'firstName': 'first_name',
                     'lastName': 'last_name',
                 }, axis=1)
                 .drop_duplicates(subset=['user_id', 'first_name', 'last_name', 'gender', 'level'])
                )
    
    user_data.to_sql('users', engine, if_exists='append', index=False)
    return user_data

    
def process_songplays(engine, log_data, song_data, artist_data, time_data):
    """Extract transaction information from logdata and write it to PostgreSQL."""
    songplays_data = (log_data
                      [['song', 'length', 'sessionId', 'userId', 'level', 'userAgent', 'ts']]
                      .merge(song_data[['title', 'duration', 'song_id', 'artist_id']], left_on=['song', 'length'], right_on=['title', 'duration'])
                      .merge(artist_data[['artist_id', 'artist_location']], left_on='artist_id', right_on='artist_id')
                      .merge(time_data[['ts', 'start_time']], left_on='ts', right_on='ts')
                      .drop(columns=['song', 'length', 'title', 'duration'])
                      .reset_index()
                      .rename(columns={
                          'index': 'songplay_id', 
                          'sessionId': 'session_id',
                          'userId': 'user_id',
                          'userAgent': 'user_agent',
                          'artist_location': 'location',
                      })
                     )
    songplays_data.to_sql('songplays', engine, if_exists='append', index=False)
    return songplays_data
    

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    engine = create_engine('postgresql://student:student@localhost:5432/sparkifydb')

    songs_metadata = process_songs_metadata_files(cur, 'data/song_data')
    artist_data = process_artists(engine, songs_metadata)
    song_data = process_songs(engine, songs_metadata)
    
    log_data = process_log_files(cur, 'data/log_data')
    time_data = process_time(engine, log_data)
    user_data = process_users(engine, log_data)
    
    songplays_data = process_songplays(engine, log_data, song_data, artist_data, time_data)

    conn.close()
    cur.close()


if __name__ == "__main__":
    main()