import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - reads json file into a dataframe
    - takes subset of columns' values from dataframe into a list
    - uses SQL query in 'song_table insert' to insert data into the songs table
    
    Parameters:
    cur:      POSTGRES cursor object
    filepath: a str holding path to a json file
    
    Returns:
    None.  Inserts data into songs table
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - reads logfile in json format into a dataframe ('df')
    - filters out rows from df where page != "Next Song"
    - converts raw numeric data in 'ts' column to ms time value
    - ms time value has relevant time values extracted into a list 'time_data'
    - user info is extracted from df
    - both the time and user data are inserted to the relevant tables using a sql query to take each row from the time and df dataframes
    - a sql query extract values from the songs and artists tables.  If there are no values for song_id or artist_id, then None values are substituded for the NULL values returned.
    - a tuple 'song_plays_data' is created out of the relevent df and query results
    - this is then inserted into the song_plays_table using the sql query 'songplay_table_insert'
    
    Parameters:
    cur:      POSTGRES cursor object
    filepath: a string holding the filepath to the JSON files
    
    Returns:
    NONE.  Inserts data into users, songs and songplays table
    """
    
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t,t.dt.hour,t.dt.day,t.dt.week, t.dt.month, t.dt.year,t.dt.weekday_name]
    column_labels = ['start_time','hour','day','week','month','year','weekday']
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts,unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - finds all json files in directory and places them into a list 'all files'
    - gets total number of files
    - loops over the 'all files' list and prints message to console: how many files found
    - loops over the 'all files' list, calls the relevant function in process/insert data, prints message to console
    
    Parameters:
    cur:      POSTGRES cursor object      
    conn:     POSTGRES connection object
    filepath: a string holding a filepath
    func:     a process function defined within the code
    
    Returns:
    None.  But calls the process functions to insert data into tables
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - creates a connection to the POSGRES database
    - create a cursor objext
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()