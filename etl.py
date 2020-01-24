import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data.iloc[0].tolist()
    
    #First error: Can't adapt type numpy.int64 - gerated from the DataFrame import. Following code revert each list item to its original type
    for i in range(len(song_data)):
        try:
            #This code converts only the not pure python data types
            song_data[i] = song_data[i].item()
        except:
            pass
        
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.iloc[0].tolist()
    for i in range(len(artist_data)):
        try:
            #This code converts only the not pure python data types
            artist_data[i] = artist_data[i].item()
        except:
            pass
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'] 

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # insert time data records
    #timestamp, hour, day, week of year, month, year, weekday
    time_data = [df['ts'].tolist(), t.dt.hour.tolist(), t.dt.day.tolist(), t.dt.weekofyear.tolist(), t.dt.month.tolist(), t.dt.year.tolist(), t.dt.weekday.tolist()]
    time_data = numpy.transpose(time_data)
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df_timestamp = df[['userId', 'firstName', 'lastName', 'gender', 'level', 'ts']]
    sorted_users_by_ts = user_df_timestamp.sort_values(by=['ts', 'userId'], ascending=False)
    last_user_logs = sorted_users_by_ts.drop_duplicates(subset=['userId'], keep='first')
    user_df = last_user_logs[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        # add row.length = duration to this select
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()