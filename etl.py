import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    '''
    This function accepts the cursor and file path and processes the data for song dataset

            Parameters:
                    filepath: File path from the calling function
                    cur: cursor obtained from the active connection to DB

    '''
    
    df = pd.read_json(filepath, lines=True)


    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", \
    "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)

       # insert song record
    song_data = list([df[["song_id", "title", "artist_id", \
    "year", "duration"]].values][0][0])
    cur.execute(song_table_insert, song_data)

def process_log_file(cur, filepath):
      
    '''
      This function accepts the cursor and file path and processes the data for log dataset

            Parameters:
                    filepath: File path from the calling function
                    cur: cursor obtained from the active connection to DB

    '''

    
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms')

    # insert time data records
    time_data = [pd.Series(t), pd.Series(t).dt.hour, \
                 pd.Series(t).dt.day,pd.Series(t).dt.week, \
                 pd.Series(t).dt.month, pd.Series(t).dt.year, \
                 pd.Series(t).dt.weekday] 
    column_labels = ("timestamp", "hour", "day", \
                     "week_of_year", "month", "year", "weekday")
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "lastName", "level"]]

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
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), 
        row.userId, row.level, songid , 
        artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    This function reads all *.json files from the specified path
    and calls process_song_file or process_log_file functions based on
    the parameter func received from the main function
    
    It also displayes the number of files processed from total files present in the table 
    
            Parameters:
            
                    cunn: Active postgres connection
                    
                    cur: Cursor obtained from the active connection to DB
                    
                    filepath:Data folder path received from main function
                    
                    func:Function to be called
                    
            Return Values:
            
                    Nil

    ''' 
        
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    
    '''
    This function creates connection to postgres database and open a cursor for further processing
    It calles process data function with connection, cursor,foldr path and name of function to be called
    name of function to be called (func) is used to identify the right function by process_data function
    Once the processing is completed, we closes the connection to the DB
    
    Parameters:
        
        Nil
        
    Return values:
        Nil
    
    '''
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    
    '''
    Calls main function
    
         Parameters:
                     Nil
        Return Value
                     Nil
    
    '''
    main()
