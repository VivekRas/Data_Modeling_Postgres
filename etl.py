#importing the required packages
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath): 
    """
    This function reads each song (json) file that will be used to update songs and artists tables 
    
    Parameters: 
    cur : Cursor
    filepath : location where json files are stored
  
    Returns: 
    Data of required fields from each json file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year','duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath): 
    """
    This function reads each log (json) file and appends the selected fields to respective tables - time, users and songplay 
    
    Parameters: 
    cur : Cursor
    filepath : location where json files are stored
  
    Returns: 
    Updates data to time, users and songplay tables in postgres.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=="NextSong"]
    df = df[df.firstName.notnull()]# removing possible empty records
    dft = df.drop_duplicates(subset='ts', keep="first") #removing duplicate values in TS column for making table time, since start_time is primary key 
    
    # convert timestamp column to datetime
    t = pd.to_datetime((dft.ts), unit='ms')
    
    # insert time data records and create a time dataframe 
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time','hour','day','week','month','year','weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    # insert the data in time dataframe to "time" Table in Postgres 
    for i, row in time_df.iterrows():
        time_data =[row.start_time, row.hour, row.day, row.week, row.month, row.year, row.weekday]
        cur.execute(time_table_insert, time_data)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender','level']]
    #user_df = user_df.drop_duplicates(subset ="userId", keep=False,inplace=True) 
    
    # insert the data in user_df dataframe to "users" Table in Postgres
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results: #save id's for matching records
            songid, artistid = results[0], results[1]
        else:
            songid, artistid = None, None

        # insert songplay record to "songplay" Table in Postgres
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, 
                         songid, artistid,row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func): 
    """
    This fns is used to 
    
    a) pick all file names in the respective songs and logs directories and 
    b) to open each file and run the respective functions - process_song_file & process_log_file.
    
    Parameters: 
    cur : Cursor
    conn : Connection
    filepath : location where json files are stored
    func : respective function that needs to be run 
    
    Returns: 
    Iterates on each json file and provides an update - how many files have been processed
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
    This function is the main fns, we use it to call all the other fns one after the other
    and finally close the connection
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__": # the etl.py program, starts here and gets redirected to main fns
    main()