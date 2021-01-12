Project : Data modelling using postgres
----------------------------------------------------------------------------------------------------------------------------

The objective of this project is to process the song data set and log data set and store them in following tables

Fact Table
---------------------
songplays- records in log data associated with song plays i
Columns
-----------
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
---------------------
users - users in the app
            Columns:user_id, first_name, last_name, gender, level

songs - songs in music database
            Columns:song_id, title, artist_id, year, duration

artists -artists in music database
            Columns:artist_id, name, location, latitude, longitude

time   - timestamps of records in songplays broken down into specific units
            Columns:start_time, hour, day, week, month, year, weekday



Song Dataset
------------------
Song data dataset is a subset of real data from the Million Song Dataset. The Million Song Dataset is a freely-available collection of audio features and metadata for a million contemporary popular music tracks.Each file is in JSON format and contains metadata about a song and the artist of that song. 

Log Dataset
------------------
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. 
These simulate activity logs from a music streaming app based on specified configurations.



General info
----------------------------------------------------------------------------------------------------------------------------

Programs 

1) etl.ipynb : Ipython notebook used to tryout the final code in etl.py. The code processes single files to test the program

2) sql_queries.py : This program contain all the sql queries used in the program. This module is imported to other programs to get access to specific queries

3) test.ipynb : Used to check the existance of data  the files after performaing the insert operations

4) create_tables :used to create/recreate the tables required for the ETL pipeline


How to run the code
--------------------------------------

1) Run create_tables.py in the terminal .This  will create the tables required for this process
2) Run etl.py to insert data in tables

![Alt text](/program execution.jpg?raw=true )

end
---

