# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists  users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists  artists;"
time_table_drop = "drop table if exists time;"


# CREATE TABLES


user_table_create = ("""create table if not exists users( 
                        user_id text primary key, 
                        first_name text, 
                        last_name text, 
                        gender text, 
                        level text
                        )
                    """)

song_table_create = ("""create table if not exists songs (
                        song_id  text primary key,  
                        title text, 
                        artist_id text not null references artists(artist_id) , 
                        year int, 
                        duration numeric
                        )
                    """)

artist_table_create =   ("""create table if not exists artists (
                            artist_id text primary key, 
                            name text, 
                            location text, 
                            latitude numeric, 
                            longitude numeric)
                        """)

time_table_create = ("""create table if not exists time (
                        start_time timestamp primary key,
                        hour int, 
                        day int,
                        week int, 
                        month int, 
                        year int, 
                        weekday int
                        )
                    """)

songplay_table_create = ("""create table if not exists songplays(
                            songplay_id SERIAL PRIMARY KEY ,
                            start_time timestamp references time(start_time) not null, 
                            user_id text not null references users(user_id) not null, 
                            level text, 
                            song_id text references songs(song_id), 
                            artist_id text references artists(artist_id), 
                            session_id int,
                            location text , 
                            user_agent text
                            )
                        """)


# INSERT RECORDS

songplay_table_insert = ("""insert into songplays(
                            start_time , 
                            user_id , 
                            level , 
                            song_id , 
                            artist_id , 
                            session_id ,
                            location ,
                            user_agent )
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s )
                        """)

user_table_insert = ("""insert into users (
                        user_id, 
                        first_name, 
                        last_name,
                        gender, 
                        level) 
                        values (%s,%s,%s,%s,%s) 
                        on conflict (user_id) do update set level = EXCLUDED.level
                    """)

song_table_insert = ("""insert into songs (
                        song_id,
                        title,
                        artist_id,
                        year, 
                        duration)
                        values (%s,%s,%s,%s,%s) 
                        on conflict do nothing
                    """)

artist_table_insert =  ("""insert into artists (
                            artist_id,
                            name,
                            location,
                            latitude,
                            longitude) 
                            values (%s,%s,%s,%s,%s) 
                            on conflict do nothing 
                            """)

time_table_insert = ("""insert into time (
                        start_time,
                        hour, 
                        day,
                        week,
                        month,
                        year, 
                        weekday) 
                        values (%s,%s,%s,%s,%s,%s,%s) 
                        on conflict do nothing

                    """)

# FIND SONGS

song_select = (  """
                    SELECT songs.song_id, artists.artist_id  FROM  songs 
                    INNER JOIN artists ON songs.artist_id = artists.artist_id  
                    WHERE songs.title = %s 
                    AND artists.name = %s  AND songs.duration = %s
                    
                """)


# QUERY LISTS

create_table_queries = [user_table_create,artist_table_create, song_table_create,  time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]