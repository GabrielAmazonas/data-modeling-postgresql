N# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
#start_time: timestamptz stands for timestamp with time zone as the timezone can be an important data.
#user_id: bigint used as type for ids.
#location: Should probably turn into a Location Dimension
songplay_table_create = ("CREATE TABLE songplays (songplay_id BIGSERIAL PRIMARY KEY, start_time timestamp not null, user_id bigint not null, level varchar(10), song_id varchar(200), artist_id varchar(200), session_id bigint, location varchar(100), user_agent varchar(200));")

user_table_create = ("CREATE TABLE users (user_id bigint NOT NULL PRIMARY KEY, first_name varchar(50), last_name varchar(50), gender varchar(1), level varchar(10));")

# Duration not inserting with datatype real, trying float.
song_table_create = ("CREATE TABLE songs (song_id varchar(200) NOT NULL PRIMARY KEY, title varchar(100), artist_id varchar(200), year int, duration real);")

artist_table_create = ("CREATE TABLE artists (artist_id varchar(200) NOT NULL PRIMARY KEY, name varchar(150), location varchar(100), latitude real, longitude real);")

time_table_create = ("CREATE TABLE time (start_time timestamp NOT NULL PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int);")

# INSERT RECORDS

songplay_table_insert = "INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (TO_TIMESTAMP(%s / 1000), %s, %s, %s, %s, %s, %s, %s)"

user_table_insert = "INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level"

song_table_insert = "INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING"

artist_table_insert = "INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING"


time_table_insert = "INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (TO_TIMESTAMP(%s / 1000), %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING"

# FIND SONGS

song_select = ("""SELECT songs.song_id as songid, artists.artist_id as artistid FROM songs \
JOIN artists ON songs.artist_id = artists.artist_id \
WHERE songs.title = %s AND artists.name = %s AND round(songs.duration:: numeric, 2) = round(%s::numeric, 2);
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]