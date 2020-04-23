
# All user information is assumed to be present
# Therefore we only allow NOT NULL input
create_users = """CREATE TABLE IF NOT EXISTS users (
            user_id smallint NOT NULL,
            first_name varchar NOT NULL,
            last_name varchar NOT NULL, 
            gender varchar(1) NOT NULL, 
            level varchar NOT NULL,
            PRIMARY KEY (user_id, level)
        );"""

# Not all artists have a location and/or lat/long
# hence these may contain null values
create_artists = """CREATE TABLE IF NOT EXISTS artists (
            artist_id varchar PRIMARY KEY,
            artist_name varchar NOT NULL,
            artist_location varchar, 
            artist_latitude numeric,
            artist_longitude numeric
        );"""

# first Foreign Key. therefore you should populate 
# the artist table first
create_songs = """CREATE TABLE IF NOT EXISTS songs (
            song_id varchar PRIMARY KEY,
            title varchar NOT NULL,
            artist_id varchar REFERENCES artists,
            year smallint NOT NULL CONSTRAINT positive_year CHECK(year >= 0),
            duration numeric NOT NULL CONSTRAINT positive_duration CHECK(duration >= 0)
        );"""

# I've included `ts` as primary key column, because 
# there wouldn't be a unique value to join upon otherwise
create_time = """CREATE TABLE IF NOT EXISTS time (
            ts bigint PRIMARY KEY,
            start_time time NOT NULL,
            hour smallint CONSTRAINT positive_hour CHECK (hour >= 0),
            day smallint CONSTRAINT positive_day CHECK (day >= 0),
            week smallint CONSTRAINT positive_week CHECK (week >= 0),
            month smallint CONSTRAINT positive_month CHECK (month >= 0),
            year smallint CONSTRAINT positive_year CHECK (year >= 0),
            weekday smallint CONSTRAINT positive_weekday CHECK (weekday >= 0)
        );"""

# I've included ts to join onto the `time` table. 
create_songplays = """CREATE TABLE IF NOT EXISTS songplays (
            songplay_id varchar PRIMARY KEY,
            ts bigint REFERENCES time,
            start_time time NOT NULL,
            user_id smallint,
            level varchar,
            song_id varchar REFERENCES songs,
            artist_id varchar REFERENCES artists,
            location varchar,
            session_id int CONSTRAINT positive_session CHECK (session_id >= 0),
            user_agent varchar,
            FOREIGN KEY (user_id, level) REFERENCES users (user_id, level)
        );"""