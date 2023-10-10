DROP_TABLE_ARTISTS = """
    DROP TABLE IF EXISTS public.artists;
"""

CREATE_TABLE_ARTISTS = """
    CREATE TABLE IF NOT EXISTS public.artists ( \
        artist_id VARCHAR(255) PRIMARY KEY SORTKEY, \
        name VARCHAR(255), \
        location VARCHAR(255), \
        latitude DOUBLE PRECISION, \
        longitude DOUBLE PRECISION);
"""

DROP_TABLE_SONGPLAYS = """
    DROP TABLE IF EXISTS public.songplays;
"""

CREATE_TABLE_SONGPLAYS = """
    CREATE TABLE IF NOT EXISTS public.songplays (
        songplay_id VARCHAR(255) PRIMARY KEY SORTKEY,
        start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
        user_id INT NOT NULL REFERENCES users(user_id),
        level VARCHAR(255),
        song_id VARCHAR(255) REFERENCES songs(song_id),
        artist_id VARCHAR(255) REFERENCES artists(artist_id),
        session_id INT, 
        location VARCHAR(255),
        user_agent VARCHAR(255));
"""

DROP_TABLE_SONGS = """
    DROP TABLE IF EXISTS public.songs;
"""

CREATE_TABLE_SONGS = """
    CREATE TABLE IF NOT EXISTS songs ( \
        song_id VARCHAR(255) PRIMARY KEY SORTKEY, \
        title VARCHAR(255), \
        artist_id VARCHAR(255), \
        year INT, \
        duration DOUBLE PRECISION);
"""

DROP_TABLE_STAGING_EVENTS = """
    DROP TABLE IF EXISTS public.staging_events;
"""

CREATE_TABLE_STAGING_EVENTS = ("""
    CREATE TABLE IF NOT EXISTS public.staging_events(
        artist VARCHAR(255),
        auth VARCHAR(255),
        firstName VARCHAR(255),
        gender VARCHAR(1),
        itemInSession INT,
        lastName VARCHAR(255),
        length DOUBLE PRECISION, 
        level VARCHAR(50),
        location VARCHAR(255),
        method VARCHAR(25),
        page VARCHAR(35),
        registration BIGINT,
        session_id BIGINT,
        song VARCHAR(255),
        status INT,	
        ts VARCHAR(50),
        user_agent TEXT,	
        user_id INT);
""")

DROP_TABLE_STAGING_SONGS = """
    DROP TABLE IF EXISTS public.staging_songs;
"""

CREATE_TABLE_STAGING_SONGS = """
    CREATE TABLE IF NOT EXISTS public.staging_songs(
        num_songs INT,
        artist_id VARCHAR(100),
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR(255),
        artist_name VARCHAR(255),
        song_id VARCHAR(100) PRIMARY KEY,
        title VARCHAR(255),
        duration DOUBLE PRECISION,
        year INT);
"""

DROP_TABLE_TIME = """
    DROP TABLE IF EXISTS public.time;
"""

CREATE_TABLE_TIME = """
    CREATE TABLE IF NOT EXISTS public.time (\
        start_time TIMESTAMP PRIMARY KEY SORTKEY,\
        hour INT,\
        day INT,\
        week INT,\
        month INT,\
        year INT,\
        weekday INT);
"""

DROP_TABLE_USERS = """
    DROP TABLE IF EXISTS public.users;
"""

CREATE_TABLE_USERS = """
    CREATE TABLE IF NOT EXISTS public.users ( \
        user_id INT PRIMARY KEY SORTKEY, \
        first_name VARCHAR(255), \
        last_name VARCHAR(255), \
        gender VARCHAR(255), \
        level VARCHAR(255));
"""