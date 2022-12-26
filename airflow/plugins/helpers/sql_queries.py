class SqlQueries:
 



    songplay_table_insert = ("""INSERT INTO "{}"
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT to_timestamp(to_char(ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration;
    """)

    user_table_insert = ("""INSERT INTO "{}" 
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""INSERT INTO "{}"
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""INSERT INTO "{}"
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""INSERT INTO "{}"
        SELECT start_time, extract(hour from start_time) AS hour, extract(day from start_time) AS day, extract(week from start_time) AS week , extract(month from start_time) AS month, extract(year from start_time) AS year, extract(dayofweek from start_time) AS dayofweek
        FROM songplays
    """)
    create_staging_songs=("""
        CREATE TABLE public.staging_songs (
	    num_songs int4,
	    artist_id varchar(256),
	    artist_name varchar(256),
	    artist_latitude numeric(18,0),
	    artist_longitude numeric(18,0),
	    artist_location varchar(256),
	    song_id varchar(256),
	    title varchar(256),
	    duration numeric(18,0),
	    "year" int4
        );
""")
    
    
    create_staging_events = ("""
        CREATE TABLE IF NOT EXISTS staging_events
        (
        artist          VARCHAR,
        auth            VARCHAR, 
        firstName       VARCHAR,
        gender          VARCHAR,   
        itemInSession   INTEGER,
        lastName        VARCHAR,
        length          FLOAT,
        level           VARCHAR, 
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    BIGINT,
        sessionId       INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          INTEGER
        );
        """)
    truncate_table_if_existed = ("""
    TRUNCATE TABLE "{}";""")
    
    drop_table_if_existed = ("""
    drop TABLE IF EXISTS "{}";""")
    
    copy_sql = ("""
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    compupdate off
    region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    JSON '{}';
    """)
    
    
    ## NO NEED FOR COPY_songs & copy_events as copy_sql is generic for any josn file
    
    COPY_songs = ("""
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    compupdate off
    region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    JSON 'auto';
    
    """)
    
    
    
    copy_events = ("""
    COPY {{}}
    FROM '{{}}'
    ACCESS_KEY_ID '{{}}'
    SECRET_ACCESS_KEY '{{}}'
    IGNOREHEADER 1
    compupdate off region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON '{}'
    """).format("s3://udacity-dend/log_json_path.json")
    quality_row_count_check = ("""
    SELECT COUNT(*) FROM {}; """)
    
    
    


