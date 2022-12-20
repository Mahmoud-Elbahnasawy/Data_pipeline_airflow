#mport configparser
#config = configparser.ConfigParser()
#config.read('dwh.cfg')


#SONG_DATA = config.get("S3","SONG_DATA")
#ARN = config.get('IAM_ROLE',"ARN")



staging_songs_copy = ("""
COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA,ARN )



creating_staging_songs=("""
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