import logging
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, hour, weekofyear, date_format, dayofyear, dayofweek
from pyspark.sql.types import DecimalType, ShortType, StringType, IntegerType, LongType, TimestampType
from pyspark.sql.types import StructField, StructType


### Use aws credential files or export ENV_VARIABLE=<value> instead
#config = configparser.ConfigParser()
#config.read('dl.cfg')
#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Create a spark session, use a newer version of hadoop-aws

    Return(s):
        spark: a pyspark session object
    '''

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.1") \
        .getOrCreate()
    return spark

def parse_song_data(spark, input_data):
    '''
    Parse JSON files of song data

    Arg(s):
        spark: a pyspark session object
        input_data: AWS S3 bucket path
    Return(s):
        df_song: parsed DataFrame of song data
    '''

    # get filepath to song data
    song_data = input_data + "song_data"

    # schema for JSON file
    schema_parse_song = StructType([
        StructField('__corrupted', StringType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('artist_id', StringType(), True),
        StructField('year', ShortType(), True),
        StructField('duration', DecimalType(10,5), True),
        StructField('artist_name', StringType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_latitude', DecimalType(9,6), True),
        StructField('artist_longitude', DecimalType(9,6), True)
    ])

    # load json files and store corrupted records in field '__corrupted'
    df_parsed_song = spark.read.json(
        song_data,
        schema = schema_parse_song, 
        recursiveFileLookup=True, 
        mode = 'PERMISSIVE',
        columnNameOfCorruptRecord='__corrupted').cache()

    # logging info
    logging.info("Total number of parsed song records: {}".format(df_parsed_song.count()))
    logging.info("Number of corrupted song records {}"\
        .format(df_parsed_song.filter("__corrupted is NOT NULL").count()))

    # get the non-corrupted records and logging info
    df_song = df_parsed_song.filter("__corrupted is NULL").drop("__corrupted")
    logging.info("Number of valid song records: {}".format(df_song.count()))

    return df_song


def process_song_data(spark, df_song_in, output_data):
    '''
    Process song DataFrame and extract columns for songs and artists tables

    Arg(s):
        spark: a pyspark session object
        df_song_in: parsed DataFrame of song data
        output_data: AWS S3 bucket for saving results
    Return(s):
        songs_final: songs table/DataFrame, cleaned and typed
        artists_final: artists table/DataFrame, cleaned and typed
    '''

    ### get songs data
    df_songs = df_song_in.select(['song_id','title', 'artist_id', 'year','duration'])
    # drop nulls
    df_songs = df_songs.dropna(how='any')
    # drop empty strings
    df_songs = df_songs.filter("song_id != '' AND title != '' AND artist_id != ''")
    # truncate song_id, title, artist_id
    trunc_ids = udf(lambda x: x[:50])
    trunc_title = udf(lambda x: x[:256])
    df_songs = df_songs.withColumn('song_id', trunc_ids(df_songs['song_id']))
    df_songs = df_songs.withColumn('artist_id', trunc_ids(df_songs['artist_id']))
    df_songs = df_songs.withColumn('title', trunc_title(df_songs['title']))
    # drop duplicates
    df_songs = df_songs.dropDuplicates()
    # impose an explicit schema for not-nullable fields
    songs_schema = StructType([
        StructField('song_id', StringType(), nullable=False),
        StructField('title', StringType(), nullable=False),
        StructField('artist_id', StringType(), nullable=False),
        StructField('year', ShortType(), nullable=False),
        StructField('duration', DecimalType(10,5), nullable=False)
    ])
    songs_final = spark.createDataFrame(df_songs.rdd, songs_schema, verifySchema=True)
    logging.info('Final number of songs: {}'.format(songs_final.count()))
    # write songs table to parquet files partitioned by year and artist
    songs_final.write.partitionBy('year', 'artist_id').parquet(output_data + "songs-table/", mode='overwrite')
    logging.info('Write to songs table done.')


    ### get artists data
    df_artists = df_song_in.select(
        col('artist_id'),
        col('artist_name').alias('name'),
        col('artist_location').alias('location'),
        col('artist_latitude').alias('lattitude'),
        col('artist_longitude').alias('longitude'))
    # drop nulls
    df_artists = df_artists.dropna(how='any', subset=['artist_id', 'name'])
    # drop empty strings
    df_artists = df_artists.filter("artist_id != '' AND name != ''")
    # truncate location. location might be NULL/None
    @udf
    def trunc_loc(line):
        if line is None:
            return line
        else:
            return line[:256]
    # truncate name, artist_id
    trunc_name = udf(lambda x: x[:256])
    trunc_ids = udf(lambda x: x[:50])
    df_artists = df_artists.withColumn('location', trunc_loc(df_artists['location']))
    df_artists = df_artists.withColumn('name', trunc_name(df_artists['name']))
    df_artists = df_artists.withColumn('artist_id', trunc_ids(df_artists['artist_id']))
    # drop duplicates
    df_artists = df_artists.dropDuplicates()
    # impose an explicit schema for not-nullable fields
    artists_schema = StructType([
        StructField('artist_id', StringType(), nullable=False),
        StructField('name', StringType(), nullable=False),
        StructField('location', StringType(), nullable=True),
        StructField('lattitude', DecimalType(9,6), nullable=True),
        StructField('longitude', DecimalType(9,6), nullable=True)
    ])
    artists_final = spark.createDataFrame(df_artists.rdd, artists_schema, verifySchema=True)
    logging.info('Final number of artists: {}'.format(artists_final.count()))
    # write artists table to parquet files
    artists_final.write.parquet(output_data + "artists-table/", mode='overwrite')
    logging.info('Write to artists table done.')

    return songs_final, artists_final


def parse_log_data(spark, input_data):
    '''
    Parse JSON files of log data

    Arg(s):
        spark: a pyspark session object
        input_data: AWS S3 bucket path
    Return(s):
        df_log: parsed DataFrame of log data
    '''

    # get filepath to log data file
    log_data = input_data + "log_data"

    # schema for JSON file
    schema_parse_log = StructType([
        StructField('__corrupted', StringType(), True),
        StructField('artist', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('lastName', StringType(), True),
        StructField('level', StringType(), True),
        StructField('location', StringType(), True),
        StructField('page', StringType(), True),
        StructField('sessionId', IntegerType(), True),
        StructField('song', StringType(), True),
        StructField('ts', LongType(), True),
        StructField('userAgent', StringType(), True),
        StructField('userId', StringType(), True)
    ])

    # load JSON files and store corrupted records in field '__corrupted'
    df_log_parsed = spark.read.json(
        log_data, 
        schema = schema_parse_log, 
        recursiveFileLookup=True, 
        mode = 'PERMISSIVE',
        columnNameOfCorruptRecord='__corrupted').cache()

    # logging info
    logging.info("Total number of parsed log records: {}".format(df_log_parsed.count()))
    logging.info("Number of corrupted log records {}"\
        .format(df_log_parsed.filter("__corrupted is NOT NULL").count()))

    # get the non-corrupted records
    df_log = df_log_parsed.filter("__corrupted is NULL").drop("__corrupted")
    logging.info("Number of valid log records: {}".format(df_log.count()))

    # filter by actions for song plays
    df_log = df_log.filter("page == 'NextSong'")

    return df_log


def process_log_data(spark, df_log_in, output_data):
    '''
    Process log DataFrame and extract columns for users, time tables

    Arg(s):
        spark: a pyspark session object
        df_log_in: parsed DataFrame of log data
        output_data: AWS S3 bucket for saving results
    Return(s):
        users_final: users table/DataFrame, cleaned and typed
        time_final: time table/DataFrame, cleaned and typed
    '''

    ### get users
    df_users = df_log_in.select(
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        col('gender'),
        col('level'))

    # drop nulls
    df_users = df_users.dropna(how='any', subset=['user_id', 'first_name', 'last_name'])
    # drop empty strings
    df_users = df_users.filter("user_id != '' AND first_name != '' AND last_name != ''")
    # truncate song_id, title, artist_id
    trunc_id_name = udf(lambda x: x[:50], StringType())
    df_users = df_users.withColumn('user_id', trunc_id_name(df_users['user_id']))
    df_users = df_users.withColumn('first_name', trunc_id_name(df_users['first_name']))
    df_users = df_users.withColumn('last_name', trunc_id_name(df_users['last_name']))
    # convert user_id to Integer and drop records if type casting failed
    df_users = df_users.withColumn('user_id', df_users.user_id.cast(IntegerType()))
    df_users = df_users.dropna(how='any', subset=['user_id'])
    # drop duplicates only on user_id. This is because there might be a user_id with 2 levels.
    # In distributed setting, the ordering of which level is loaded cannot be predicted
    df_users = df_users.dropDuplicates(subset=['user_id'])
    # impose an explict schema for not-nullable fields
    users_schema = StructType([
        StructField('user_id', IntegerType(), nullable=False),
        StructField('first_name', StringType(), nullable=False),
        StructField('last_name', StringType(), nullable=False),
        StructField('gender', StringType(), nullable=True),
        StructField('level', StringType(), nullable=True)
    ])
    users_final = spark.createDataFrame(df_users.rdd, users_schema, verifySchema=True)
    logging.info('Final number of users: {}'.format(users_final.count()))
    #  write users table to parquet files
    users_final.write.parquet(output_data + "users-table/", mode='overwrite')
    logging.info('Write to users table done.')


    ### get timestamps
    # drop NULL and duplicates
    df_time = df_log_in.select('ts').dropna().dropDuplicates()
    # convert timestamp
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    df_time = df_time.withColumn('start_time', get_timestamp(df_time.ts))
    df_time = df_time.drop('ts')
    # extract fields: hour, dayofyear, weekofyear, month, year, dayofweek
    df_time = df_time.withColumn('hour', hour(df_time['start_time']))
    df_time = df_time.withColumn('day', dayofyear(df_time['start_time']))
    df_time = df_time.withColumn('week', weekofyear(df_time['start_time']))
    df_time = df_time.withColumn('month', month(df_time['start_time']))
    df_time = df_time.withColumn('year', year(df_time['start_time']))
    df_time = df_time.withColumn('weekday', dayofweek(df_time['start_time']))
    # impose an explict schema for not-nullable fields
    time_schema = StructType([
        StructField('start_time', TimestampType(), nullable=False),
        StructField('hour', IntegerType(), nullable=False),
        StructField('day', IntegerType(), nullable=False),
        StructField('week', IntegerType(), nullable=False),
        StructField('month', IntegerType(), nullable=False),
        StructField('year', IntegerType(), nullable=False),
        StructField('weekday', IntegerType(), nullable=False)
    ])
    time_final = spark.createDataFrame(df_time.rdd, time_schema, verifySchema=True)
    logging.info('Final number of timestamps: {}'.format(time_final.count()))
    #  write time table to parquet files
    time_final.write.partitionBy('year', 'month').parquet(output_data + "time-table/", mode='overwrite')
    logging.info('Write to time table done.')

    return users_final, time_final


def process_songplay_table(spark, df_song_in, df_log_in, df_time_in, output_data):
    '''
    Extract columns for songplay table from log and song data

    Arg(s):
        spark: a pyspark session object
        df_song_in: parsed DataFrame of song data
        df_log_in: parsed DataFrame of log data
        df_time_in: parsed DataFrame of time dimension, cleaned and typed
        output_data: AWS S3 bucket for saving results
    Return(s):
        songplay_final: songplays table/DataFrame, cleaned and typed
    '''

    # select sub fields
    df_log_sub = df_log_in.select(monotonically_increasing_id().alias('songplay_id'),
                                col('userId').alias('user_id'),
                                col('level'),
                                col('sessionId').alias('session_id'),
                                col('location'),
                                col('userAgent').alias('user_agent'),
                                col('artist'),
                                col('song'),
                                col('ts'))
    df_song_sub = df_song_in.select(col('song_id'),
                                    col('title').alias('song'),
                                    col('artist_id'),
                                    col('artist_name').alias('artist'))

    # join tables
    df_songplay = df_log_sub.join(df_song_sub, on = ['artist', 'song'], how='inner')
    df_songplay = df_songplay.select(['songplay_id', 'ts', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'])

    # drop nulls and empty strings
    df_songplay = df_songplay.dropna(how='any', subset=['user_id', 'song_id', 'artist_id', 'ts'])
    df_songplay = df_songplay.filter("user_id != '' AND song_id != '' AND artist_id != ''")

    # convert datetime
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    df_songplay = df_songplay.withColumn('start_time', get_timestamp(df_songplay.ts))
    df_songplay = df_songplay.drop('ts')

    # convert user_id to Integer and drop records if type casting failed
    df_songplay = df_songplay.withColumn('user_id', df_songplay.user_id.cast(IntegerType()))
    df_songplay = df_songplay.dropna(how='any', subset=['user_id'])

    # truncate long strings for location, user_agent which might be NULL/None
    @udf
    def trunc_long_str(line):
        if line is None:
            return line
        else:
            return line[:256]
    df_songplay = df_songplay.withColumn('location', trunc_long_str(df_songplay['location']))
    df_songplay = df_songplay.withColumn('user_agent', trunc_long_str(df_songplay['user_agent']))

    # drop duplicates on all columns except for 'songplay_id' since it's auto-generated
    df_songplay = df_songplay.dropDuplicates(subset=['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'])

    # join with time table to get year and month
    df_songplay = df_songplay.join(df_time_in, on = ['start_time'], how='inner')
    df_songplay = df_songplay.select(['songplay_id', 'start_time', 'year', 'month', 'user_id',
                                     'level', 'song_id', 'artist_id', 'session_id', 'location',
                                     'user_agent'])

    # impose an explicit schema for not-nullable fields
    songplay_schema = StructType([
        StructField('songplay_id', LongType(), nullable=False),
        StructField('start_time', TimestampType(), nullable=False),
        StructField('year', IntegerType(), nullable=False),
        StructField('month', IntegerType(), nullable=False),
        StructField('user_id', IntegerType(), nullable=False),
        StructField('level', StringType(), nullable=True),
        StructField('song_id', StringType(), nullable=False),
        StructField('artist_id', StringType(), nullable=False),
        StructField('session_id', IntegerType(), nullable=True),
        StructField('location', StringType(), nullable=True),
        StructField('user_agent', StringType(), nullable=True)
    ])
    songplay_final = spark.createDataFrame(df_songplay.rdd, songplay_schema, verifySchema=True)
    logging.info("Final number of songplays: {}".format(songplay_final.count()))

    # write songplays table to parquet files partitioned by year and month
    songplay_final.write.partitionBy('year', 'month').parquet(output_data + "songplays_table/", mode='overwrite')
    logging.info('Write to songplays table done.')

    return songplay_final


def main():

    spark = create_spark_session()

    ### Run on local machine (need to have song, log data downloaded on the machine)
    #logging.basicConfig(filename='./data/out/spark-etl-log.log', filemode='w', level=logging.INFO)
    #input_data = "./data/"
    #output_data = "./data/out/"

    ### Run on AWS EMR cluster
    logging.basicConfig(filename='./spark-etl-log.log', filemode='w', level=logging.INFO)
    input_data = "s3://udacity-dend/"
    output_data = "s3://udacity-de-datalake/"

    # process song data
    df_song = parse_song_data(spark, input_data)
    songs_final, artists_final = process_song_data(spark, df_song, output_data)

    # process log data
    df_log = parse_log_data(spark, input_data)
    users_final, time_final = process_log_data(spark, df_log, output_data)

    # process songplay records, requires joing tables/DataFrames from previous steps
    songplay_final = process_songplay_table(spark, df_song, df_log, time_final, output_data)

    # stop spark and close logging
    spark.stop()
    logging.info('Spark job done.')
    logging.shutdown()


if __name__ == "__main__":
    main()
