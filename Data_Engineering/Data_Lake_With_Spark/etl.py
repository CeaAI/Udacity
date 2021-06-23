import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

'''
    This function creates a spark session using the IAM users keys
'''
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

'''
    This function process the song data stored in s3 and 
    uses it to create the song and artists table which is then outputed back
    
    Input
    * spark, the spark session
    * input_data, the s3 path containing the log data
    * output_data, the path to write the parquet files to.

'''   
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "{}/song_data/*/*/*/*.json".format(input_data)
    
    # read song data file
    df = spark.read.json(song_data)
   
    # extract columns to create songs table
    songs_table = df.select("song_id", "artist_id", "title", "year", "duration")\
    .drop_duplicates(subset=["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year", "artist_id")\
    .parquet("{}/song_table.parquet".format(output_data))

    # extract columns to create artists table
    artists_table = df.select("artist_id").withColumnRenamed("name", "artist_name")\
    .withColumnRenamed ("location", "artist_location")\
    .withColumnRenamed ("longitude","artist_longitude")\
    .withColumnRenamed("latitude", "artist_latitude")\
    .drop_duplicates(subset=["artist_id"])
    
    # write artists table to parquet files
    artists_table = artists_table.write.parquet("{}/artist_table.parquet".format(output_data))
                                               
'''
    This function process the log data stored in s3 and uses it to create the time, 
    users and songplays table which is then outputed back
    
    Input
    * spark, the spark session
    * input_data, the s3 path containing the log data
    * output_data, the path to write the parquet files to
    
'''
                                                                                            
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "{}/log_data/*/*/*.json".format(input_data)

    # read log data file
    df = spark.read.json(log_data)

    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userid","firstname","lastname","gender","level")\
    .drop_duplicates(subset=["userid"])
    
    # write users table to parquet files
    users_table = users_table.write.parquet("{}/user_table.parquet".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    
    df = df.withColumn('timestamp', get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    df = df.withColumn('start_time', from_unixtime("timestamp"))
    
    # extract columns to create time table
    time_table = df.select("start_time").withColumn("hour",hour(col('start_time')))\
    .withColumn("day",dayofmonth(col("start_time")))\
    .withColumn("week", weekofyear(col("start_time")))\
    .withColumn("month",month(col("start_time")))\
    .withColumn("year",year(col("start_time")))\
    .withColumn("weekday",date_format(col("start_time"),'u'))
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year", "month")\
    .parquet("{}/time_table.parquet".format(output_data))

    # read in song data to use for songplays table
    song_data = "{}/song_data/*/*/*/*.json".format(input_data)
    song_df = spark.read.json(song_data)
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df["artist"] == song_df["artist_name"],"inner")\
    .where(df["song"] == song_df["title"])\
    .where(df["length"] == song_df["duration"])\
    .select("start_time", "userid", "level", "song_id",
            "artist_id", "sessionid", "location", "useragent")\
    .withColumn("month",month(col("start_time")))\
    .withColumn("year",year(col("start_time")))

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy("year","month")\
    .parquet("{}/songplays_table.parquet".format(output_data))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
#     input_data ="./data"
    output_data = "./data/output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
