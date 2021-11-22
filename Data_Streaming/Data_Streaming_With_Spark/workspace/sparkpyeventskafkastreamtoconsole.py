from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

stedi_events_schema = StructType(
    [
        StructField("customer", StringType()), 
        StructField("score", StringType()),
        StructField("riskDate", DateType())
    ]

)

spark = SparkSession.builder.appName("stedi-checks").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

stedi_checks_raw_streaming_df = spark\
.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers", 'localhost:9092')\
.option("subscribe", "stedi-events")\
.option("startingOffsets", "earliest")\
.load()
                            
stedi_checks_streaming_df = stedi_checks_raw_streaming_df.selectExpr("cast(key as string) key", "cast(value as string) value")

stedi_checks_streaming_df.withColumn("value", from_json("value", stedi_events_schema))\
.select(col('value.*'))\
.createOrReplaceTempView("CustomerRisk")

stedi_events_select_df = spark.sql("select customer, score from CustomerRisk")

stedi_events_select_df.writeStream.outputMode("append").format("console").start().awaitTermination()

# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 