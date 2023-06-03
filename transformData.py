from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.functions import when

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Remote Hive Metastore Connection") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("select * from flight_staging.fact_flights")

# Convert column lpep_pickup_datetime & lpep_dropoff_datetime into datetime format
df = df.withColumn("firstseen", from_unixtime(col("firstseen") / 1000000).cast("timestamp"))
df = df.withColumn("lastseen", from_unixtime(col("lastseen") / 1000000).cast("timestamp"))

