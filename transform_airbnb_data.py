from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

S3_INPUT_DATA = 's3://new-york-city-airbnb-2023/NYC-airbnb-2023/row-data/'
S3_OUTPUT_DATA = 's3://new-york-city-airbnb-2023/NYC-airbnb-2023/transformed-data/'

# Initialize SparkSession
spark = SparkSession.builder.master("local[1]")\
    .appName('techycrispy.com')\
    .getOrCreate()

# Read input data from S3
df = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(S3_INPUT_DATA)

# Cast columns to appropriate data types
df = df.withColumn("id", df["id"].cast(IntegerType()))\
    .withColumn("host_id", df["host_id"].cast(IntegerType()))\
    .withColumn("latitude", df["latitude"].cast(FloatType()))\
    .withColumn("longitude", df["longitude"].cast(FloatType()))\
    .withColumn("minimum_nights", df["minimum_nights"].cast(IntegerType()))\
    .withColumn("number_of_reviews", df["number_of_reviews"].cast(IntegerType()))\
    .withColumn("last_review", df["last_review"].cast(DateType()))\
    .withColumn("reviews_per_month", df["reviews_per_month"].cast(FloatType()))\
    .withColumn("calculated_host_listings_count", df["calculated_host_listings_count"].cast(IntegerType()))\
    .withColumn("availability_365", df["availability_365"].cast(IntegerType()))\
    .withColumn("number_of_reviews_ltm", df["number_of_reviews_ltm"].cast(IntegerType()))\
    .withColumn("license", df["license"].cast(StringType()))\
    .withColumn("price", df["price"].cast(IntegerType()))

# Count null values in each column
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()

# Drop columns with more than 400 null values
cols_to_drop = [k for k, v in null_counts.items() if v > 400]
df_dropped = df.drop(*cols_to_drop)

# Count rows after dropping columns
print(f"Row count after dropping columns: {df_dropped.count()}")

# Aggregate and group data
df1 = df.groupBy("room_type").agg(count("room_type").alias("counted"))
count_room_type = df1.sort(df1["counted"].desc()).limit(4)

df2 = df.groupBy("room_type").agg(sum("price").alias("total"))
total_earn_room_type = df2.sort(df2["total"].desc()).limit(4)

# Write outputs to S3
df.write.mode('overwrite').csv(S3_OUTPUT_DATA + "/airbnb-dataframe")
count_room_type.write.mode('overwrite').csv(S3_OUTPUT_DATA + "/count-room-type")
total_earn_room_type.write.mode('overwrite').csv(S3_OUTPUT_DATA + "/total-earn-room-type")
df_dropped.write.mode('overwrite').csv(S3_OUTPUT_DATA + "/droping")
