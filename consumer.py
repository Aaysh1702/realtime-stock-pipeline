from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# 1. Initialize Spark Engine (Now downloading Kafka AND Postgres drivers)
spark = SparkSession.builder \
    .appName("RealTimeStockEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Define the Schema Blueprint
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# 3. Read the Raw Stream from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "stock-prices") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse the Raw Bytes
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# --- NEW: 5. The Database Writing Function ---
def save_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://127.0.0.1:5432/stock_data") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "live_prices") \
        .option("user", "myuser") \
        .option("password", "mypassword") \
        .mode("append") \
        .save()

# --- NEW: 6. Route the Stream to Postgres instead of the Console ---
query = parsed_stream.writeStream \
    .foreachBatch(save_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()