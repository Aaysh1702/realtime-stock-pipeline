from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType


spark = SparkSession.builder \
    .appName("RealTimeStockEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("timestamp", StringType(), True)
])


raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "stock-prices") \
    .option("startingOffsets", "latest") \
    .load()


parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


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


query = parsed_stream.writeStream \
    .foreachBatch(save_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()