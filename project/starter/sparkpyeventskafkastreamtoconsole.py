from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, FloatType

stedi_schema = StructType([
    StructField("customer",StringType()),
    StructField("score",FloatType()),
    StructField("riskDate",StringType()),
])

spark = SparkSession.builder.appName("stedi-app-events").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers","kafka:19092")\
            .option("subscribe","stedi-events")\
            .option("startingOffsets","earliest").load()

df = df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")

df.withColumn("value", from_json(col("value"),stedi_schema))\
  .select(col("value.*"))\
  .createOrReplaceTempView("CustomerRisk")

df_new = spark.sql("select customer, score from CustomerRisk")

df_new.writeStream.format("console").outputMode("append").start().awaitTermination()