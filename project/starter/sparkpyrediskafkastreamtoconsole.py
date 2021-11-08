from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

redis_schema = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr",BooleanType()),
        StructField("zsetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                 \
        )
    ]
)

customer_schema = StructType([
    StructField("customerName",StringType()),
    StructField("email",StringType()),
    StructField("phone",StringType()),
    StructField("birthDay",StringType()),
])

spark = SparkSession.builder.appName("customer-data").getOrCreate()

spark.sparkContext.setLogLevel('WARN')

redisDataDF = spark.readStream\
                   .format("kafka")\
                   .option("kafka.bootstrap.servers", "kafka:19092")\
                   .option("subscribe","redis-server")\
                   .option("startingOffsets","earliest")\
                   .load()

df = redisDataDF.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")

df.withColumn("value",from_json("value",redis_schema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("RedisData")

customerDataDf=spark.sql("select key, zsetEntries[0].element as customerRecord from RedisData")

customerDataDf_1= customerDataDf.withColumn("customerRecord", unbase64(customerDataDf.customerRecord).cast("string"))

customerDataDf_1\
    .withColumn("customer", from_json("customerRecord", customer_schema))\
    .select(col('customer.*'))\
    .createOrReplaceTempView("Customer")\

emailAndBirthYearStreamingDF = spark.sql("select email as email, birthDay from Customer where birthDay is not null")

emailAndBirthYearStreamingDF = emailAndBirthYearStreamingDF.select('email',split(emailAndBirthYearStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

emailAndBirthYearStreamingDF.writeStream.format("console").outputMode("append").start().awaitTermination()