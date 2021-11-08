from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, FloatType, ArrayType

# StructType for the Kafka redis-server topic
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

#StructType for the Customer JSON
customer_schema = StructType([
    StructField("customerName",StringType()),
    StructField("email",StringType()),
    StructField("phone",StringType()),
    StructField("birthDay",StringType()),
])

# StructType for the Kafka stedi-events topic
stedi_schema = StructType([
    StructField("customer",StringType()),
    StructField("score",FloatType()),
    StructField("riskDate",StringType()),
])

# spark application object
spark = SparkSession.builder.appName("").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Reading data from redis-server topic with earliest as option
redisDataDF = spark.readStream\
                   .format("kafka")\
                   .option("kafka.bootstrap.servers", "kafka:19092")\
                   .option("subscribe","redis-server")\
                   .option("startingOffsets","earliest")\
                   .load()

df = redisDataDF.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")

# storing them in a temporary view called RedisSortedSet
df.withColumn("value",from_json("value",redis_schema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("RedisSortedSet")

# TO-DO: execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
customerDataDf=spark.sql("select key, zsetEntries[0].element as encodedCustomer from RedisSortedSet")

# Decoding encoded customer into JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}
customerDataDf_1= customerDataDf.withColumn("customer", unbase64(customerDataDf.encodedCustomer).cast("string"))

# parsing JSON in the Customer record and storing in a temporary view called CustomerRecords
customerDataDf_1\
    .withColumn("customer", from_json("customer", customer_schema))\
    .select(col('customer.*'))\
    .createOrReplaceTempView("CustomerRecords")\

# selecting email, birthDay fields where birthDay is not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = spark.sql("select email as email, birthDay from CustomerRecords where birthDay is not null")

# Selecting only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select('email',split(emailAndBirthDayStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

# Reading stedi-events as stream
stedi_events_df = spark.readStream.format("kafka")\
                        .option("kafka.bootstrap.servers","kafka:19092")\
                        .option("subscribe","stedi-events")\
                        .option("startingOffsets","earliest").load()
                                   
# cast value column in the stedi_events_df dataframe as a STRING
stedi_events_df = stedi_events_df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")

# storing them in a temporary view called CustomerRisk
stedi_events_df.withColumn("value", from_json(col("value"),stedi_schema))\
               .select(col("value.*"))\
               .createOrReplaceTempView("CustomerRisk")

# Executing sql statement against a temporary view by selecting the customer and the score from the temporary view and creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

# join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
customerRiskStreamingDF_new = emailAndBirthYearStreamingDF.join(customerRiskStreamingDF, expr( """
   email=customer
"""))

#customerRiskStreamingDF_new.writeStream.format("console").outputMode("append").start().awaitTermination()

customerRiskStreamingDF_new\
    .selectExpr("cast(email as string) as key", "to_json(struct(*)) as value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:19092")\
    .option("topic", "stedi-customer-risk")\
    .option("checkpointLocation","/home/workspace/kafkacheckpoint2")\
    .start()\
    .awaitTermination()

# TO-DO: sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application 
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"} 
