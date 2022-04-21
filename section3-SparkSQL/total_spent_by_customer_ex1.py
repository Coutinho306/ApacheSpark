from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomerEX1").getOrCreate()

schema = StructType([StructField("customerID", IntegerType(), True), \
                     StructField("itemID", IntegerType(), True), \
                     StructField("amountSpent", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("/home/pato/desenv/section3-SparkSQL/datasets/customer-orders.csv")
df.printSchema()

# Filter out all but TMIN entries
amountSpent = df.select("customerID", "amountSpent")

# Select only stationID and temperature
amountSpentByCustomer = amountSpent.groupBy("customerID")\
                        .agg(func.round(func.sum("amountSpent"), 2).alias("totalSpent"))\
                        .sort("totalSpent")

amountSpentByCustomer.show(amountSpentByCustomer.count())
    
spark.stop()                           