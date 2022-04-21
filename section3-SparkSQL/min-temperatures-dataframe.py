from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("/home/pato/desenv/section3-SparkSQL/datasets")
df.printSchema()

# Filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# Select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
minTempsByStation = stationTemps.groupBy("stationID").agg(func.min("temperature").alias("temperatureC"))
minTempsByStation.show()

# Convert temperature to fahrenheit and sort the dataset
minTempsByStationF = minTempsByStation.withColumn("temperatureF",
                                                  func.round(func.col("temperatureC") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                  .select("stationID", "temperatureC", "temperatureF").sort("temperatureC")
                                                  
# Collect, format, and print the results
minTempsByStationF.show(30)
    
spark.stop()

                                                  