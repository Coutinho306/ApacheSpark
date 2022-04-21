from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FriendsByAgeEX1").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("section3-SparkSQL/datasets/fakefriends-header.csv").cache()

people.createOrReplaceTempView("people")

people.printSchema()

results = spark.sql("SELECT age, avg(friends) FROM people GROUP BY age")

for result in results.collect():
  print(result)

spark.stop()
