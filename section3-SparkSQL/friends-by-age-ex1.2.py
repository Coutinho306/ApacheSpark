from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.appName("FriendsByAgeEX1.2").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("section3-SparkSQL/datasets/fakefriends-header.csv").cache()

people.printSchema()

friendsByAge = people.select("age", "friends")

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friendsAvg")).orderBy("age").show()

spark.stop()