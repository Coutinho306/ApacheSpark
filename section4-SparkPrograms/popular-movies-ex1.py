from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

spark = SparkSession.builder.appName("PopularMoviesEX1").getOrCreate()

# Create schema when reading u.data
schemaDF = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Create schema when reading u.item
schemaItems = StructType([ \
                     StructField("movieID", IntegerType(), True), \
                     StructField("movieTitle", StringType(), True), \
                     StructField("releaseDate", StringType(), True), \
                     StructField("videoReleaseDate", StringType(), True), \
                     StructField("imdbURL", StringType(), True), \
                     StructField("unknown", IntegerType(), True), \
                     StructField("action", IntegerType(), True), \
                     StructField("adventure", IntegerType(), True), \
                     StructField("children", IntegerType(), True), \
                     StructField("comedy", IntegerType(), True), \
                     StructField("crime", IntegerType(), True), \
                     StructField("documentary", IntegerType(), True), \
                     StructField("drama", IntegerType(), True), \
                     StructField("fantasy", IntegerType(), True), \
                     StructField("filmNoir", IntegerType(), True), \
                     StructField("horror", IntegerType(), True), \
                     StructField("musical", IntegerType(), True), \
                     StructField("mystery", IntegerType(), True), \
                     StructField("romance", IntegerType(), True), \
                     StructField("sciFi", IntegerType(), True), \
                     StructField("thriller", IntegerType(), True), \
                     StructField("war", IntegerType(), True), \
                     StructField("howesternrror", IntegerType(), True)])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schemaDF)\
                                                .csv("/home/pato/desenv/section1-Intro/datasets/ml-100k/u.data")

itemsDF = spark.read.option("sep", "|").schema(schemaItems)\
                                                .csv("/home/pato/desenv/section1-Intro/datasets/ml-100k/u.item")

moviesDF.createOrReplaceTempView("movies")
itemsDF.createOrReplaceTempView("items")

results = spark.sql("""SELECT d.movieID, 
                              i.movieTitle, 
                              COUNT(d.movieID) as movieQtty
                        FROM movies d INNER JOIN items i 
                        ON d.movieID = i.movieID 
                        GROUP BY d.movieID, i.movieTitle 
                        ORDER BY movieQtty DESC""")\
            .show(10, False)

# Stop the session
spark.stop()
