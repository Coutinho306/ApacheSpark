from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("section3-SparkSQL/datasets/Book.txt")

# Split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word")).groupBy("word").count().sort("count")

# Count up the occurrences of each word
#wordCounts = lowercaseWords.groupBy("word").count().sort("count")

# Show the results.
lowercaseWords.show(lowercaseWords.count())
