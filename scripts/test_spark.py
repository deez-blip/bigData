from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkTest") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option("header", True).csv("data/sources/clients.csv")
df.show(5)

spark.stop()