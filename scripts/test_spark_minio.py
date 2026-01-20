from pyspark.sql import SparkSession

RUN_ID = "20260120_101037"

spark = (
    SparkSession.builder
    .appName("MinIOReadWriteTest")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

input_path = f"s3a://bronze/runs/{RUN_ID}/clients.csv"
output_path = f"s3a://silver/tests/spark_minio_test_{RUN_ID}/"

df = spark.read.option("header", True).csv(input_path)
df.show(5)

df.write.mode("overwrite").option("header", True).csv(output_path)

print("✅ Write OK to:", output_path)

spark.stop()